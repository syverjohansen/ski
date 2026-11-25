#!/usr/bin/env python3
"""Atomically promote a validated Nordic-combined schedule candidate."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from schedule_contract import (
    RACES_COLUMNS,
    REPORT_SCHEMA,
    WEEKENDS_COLUMNS,
    ScheduleContractError,
    file_sha256,
    read_csv,
    validate_races,
    validate_relationship,
    validate_season_structure,
    write_json_atomic,
)

RECEIPT_SCHEMA = "nordicnumbers.nordic-combined-schedule-promotion/v1"


@contextmanager
def promotion_lock(accepted_dir: Path) -> Iterator[None]:
    lock_path = accepted_dir / ".schedule-promotion.lock"
    accepted_dir.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ScheduleContractError(f"another promotion holds {lock_path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_report(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    observed = file_sha256(path)
    if observed != expected_sha256:
        raise ScheduleContractError(
            f"shadow report SHA-256 mismatch: expected {expected_sha256}, observed {observed}"
        )
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScheduleContractError(f"cannot read shadow report {path}: {exc}") from exc
    if report.get("schema_version") != REPORT_SCHEMA:
        raise ScheduleContractError("unsupported shadow report schema")
    return report, observed


def _evidence_path(details: dict[str, Any], label: str) -> Path:
    try:
        path = Path(details["file"]).expanduser().resolve()
        expected = details["sha256"]
    except (KeyError, TypeError) as exc:
        raise ScheduleContractError(f"missing {label} file evidence") from exc
    if file_sha256(path) != expected:
        raise ScheduleContractError(f"{label} SHA-256 mismatch")
    return path


def _copy_sync(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())


def _restore(
    backup_races: Path,
    backup_weekends: Path,
    accepted_races: Path,
    accepted_weekends: Path,
) -> None:
    _copy_sync(backup_races, accepted_races)
    _copy_sync(backup_weekends, accepted_weekends)


def promote_schedule(
    shadow_report: Path,
    *,
    expected_report_sha256: str,
    accepted_dir: Path,
    approve_season: int,
    receipt_path: Path,
) -> dict[str, Any]:
    shadow_report = shadow_report.expanduser().resolve()
    accepted_dir = accepted_dir.expanduser().resolve()
    receipt_path = receipt_path.expanduser().resolve()
    report, report_sha = _load_report(shadow_report, expected_report_sha256)
    transition = report.get("season_transition", {})
    promotion = report.get("promotion", {})
    if report.get("status") != "passed" or not transition.get("valid"):
        raise ScheduleContractError("shadow report did not pass")
    if transition.get("kind") not in {"same_season", "next_season"}:
        raise ScheduleContractError("unsupported season transition")
    expected_status = "eligible" if transition["kind"] == "same_season" else "approval_required"
    if promotion.get("status") != expected_status:
        raise ScheduleContractError(f"candidate is not {expected_status}")
    candidate_season = transition.get("candidate_season")
    if candidate_season != approve_season:
        raise ScheduleContractError(
            f"approved season {approve_season} does not match candidate season {candidate_season}"
        )

    candidate = report["candidate"]
    accepted = report["accepted"]
    candidate_races = _evidence_path(candidate["races"], "candidate races")
    candidate_weekends = _evidence_path(candidate["weekends"], "candidate weekends")
    accepted_races = accepted_dir / "races.csv"
    accepted_weekends = accepted_dir / "weekends.csv"
    if receipt_path in {accepted_races, accepted_weekends}:
        raise ScheduleContractError("receipt cannot overwrite accepted data")

    for name, path in {"races": accepted_races, "weekends": accepted_weekends}.items():
        evidence = accepted[name]
        if Path(evidence["file"]).expanduser().resolve() != path:
            raise ScheduleContractError(f"report validated a different accepted {name} file")
        if file_sha256(path) != evidence["sha256"]:
            raise ScheduleContractError(f"accepted {name} changed after shadow validation")

    race_rows = read_csv(candidate_races, RACES_COLUMNS)
    weekend_rows = read_csv(candidate_weekends, WEEKENDS_COLUMNS)
    validate_races(race_rows)
    validate_season_structure(race_rows, expected_season=approve_season)
    validate_relationship(race_rows, weekend_rows)

    timestamp = datetime.now(timezone.utc)
    promotion_id = f"season-{approve_season}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}-{report_sha[:12]}"
    backup_dir = accepted_dir / ".schedule-backups" / promotion_id
    with promotion_lock(accepted_dir):
        before = {"races": file_sha256(accepted_races), "weekends": file_sha256(accepted_weekends)}
        if before != {name: accepted[name]["sha256"] for name in ("races", "weekends")}:
            raise ScheduleContractError("accepted schedule changed while acquiring lock")
        backup_dir.mkdir(parents=True, exist_ok=False)
        backup_races = backup_dir / "races.csv"
        backup_weekends = backup_dir / "weekends.csv"
        _copy_sync(accepted_races, backup_races)
        _copy_sync(accepted_weekends, backup_weekends)
        staged = []
        try:
            for source, name in ((candidate_races, "races.csv"), (candidate_weekends, "weekends.csv")):
                with tempfile.NamedTemporaryFile(dir=accepted_dir, prefix=f".{name}.", delete=False) as handle:
                    temporary = Path(handle.name)
                staged.append(temporary)
                _copy_sync(source, temporary)
            os.replace(staged.pop(0), accepted_races)
            os.replace(staged.pop(0), accepted_weekends)
        except Exception as exc:
            _restore(backup_races, backup_weekends, accepted_races, accepted_weekends)
            raise ScheduleContractError(f"promotion failed; accepted pair restored: {exc}") from exc
        finally:
            for path in staged:
                path.unlink(missing_ok=True)

        after = {"races": file_sha256(accepted_races), "weekends": file_sha256(accepted_weekends)}
        expected_after = {name: candidate[name]["sha256"] for name in ("races", "weekends")}
        if after != expected_after:
            _restore(backup_races, backup_weekends, accepted_races, accepted_weekends)
            raise ScheduleContractError("post-promotion verification failed; pair restored")
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "status": "passed",
            "performed": True,
            "transition": transition["kind"],
            "promotion_id": promotion_id,
            "promoted_at": timestamp.isoformat(),
            "approved_season": approve_season,
            "shadow_report": {"file": str(shadow_report), "sha256": report_sha},
            "before": before,
            "after": after,
            "backup": {"directory": str(backup_dir)},
        }
        try:
            write_json_atomic(receipt_path, receipt)
        except OSError as exc:
            _restore(backup_races, backup_weekends, accepted_races, accepted_weekends)
            raise ScheduleContractError(f"receipt failed; accepted pair restored: {exc}") from exc
        return {**receipt, "receipt": str(receipt_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shadow-report", required=True, type=Path)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--accepted-dir", required=True, type=Path)
    parser.add_argument("--approve-season", required=True, type=int)
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = promote_schedule(
            args.shadow_report,
            expected_report_sha256=args.expected_report_sha256,
            accepted_dir=args.accepted_dir,
            approve_season=args.approve_season,
            receipt_path=args.receipt,
        )
    except (OSError, ScheduleContractError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
