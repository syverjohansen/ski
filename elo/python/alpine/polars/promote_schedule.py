#!/usr/bin/env python3
"""Explicitly promote an approved Alpine season schedule.

The shadow report is immutable evidence. Promotion requires its exact SHA-256,
an explicit season approval, unchanged accepted inputs, and candidate files
whose hashes still match the report. The accepted pair is backed up before it
is replaced, and any replacement failure restores both original files.
"""

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


RECEIPT_SCHEMA = "nordicnumbers.alpine-schedule-promotion/v1"


@contextmanager
def promotion_lock(accepted_dir: Path) -> Iterator[None]:
    """Serialize cooperating schedule promotions for one accepted directory."""
    lock_path = accepted_dir / ".schedule-promotion.lock"
    accepted_dir.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ScheduleContractError(
                f"another schedule promotion holds {lock_path}"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_report(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    observed_sha256 = file_sha256(path)
    if observed_sha256 != expected_sha256:
        raise ScheduleContractError(
            "shadow report SHA-256 mismatch: "
            f"expected {expected_sha256}, observed {observed_sha256}"
        )
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScheduleContractError(f"cannot read shadow report {path}: {exc}") from exc
    if report.get("schema_version") != REPORT_SCHEMA:
        raise ScheduleContractError(
            f"unsupported shadow report schema: {report.get('schema_version')!r}"
        )
    return report, observed_sha256


def require_report_file(
    details: dict[str, Any],
    *,
    label: str,
) -> Path:
    try:
        path = Path(details["file"]).expanduser().resolve()
        expected_sha256 = details["sha256"]
    except (KeyError, TypeError) as exc:
        raise ScheduleContractError(
            f"shadow report is missing {label} file evidence"
        ) from exc
    observed_sha256 = file_sha256(path)
    if observed_sha256 != expected_sha256:
        raise ScheduleContractError(
            f"{label} SHA-256 mismatch: expected {expected_sha256}, "
            f"observed {observed_sha256}"
        )
    return path


def copy_and_sync(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())


def restore_pair(
    backup_races: Path,
    backup_weekends: Path,
    accepted_races: Path,
    accepted_weekends: Path,
) -> None:
    copy_and_sync(backup_races, accepted_races)
    copy_and_sync(backup_weekends, accepted_weekends)


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
    report, report_sha256 = load_report(
        shadow_report,
        expected_report_sha256,
    )

    promotion = report.get("promotion", {})
    transition = report.get("season_transition", {})
    if report.get("status") != "passed":
        raise ScheduleContractError("shadow report did not pass")
    if promotion.get("status") != "approval_required":
        raise ScheduleContractError(
            "season promotion requires report status 'approval_required'"
        )
    if (
        transition.get("kind") != "next_season"
        or not transition.get("valid")
        or not transition.get("approval_required")
    ):
        raise ScheduleContractError("shadow report is not a valid next-season rollover")
    candidate_season = transition.get("candidate_season")
    if candidate_season != approve_season:
        raise ScheduleContractError(
            f"approved season {approve_season} does not match candidate "
            f"season {candidate_season}"
        )

    candidate = report.get("candidate", {})
    accepted = report.get("accepted", {})
    candidate_races = require_report_file(
        candidate.get("races", {}),
        label="candidate races",
    )
    candidate_weekends = require_report_file(
        candidate.get("weekends", {}),
        label="candidate weekends",
    )
    accepted_races = accepted_dir / "races.csv"
    accepted_weekends = accepted_dir / "weekends.csv"
    if receipt_path in {accepted_races, accepted_weekends}:
        raise ScheduleContractError(
            "promotion receipt cannot overwrite an accepted schedule file"
        )

    expected_accepted_paths = {
        "races": accepted_races,
        "weekends": accepted_weekends,
    }
    for name, current_path in expected_accepted_paths.items():
        evidence = accepted.get(name, {})
        reported_path = Path(evidence.get("file", "")).expanduser().resolve()
        if reported_path != current_path:
            raise ScheduleContractError(
                f"shadow report validated a different accepted {name} file: "
                f"{reported_path}"
            )
        observed = file_sha256(current_path)
        if observed != evidence.get("sha256"):
            raise ScheduleContractError(
                f"accepted {name} changed after shadow validation"
            )

    race_rows = read_csv(candidate_races, RACES_COLUMNS)
    weekend_rows = read_csv(candidate_weekends, WEEKENDS_COLUMNS)
    validate_races(race_rows)
    validate_season_structure(race_rows, expected_season=approve_season)
    validate_relationship(race_rows, weekend_rows)

    timestamp = datetime.now(timezone.utc)
    promotion_id = (
        f"season-{approve_season}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}-"
        f"{report_sha256[:12]}"
    )
    backup_dir = accepted_dir / ".schedule-backups" / promotion_id

    with promotion_lock(accepted_dir):
        # Recheck inside the lock so validation cannot race another promoter.
        before = {
            "races": file_sha256(accepted_races),
            "weekends": file_sha256(accepted_weekends),
        }
        if before["races"] != accepted["races"]["sha256"] or before[
            "weekends"
        ] != accepted["weekends"]["sha256"]:
            raise ScheduleContractError(
                "accepted schedule changed while acquiring the promotion lock"
            )

        backup_dir.mkdir(parents=True, exist_ok=False)
        backup_races = backup_dir / "races.csv"
        backup_weekends = backup_dir / "weekends.csv"
        copy_and_sync(accepted_races, backup_races)
        copy_and_sync(accepted_weekends, backup_weekends)

        staged: list[Path] = []
        try:
            for source, name in (
                (candidate_races, "races.csv"),
                (candidate_weekends, "weekends.csv"),
            ):
                with tempfile.NamedTemporaryFile(
                    dir=accepted_dir,
                    prefix=f".{name}.promotion.",
                    delete=False,
                ) as handle:
                    temporary = Path(handle.name)
                staged.append(temporary)
                copy_and_sync(source, temporary)

            os.replace(staged[0], accepted_races)
            staged.pop(0)
            os.replace(staged[0], accepted_weekends)
            staged.pop(0)
        except Exception as exc:
            restore_pair(
                backup_races,
                backup_weekends,
                accepted_races,
                accepted_weekends,
            )
            raise ScheduleContractError(
                f"promotion failed; accepted pair restored from {backup_dir}: {exc}"
            ) from exc
        finally:
            for temporary in staged:
                temporary.unlink(missing_ok=True)

        after = {
            "races": file_sha256(accepted_races),
            "weekends": file_sha256(accepted_weekends),
        }
        expected_after = {
            "races": candidate["races"]["sha256"],
            "weekends": candidate["weekends"]["sha256"],
        }
        if after != expected_after:
            restore_pair(
                backup_races,
                backup_weekends,
                accepted_races,
                accepted_weekends,
            )
            raise ScheduleContractError(
                "post-promotion verification failed; accepted pair restored"
            )

        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "status": "passed",
            "performed": True,
            "promotion_id": promotion_id,
            "promoted_at": timestamp.isoformat(),
            "approved_season": approve_season,
            "shadow_report": {
                "file": str(shadow_report),
                "sha256": report_sha256,
            },
            "before": before,
            "after": after,
            "backup": {
                "directory": str(backup_dir),
                "races_sha256": file_sha256(backup_races),
                "weekends_sha256": file_sha256(backup_weekends),
            },
        }
        try:
            write_json_atomic(receipt_path, receipt)
        except OSError as exc:
            restore_pair(
                backup_races,
                backup_weekends,
                accepted_races,
                accepted_weekends,
            )
            raise ScheduleContractError(
                "promotion receipt could not be written; accepted pair restored: "
                f"{exc}"
            ) from exc
        return {**receipt, "receipt": str(receipt_path)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shadow-report", required=True, type=Path)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--accepted-dir", required=True, type=Path)
    parser.add_argument("--approve-season", required=True, type=int)
    parser.add_argument("--receipt", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
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
