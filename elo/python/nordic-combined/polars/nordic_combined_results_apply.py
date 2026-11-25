#!/usr/bin/env python3
"""Atomically apply an explicitly approved Nordic-combined results candidate."""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from nordic_combined_results_promote import PROMOTION_SCHEMA, PromotionError

RECEIPT_SCHEMA = "nordicnumbers.nordic-combined-results-application/v1"
ACCEPTED_FILES = ("men_chrono.csv", "ladies_chrono.csv", "team_sprint_chrono.csv")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _copy_and_sync(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())


@contextmanager
def application_lock(accepted_dir: Path) -> Iterator[None]:
    lock_path = accepted_dir / ".results-promotion.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PromotionError(f"another results application holds {lock_path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_report(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    observed = _sha(path)
    if observed != expected_sha256:
        raise PromotionError(
            f"promotion report SHA-256 mismatch: expected {expected_sha256}, observed {observed}"
        )
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PromotionError(f"invalid promotion report: {path}") from exc
    if report.get("schema_version") != PROMOTION_SCHEMA:
        raise PromotionError("unsupported promotion report schema")
    if (
        report.get("status") != "promotion_candidate"
        or report.get("atomic_candidate_set") is not True
        or report.get("accepted_files_modified") is not False
        or report.get("application_performed") is not False
    ):
        raise PromotionError("promotion report is not an isolated atomic candidate")
    return report, observed


def _split(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _mixed_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return [row for row in csv.DictReader(handle) if row.get("Sex") == "Mixed"]


def _validate_team_candidate(
    path: Path, accepted_path: Path, expected_new_teams: int
) -> None:
    mixed = _mixed_rows(path)
    accepted_keys = {
        (row["Date"], row["Nation"], row["Place"]) for row in _mixed_rows(accepted_path)
    }
    candidate_keys = {(row["Date"], row["Nation"], row["Place"]) for row in mixed}
    if not accepted_keys.issubset(candidate_keys):
        raise PromotionError("team-sprint candidate removes an accepted mixed team")
    if len(candidate_keys - accepted_keys) != expected_new_teams:
        raise PromotionError(
            "team-sprint candidate mixed-team delta does not match mapped team count"
        )
    seen: set[tuple[str, str, str]] = set()
    for number, row in enumerate(mixed, 2):
        key = (row["Date"], row["Nation"], row["Place"])
        members = _split(row["MemberIDs"])
        names = _split(row["TeamMembers"])
        if key in seen:
            raise PromotionError(f"duplicate mixed team {key}")
        if row["TeamSize"] != "2" or len(members) != 2 or len(set(members)) != 2 or len(names) != 2:
            raise PromotionError(f"{path}:{number}: invalid mixed team")
        seen.add(key)


def _validate_candidate_set(
    report: dict[str, Any], report_dir: Path, accepted_dir: Path
) -> tuple[dict[str, Path], dict[str, str], dict[str, str]]:
    outputs = report.get("outputs")
    baselines = report.get("accepted_before")
    if not isinstance(outputs, dict) or set(outputs) != set(ACCEPTED_FILES):
        raise PromotionError("promotion report has incomplete outputs")
    if not isinstance(baselines, dict) or set(baselines) != set(ACCEPTED_FILES):
        raise PromotionError("promotion report has incomplete accepted baseline")
    members = report.get("mapped_member_rows")
    teams = report.get("mapped_team_rows")
    if not isinstance(members, int) or not isinstance(teams, int) or members != teams * 2:
        raise PromotionError("promotion report has invalid member/team counts")

    candidates: dict[str, Path] = {}
    candidate_hashes: dict[str, str] = {}
    baseline_hashes: dict[str, str] = {}
    baseline_rows: dict[str, int] = {}
    for name in ACCEPTED_FILES:
        candidate = report_dir / name
        accepted = accepted_dir / name
        output = outputs[name]
        baseline = baselines[name]
        if not isinstance(output, dict) or not isinstance(baseline, dict):
            raise PromotionError(f"malformed promotion evidence for {name}")
        if output.get("file") != name or Path(str(baseline.get("file", ""))).name != name:
            raise PromotionError(f"unexpected file evidence for {name}")
        expected_candidate = output.get("sha256")
        expected_baseline = baseline.get("sha256")
        if not isinstance(expected_candidate, str) or _sha(candidate) != expected_candidate:
            raise PromotionError(f"candidate checksum mismatch: {name}")
        if output.get("rows") != _rows(candidate):
            raise PromotionError(f"candidate row-count mismatch: {name}")
        if not isinstance(expected_baseline, str) or _sha(accepted) != expected_baseline:
            raise PromotionError(f"accepted history changed: {name}")
        candidates[name] = candidate
        candidate_hashes[name] = expected_candidate
        baseline_hashes[name] = expected_baseline
        baseline_rows[name] = _rows(accepted)

    if _rows(candidates["men_chrono.csv"]) != baseline_rows["men_chrono.csv"] + teams:
        raise PromotionError("men candidate row delta does not match team count")
    if _rows(candidates["ladies_chrono.csv"]) != baseline_rows["ladies_chrono.csv"] + teams:
        raise PromotionError("ladies candidate row delta does not match team count")
    if _rows(candidates["team_sprint_chrono.csv"]) != baseline_rows["team_sprint_chrono.csv"] + teams:
        raise PromotionError("team-sprint candidate row delta does not match team count")
    _validate_team_candidate(
        candidates["team_sprint_chrono.csv"],
        accepted_dir / "team_sprint_chrono.csv",
        teams,
    )
    return candidates, candidate_hashes, baseline_hashes


def apply_promotion(
    promotion_report: Path, *, expected_report_sha256: str, accepted_dir: Path,
    approve_season: int, backup_root: Path, receipt_path: Path,
) -> dict[str, object]:
    promotion_report = promotion_report.expanduser().resolve()
    accepted_dir = accepted_dir.expanduser().resolve()
    backup_root = backup_root.expanduser().resolve()
    receipt_path = receipt_path.expanduser().resolve()
    report, report_hash = _load_report(promotion_report, expected_report_sha256)
    if report.get("season") != approve_season:
        raise PromotionError(
            f"approved season {approve_season} does not match candidate season {report.get('season')}"
        )
    if accepted_dir in backup_root.parents or backup_root == accepted_dir:
        raise PromotionError("backup root cannot be inside accepted data")
    if accepted_dir in receipt_path.parents:
        raise PromotionError("receipt cannot be inside accepted data")
    accepted_paths = {name: accepted_dir / name for name in ACCEPTED_FILES}
    candidates, candidate_hashes, baseline_hashes = _validate_candidate_set(
        report, promotion_report.parent, accepted_dir
    )

    timestamp = datetime.now(UTC)
    promotion_id = (
        f"season-{approve_season}-{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}-{report_hash[:12]}"
    )
    backup_dir = backup_root / promotion_id
    with application_lock(accepted_dir):
        if {name: _sha(path) for name, path in accepted_paths.items()} != baseline_hashes:
            raise PromotionError("accepted histories changed while acquiring the lock")
        backup_dir.mkdir(parents=True, exist_ok=False)
        backups = {name: backup_dir / name for name in ACCEPTED_FILES}
        for name, accepted in accepted_paths.items():
            _copy_and_sync(accepted, backups[name])

        staged: dict[str, Path] = {}
        try:
            for name, candidate in candidates.items():
                with tempfile.NamedTemporaryFile(
                    dir=accepted_dir, prefix=f".{name}.promotion.", delete=False
                ) as handle:
                    temporary = Path(handle.name)
                _copy_and_sync(candidate, temporary)
                staged[name] = temporary
            for name, accepted in accepted_paths.items():
                os.replace(staged.pop(name), accepted)
        except Exception as exc:
            for name, backup in backups.items():
                _copy_and_sync(backup, accepted_paths[name])
            raise PromotionError(f"application failed; accepted histories restored: {exc}") from exc
        finally:
            for temporary in staged.values():
                temporary.unlink(missing_ok=True)

        after = {name: _sha(path) for name, path in accepted_paths.items()}
        if after != candidate_hashes:
            for name, backup in backups.items():
                _copy_and_sync(backup, accepted_paths[name])
            raise PromotionError("post-application verification failed; accepted histories restored")
        receipt: dict[str, object] = {
            "schema_version": RECEIPT_SCHEMA,
            "status": "passed", "performed": True,
            "promotion_id": promotion_id, "promoted_at": timestamp.isoformat(),
            "approved_season": approve_season,
            "promotion_report": {"file": str(promotion_report), "sha256": report_hash},
            "promotion_evidence": report,
            "before": baseline_hashes, "after": after,
            "backup": {
                "directory": str(backup_dir),
                "sha256": {name: _sha(path) for name, path in backups.items()},
            },
        }
        try:
            _atomic_json(receipt_path, receipt)
        except OSError as exc:
            for name, backup in backups.items():
                _copy_and_sync(backup, accepted_paths[name])
            raise PromotionError(f"receipt could not be written; accepted histories restored: {exc}") from exc
        return {**receipt, "receipt": str(receipt_path)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promotion-report", required=True, type=Path)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--accepted-dir", required=True, type=Path)
    parser.add_argument("--approve-season", required=True, type=int)
    parser.add_argument("--backup-root", required=True, type=Path)
    parser.add_argument("--receipt", dest="receipt_path", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    try:
        receipt = apply_promotion(**vars(parse_args()))
    except PromotionError as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
