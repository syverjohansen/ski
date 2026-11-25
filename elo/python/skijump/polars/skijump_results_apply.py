#!/usr/bin/env python3
"""Atomically apply an approved ski-jumping results promotion candidate."""

from __future__ import annotations

import argparse
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

from skijump_results_promote import (
    PROMOTION_SCHEMA,
    PromotionError,
    _validate_mixed_teams,
)

RECEIPT_SCHEMA = "nordicnumbers.skijump-results-application/v1"
ACCEPTED_FILES = {
    "candidate_men_chrono.csv": "men_chrono.csv",
    "candidate_ladies_chrono.csv": "ladies_chrono.csv",
    "candidate_mixed_team_chrono.csv": "mixed_team_chrono.csv",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
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
            raise PromotionError(
                f"another results application holds {lock_path}"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_report(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    observed = _sha256(path)
    if observed != expected_sha256:
        raise PromotionError(
            f"promotion report SHA-256 mismatch: expected {expected_sha256}, "
            f"observed {observed}"
        )
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PromotionError(f"invalid promotion report: {path}") from exc
    if report.get("schema_version") != PROMOTION_SCHEMA:
        raise PromotionError("unsupported promotion report schema")
    if (
        report.get("status") != "promotion_candidate"
        or report.get("accepted_files_modified") is not False
        or report.get("atomic_candidate_set") is not True
    ):
        raise PromotionError("promotion report is not an isolated atomic candidate")
    return report, observed


def _validate_candidate_set(
    report: dict[str, Any], report_dir: Path, accepted_dir: Path
) -> tuple[dict[str, Path], dict[str, str], dict[str, str]]:
    outputs = report.get("outputs")
    accepted_before = report.get("accepted_before")
    expected_names = set(ACCEPTED_FILES.values())
    if not isinstance(outputs, dict) or set(outputs) != expected_names:
        raise PromotionError("promotion report has incomplete outputs")
    if not isinstance(accepted_before, dict) or set(accepted_before) != expected_names:
        raise PromotionError("promotion report has incomplete accepted baseline")

    candidates: dict[str, Path] = {}
    candidate_hashes: dict[str, str] = {}
    baseline_hashes: dict[str, str] = {}
    for accepted_name in ACCEPTED_FILES.values():
        candidate = report_dir / accepted_name
        receipt = outputs[accepted_name]
        baseline = accepted_before[accepted_name]
        accepted = accepted_dir / accepted_name
        if not isinstance(receipt, dict) or not isinstance(baseline, dict):
            raise PromotionError(f"malformed promotion evidence for {accepted_name}")
        if receipt.get("file") != accepted_name:
            raise PromotionError(f"unexpected candidate filename for {accepted_name}")
        expected_candidate_hash = receipt.get("sha256")
        expected_baseline_hash = baseline.get("sha256")
        if (
            not isinstance(expected_candidate_hash, str)
            or _sha256(candidate) != expected_candidate_hash
        ):
            raise PromotionError(f"candidate checksum mismatch: {accepted_name}")
        if (
            not isinstance(expected_baseline_hash, str)
            or _sha256(accepted) != expected_baseline_hash
        ):
            raise PromotionError(f"accepted history changed: {accepted_name}")
        candidates[accepted_name] = candidate
        candidate_hashes[accepted_name] = expected_candidate_hash
        baseline_hashes[accepted_name] = expected_baseline_hash

    eligible_dates = set(report.get("eligible_mixed_team_dates") or [])
    _validate_mixed_teams(candidates["mixed_team_chrono.csv"], eligible_dates)
    return candidates, candidate_hashes, baseline_hashes


def apply_promotion(
    promotion_report: Path,
    *,
    expected_report_sha256: str,
    accepted_dir: Path,
    approve_season: int,
    backup_root: Path,
    receipt_path: Path,
) -> dict[str, object]:
    promotion_report = promotion_report.expanduser().resolve()
    accepted_dir = accepted_dir.expanduser().resolve()
    backup_root = backup_root.expanduser().resolve()
    receipt_path = receipt_path.expanduser().resolve()
    report, report_hash = _load_report(promotion_report, expected_report_sha256)
    if report.get("season") != approve_season:
        raise PromotionError(
            f"approved season {approve_season} does not match candidate season "
            f"{report.get('season')}"
        )
    accepted_paths = {name: accepted_dir / name for name in ACCEPTED_FILES.values()}
    if receipt_path in set(accepted_paths.values()):
        raise PromotionError("receipt cannot overwrite an accepted history")
    candidates, candidate_hashes, baseline_hashes = _validate_candidate_set(
        report, promotion_report.parent, accepted_dir
    )

    timestamp = datetime.now(UTC)
    promotion_id = (
        f"season-{approve_season}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}-"
        f"{report_hash[:12]}"
    )
    backup_dir = backup_root / promotion_id
    with application_lock(accepted_dir):
        if {
            name: _sha256(path) for name, path in accepted_paths.items()
        } != baseline_hashes:
            raise PromotionError("accepted histories changed while acquiring the lock")
        backup_dir.mkdir(parents=True, exist_ok=False)
        backup_paths = {name: backup_dir / name for name in accepted_paths}
        for name, accepted in accepted_paths.items():
            _copy_and_sync(accepted, backup_paths[name])

        staged: dict[str, Path] = {}
        try:
            for name, candidate in candidates.items():
                with tempfile.NamedTemporaryFile(
                    dir=accepted_dir,
                    prefix=f".{name}.promotion.",
                    delete=False,
                ) as handle:
                    temporary = Path(handle.name)
                _copy_and_sync(candidate, temporary)
                staged[name] = temporary
            for name, accepted in accepted_paths.items():
                os.replace(staged.pop(name), accepted)
        except Exception as exc:
            for name, backup in backup_paths.items():
                _copy_and_sync(backup, accepted_paths[name])
            raise PromotionError(
                f"application failed; accepted histories restored: {exc}"
            ) from exc
        finally:
            for temporary in staged.values():
                temporary.unlink(missing_ok=True)

        after = {name: _sha256(path) for name, path in accepted_paths.items()}
        if after != candidate_hashes:
            for name, backup in backup_paths.items():
                _copy_and_sync(backup, accepted_paths[name])
            raise PromotionError(
                "post-application verification failed; accepted histories restored"
            )
        receipt: dict[str, object] = {
            "schema_version": RECEIPT_SCHEMA,
            "status": "passed",
            "performed": True,
            "promotion_id": promotion_id,
            "promoted_at": timestamp.isoformat(),
            "approved_season": approve_season,
            "promotion_report": {
                "file": str(promotion_report),
                "sha256": report_hash,
            },
            "promotion_evidence": report,
            "before": baseline_hashes,
            "after": after,
            "backup": {
                "directory": str(backup_dir),
                "sha256": {name: _sha256(path) for name, path in backup_paths.items()},
            },
        }
        try:
            _atomic_json(receipt_path, receipt)
        except OSError as exc:
            for name, backup in backup_paths.items():
                _copy_and_sync(backup, accepted_paths[name])
            raise PromotionError(
                f"receipt could not be written; accepted histories restored: {exc}"
            ) from exc
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
    args = parse_args()
    try:
        receipt = apply_promotion(**vars(args))
    except PromotionError as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
