#!/usr/bin/env python3
"""Apply an explicitly approved Biathlon review bundle as one transaction.

The command revalidates the complete checksummed review, builds both accepted
trees in an isolated workspace, and verifies their final fingerprints before
touching accepted data. A single cross-tree lock, durable backups, adjacent
staging files, post-write verification, and rollback protect the multi-file
commit. Publication is not part of this command.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from biathlon_promotion_review import DERIVED_FILES, REVIEW_SCHEMA
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError

RECEIPT_SCHEMA = "nordicnumbers.biathlon-promotion-application/v1"
RATING_NAMES = {
    f"{sex}{suffix}.csv"
    for sex in ("M", "L")
    for suffix in ("", "_Individual", "_Sprint", "_Pursuit", "_Mass_Start")
}
FULL_TARGETS = {
    f"{policy}/{filename}"
    for policy in ("individual", "relay")
    for filename in (
        *RATING_NAMES,
        "men_chrono.csv",
        "ladies_chrono.csv",
        "men_chrono_elevation.csv",
        "ladies_chrono_elevation.csv",
    )
}


class ApplyError(RuntimeError):
    """The review bundle cannot be safely applied."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _csv_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            if not fields or len(fields) != len(set(fields)):
                raise ApplyError(f"invalid CSV header: {path}")
            return fields, list(reader)
    except (OSError, csv.Error) as error:
        raise ApplyError(f"invalid CSV artifact: {path}") from error


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _content_fingerprint(receipt: dict[str, Any]) -> dict[str, Any]:
    return {key: receipt.get(key) for key in ("file_count", "total_bytes", "sha256")}


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _copy_and_sync(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _tree_receipts(individual_dir: Path, relay_dir: Path) -> dict[str, dict[str, Any]]:
    return {
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }


def _same_tree_receipts(
    observed: dict[str, dict[str, Any]], expected: dict[str, Any]
) -> bool:
    return all(
        isinstance(expected.get(policy), dict)
        and _content_fingerprint(observed[policy])
        == _content_fingerprint(expected[policy])
        for policy in ("individual", "relay")
    )


def _load_review(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    if not path.is_file():
        raise ApplyError(f"review report does not exist: {path}")
    observed = _sha256(path)
    if observed != expected_sha256:
        raise ApplyError(
            f"review report SHA-256 mismatch: expected {expected_sha256}, "
            f"observed {observed}"
        )
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ApplyError(f"invalid review report: {path}") from error
    if not isinstance(report, dict) or report.get("schema_version") != REVIEW_SCHEMA:
        raise ApplyError("unsupported review report schema")
    if (
        report.get("status") != "promotion_review_candidate"
        or report.get("atomic_candidate_set_complete") is not True
        or report.get("pending_materialization") != []
        or report.get("remaining_blockers") != []
        or report.get("apply_ready") is not True
        or report.get("accepted_files_modified") is not False
        or report.get("application") != {"status": "disabled", "performed": False}
        or report.get("publication") != {"status": "disabled"}
    ):
        raise ApplyError("review is not an approved, complete atomic candidate")
    return report, observed


def _validate_inventory(report: dict[str, Any], review_dir: Path) -> dict[str, Path]:
    receipts = report.get("artifacts")
    if not isinstance(receipts, list) or not receipts:
        raise ApplyError("review artifact inventory is missing")
    receipt_by_name: dict[str, dict[str, Any]] = {}
    for receipt in receipts:
        if not isinstance(receipt, dict) or not isinstance(receipt.get("file"), str):
            raise ApplyError("review artifact receipt is malformed")
        name = receipt["file"]
        if name in receipt_by_name:
            raise ApplyError(f"duplicate review artifact receipt: {name}")
        receipt_by_name[name] = receipt
    actual = {
        path.relative_to(review_dir).as_posix(): path
        for path in review_dir.rglob("*")
        if path.is_file() and path.name != "review_report.json"
    }
    if set(actual) != set(receipt_by_name):
        raise ApplyError("review artifact inventory does not match bundle contents")
    for name, path in actual.items():
        receipt = receipt_by_name[name]
        if (
            receipt.get("bytes") != path.stat().st_size
            or receipt.get("sha256") != _sha256(path)
            or (path.suffix == ".csv" and receipt.get("rows") != _csv_rows(path))
        ):
            raise ApplyError(f"review artifact receipt drifted: {name}")
    return actual


def _artifact_path(name: str, artifacts: dict[str, Path]) -> Path:
    candidates = (name, f"runtime_evidence/{name}")
    matches = [artifacts[item] for item in candidates if item in artifacts]
    if len(matches) != 1:
        raise ApplyError(
            f"target-plan artifact cannot be resolved exactly once: {name}"
        )
    return matches[0]


def _expected_full_mapping() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for relative in DERIVED_FILES:
        _, policy, filename = relative.split("/", 2)
        mapping[f"derived_candidates/{relative}"] = f"{policy}/{filename}"
    return mapping


def _validate_target_plan(
    report: dict[str, Any], artifacts: dict[str, Path]
) -> list[tuple[str, Path, str]]:
    plan = report.get("target_plan")
    if not isinstance(plan, list):
        raise ApplyError("review target plan is missing")
    season = str(report.get("season_id", ""))
    if len(season) != 4 or not season.isdigit():
        raise ApplyError("review season identity is invalid")
    season_year = f"20{season[2:]}"
    full_mapping = _expected_full_mapping()
    if set(full_mapping.values()) != FULL_TARGETS:
        raise ApplyError("internal full-target contract is inconsistent")
    operations: list[tuple[str, Path, str]] = []
    observed_full: dict[str, str] = {}
    observed_merges: dict[str, set[str]] = defaultdict(set)
    write_targets: set[str] = set()
    for item in plan:
        if not isinstance(item, dict):
            raise ApplyError("target-plan item is malformed")
        artifact = item.get("artifact")
        operation = item.get("operation")
        targets = item.get("targets")
        if (
            not isinstance(artifact, str)
            or operation
            not in {"evidence_only", "merge_event_rows", "replace_full_artifact"}
            or not isinstance(targets, list)
            or any(not isinstance(target, str) for target in targets)
        ):
            raise ApplyError("target-plan item has invalid fields")
        if operation == "evidence_only":
            if targets:
                raise ApplyError(
                    f"evidence-only artifact has write targets: {artifact}"
                )
            _artifact_path(artifact, artifacts)
            continue
        source = _artifact_path(artifact, artifacts)
        for target in targets:
            parts = Path(target).parts
            if len(parts) != 2 or parts[0] not in {"individual", "relay"}:
                raise ApplyError(f"unsafe accepted target: {target}")
            if target in write_targets:
                raise ApplyError(f"accepted target is written more than once: {target}")
            write_targets.add(target)
            operations.append((operation, source, target))
        if operation == "replace_full_artifact":
            if len(targets) != 1:
                raise ApplyError(f"full artifact has invalid targets: {artifact}")
            observed_full[artifact] = targets[0]
        else:
            observed_merges[artifact].update(targets)

    expected_merges = {
        "candidates/schedule_candidate.csv": {f"individual/races{season_year}.csv"},
        "candidates/men_results_candidate.csv": {
            "individual/men_scrape_update.csv",
            "relay/men_scrape_update.csv",
        },
        "candidates/ladies_results_candidate.csv": {
            "individual/ladies_scrape_update.csv",
            "relay/ladies_scrape_update.csv",
        },
    }
    if observed_full != full_mapping:
        raise ApplyError("review full-replacement plan is incomplete or unexpected")
    for artifact, targets in observed_merges.items():
        if artifact.startswith("relay_team_parity/team_candidates/"):
            filename = Path(artifact).name.replace(".csv", "_chrono.csv")
            expected_merges[artifact] = {f"relay/{filename}"}
    if observed_merges != expected_merges:
        raise ApplyError("review event-merge plan is incomplete or unexpected")
    if not any(
        name.startswith("relay_team_parity/team_candidates/")
        for name in observed_merges
    ):
        raise ApplyError("review contains no relay-team event candidates")
    return operations


def _event_fields(target: str, fields: list[str]) -> tuple[str, ...]:
    name = Path(target).name
    if name.startswith("races"):
        return ("Date", "RaceType", "Sex")
    base = ("Date", "RaceType")
    return (*base, "Sex") if "Sex" in fields else base


def _row_identity(
    target: str, row: dict[str, str], event_fields: tuple[str, ...]
) -> tuple[str, ...]:
    name = Path(target).name
    if name.startswith("races"):
        fields = ("Date", "RaceType", "Sex")
    elif name in {"men_scrape_update.csv", "ladies_scrape_update.csv"}:
        fields = ("Date", "RaceType", "Sex", "ID", "Leg")
    else:
        fields = (*event_fields, "Nation")
    return tuple(row[field] for field in fields)


def _merge_event_rows(source: Path, target_path: Path, target: str) -> None:
    target_fields, accepted = _read_csv(target_path)
    source_fields, candidate = _read_csv(source)
    if source_fields != target_fields:
        raise ApplyError(f"event candidate schema differs from target: {target}")
    if not candidate:
        raise ApplyError(f"event candidate contains no rows: {source}")
    event_fields = _event_fields(target, source_fields)
    if any(field not in source_fields for field in event_fields):
        raise ApplyError(f"event candidate lacks merge identity: {source}")
    selected = {tuple(row[field] for field in event_fields) for row in candidate}
    accepted_events = {tuple(row[field] for field in event_fields) for row in accepted}
    missing = selected - accepted_events
    if missing:
        raise ApplyError(
            f"event candidate is not an accepted-history replacement for {target}: "
            f"{sorted(missing)[:3]}"
        )

    # Individual and relay-inclusive histories can number the same official
    # event differently. Align each candidate copy to the target being rebuilt.
    if "Race" in source_fields and not Path(target).name.startswith("races"):
        race_by_event: dict[tuple[str, ...], set[str]] = defaultdict(set)
        for row in accepted:
            event = tuple(row[field] for field in event_fields)
            if event in selected:
                race_by_event[event].add(row["Race"])
        for event, numbers in race_by_event.items():
            if len(numbers) != 1:
                raise ApplyError(
                    f"ambiguous accepted race numbering for {target}: {event}"
                )
        candidate = [dict(row) for row in candidate]
        for row in candidate:
            event = tuple(row[field] for field in event_fields)
            row["Race"] = next(iter(race_by_event[event]))

    identities = [_row_identity(target, row, event_fields) for row in candidate]
    if len(identities) != len(set(identities)):
        raise ApplyError(f"event candidate has duplicate row identities: {source}")
    rows_by_event: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for row in candidate:
        rows_by_event[tuple(row[field] for field in event_fields)].append(row)
    emitted: set[tuple[str, ...]] = set()
    merged: list[dict[str, str]] = []
    for row in accepted:
        event = tuple(row[field] for field in event_fields)
        if event not in selected:
            merged.append(row)
        elif event not in emitted:
            merged.extend(rows_by_event[event])
            emitted.add(event)
    _write_csv(target_path, target_fields, merged)


def _validate_candidate_trees(
    operations: list[tuple[str, Path, str]],
    individual_dir: Path,
    relay_dir: Path,
) -> dict[str, dict[str, Any]]:
    roots = {"individual": individual_dir, "relay": relay_dir}
    for operation, source, target in operations:
        policy, relative = target.split("/", 1)
        destination = roots[policy] / relative
        if not destination.is_file():
            raise ApplyError(f"accepted target does not exist: {target}")
        if operation == "replace_full_artifact":
            shutil.copyfile(source, destination)
        else:
            _merge_event_rows(source, destination, target)
        _read_csv(destination)
    return _tree_receipts(individual_dir, relay_dir)


@contextmanager
def application_lock(individual_dir: Path, relay_dir: Path) -> Iterator[None]:
    common = Path(os.path.commonpath((individual_dir, relay_dir)))
    lock_path = common / ".biathlon-promotion.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ApplyError(
                f"another Biathlon application holds {lock_path}"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _restore(backup_paths: dict[str, Path], accepted_paths: dict[str, Path]) -> None:
    staged: dict[str, Path] = {}
    try:
        for target, backup in backup_paths.items():
            accepted = accepted_paths[target]
            with tempfile.NamedTemporaryFile(
                dir=accepted.parent, prefix=f".{accepted.name}.restore.", delete=False
            ) as handle:
                temporary = Path(handle.name)
            _copy_and_sync(backup, temporary)
            staged[target] = temporary
        for target, accepted in accepted_paths.items():
            os.replace(staged[target], accepted)
            staged.pop(target)
        for directory in {path.parent for path in accepted_paths.values()}:
            _fsync_directory(directory)
    finally:
        for path in staged.values():
            path.unlink(missing_ok=True)


def _safe_external_path(
    path: Path, accepted_roots: tuple[Path, Path], label: str
) -> Path:
    resolved = path.expanduser().resolve()
    for root in accepted_roots:
        if resolved == root or root in resolved.parents:
            raise ApplyError(f"{label} cannot be inside accepted data")
    return resolved


def apply_review(
    review_report: Path,
    *,
    expected_report_sha256: str,
    individual_dir: Path,
    relay_dir: Path,
    approve_run_id: str,
    approve_event_id: str,
    transaction_root: Path,
    backup_root: Path,
    receipt_path: Path,
) -> dict[str, object]:
    review_report = review_report.expanduser().resolve()
    review_dir = review_report.parent
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    roots = (individual_dir, relay_dir)
    transaction_root = _safe_external_path(transaction_root, roots, "transaction root")
    backup_root = _safe_external_path(backup_root, roots, "backup root")
    receipt_path = _safe_external_path(receipt_path, roots, "receipt")
    report, report_hash = _load_review(review_report, expected_report_sha256)
    if report.get("run_id") != approve_run_id:
        raise ApplyError("approved run ID does not match the review")
    if report.get("event_id") != approve_event_id:
        raise ApplyError("approved event ID does not match the review")
    artifacts = _validate_inventory(report, review_dir)
    operations = _validate_target_plan(report, artifacts)
    expected_before = report.get("accepted_current")
    if not isinstance(expected_before, dict):
        raise ApplyError("review has no accepted-tree baseline")
    current = _tree_receipts(individual_dir, relay_dir)
    if not _same_tree_receipts(current, expected_before):
        raise ApplyError("accepted Biathlon trees changed after review")

    transaction_root.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix="biathlon-apply.", dir=transaction_root))
    try:
        candidate_individual = workspace / "individual"
        candidate_relay = workspace / "relay"
        shutil.copytree(individual_dir, candidate_individual)
        shutil.copytree(relay_dir, candidate_relay)
        expected_after = _validate_candidate_trees(
            operations, candidate_individual, candidate_relay
        )
        candidate_roots = {
            "individual": candidate_individual,
            "relay": candidate_relay,
        }
        accepted_roots = {"individual": individual_dir, "relay": relay_dir}
        accepted_paths = {
            target: accepted_roots[target.split("/", 1)[0]] / target.split("/", 1)[1]
            for _, _, target in operations
        }
        candidate_paths = {
            target: candidate_roots[target.split("/", 1)[0]] / target.split("/", 1)[1]
            for target in accepted_paths
        }

        timestamp = datetime.now(UTC)
        application_id = (
            f"{approve_event_id}-{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}-"
            f"{report_hash[:12]}"
        )
        backup_dir = backup_root / application_id
        with application_lock(individual_dir, relay_dir):
            locked_current = _tree_receipts(individual_dir, relay_dir)
            if not _same_tree_receipts(locked_current, expected_before):
                raise ApplyError("accepted Biathlon trees changed while acquiring lock")
            backup_dir.mkdir(parents=True, exist_ok=False)
            backup_paths: dict[str, Path] = {}
            for target, accepted in accepted_paths.items():
                backup = backup_dir / target
                backup.parent.mkdir(parents=True, exist_ok=True)
                _copy_and_sync(accepted, backup)
                backup_paths[target] = backup

            staged: dict[str, Path] = {}
            try:
                for target, candidate in candidate_paths.items():
                    accepted = accepted_paths[target]
                    with tempfile.NamedTemporaryFile(
                        dir=accepted.parent,
                        prefix=f".{accepted.name}.promotion.",
                        delete=False,
                    ) as handle:
                        temporary = Path(handle.name)
                    _copy_and_sync(candidate, temporary)
                    staged[target] = temporary
                for target, accepted in accepted_paths.items():
                    os.replace(staged[target], accepted)
                    staged.pop(target)
                for directory in {path.parent for path in accepted_paths.values()}:
                    _fsync_directory(directory)
            except Exception as error:
                _restore(backup_paths, accepted_paths)
                raise ApplyError(
                    f"application failed; accepted Biathlon trees restored: {error}"
                ) from error
            finally:
                for temporary in staged.values():
                    temporary.unlink(missing_ok=True)

            observed_after = _tree_receipts(individual_dir, relay_dir)
            if not _same_tree_receipts(observed_after, expected_after):
                _restore(backup_paths, accepted_paths)
                raise ApplyError(
                    "post-application tree verification failed; accepted trees restored"
                )
            receipt: dict[str, object] = {
                "schema_version": RECEIPT_SCHEMA,
                "status": "passed",
                "performed": True,
                "publication": {"status": "disabled"},
                "application_id": application_id,
                "applied_at": timestamp.isoformat(),
                "approved_run_id": approve_run_id,
                "approved_event_id": approve_event_id,
                "review_report": {
                    "file": str(review_report),
                    "sha256": report_hash,
                },
                "before": locked_current,
                "after": observed_after,
                "targets": {
                    target: {
                        "before_sha256": _sha256(backup_paths[target]),
                        "after_sha256": _sha256(accepted_paths[target]),
                    }
                    for target in accepted_paths
                },
                "backup": {
                    "directory": str(backup_dir),
                    "file_count": len(backup_paths),
                },
            }
            try:
                _atomic_json(receipt_path, receipt)
            except OSError as error:
                _restore(backup_paths, accepted_paths)
                raise ApplyError(
                    f"receipt could not be written; accepted trees restored: {error}"
                ) from error
            return {**receipt, "receipt": str(receipt_path)}
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--review-report", required=True, type=Path)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--individual-dir", required=True, type=Path)
    parser.add_argument("--relay-dir", required=True, type=Path)
    parser.add_argument("--approve-run-id", required=True)
    parser.add_argument("--approve-event-id", required=True)
    parser.add_argument("--transaction-root", required=True, type=Path)
    parser.add_argument("--backup-root", required=True, type=Path)
    parser.add_argument("--receipt", dest="receipt_path", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    try:
        receipt = apply_review(**vars(parse_args()))
    except (ApplyError, IBUContractError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
