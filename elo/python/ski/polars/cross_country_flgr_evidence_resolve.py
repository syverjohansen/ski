#!/usr/bin/env python3
"""Resolve a hashed FLGR evidence bundle into one chronological identity input.

The resolver rechecks every raw-page checksum, candidate reference, and
proposed target mapping before invoking the reviewed identity contract. It
writes a combined replay input without modifying the evidence bundle,
crosswalk, or accepted FIS histories.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
from collections import Counter
from collections.abc import Iterable
from pathlib import Path

from cross_country_flgr_identity import resolve_flgr_identities

INDEX_FIELDS = {
    "event_id",
    "date",
    "sex",
    "source_sha256",
    "target_athlete_id",
    "proposed_canonical_athlete_id",
    "proposed_canonical_name",
    "mapping_status",
    "raw_file",
    "candidate_file",
}
REPORT_SCHEMA = "nordicnumbers.flgr-evidence-identity-batch-report/v1"


class FlgrEvidenceResolveError(ValueError):
    """Raised when an acquired evidence bundle is inconsistent or tampered."""


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), list(reader)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _safe_bundle_path(evidence_dir: Path, relative: str, field: str) -> Path:
    relative_path = Path(relative)
    if relative_path.is_absolute():
        raise FlgrEvidenceResolveError(f"{field} must be relative to evidence bundle")
    resolved = (evidence_dir / relative_path).resolve()
    root = evidence_dir.resolve()
    if root not in resolved.parents or not resolved.is_file():
        raise FlgrEvidenceResolveError(f"unsafe or missing {field}: {relative}")
    return resolved


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_path = source.expanduser().resolve()
        if resolved == source_path.parent or source_path.parent in resolved.parents:
            raise FlgrEvidenceResolveError(
                "batch identity output cannot be inside a source directory"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise FlgrEvidenceResolveError("batch identity output directory must be empty")
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, str]], fields: list[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def resolve_evidence_bundle(
    *,
    evidence_dir: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    crosswalk_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    """Verify and resolve every candidate referenced by an evidence index."""
    evidence_dir = evidence_dir.expanduser().resolve()
    index_path = evidence_dir / "evidence-index.csv"
    if not index_path.is_file():
        raise FlgrEvidenceResolveError("evidence bundle is missing evidence-index.csv")
    output_dir = _validate_output_dir(
        output_dir, (index_path, men_chrono, ladies_chrono, crosswalk_path)
    )
    source_hashes = {
        "evidence_index": _sha256(index_path),
        "men_chrono": _sha256(men_chrono),
        "ladies_chrono": _sha256(ladies_chrono),
        "crosswalk": _sha256(crosswalk_path),
    }
    fields, index_rows = _read_csv(index_path)
    missing = sorted(INDEX_FIELDS - set(fields))
    if missing:
        raise FlgrEvidenceResolveError(f"evidence index is missing fields: {missing}")
    if not index_rows:
        raise FlgrEvidenceResolveError("evidence index is empty")

    rows_by_event: dict[str, list[tuple[int, dict[str, str]]]] = {}
    for line, index_row in enumerate(index_rows, start=2):
        event_id = index_row["event_id"].strip()
        if not event_id.startswith("flgr:"):
            raise FlgrEvidenceResolveError(
                f"evidence index line {line} has invalid event ID"
            )
        if index_row["mapping_status"] != "proposed_requires_review":
            raise FlgrEvidenceResolveError(
                f"evidence index line {line} has unexpected mapping status"
            )
        rows_by_event.setdefault(event_id, []).append((line, index_row))

    combined_rows: list[dict[str, str]] = []
    event_reports: list[dict[str, object]] = []
    metadata_fields = (
        "date",
        "sex",
        "source_sha256",
        "raw_file",
        "candidate_file",
    )
    for event_id, event_index_rows in rows_by_event.items():
        _, index_row = event_index_rows[0]
        for line, additional_row in event_index_rows[1:]:
            if any(
                additional_row[field] != index_row[field] for field in metadata_fields
            ):
                raise FlgrEvidenceResolveError(
                    f"evidence index line {line} conflicts with {event_id} metadata"
                )
        target_ids = [row["target_athlete_id"].strip() for _, row in event_index_rows]
        if len(target_ids) != len(set(target_ids)):
            raise FlgrEvidenceResolveError(
                f"evidence index has duplicate target for {event_id}"
            )
        raw_path = _safe_bundle_path(evidence_dir, index_row["raw_file"], "raw_file")
        candidate_path = _safe_bundle_path(
            evidence_dir, index_row["candidate_file"], "candidate_file"
        )
        raw_sha256 = _sha256(raw_path)
        if raw_sha256 != index_row["source_sha256"]:
            raise FlgrEvidenceResolveError(
                f"raw source checksum changed for {event_id}"
            )
        candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
        if (
            candidate.get("event_id") != event_id
            or candidate.get("date") != index_row["date"]
            or candidate.get("sex") != index_row["sex"]
            or candidate.get("source_sha256") != raw_sha256
        ):
            raise FlgrEvidenceResolveError(
                f"candidate metadata does not match evidence index for {event_id}"
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            event_output = Path(temporary_directory) / "identity"
            report = resolve_flgr_identities(
                candidate_path=candidate_path,
                men_chrono=men_chrono,
                ladies_chrono=ladies_chrono,
                crosswalk_path=crosswalk_path,
                output_dir=event_output,
            )
            _, resolved_rows = _read_csv(event_output / "flgr-identity-results.csv")

        for _, target_index_row in event_index_rows:
            target_athlete_id = target_index_row["target_athlete_id"].strip()
            target = next(
                (
                    row
                    for row in resolved_rows
                    if row["athlete_id"] == target_athlete_id
                ),
                None,
            )
            if target is None:
                raise FlgrEvidenceResolveError(
                    f"resolved event {event_id} is missing target {target_athlete_id}"
                )
            expected_id = target_index_row["proposed_canonical_athlete_id"].strip()
            expected_name = target_index_row["proposed_canonical_name"].strip()
            if (
                target["identity_status"] != "reviewed_crosswalk"
                or target["canonical_athlete_id"] != expected_id
                or target["canonical_name"] != expected_name
            ):
                raise FlgrEvidenceResolveError(
                    f"target mapping for {target_athlete_id} has not been reviewed "
                    "exactly"
                )
        combined_rows.extend(resolved_rows)
        event_reports.append(
            {
                "event_id": event_id,
                "result_rows": report["result_rows"],
                "reviewed_crosswalk_rows": report["reviewed_crosswalk_rows"],
                "flgr_only_rows": report["flgr_only_rows"],
                "reviewed_targets": len(event_index_rows),
                "source_sha256": raw_sha256,
            }
        )

    combined_rows.sort(
        key=lambda row: (
            row["date"],
            row["event_id"],
            int(row["place"]),
            row["athlete_id"],
        )
    )
    output_fields = list(combined_rows[0])
    _atomic_text(
        output_dir / "flgr-identity-results.csv",
        _csv_text(combined_rows, output_fields),
    )
    after_hashes = {
        "evidence_index": _sha256(index_path),
        "men_chrono": _sha256(men_chrono),
        "ladies_chrono": _sha256(ladies_chrono),
        "crosswalk": _sha256(crosswalk_path),
    }
    if after_hashes != source_hashes:
        raise FlgrEvidenceResolveError(
            "source evidence changed during batch resolution"
        )

    identity_statuses = Counter(row["identity_status"] for row in combined_rows)
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "source_sha256": source_hashes,
        "event_count": len(event_reports),
        "result_rows": len(combined_rows),
        "identity_status_rows": dict(sorted(identity_statuses.items())),
        "events": event_reports,
        "mapping_suggestions_applied": False,
        "publication_eligible": False,
        "source_files_modified": False,
        "status": "resolved_for_prediction_only_replay",
    }
    _atomic_text(
        output_dir / "flgr-identity-batch-report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--men-chrono", type=Path, required=True)
    parser.add_argument("--ladies-chrono", type=Path, required=True)
    parser.add_argument("--crosswalk", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = resolve_evidence_bundle(
        evidence_dir=args.evidence_dir,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        crosswalk_path=args.crosswalk,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
