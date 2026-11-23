#!/usr/bin/env python3
"""Build a reviewable fixed-anchor calibration cohort from FIS evidence.

Previously reviewed aliases remain reviewed. New exact unique-name connections
are emitted only as proposals; they cannot enter the calibration backtest until
an operator changes their status after reviewing the recorded evidence.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from cross_country_results_backfill import validate_results_url
from cross_country_results_identity import normalized_name

REPORT_SCHEMA = "nordicnumbers.fis-prediction-calibration-cohort/v1"
CROSSWALK_FILENAME = "fis-prediction-calibration-crosswalk.csv"
REPORT_FILENAME = "report.json"
CROSSWALK_FIELDS = (
    "sex",
    "prediction_athlete_id",
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "canonical_athlete_id",
    "canonical_name",
    "status",
    "evidence",
)


class FisPredictionCohortError(ValueError):
    """Raised when evidence cannot form a safe calibration cohort."""


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), list(reader)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise FisPredictionCohortError(
                "cohort output cannot overlap evidence or accepted sources"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise FisPredictionCohortError("cohort output directory must be empty")
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CROSSWALK_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _load_histories(
    men_chrono: Path, ladies_chrono: Path
) -> tuple[
    dict[tuple[str, tuple[str, ...]], set[str]],
    dict[tuple[str, str], list[dict[str, str]]],
]:
    required = {"Date", "Event", "Season", "Race", "ID", "Skier", "Place"}
    names: dict[tuple[str, tuple[str, ...]], set[str]] = defaultdict(set)
    histories: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for sex, path in (("M", men_chrono), ("L", ladies_chrono)):
        fields, rows = _read_csv(path)
        missing = sorted(required - set(fields))
        if missing:
            raise FisPredictionCohortError(f"{path} is missing columns: {missing}")
        for line, row in enumerate(rows, start=2):
            athlete_id = row["ID"].strip()
            athlete_name = row["Skier"].strip()
            try:
                date.fromisoformat(row["Date"].strip())
                int(row["Season"])
                int(row["Race"])
            except ValueError as error:
                raise FisPredictionCohortError(
                    f"{path}:{line}: invalid accepted history row"
                ) from error
            if not athlete_id or not athlete_name:
                raise FisPredictionCohortError(
                    f"{path}:{line}: incomplete accepted athlete identity"
                )
            histories[(sex, athlete_id)].append(row)
            names[(sex, normalized_name(athlete_name))].add(athlete_id)
    return names, histories


def _load_aliases(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    fields, rows = _read_csv(path)
    required = {
        "sex",
        "fis_competitor_id",
        "fis_code",
        "fis_name",
        "canonical_id",
        "canonical_name",
    }
    missing = sorted(required - set(fields))
    if missing:
        raise FisPredictionCohortError(f"{path} is missing columns: {missing}")
    aliases = {}
    for line, row in enumerate(rows, start=2):
        key = (row["sex"].strip(), row["fis_competitor_id"].strip())
        if (
            key[0] not in {"M", "L"}
            or not key[1]
            or not row["fis_code"].strip()
            or not row["fis_name"].strip()
            or not row["canonical_id"].strip()
            or not row["canonical_name"].strip()
        ):
            raise FisPredictionCohortError(f"{path}:{line}: incomplete alias")
        if key in aliases:
            raise FisPredictionCohortError(f"{path}:{line}: duplicate alias")
        aliases[key] = row
    return aliases


def _load_results(paths: Iterable[Path]) -> dict[tuple[str, str], list[dict[str, str]]]:
    required = {
        "date",
        "sex",
        "fis_race_id",
        "fis_competitor_id",
        "fis_code",
        "name",
        "place",
        "source_url",
        "source_sha256",
    }
    athletes: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    identities = {}
    seen = set()
    for path in paths:
        fields, rows = _read_csv(path)
        missing = sorted(required - set(fields))
        if missing:
            raise FisPredictionCohortError(f"{path} is missing columns: {missing}")
        for line, row in enumerate(rows, start=2):
            sex = row["sex"].strip()
            competitor_id = row["fis_competitor_id"].strip()
            fis_code = row["fis_code"].strip()
            fis_name = row["name"].strip()
            race_id = row["fis_race_id"].strip()
            checksum = row["source_sha256"].strip().lower()
            try:
                date.fromisoformat(row["date"].strip())
                place = int(row["place"])
            except ValueError as error:
                raise FisPredictionCohortError(
                    f"{path}:{line}: invalid result row"
                ) from error
            try:
                source_race_id = validate_results_url(row["source_url"].strip())
            except Exception as error:
                raise FisPredictionCohortError(
                    f"{path}:{line}: invalid official FIS result URL"
                ) from error
            if (
                sex not in {"M", "L"}
                or not competitor_id
                or not fis_code
                or not fis_name
                or not race_id
                or source_race_id != race_id
                or place < 1
                or len(checksum) != 64
                or any(character not in "0123456789abcdef" for character in checksum)
                or not row["source_url"].strip()
            ):
                raise FisPredictionCohortError(
                    f"{path}:{line}: incomplete result evidence"
                )
            key = (sex, competitor_id)
            identity = (fis_code, fis_name)
            if key in identities and identities[key] != identity:
                raise FisPredictionCohortError(
                    f"FIS identity changed for {sex}/{competitor_id}"
                )
            identities[key] = identity
            result_key = (row["date"].strip(), race_id, sex, competitor_id)
            if result_key in seen:
                continue
            seen.add(result_key)
            athletes[key].append(row)
    if not athletes:
        raise FisPredictionCohortError("prediction evidence contains no athletes")
    return athletes


def _first_a_tier(history: list[dict[str, str]]) -> dict[str, str] | None:
    classified = [
        row
        for row in history
        if row["Event"].strip() != "Offseason"
        and int(row["Race"]) > 0
        and int(float(row["Place"] or 0)) > 0
    ]
    if not classified:
        return None
    return min(classified, key=lambda row: (row["Date"], int(row["Race"])))


def build_fis_prediction_calibration_cohort(
    *,
    results: list[Path],
    men_chrono: Path,
    ladies_chrono: Path,
    aliases_path: Path,
    output_dir: Path,
    minimum_athletes: int = 20,
) -> dict[str, object]:
    """Emit reviewed and proposed debut mappings without auto-approving proposals."""
    if not results:
        raise FisPredictionCohortError("at least one results file is required")
    if minimum_athletes < 1:
        raise FisPredictionCohortError("minimum_athletes must be positive")
    sources = [*results, men_chrono, ladies_chrono, aliases_path]
    output_dir = _validate_output_dir(output_dir, sources)
    source_hashes = {str(path): _sha256(path) for path in sources}
    name_index, histories = _load_histories(men_chrono, ladies_chrono)
    aliases = _load_aliases(aliases_path)
    evidence_by_athlete = _load_results(results)

    rows = []
    selected_canonical_ids = set()
    ambiguous_identities = []
    no_later_debut = 0
    no_pre_debut_evidence = 0
    for key, evidence_rows in sorted(evidence_by_athlete.items()):
        sex, competitor_id = key
        sample = evidence_rows[0]
        alias = aliases.get(key)
        if alias:
            if (
                alias["fis_code"].strip() != sample["fis_code"].strip()
                or alias["fis_name"].strip() != sample["name"].strip()
            ):
                raise FisPredictionCohortError(
                    f"reviewed alias does not match FIS evidence for {sex}/{competitor_id}"
                )
            canonical_ids = {alias["canonical_id"].strip()}
            status = "reviewed"
        else:
            canonical_ids = name_index.get(
                (sex, normalized_name(sample["name"].strip())), set()
            )
            status = "proposed_unique_name"
        if len(canonical_ids) != 1:
            if canonical_ids:
                ambiguous_identities.append(
                    {
                        "sex": sex,
                        "fis_competitor_id": competitor_id,
                        "fis_name": sample["name"].strip(),
                        "candidate_canonical_ids": sorted(canonical_ids),
                    }
                )
            continue
        canonical_id = next(iter(canonical_ids))
        canonical_key = (sex, canonical_id)
        if canonical_key in selected_canonical_ids:
            raise FisPredictionCohortError(
                f"multiple FIS identities map to canonical athlete {sex}/{canonical_id}"
            )
        history = histories.get((sex, canonical_id), [])
        if alias and not history:
            raise FisPredictionCohortError(
                f"reviewed alias has no accepted history for {sex}/{canonical_id}"
            )
        debut = _first_a_tier(history)
        if debut is None:
            no_later_debut += 1
            continue
        eligible_evidence = [
            row for row in evidence_rows if row["date"].strip() < debut["Date"].strip()
        ]
        if not eligible_evidence:
            no_pre_debut_evidence += 1
            continue
        latest = max(
            eligible_evidence,
            key=lambda row: (row["date"].strip(), row["fis_race_id"].strip()),
        )
        canonical_names = {row["Skier"].strip() for row in history}
        canonical_name = (
            alias["canonical_name"].strip() if alias else debut["Skier"].strip()
        )
        if canonical_name not in canonical_names:
            raise FisPredictionCohortError(
                f"canonical name does not match history for {sex}/{canonical_id}"
            )
        rows.append(
            {
                "sex": sex,
                "prediction_athlete_id": f"fis:{competitor_id}",
                "fis_competitor_id": competitor_id,
                "fis_code": sample["fis_code"].strip(),
                "fis_name": sample["name"].strip(),
                "canonical_athlete_id": canonical_id,
                "canonical_name": canonical_name,
                "status": status,
                "evidence": (
                    f"lower-tier FIS race {latest['fis_race_id']} on "
                    f"{latest['date']} ({latest['source_url']}); first accepted A-tier "
                    f"race {debut['Race']} on {debut['Date']}"
                ),
            }
        )
        selected_canonical_ids.add(canonical_key)

    if not rows:
        raise FisPredictionCohortError("no pre-debut calibration candidates found")
    reviewed = sum(row["status"] == "reviewed" for row in rows)
    proposed = len(rows) - reviewed
    _atomic_text(output_dir / CROSSWALK_FILENAME, _csv_text(rows))
    after_hashes = {str(path): _sha256(path) for path in sources}
    if after_hashes != source_hashes:
        raise FisPredictionCohortError(
            "calibration sources changed during cohort build"
        )
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "source_sha256": source_hashes,
        "candidate_athletes": len(rows),
        "reviewed_athletes": reviewed,
        "proposed_athletes": proposed,
        "minimum_athletes": minimum_athletes,
        "review_queue_ready": len(rows) >= minimum_athletes,
        "calibration_ready": reviewed >= minimum_athletes,
        "ambiguous_identities": ambiguous_identities,
        "athletes_without_later_debut": no_later_debut,
        "athletes_without_pre_debut_evidence": no_pre_debut_evidence,
        "automatic_approvals": 0,
        "publication_eligible": False,
        "accepted_histories_modified": False,
        "status": "cohort_built_pending_identity_review",
    }
    _atomic_text(
        output_dir / REPORT_FILENAME,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", action="append", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--minimum-athletes", type=int, default=20)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_fis_prediction_calibration_cohort(
        results=args.results,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        aliases_path=args.aliases,
        minimum_athletes=args.minimum_athletes,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
