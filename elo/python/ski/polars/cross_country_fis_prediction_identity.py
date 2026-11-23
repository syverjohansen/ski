#!/usr/bin/env python3
"""Classify lower-tier FIS participants and build fixed-anchor comparisons.

Verification is evaluated strictly as of each evidence race. A participant is
an immutable anchor only after a prior accepted A-tier result. Everyone else
retains a stable FIS prediction identity and is compared only with those fixed
anchors; unknown-versus-unknown order never enters the output.
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
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from cross_country_prediction_sources import (
    PredictionParticipant,
    build_fixed_anchor_comparisons,
)
from cross_country_results_elo_replay import (
    BASE_ELO,
    PREVIOUS_COLUMNS,
    STREAMS,
    regress_rating,
)
from cross_country_results_identity import normalized_name

PARTICIPANT_FIELDS = (
    "prediction_athlete_id",
    "a_tier_verified",
    "canonical_athlete_id",
    "canonical_name",
    "verification_method",
    "anchor_date",
    "anchor_season",
    "anchor_race",
    "anchor_event",
    *STREAMS,
)
COMPARISON_FIELDS = (
    "fis_race_id",
    "date",
    "sex",
    "distance",
    "technique",
    "target_prediction_athlete_id",
    "target_fis_competitor_id",
    "target_fis_code",
    "target_name",
    "target_place",
    "anchor_canonical_athlete_id",
    "anchor_name",
    "anchor_place",
    "score",
    *[f"anchor_{stream}" for stream in STREAMS],
)
REPORT_SCHEMA = "nordicnumbers.fis-prediction-identity/v1"


class FisPredictionIdentityError(RuntimeError):
    """Raised when lower-tier identity evidence cannot be classified safely."""


@dataclass(frozen=True)
class VerifiedAnchor:
    canonical_id: str
    canonical_name: str
    method: str
    row: dict[str, str]
    ratings: dict[str, float]


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), list(reader)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _integer(value: str, *, default: int = 0) -> int:
    raw = (value or "").strip()
    return int(float(raw)) if raw else default


def _rating(row: dict[str, str], field: str) -> float:
    raw = (row.get(field) or "").strip()
    return float(raw) if raw else BASE_ELO


def _season_for(value: str) -> int:
    raced = date.fromisoformat(value)
    return raced.year + 1 if raced.month >= 7 else raced.year


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise FisPredictionIdentityError(
                "identity output cannot overlap prediction or accepted sources"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise FisPredictionIdentityError("identity output directory must be empty")
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]], fields: Iterable[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _load_histories(
    men_chrono: Path, ladies_chrono: Path
) -> tuple[
    dict[tuple[str, str], list[dict[str, str]]],
    dict[tuple[str, tuple[str, ...]], set[str]],
    dict[tuple[str, str], set[str]],
]:
    histories: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    name_index: dict[tuple[str, tuple[str, ...]], set[str]] = defaultdict(set)
    names_by_id: dict[tuple[str, str], set[str]] = defaultdict(set)
    required = {
        "Date",
        "Event",
        "Season",
        "Race",
        "ID",
        "Skier",
        *STREAMS,
        *PREVIOUS_COLUMNS.values(),
    }
    for sex, path in (("M", men_chrono), ("L", ladies_chrono)):
        fields, rows = _read_csv(path)
        missing = sorted(required - set(fields))
        if missing:
            raise FisPredictionIdentityError(f"{path} is missing columns: {missing}")
        for line, row in enumerate(rows, start=2):
            athlete_id = row["ID"].strip()
            name = row["Skier"].strip()
            if not athlete_id or not name:
                raise FisPredictionIdentityError(
                    f"{path}:{line}: missing accepted athlete ID or name"
                )
            key = (sex, athlete_id)
            histories[key].append(row)
            name_index[(sex, normalized_name(name))].add(athlete_id)
            names_by_id[key].add(name)
    return histories, name_index, names_by_id


def _load_aliases(
    aliases_path: Path,
    histories: dict[tuple[str, str], list[dict[str, str]]],
    names_by_id: dict[tuple[str, str], set[str]],
) -> dict[tuple[str, str], dict[str, str]]:
    _, rows = _read_csv(aliases_path)
    aliases = {}
    for line, row in enumerate(rows, start=2):
        sex = (row.get("sex") or "").strip()
        competitor_id = (row.get("fis_competitor_id") or "").strip()
        canonical_id = (row.get("canonical_id") or "").strip()
        canonical_name = (row.get("canonical_name") or "").strip()
        key = (sex, competitor_id)
        if (
            sex not in {"M", "L"}
            or not competitor_id
            or not (row.get("fis_code") or "").strip()
            or not (row.get("fis_name") or "").strip()
            or not canonical_id
            or not canonical_name
        ):
            raise FisPredictionIdentityError(f"{aliases_path}:{line}: incomplete alias")
        if key in aliases:
            raise FisPredictionIdentityError(f"{aliases_path}:{line}: duplicate alias")
        canonical_key = (sex, canonical_id)
        if (
            canonical_key not in histories
            or canonical_name not in names_by_id[canonical_key]
        ):
            raise FisPredictionIdentityError(
                f"{aliases_path}:{line}: alias does not match accepted history"
            )
        aliases[key] = row
    return aliases


def _latest_anchor(
    history: list[dict[str, str]], event_date: str
) -> dict[str, str] | None:
    prior = [row for row in history if row["Date"].strip() < event_date]
    if not any(_integer(row["Race"]) > 0 for row in prior):
        return None
    return max(
        prior,
        key=lambda row: (
            row["Date"].strip(),
            _integer(row["Season"], default=-1),
            _integer(row["Race"], default=-1),
        ),
    )


def _anchor_ratings(anchor: dict[str, str], event_date: str) -> dict[str, float]:
    boundaries = max(0, _season_for(event_date) - int(anchor["Season"]))
    ratings = {}
    for stream, previous_column in PREVIOUS_COLUMNS.items():
        source_column = previous_column if anchor["Event"] == "Offseason" else stream
        ratings[stream] = regress_rating(_rating(anchor, source_column), boundaries)
    return ratings


def _verified_anchor(
    *,
    row: dict[str, str],
    histories: dict[tuple[str, str], list[dict[str, str]]],
    name_index: dict[tuple[str, tuple[str, ...]], set[str]],
    aliases: dict[tuple[str, str], dict[str, str]],
) -> VerifiedAnchor | None:
    sex = row["sex"].strip()
    competitor_id = row["fis_competitor_id"].strip()
    fis_code = row["fis_code"].strip()
    fis_name = row["name"].strip()
    event_date = row["date"].strip()
    alias = aliases.get((sex, competitor_id))
    if alias:
        if (
            alias["fis_code"].strip() != fis_code
            or alias["fis_name"].strip() != fis_name
        ):
            raise FisPredictionIdentityError(
                f"FIS identity changed for reviewed alias {sex}/{competitor_id}"
            )
        candidates = {alias["canonical_id"].strip()}
        method = "reviewed_alias"
    else:
        candidates = name_index.get((sex, normalized_name(fis_name)), set())
        method = "unique_prior_a_tier_name"

    eligible = []
    for canonical_id in candidates:
        anchor = _latest_anchor(histories[(sex, canonical_id)], event_date)
        if anchor is not None:
            eligible.append((canonical_id, anchor))
    if not eligible:
        return None
    if len(eligible) != 1:
        raise FisPredictionIdentityError(
            f"ambiguous prior A-tier identity for {sex}/{competitor_id} {fis_name!r}"
        )
    canonical_id, anchor = eligible[0]
    return VerifiedAnchor(
        canonical_id=canonical_id,
        canonical_name=anchor["Skier"].strip(),
        method=method,
        row=anchor,
        ratings=_anchor_ratings(anchor, event_date),
    )


def classify_fis_prediction_identities(
    *,
    results: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    aliases_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    """Write participant classifications and fixed-anchor comparison evidence."""
    output_dir = _validate_output_dir(
        output_dir, (results, men_chrono, ladies_chrono, aliases_path)
    )
    source_hashes = {
        "results": _sha256(results),
        "men_chrono": _sha256(men_chrono),
        "ladies_chrono": _sha256(ladies_chrono),
        "aliases": _sha256(aliases_path),
    }
    raw_fields, raw_rows = _read_csv(results)
    required = {
        "date",
        "sex",
        "distance",
        "technique",
        "fis_race_id",
        "fis_competitor_id",
        "fis_code",
        "name",
        "place",
    }
    missing = sorted(required - set(raw_fields))
    if missing:
        raise FisPredictionIdentityError(
            f"prediction results missing fields: {missing}"
        )
    if not raw_rows:
        raise FisPredictionIdentityError("prediction results are empty")

    histories, name_index, names_by_id = _load_histories(men_chrono, ladies_chrono)
    aliases = _load_aliases(aliases_path, histories, names_by_id)
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    seen_athletes = set()
    for line, row in enumerate(raw_rows, start=2):
        try:
            date.fromisoformat(row["date"])
            place = int(row["place"])
        except ValueError as exc:
            raise FisPredictionIdentityError(
                f"{results}:{line}: invalid date or place"
            ) from exc
        sex = row["sex"].strip()
        competitor_id = row["fis_competitor_id"].strip()
        if sex not in {"M", "L"} or not competitor_id or place < 1:
            raise FisPredictionIdentityError(
                f"{results}:{line}: incomplete prediction identity"
            )
        event_key = (row["date"].strip(), row["fis_race_id"].strip())
        athlete_key = (*event_key, sex, competitor_id)
        if athlete_key in seen_athletes:
            raise FisPredictionIdentityError(
                f"duplicate athlete {competitor_id} in FIS race {event_key[1]}"
            )
        seen_athletes.add(athlete_key)
        grouped[event_key].append(row)

    participant_rows: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []
    verified_count = 0
    unverified_count = 0
    anchor_free_events = 0
    for event_key in sorted(grouped):
        event_rows = sorted(
            grouped[event_key],
            key=lambda row: (int(row["place"]), row["fis_competitor_id"]),
        )
        anchors_by_prediction_id: dict[str, VerifiedAnchor] = {}
        classified_event = []
        participants = []
        for row in event_rows:
            anchor = _verified_anchor(
                row=row,
                histories=histories,
                name_index=name_index,
                aliases=aliases,
            )
            prediction_id = (
                f"canonical:{anchor.canonical_id}"
                if anchor
                else f"fis:{row['fis_competitor_id'].strip()}"
            )
            if anchor:
                verified_count += 1
                anchors_by_prediction_id[prediction_id] = anchor
            else:
                unverified_count += 1
            participants.append(
                PredictionParticipant(
                    athlete_id=prediction_id,
                    place=int(row["place"]),
                    a_tier_verified=anchor is not None,
                    rating=anchor.ratings["Elo"] if anchor else None,
                )
            )
            classified_event.append((row, prediction_id, anchor))

            participant_output: dict[str, object] = {
                **row,
                "prediction_athlete_id": prediction_id,
                "a_tier_verified": str(anchor is not None).lower(),
                "canonical_athlete_id": anchor.canonical_id if anchor else "",
                "canonical_name": anchor.canonical_name if anchor else "",
                "verification_method": anchor.method
                if anchor
                else "never_a_tier_as_of_event",
                "anchor_date": anchor.row["Date"] if anchor else "",
                "anchor_season": anchor.row["Season"] if anchor else "",
                "anchor_race": anchor.row["Race"] if anchor else "",
                "anchor_event": anchor.row["Event"] if anchor else "",
            }
            for stream in STREAMS:
                participant_output[stream] = anchor.ratings[stream] if anchor else ""
            participant_rows.append(participant_output)

        comparisons = build_fixed_anchor_comparisons(participants)
        if not anchors_by_prediction_id:
            anchor_free_events += 1
        rows_by_prediction_id = {
            prediction_id: row for row, prediction_id, _ in classified_event
        }
        for row, prediction_id, anchor in classified_event:
            if anchor:
                continue
            for comparison in comparisons[prediction_id]:
                anchor_state = anchors_by_prediction_id[comparison.anchor_athlete_id]
                anchor_result = rows_by_prediction_id[comparison.anchor_athlete_id]
                output: dict[str, object] = {
                    "fis_race_id": row["fis_race_id"],
                    "date": row["date"],
                    "sex": row["sex"],
                    "distance": row["distance"],
                    "technique": row["technique"],
                    "target_prediction_athlete_id": prediction_id,
                    "target_fis_competitor_id": row["fis_competitor_id"],
                    "target_fis_code": row["fis_code"],
                    "target_name": row["name"],
                    "target_place": row["place"],
                    "anchor_canonical_athlete_id": anchor_state.canonical_id,
                    "anchor_name": anchor_state.canonical_name,
                    "anchor_place": anchor_result["place"],
                    "score": comparison.score,
                }
                for stream in STREAMS:
                    output[f"anchor_{stream}"] = anchor_state.ratings[stream]
                comparison_rows.append(output)

    _atomic_text(
        output_dir / "fis-prediction-participants.csv",
        _csv_text(participant_rows, (*raw_fields, *PARTICIPANT_FIELDS)),
    )
    _atomic_text(
        output_dir / "fis-anchor-comparisons.csv",
        _csv_text(comparison_rows, COMPARISON_FIELDS),
    )
    after_hashes = {
        "results": _sha256(results),
        "men_chrono": _sha256(men_chrono),
        "ladies_chrono": _sha256(ladies_chrono),
        "aliases": _sha256(aliases_path),
    }
    if after_hashes != source_hashes:
        raise FisPredictionIdentityError(
            "identity sources changed during classification"
        )

    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "source_sha256": source_hashes,
        "event_count": len(grouped),
        "participant_rows": len(participant_rows),
        "verified_anchor_appearances": verified_count,
        "never_a_tier_appearances": unverified_count,
        "comparison_rows": len(comparison_rows),
        "anchor_free_events": anchor_free_events,
        "unknown_vs_unknown_comparisons": 0,
        "accepted_histories_modified": False,
        "publication_eligible": False,
        "status": "classified_fixed_anchor_evidence",
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = classify_fis_prediction_identities(
        results=args.results,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        aliases_path=args.aliases,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
