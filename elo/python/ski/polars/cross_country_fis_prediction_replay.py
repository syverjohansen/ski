#!/usr/bin/env python3
"""Replay never-A-tier FIS prediction ratings against fixed A-tier anchors.

Verified athletes never receive an update. Unknown-versus-unknown ordering is
absent from the comparison input, so the only information entering a predicted
rating is a never-A-tier athlete's outcome against immutable official anchors.
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

from cross_country_results_elo_replay import (
    BASE_ELO,
    ELO_DIFFERENCE,
    PREVIOUS_COLUMNS,
    ROLLOVER_DISCOUNT,
    STREAMS,
    regress_rating,
    stream_includes,
)

REPORT_SCHEMA = "nordicnumbers.fis-prediction-replay/v1"
LEDGER_FILENAME = "fis-prediction-ledger.csv"
REPORT_FILENAME = "report.json"


class FisPredictionReplayError(RuntimeError):
    """Raised when fixed-anchor prediction evidence cannot be replayed safely."""


@dataclass
class PredictionState:
    prediction_athlete_id: str
    fis_competitor_id: str
    fis_code: str
    name: str
    last_season: int
    experience: int
    ratings: dict[str, float]


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
            raise FisPredictionReplayError(
                "replay output cannot overlap prediction evidence sources"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise FisPredictionReplayError("replay output directory must be empty")
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


def _season_for(row: dict[str, str]) -> int:
    raw = (row.get("season") or "").strip()
    if raw:
        return int(raw)
    raced = date.fromisoformat(row["date"])
    return raced.year + 1 if raced.month >= 7 else raced.year


def _expected(target_rating: float, anchor_rating: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((anchor_rating - target_rating) / ELO_DIFFERENCE))


def _updated_rating(
    rating: float, comparisons: list[dict[str, str]], stream: str, k_factor: float
) -> float:
    residual = 0.0
    anchor_field = f"anchor_{stream}"
    for comparison in comparisons:
        try:
            anchor_rating = float(comparison[anchor_field])
            score = float(comparison["score"])
        except (KeyError, ValueError) as exc:
            raise FisPredictionReplayError(
                f"invalid fixed-anchor comparison for {stream}"
            ) from exc
        if score not in {0.0, 0.5, 1.0}:
            raise FisPredictionReplayError(f"invalid comparison score: {score}")
        residual += score - _expected(rating, anchor_rating)
    return rating + k_factor * residual


def replay_fis_prediction_elo(
    *,
    participants: Path,
    comparisons: Path,
    output_dir: Path,
    k_factor: float = 1.0,
) -> dict[str, object]:
    """Replay prediction-only ratings while leaving every anchor immutable."""
    if not 0.0 < k_factor <= 10.0:
        raise FisPredictionReplayError("k_factor must be greater than 0 and at most 10")
    output_dir = _validate_output_dir(output_dir, (participants, comparisons))
    source_hashes = {
        "participants": _sha256(participants),
        "comparisons": _sha256(comparisons),
    }
    participant_fields, participant_rows = _read_csv(participants)
    comparison_fields, comparison_rows = _read_csv(comparisons)
    required_participants = {
        "fis_race_id",
        "date",
        "sex",
        "distance",
        "technique",
        "place",
        "fis_competitor_id",
        "fis_code",
        "name",
        "prediction_athlete_id",
        "a_tier_verified",
    }
    required_comparisons = {
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
    }
    missing_participants = sorted(required_participants - set(participant_fields))
    missing_comparisons = sorted(required_comparisons - set(comparison_fields))
    if missing_participants:
        raise FisPredictionReplayError(
            f"participants are missing fields: {missing_participants}"
        )
    if missing_comparisons:
        raise FisPredictionReplayError(
            f"comparisons are missing fields: {missing_comparisons}"
        )
    if not participant_rows:
        raise FisPredictionReplayError("participants are empty")

    comparisons_by_target_event: dict[tuple[str, str, str], list[dict[str, str]]] = (
        defaultdict(list)
    )
    for line, row in enumerate(comparison_rows, start=2):
        key = (
            row["date"].strip(),
            row["fis_race_id"].strip(),
            row["target_prediction_athlete_id"].strip(),
        )
        if not all(key) or not row["target_fis_competitor_id"].strip():
            raise FisPredictionReplayError(
                f"{comparisons}:{line}: incomplete comparison identity"
            )
        comparisons_by_target_event[key].append(row)

    events: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    seen_event_athletes = set()
    for line, row in enumerate(participant_rows, start=2):
        event_key = (row["date"].strip(), row["fis_race_id"].strip())
        prediction_id = row["prediction_athlete_id"].strip()
        verified = row["a_tier_verified"].strip().lower()
        if verified not in {"true", "false"}:
            raise FisPredictionReplayError(
                f"{participants}:{line}: invalid A-tier verification flag"
            )
        if not all(event_key) or not prediction_id:
            raise FisPredictionReplayError(
                f"{participants}:{line}: incomplete participant identity"
            )
        athlete_key = (*event_key, prediction_id)
        if athlete_key in seen_event_athletes:
            raise FisPredictionReplayError(
                f"duplicate participant {prediction_id} in FIS race {event_key[1]}"
            )
        seen_event_athletes.add(athlete_key)
        events[event_key].append(row)

    unverified_ids = {
        row["prediction_athlete_id"].strip()
        for row in participant_rows
        if row["a_tier_verified"].strip().lower() == "false"
    }
    for key, comparison_group in comparisons_by_target_event.items():
        if key[2] not in unverified_ids:
            raise FisPredictionReplayError(
                f"comparison targets verified or unknown participant {key[2]}"
            )
        matching = [
            row
            for row in events.get((key[0], key[1]), [])
            if row["prediction_athlete_id"].strip() == key[2]
            and row["a_tier_verified"].strip().lower() == "false"
        ]
        if len(matching) != 1:
            raise FisPredictionReplayError(
                f"comparison target {key[2]} does not match one event participant"
            )
        participant = matching[0]
        expected_target = {
            "sex": participant["sex"].strip(),
            "distance": participant["distance"].strip(),
            "technique": participant["technique"].strip(),
            "target_fis_competitor_id": participant["fis_competitor_id"].strip(),
            "target_fis_code": participant["fis_code"].strip(),
            "target_name": participant["name"].strip(),
            "target_place": participant["place"].strip(),
        }
        anchors = set()
        for comparison in comparison_group:
            for field, expected in expected_target.items():
                if comparison[field].strip() != expected:
                    raise FisPredictionReplayError(
                        f"comparison target metadata mismatch for {key[2]}: {field}"
                    )
            anchor_id = comparison["anchor_canonical_athlete_id"].strip()
            if not anchor_id or not comparison["anchor_name"].strip():
                raise FisPredictionReplayError(
                    "comparison has incomplete anchor identity"
                )
            if anchor_id in anchors:
                raise FisPredictionReplayError(
                    f"duplicate fixed anchor {anchor_id} for target {key[2]}"
                )
            anchors.add(anchor_id)

    states: dict[str, PredictionState] = {}
    ledger: list[dict[str, object]] = []
    anchor_free_appearances = 0
    comparison_count = 0
    for event_key in sorted(events):
        event_rows = sorted(
            events[event_key],
            key=lambda row: (int(row["place"]), row["prediction_athlete_id"]),
        )
        for row in event_rows:
            if row["a_tier_verified"].strip().lower() == "true":
                continue
            prediction_id = row["prediction_athlete_id"].strip()
            season = _season_for(row)
            state = states.get(prediction_id)
            if state is None:
                state = PredictionState(
                    prediction_athlete_id=prediction_id,
                    fis_competitor_id=row["fis_competitor_id"].strip(),
                    fis_code=row["fis_code"].strip(),
                    name=row["name"].strip(),
                    last_season=season,
                    experience=0,
                    ratings={stream: BASE_ELO for stream in STREAMS},
                )
                states[prediction_id] = state
            elif (
                state.fis_competitor_id != row["fis_competitor_id"].strip()
                or state.fis_code != row["fis_code"].strip()
                or state.name != row["name"].strip()
            ):
                raise FisPredictionReplayError(
                    f"prediction identity changed for {prediction_id}"
                )
            boundaries = season - state.last_season
            if boundaries < 0:
                raise FisPredictionReplayError("events are not in chronological order")
            if boundaries:
                state.ratings = {
                    stream: regress_rating(rating, boundaries)
                    for stream, rating in state.ratings.items()
                }
                state.last_season = season

            target_key = (*event_key, prediction_id)
            target_comparisons = comparisons_by_target_event.get(target_key, [])
            comparison_count += len(target_comparisons)
            previous = dict(state.ratings)
            updated = dict(previous)
            if target_comparisons:
                for stream in STREAMS:
                    if stream_includes(stream, row["distance"], row["technique"]):
                        updated[stream] = _updated_rating(
                            previous[stream], target_comparisons, stream, k_factor
                        )
                update_status = "updated_from_fixed_anchors"
            else:
                anchor_free_appearances += 1
                update_status = "anchor_free_no_update"
            state.experience += 1
            state.ratings = updated
            output: dict[str, object] = {
                "fis_race_id": row["fis_race_id"],
                "date": row["date"],
                "season": season,
                "sex": row["sex"],
                "distance": row["distance"],
                "technique": row["technique"],
                "place": row["place"],
                "prediction_athlete_id": prediction_id,
                "fis_competitor_id": state.fis_competitor_id,
                "fis_code": state.fis_code,
                "name": state.name,
                "experience": state.experience,
                "anchor_comparisons": len(target_comparisons),
                "k_factor": k_factor,
                "update_status": update_status,
            }
            for stream, previous_column in PREVIOUS_COLUMNS.items():
                output[previous_column] = previous[stream]
                output[stream] = updated[stream]
            ledger.append(output)

    if not ledger:
        raise FisPredictionReplayError("participants contain no never-A-tier athletes")
    fields = list(ledger[0])
    _atomic_text(output_dir / LEDGER_FILENAME, _csv_text(ledger, fields))
    after_hashes = {
        "participants": _sha256(participants),
        "comparisons": _sha256(comparisons),
    }
    if after_hashes != source_hashes:
        raise FisPredictionReplayError("prediction evidence changed during replay")
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "source_sha256": source_hashes,
        "event_count": len(events),
        "never_a_tier_athletes": len(states),
        "ledger_rows": len(ledger),
        "fixed_anchor_comparisons": comparison_count,
        "unknown_vs_unknown_comparisons": 0,
        "anchor_free_appearances": anchor_free_appearances,
        "k_factor": k_factor,
        "calibration_status": "pending_fixed_anchor_backtest",
        "rollover": {"base_elo": BASE_ELO, "discount": ROLLOVER_DISCOUNT},
        "verified_anchor_updates": 0,
        "publication_eligible": False,
        "status": "replayed_prediction_only",
    }
    _atomic_text(
        output_dir / REPORT_FILENAME,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--participants", required=True, type=Path)
    parser.add_argument("--comparisons", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--k-factor", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = replay_fis_prediction_elo(
        participants=args.participants,
        comparisons=args.comparisons,
        output_dir=args.output_dir,
        k_factor=args.k_factor,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
