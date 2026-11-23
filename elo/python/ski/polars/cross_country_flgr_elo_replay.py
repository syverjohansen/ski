#!/usr/bin/env python3
"""Replay reviewed FLGR results into an isolated prediction-only Elo ledger.

The official FIS chrono histories are immutable inputs. Reviewed FLGR athletes
start from their most recent accepted FIS rating before their first bridge
event; FLGR-only athletes start at 1300. Both groups then update through the
same nine-stream Elo math and offseason regression used by Nordic Numbers.
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
    PREVIOUS_COLUMNS,
    ROLLOVER_DISCOUNT,
    STREAMS,
    regress_rating,
    stream_includes,
    update_ratings,
)

REPORT_SCHEMA = "nordicnumbers.flgr-elo-replay-report/v1"
LEDGER_FILENAME = "flgr-bridge-ledger.csv"
REPORT_FILENAME = "flgr-bridge-report.json"
IDENTITY_STATUSES = {"reviewed_crosswalk", "flgr_only"}
TEAM_DISTANCES = {"Rel", "Ts"}


class FlgrEloReplayError(ValueError):
    """Raised when FLGR evidence cannot be replayed safely."""


@dataclass
class BridgeState:
    athlete_id: str
    source_name: str
    canonical_athlete_id: str
    canonical_name: str
    identity_status: str
    anchor_source: str
    experience: int
    last_season: int
    ratings: dict[str, float]


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), list(reader)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rating(row: dict[str, str], field: str) -> float:
    raw = (row.get(field) or "").strip()
    return float(raw) if raw else BASE_ELO


def _integer(value: str, *, default: int = 0) -> int:
    raw = (value or "").strip()
    return int(float(raw)) if raw else default


def _season_for(value: str) -> int:
    raced = date.fromisoformat(value)
    return raced.year + 1 if raced.month >= 7 else raced.year


def _boolean(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false"}:
        return False
    raise FlgrEloReplayError(f"invalid boolean value: {value!r}")


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_path = source.expanduser().resolve()
        if resolved == source_path.parent or source_path.parent in resolved.parents:
            raise FlgrEloReplayError(
                "replay output cannot be inside a source directory"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise FlgrEloReplayError("replay output directory must be empty")
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]], fields: list[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _validate_identity_rows(rows: list[dict[str, str]]) -> None:
    required = {
        "event_id",
        "date",
        "season",
        "sex",
        "distance",
        "technique",
        "mass_start",
        "athlete_id",
        "source_competitor_id",
        "source_name",
        "place",
        "canonical_athlete_id",
        "canonical_name",
        "identity_status",
        "source_sha256",
    }
    if not rows:
        raise FlgrEloReplayError("identity results are empty")
    missing = sorted(required - set(rows[0]))
    if missing:
        raise FlgrEloReplayError(f"identity results are missing fields: {missing}")

    flgr_mappings: dict[tuple[str, str], tuple[str, ...]] = {}
    canonical_claims: dict[tuple[str, str], str] = {}
    event_shapes: dict[str, tuple[str, ...]] = {}
    event_athletes: set[tuple[str, str]] = set()
    event_counts: dict[str, int] = defaultdict(int)
    for line, row in enumerate(rows, start=2):
        event_id = row["event_id"].strip()
        sex = row["sex"].strip()
        athlete_id = row["athlete_id"].strip()
        source_id = row["source_competitor_id"].strip()
        status = row["identity_status"].strip()
        canonical_id = row["canonical_athlete_id"].strip()
        canonical_name = row["canonical_name"].strip()
        if not event_id.startswith("flgr:"):
            raise FlgrEloReplayError(f"line {line}: invalid FLGR event identity")
        if sex not in {"M", "L"}:
            raise FlgrEloReplayError(f"line {line}: invalid sex {sex!r}")
        if not source_id.isdigit() or athlete_id != f"flgr:{source_id}":
            raise FlgrEloReplayError(f"line {line}: invalid FLGR athlete identity")
        if not row["source_name"].strip():
            raise FlgrEloReplayError(f"line {line}: missing source athlete name")
        if status not in IDENTITY_STATUSES:
            raise FlgrEloReplayError(f"line {line}: invalid identity status {status!r}")
        if status == "reviewed_crosswalk" and (not canonical_id or not canonical_name):
            raise FlgrEloReplayError(f"line {line}: incomplete reviewed identity")
        if status == "flgr_only" and (canonical_id or canonical_name):
            raise FlgrEloReplayError(
                f"line {line}: FLGR-only identity has canonical data"
            )
        mapping_key = (sex, athlete_id)
        identity_shape = (
            row["source_name"].strip(),
            status,
            canonical_id,
            canonical_name,
        )
        previous_mapping = flgr_mappings.setdefault(mapping_key, identity_shape)
        if previous_mapping != identity_shape:
            raise FlgrEloReplayError(f"conflicting mapping for {sex}/{athlete_id}")
        if canonical_id:
            canonical_key = (sex, canonical_id)
            previous_claim = canonical_claims.setdefault(canonical_key, athlete_id)
            if previous_claim != athlete_id:
                raise FlgrEloReplayError(
                    f"canonical athlete {sex}/{canonical_id} is claimed more than once"
                )

        try:
            raced = date.fromisoformat(row["date"])
            season = int(row["season"])
            place = int(row["place"])
        except ValueError as error:
            raise FlgrEloReplayError(
                f"line {line}: invalid date, season, or place"
            ) from error
        if season != _season_for(raced.isoformat()):
            raise FlgrEloReplayError(
                f"line {line}: date {raced} does not belong to season {season}"
            )
        if place < 1:
            raise FlgrEloReplayError(f"line {line}: invalid place {place}")
        if row["distance"] in TEAM_DISTANCES:
            raise FlgrEloReplayError("team events cannot enter the individual bridge")
        if row["technique"] not in {"C", "F", "P", "N/A"}:
            raise FlgrEloReplayError(
                f"line {line}: unsupported technique {row['technique']!r}"
            )
        _boolean(row["mass_start"])
        checksum = row["source_sha256"].strip().lower()
        if len(checksum) != 64 or any(
            character not in "0123456789abcdef" for character in checksum
        ):
            raise FlgrEloReplayError(f"line {line}: invalid source SHA-256")

        event_key = (event_id, athlete_id)
        if event_key in event_athletes:
            raise FlgrEloReplayError(f"duplicate athlete {athlete_id} in {event_id}")
        event_athletes.add(event_key)
        event_counts[event_id] += 1
        shape = (
            row["date"],
            row["season"],
            sex,
            row["distance"],
            row["technique"],
            str(_boolean(row["mass_start"])),
            checksum,
        )
        previous_shape = event_shapes.setdefault(event_id, shape)
        if previous_shape != shape:
            raise FlgrEloReplayError(f"inconsistent metadata for event {event_id}")

    undersized = sorted(event for event, count in event_counts.items() if count < 2)
    if undersized:
        raise FlgrEloReplayError(
            f"events need at least two classified athletes: {undersized}"
        )


def _load_history(path: Path) -> list[dict[str, str]]:
    fields, rows = _read_csv(path)
    required = {
        "Date",
        "Event",
        "Season",
        "Race",
        "ID",
        "Skier",
        "Exp",
        *STREAMS,
        *PREVIOUS_COLUMNS.values(),
    }
    missing = sorted(required - set(fields))
    if missing:
        raise FlgrEloReplayError(f"{path} is missing columns: {missing}")
    return rows


def _anchor_before(
    history: list[dict[str, str]], athlete_id: str, first_event_date: str
) -> dict[str, str] | None:
    eligible = [
        row
        for row in history
        if row["ID"].strip() == athlete_id and row["Date"].strip() < first_event_date
    ]
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda row: (
            row["Date"].strip(),
            _integer(row.get("Season", ""), default=-1),
            _integer(row.get("Race", ""), default=-1),
        ),
    )


def _initial_state(row: dict[str, str], history: list[dict[str, str]]) -> BridgeState:
    first_season = int(row["season"])
    canonical_id = row["canonical_athlete_id"].strip()
    if not canonical_id:
        return BridgeState(
            athlete_id=row["athlete_id"],
            source_name=row["source_name"],
            canonical_athlete_id="",
            canonical_name="",
            identity_status="flgr_only",
            anchor_source="newcomer_1300",
            experience=0,
            last_season=first_season,
            ratings={stream: BASE_ELO for stream in STREAMS},
        )

    anchor = _anchor_before(history, canonical_id, row["date"])
    if anchor is None:
        return BridgeState(
            athlete_id=row["athlete_id"],
            source_name=row["source_name"],
            canonical_athlete_id=canonical_id,
            canonical_name=row["canonical_name"],
            identity_status="reviewed_crosswalk",
            anchor_source="reviewed_newcomer_1300",
            experience=0,
            last_season=first_season,
            ratings={stream: BASE_ELO for stream in STREAMS},
        )
    anchor_season = int(anchor["Season"])
    boundaries = max(0, first_season - anchor_season)
    ratings = {}
    for stream, previous_column in PREVIOUS_COLUMNS.items():
        starting_column = previous_column if anchor["Event"] == "Offseason" else stream
        ratings[stream] = regress_rating(_rating(anchor, starting_column), boundaries)
    return BridgeState(
        athlete_id=row["athlete_id"],
        source_name=row["source_name"],
        canonical_athlete_id=canonical_id,
        canonical_name=row["canonical_name"],
        identity_status="reviewed_crosswalk",
        anchor_source="accepted_fis_history",
        experience=_integer(anchor.get("Exp", "")),
        last_season=first_season,
        ratings=ratings,
    )


def replay_flgr_elo(
    *,
    identity_results: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    output_dir: Path,
    k_factor: float = 1.0,
) -> dict[str, object]:
    """Replay all supplied FLGR events from scratch into an isolated ledger."""
    if not 0.0 < k_factor <= 10.0:
        raise FlgrEloReplayError("k_factor must be greater than 0 and at most 10")
    output_dir = _validate_output_dir(
        output_dir, (identity_results, men_chrono, ladies_chrono)
    )
    source_hashes = {
        "identity_results": _sha256(identity_results),
        "men_chrono": _sha256(men_chrono),
        "ladies_chrono": _sha256(ladies_chrono),
    }
    _, rows = _read_csv(identity_results)
    _validate_identity_rows(rows)
    histories = {"M": _load_history(men_chrono), "L": _load_history(ladies_chrono)}

    first_rows: dict[tuple[str, str], dict[str, str]] = {}
    for row in sorted(rows, key=lambda item: (item["date"], item["event_id"])):
        first_rows.setdefault((row["sex"], row["athlete_id"]), row)
    states = {
        key: _initial_state(row, histories[row["sex"]])
        for key, row in first_rows.items()
    }

    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["date"], row["event_id"])].append(row)

    ledger: list[dict[str, object]] = []
    for event_key in sorted(grouped):
        event_rows = sorted(
            grouped[event_key], key=lambda row: (int(row["place"]), row["athlete_id"])
        )
        season = int(event_rows[0]["season"])
        state_rows: list[BridgeState] = []
        for row in event_rows:
            state = states[(row["sex"], row["athlete_id"])]
            boundaries = season - state.last_season
            if boundaries < 0:
                raise FlgrEloReplayError("events are not in chronological season order")
            if boundaries:
                state.ratings = {
                    stream: regress_rating(value, boundaries)
                    for stream, value in state.ratings.items()
                }
                state.last_season = season
            state_rows.append(state)

        places = [int(row["place"]) for row in event_rows]
        distance = event_rows[0]["distance"]
        technique = event_rows[0]["technique"]
        previous = {
            stream: [state.ratings[stream] for state in state_rows]
            for stream in STREAMS
        }
        updated = {stream: list(values) for stream, values in previous.items()}
        for stream in STREAMS:
            if stream_includes(stream, distance, technique):
                updated[stream] = update_ratings(previous[stream], places, k_factor)

        for index, (row, state) in enumerate(zip(event_rows, state_rows)):
            state.experience += 1
            output: dict[str, object] = {
                "event_id": row["event_id"],
                "date": row["date"],
                "season": season,
                "sex": row["sex"],
                "distance": distance,
                "technique": technique,
                "mass_start": _boolean(row["mass_start"]),
                "place": int(row["place"]),
                "athlete_id": state.athlete_id,
                "source_name": state.source_name,
                "canonical_athlete_id": state.canonical_athlete_id,
                "canonical_name": state.canonical_name,
                "identity_status": state.identity_status,
                "anchor_source": state.anchor_source,
                "experience": state.experience,
                "source_sha256": row["source_sha256"],
            }
            for stream, previous_column in PREVIOUS_COLUMNS.items():
                output[previous_column] = previous[stream][index]
                output[stream] = updated[stream][index]
                state.ratings[stream] = updated[stream][index]
            ledger.append(output)

    after_hashes = {
        "identity_results": _sha256(identity_results),
        "men_chrono": _sha256(men_chrono),
        "ladies_chrono": _sha256(ladies_chrono),
    }
    if after_hashes != source_hashes:
        raise FlgrEloReplayError("source evidence changed during isolated replay")

    fields = list(ledger[0])
    _atomic_text(output_dir / LEDGER_FILENAME, _csv_text(ledger, fields))
    reviewed_crosswalk = sum(
        state.identity_status == "reviewed_crosswalk" for state in states.values()
    )
    reviewed_anchors = sum(
        state.anchor_source == "accepted_fis_history" for state in states.values()
    )
    reviewed_newcomers = sum(
        state.anchor_source == "reviewed_newcomer_1300" for state in states.values()
    )
    event_count = len(grouped)
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "source_sha256": source_hashes,
        "event_count": event_count,
        "result_rows": len(ledger),
        "athlete_count": len(states),
        "reviewed_crosswalk_athletes": reviewed_crosswalk,
        "reviewed_anchor_athletes": reviewed_anchors,
        "reviewed_newcomer_athletes": reviewed_newcomers,
        "flgr_only_athletes": len(states) - reviewed_crosswalk,
        "k_factor": k_factor,
        "calibration_status": "pending_backtest",
        "publication_eligible": False,
        "rollover": {"base_elo": BASE_ELO, "discount": ROLLOVER_DISCOUNT},
        "official_histories_modified": False,
        "status": "replayed_prediction_only",
    }
    _atomic_text(
        output_dir / REPORT_FILENAME,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity-results", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--k-factor", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = replay_flgr_elo(
        identity_results=args.identity_results,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        output_dir=args.output_dir,
        k_factor=args.k_factor,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
