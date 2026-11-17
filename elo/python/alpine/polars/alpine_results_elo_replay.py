#!/usr/bin/env python3
"""Replay mapped Alpine results from accepted chrono ratings.

The replay reproduces the legacy Nordic Numbers multi-competitor Elo formula,
including the delayed season rollover discount.  It writes only incremental
candidate rows and never modifies the accepted chrono histories.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


BASE_ELO = 1300.0
ROLLOVER_DISCOUNT = 0.85
ELO_DIFFERENCE = 400.0

STREAMS: dict[str, frozenset[str] | None] = {
    "Elo": None,
    "Downhill_Elo": frozenset({"Downhill"}),
    "Super G_Elo": frozenset({"Super G"}),
    "Giant Slalom_Elo": frozenset({"Giant Slalom"}),
    "Slalom_Elo": frozenset({"Slalom"}),
    "Combined_Elo": frozenset({"Combined"}),
    "Tech_Elo": frozenset({"Giant Slalom", "Slalom"}),
    "Speed_Elo": frozenset({"Downhill", "Super G"}),
}
PREVIOUS_COLUMNS = {
    "Elo": "Pelo",
    "Downhill_Elo": "Downhill_Pelo",
    "Super G_Elo": "Super G_Pelo",
    "Giant Slalom_Elo": "Giant Slalom_Pelo",
    "Slalom_Elo": "Slalom_Pelo",
    "Combined_Elo": "Combined_Pelo",
    "Tech_Elo": "Tech_Pelo",
    "Speed_Elo": "Speed_Pelo",
}


class EloReplayError(RuntimeError):
    """Raised when a replay cannot preserve the accepted Elo contract."""


@dataclass
class AthleteState:
    name: str
    nation: str
    birthday: str
    experience: int
    ratings: dict[str, float]


def actual_scores(places: list[int]) -> list[float]:
    return [
        sum(1.0 if other > place else 0.5 if other == place else 0.0 for other in places)
        - 0.5
        for place in places
    ]


def expected_scores(ratings: list[float]) -> list[float]:
    return [
        sum(
            1.0 / (1.0 + 10.0 ** ((other - rating) / ELO_DIFFERENCE))
            for other_index, other in enumerate(ratings)
            if other_index != index
        )
        for index, rating in enumerate(ratings)
    ]


def update_ratings(
    ratings: list[float], places: list[int], k_factor: float
) -> list[float]:
    actual = actual_scores(places)
    expected = expected_scores(ratings)
    updated = [
        rating + k_factor * (score - expectation)
        for rating, score, expectation in zip(ratings, actual, expected)
    ]
    if abs(sum(updated) - sum(ratings)) > 1e-8:
        raise EloReplayError("Elo update did not conserve the race rating total")
    return updated


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _rating_value(row: dict[str, str], column: str) -> float:
    raw = (row.get(column) or "").strip()
    return float(raw) if raw else BASE_ELO


def load_starting_states(
    chrono: Path, candidate_season: int
) -> tuple[list[str], dict[str, AthleteState], list[dict[str, str]]]:
    fields, rows = _read_rows(chrono)
    required = {
        "Date", "Event", "Season", "ID", "Skier", "Nation", "Birthday", "Exp",
        *STREAMS,
        *PREVIOUS_COLUMNS.values(),
    }
    missing = sorted(required - set(fields))
    if missing:
        raise EloReplayError(f"{chrono} is missing columns: {missing}")
    latest: dict[str, dict[str, str]] = {}
    latest_order: dict[str, tuple[str, int, int]] = {}
    for row in rows:
        athlete_id = row["ID"].strip()
        order = (row["Date"], int(row["Season"]), int(row["Race"]))
        if athlete_id and order >= latest_order.get(athlete_id, ("", -1, -1)):
            latest[athlete_id] = row
            latest_order[athlete_id] = order

    states: dict[str, AthleteState] = {}
    for athlete_id, row in latest.items():
        ratings: dict[str, float] = {}
        for elo_column, pelo_column in PREVIOUS_COLUMNS.items():
            if (
                row["Event"] == "Offseason"
                and int(row["Season"]) == candidate_season - 1
            ):
                previous = _rating_value(row, pelo_column)
                ratings[elo_column] = (
                    previous * ROLLOVER_DISCOUNT
                    + BASE_ELO * (1.0 - ROLLOVER_DISCOUNT)
                )
            else:
                ratings[elo_column] = _rating_value(row, elo_column)
        states[athlete_id] = AthleteState(
            name=row["Skier"].strip(),
            nation=row["Nation"].strip(),
            birthday=row["Birthday"].strip(),
            experience=int(float(row["Exp"] or 0)),
            ratings=ratings,
        )
    return fields, states, rows


def stream_includes(stream: str, distance: str) -> bool:
    disciplines = STREAMS[stream]
    return disciplines is None or distance in disciplines


def calculate_k_factors(
    history: list[dict[str, str]], candidates: list[dict[str, str]], season: int
) -> dict[str, float]:
    factors: dict[str, float] = {}
    for stream in STREAMS:
        historical_counts: Counter[int] = Counter()
        for row in history:
            if row.get("Event") == "Offseason" or not stream_includes(
                stream, row.get("Distance", "")
            ):
                continue
            historical_counts[int(row["Season"])] += 1
        current_entries = sum(
            1
            for row in candidates
            if int(row["season"]) == season
            and stream_includes(stream, row["distance"])
        )
        if current_entries == 0:
            continue
        max_entries = max([current_entries, *historical_counts.values()])
        factors[stream] = max(1.0, min(5.0, max_entries / current_entries))
    return factors


def _age_on(birthday: str, race_date: str) -> float:
    if not birthday:
        return 22.0
    born = datetime.fromisoformat(birthday.replace("Z", "+00:00")).date()
    raced = date.fromisoformat(race_date)
    return (raced - born).days / 365.25


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise EloReplayError(
                f"replay output {resolved} cannot be inside source directory {parent}"
            )
    return resolved


def _csv_text(rows: list[dict[str, object]], fields: list[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _validate_candidates(rows: list[dict[str, str]]) -> int:
    required = {
        "date", "sex", "season", "race", "city", "country", "distance", "place",
        "canonical_athlete_id", "canonical_name", "canonical_nation",
    }
    if not rows:
        raise EloReplayError("mapped results are empty")
    missing = sorted(required - set(rows[0]))
    if missing:
        raise EloReplayError(f"mapped results are missing fields: {missing}")
    seasons = {int(row["season"]) for row in rows}
    if len(seasons) != 1:
        raise EloReplayError(f"replay requires exactly one season, found {sorted(seasons)}")
    seen: set[tuple[str, int, int, str]] = set()
    race_shapes: dict[tuple[str, int, int], tuple[str, str, str, str]] = {}
    for row in rows:
        key = (
            row["sex"], int(row["season"]), int(row["race"]),
            row["canonical_athlete_id"],
        )
        if key in seen:
            raise EloReplayError(f"duplicate athlete within race: {key}")
        seen.add(key)
        if int(row["place"]) < 1:
            raise EloReplayError(f"invalid place for {key}: {row['place']}")
        race_key = (row["sex"], int(row["season"]), int(row["race"]))
        shape = (row["date"], row["city"], row["country"], row["distance"])
        previous_shape = race_shapes.setdefault(race_key, shape)
        if previous_shape != shape:
            raise EloReplayError(
                f"inconsistent race metadata for {race_key}: "
                f"{previous_shape} != {shape}"
            )
    return next(iter(seasons))


def replay_sex(
    *,
    sex: str,
    candidates: list[dict[str, str]],
    chrono: Path,
    season: int,
) -> tuple[list[str], list[dict[str, object]], dict[str, float]]:
    fields, states, history = load_starting_states(chrono, season)
    sex_candidates = [row for row in candidates if row["sex"] == sex]
    k_factors = calculate_k_factors(history, sex_candidates, season)
    existing_races = {
        (int(row["Season"]), int(row["Race"]), row["Date"])
        for row in history
        if row["Event"] != "Offseason"
    }
    grouped: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    for row in sex_candidates:
        race_key = (int(row["season"]), int(row["race"]), row["date"])
        if race_key in existing_races:
            raise EloReplayError(f"candidate race already exists in {chrono}: {race_key}")
        grouped[(row["date"], int(row["race"]))].append(row)

    output: list[dict[str, object]] = []
    for race_key in sorted(grouped):
        race_rows = sorted(grouped[race_key], key=lambda row: int(row["place"]))
        athlete_ids = [row["canonical_athlete_id"] for row in race_rows]
        for row, athlete_id in zip(race_rows, athlete_ids):
            if athlete_id not in states:
                raise EloReplayError(
                    f"canonical athlete {sex}/{athlete_id} has no accepted rating history"
                )
            state = states[athlete_id]
            if state.name != row["canonical_name"]:
                raise EloReplayError(
                    f"canonical name changed for {sex}/{athlete_id}: "
                    f"{state.name!r} != {row['canonical_name']!r}"
                )

        distance = race_rows[0]["distance"]
        places = [int(row["place"]) for row in race_rows]
        previous_by_stream = {
            stream: [states[athlete_id].ratings[stream] for athlete_id in athlete_ids]
            for stream in STREAMS
        }
        updated_by_stream = {
            stream: list(values) for stream, values in previous_by_stream.items()
        }
        for stream, previous in previous_by_stream.items():
            if not stream_includes(stream, distance):
                continue
            k_factor = k_factors[stream] * (0.8 if distance == "Combined" else 1.0)
            updated_by_stream[stream] = update_ratings(previous, places, k_factor)

        for index, (row, athlete_id) in enumerate(zip(race_rows, athlete_ids)):
            state = states[athlete_id]
            state.experience += 1
            result: dict[str, object] = {
                "Date": row["date"],
                "City": row["city"],
                "Country": row["country"],
                "Sex": sex,
                "Distance": distance,
                "Event": "World Cup",
                "MS": 0,
                "Technique": "N/A",
                "Place": int(row["place"]),
                "Skier": state.name,
                "Nation": state.nation,
                "ID": int(athlete_id),
                "Season": season,
                "Race": int(row["race"]),
                "Birthday": state.birthday,
                "Age": _age_on(state.birthday, row["date"]),
                "Exp": state.experience,
            }
            for stream, previous_column in PREVIOUS_COLUMNS.items():
                result[previous_column] = previous_by_stream[stream][index]
                result[stream] = updated_by_stream[stream][index]
                state.ratings[stream] = updated_by_stream[stream][index]
            output.append(result)
    return fields, output, k_factors


def replay_results(
    *,
    mapped_results: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    output_dir: Path,
) -> dict[str, object]:
    output_dir = _validate_output_dir(
        output_dir, (mapped_results, men_chrono, ladies_chrono)
    )
    _, candidates = _read_rows(mapped_results)
    season = _validate_candidates(candidates)
    result_counts: dict[str, int] = {}
    k_report: dict[str, dict[str, float]] = {}
    for sex, chrono, filename in (
        ("M", men_chrono, "men_elo_rows.csv"),
        ("L", ladies_chrono, "ladies_elo_rows.csv"),
    ):
        fields, rows, factors = replay_sex(
            sex=sex, candidates=candidates, chrono=chrono, season=season
        )
        _atomic_text(output_dir / filename, _csv_text(rows, fields))
        result_counts[sex] = len(rows)
        k_report[sex] = dict(sorted(factors.items()))

    race_count = len(
        {(row["date"], row["sex"], row["race"]) for row in candidates}
    )
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.alpine-elo-replay/v1",
        "season": season,
        "input_sha256": hashlib.sha256(mapped_results.read_bytes()).hexdigest(),
        "race_count": race_count,
        "result_rows": result_counts,
        "k_factors": k_report,
        "rollover": {"base_elo": BASE_ELO, "discount": ROLLOVER_DISCOUNT},
        "status": "replayed",
        "source_histories_modified": False,
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapped-results", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = replay_results(
        mapped_results=args.mapped_results,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
