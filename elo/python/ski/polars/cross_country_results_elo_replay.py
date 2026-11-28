#!/usr/bin/env python3
"""Replay mapped cross-country results from accepted chrono ratings.

The replay implements the existing nine-stream Nordic Numbers Elo contract and
the delayed offseason discount. It writes incremental candidate rows only and
verifies that accepted chrono histories remain byte-for-byte unchanged.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

BASE_ELO = 1300.0
ROLLOVER_DISCOUNT = 0.85
ELO_DIFFERENCE = 400.0
TEAM_DISTANCES = {"Rel", "Ts"}

STREAMS = (
    "Elo",
    "Distance_Elo",
    "Distance_C_Elo",
    "Distance_F_Elo",
    "Sprint_Elo",
    "Sprint_C_Elo",
    "Sprint_F_Elo",
    "Classic_Elo",
    "Freestyle_Elo",
)
PREVIOUS_COLUMNS = {
    "Elo": "Pelo",
    "Distance_Elo": "Distance_Pelo",
    "Distance_C_Elo": "Distance_C_Pelo",
    "Distance_F_Elo": "Distance_F_Pelo",
    "Sprint_Elo": "Sprint_Pelo",
    "Sprint_C_Elo": "Sprint_C_Pelo",
    "Sprint_F_Elo": "Sprint_F_Pelo",
    "Classic_Elo": "Classic_Pelo",
    "Freestyle_Elo": "Freestyle_Pelo",
}


class EloReplayError(RuntimeError):
    """Raised when replay cannot preserve the accepted Elo contract."""


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


def stream_includes(stream: str, distance: str, technique: str) -> bool:
    is_sprint = distance == "Sprint"
    is_distance = not is_sprint and distance not in TEAM_DISTANCES
    is_classic = technique in {"C", "N/A"}
    is_freestyle = technique == "F"
    rules = {
        "Elo": True,
        "Distance_Elo": is_distance,
        "Distance_C_Elo": is_distance and is_classic,
        "Distance_F_Elo": is_distance and is_freestyle,
        "Sprint_Elo": is_sprint,
        "Sprint_C_Elo": is_sprint and is_classic,
        "Sprint_F_Elo": is_sprint and is_freestyle,
        "Classic_Elo": is_classic,
        "Freestyle_Elo": is_freestyle,
    }
    return rules[stream]


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _rating_value(row: dict[str, str], column: str) -> float:
    raw = (row.get(column) or "").strip()
    return float(raw) if raw else BASE_ELO


def regress_rating(rating: float, season_boundaries: int) -> float:
    """Apply one offseason regression for every missed season boundary."""
    for _ in range(season_boundaries):
        rating = rating * ROLLOVER_DISCOUNT + BASE_ELO * (1.0 - ROLLOVER_DISCOUNT)
    return rating


def load_starting_states(
    chrono: Path, candidate_season: int
) -> tuple[list[str], dict[str, AthleteState], list[dict[str, str]]]:
    fields, rows = _read_rows(chrono)
    required = {
        "Date",
        "Event",
        "Season",
        "Race",
        "ID",
        "Skier",
        "Nation",
        "Birthday",
        "Exp",
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
        row_season = int(row["Season"])
        season_boundaries = max(0, candidate_season - row_season)
        for elo_column, pelo_column in PREVIOUS_COLUMNS.items():
            starting_column = pelo_column if row["Event"] == "Offseason" else elo_column
            previous = _rating_value(row, starting_column)
            ratings[elo_column] = regress_rating(previous, season_boundaries)
        states[athlete_id] = AthleteState(
            name=row["Skier"].strip(),
            nation=row["Nation"].strip(),
            birthday=row["Birthday"].strip(),
            experience=int(float(row["Exp"] or 0)),
            ratings=ratings,
        )
    return fields, states, rows


def add_prediction_starting_states(
    *,
    states: dict[str, AthleteState],
    candidates: list[dict[str, str]],
    sex: str,
    season: int,
    prediction_ledger: Path | None,
) -> dict[str, str]:
    """Seed first A-tier starts from lower-tier Elo, or the newcomer prior."""
    ledger_rows: list[dict[str, str]] = []
    if prediction_ledger is not None:
        fields, ledger_rows = _read_rows(prediction_ledger)
        required = {
            "date",
            "season",
            "sex",
            "prediction_athlete_id",
            "fis_competitor_id",
            "fis_code",
            *STREAMS,
        }
        if missing := sorted(required - set(fields)):
            raise EloReplayError(
                f"{prediction_ledger} is missing prediction fields: {missing}"
            )

    ledger_index: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for line, row in enumerate(ledger_rows, start=2):
        competitor_id = row["fis_competitor_id"].strip()
        if row["prediction_athlete_id"].strip() != f"fis:{competitor_id}":
            raise EloReplayError(
                f"{prediction_ledger}:{line}: unstable prediction identity"
            )
        try:
            date.fromisoformat(row["date"].strip())
            int(row["season"])
            for stream in STREAMS:
                float(row[stream])
        except ValueError as exc:
            raise EloReplayError(
                f"{prediction_ledger}:{line}: invalid prediction state"
            ) from exc
        ledger_index[(row["sex"].strip(), competitor_id)].append(row)
    for rows in ledger_index.values():
        rows.sort(key=lambda row: (row["date"], int(row.get("fis_race_id") or 0)))

    first_candidates: dict[str, dict[str, str]] = {}
    for row in sorted(candidates, key=lambda item: (item["date"], int(item["race"]))):
        if row["sex"] == sex:
            first_candidates.setdefault(row["canonical_athlete_id"], row)

    sources: dict[str, str] = {}
    for athlete_id, row in first_candidates.items():
        if athlete_id in states:
            continue
        fis_code = row.get("fis_code", "").strip()
        competitor_id = row.get("fis_competitor_id", "").strip()
        if not fis_code.isdigit() or athlete_id != fis_code or not competitor_id:
            raise EloReplayError(
                f"canonical athlete {sex}/{athlete_id} has no accepted rating history"
            )
        eligible = [
            item
            for item in ledger_index.get((sex, competitor_id), [])
            if item["date"] < row["date"] and item["fis_code"].strip() == fis_code
        ]
        if eligible:
            selected = eligible[-1]
            boundaries = max(0, season - int(selected["season"]))
            ratings = {
                stream: regress_rating(float(selected[stream]), boundaries)
                for stream in STREAMS
            }
            source = "fixed_anchor_prediction_elo"
        else:
            ratings = {stream: BASE_ELO for stream in STREAMS}
            source = "newcomer_prior"
        states[athlete_id] = AthleteState(
            name=row["canonical_name"],
            nation=row["canonical_nation"],
            birthday="",
            experience=0,
            ratings=ratings,
        )
        sources[athlete_id] = source
    return sources


def calculate_k_factors(
    history: list[dict[str, str]], candidates: list[dict[str, str]], season: int
) -> dict[str, float]:
    factors: dict[str, float] = {}
    for stream in STREAMS:
        counts: Counter[int] = Counter()
        for row in history:
            if row.get("Event") == "Offseason" or not stream_includes(
                stream, row.get("Distance", ""), row.get("Technique", "")
            ):
                continue
            counts[int(row["Season"])] += 1
        candidate_entries = sum(
            1
            for row in candidates
            if int(row["season"]) == season
            and stream_includes(stream, row["distance"], row["technique"])
        )
        if candidate_entries == 0:
            continue
        counts[season] += candidate_entries
        current_entries = counts[season]
        factors[stream] = max(1.0, min(5.0, max(counts.values()) / current_entries))
    return factors


def _age_on(birthday: str, race_date: str) -> float:
    if not birthday:
        return 22.0
    normalized_birthday = (
        f"{birthday[:-1]}+00:00" if birthday.endswith("Z") else birthday
    )
    born = datetime.fromisoformat(normalized_birthday).date()
    raced = date.fromisoformat(race_date)
    return (raced - born).days / 365.25


def _event_for(row: dict[str, str]) -> str:
    if int(row["championship"]) != 1:
        return "World Cup"
    return (
        "Olympic Winter Games"
        if int(row["season"]) % 4 == 2
        else "World Championship"
    )


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
        "date",
        "sex",
        "season",
        "race",
        "city",
        "country",
        "distance",
        "technique",
        "mass_start",
        "championship",
        "place",
        "canonical_athlete_id",
        "canonical_name",
        "canonical_nation",
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
    race_shapes: dict[tuple[str, int, int], tuple[str, ...]] = {}
    for row in rows:
        if row["sex"] not in {"M", "L"}:
            raise EloReplayError(f"unsupported sex: {row['sex']!r}")
        if row["distance"] in TEAM_DISTANCES:
            raise EloReplayError(f"team event cannot enter individual replay: {row['distance']}")
        if row["technique"] not in {"C", "F", "P", "N/A"}:
            raise EloReplayError(f"unsupported technique: {row['technique']!r}")
        if row["mass_start"] not in {"0", "1"}:
            raise EloReplayError(f"invalid mass_start: {row['mass_start']!r}")
        if row["championship"] not in {"0", "1"}:
            raise EloReplayError(f"invalid championship: {row['championship']!r}")
        race_date = date.fromisoformat(row["date"])
        expected_season = race_date.year + 1 if race_date.month >= 7 else race_date.year
        if int(row["season"]) != expected_season:
            raise EloReplayError(
                f"date {row['date']} belongs to season {expected_season}, "
                f"not {row['season']}"
            )
        key = (
            row["sex"],
            int(row["season"]),
            int(row["race"]),
            row["canonical_athlete_id"],
        )
        if key in seen:
            raise EloReplayError(f"duplicate athlete within race: {key}")
        seen.add(key)
        if int(row["place"]) < 1:
            raise EloReplayError(f"invalid place for {key}: {row['place']}")
        race_key = (row["sex"], int(row["season"]), int(row["race"]))
        shape = (
            row["date"],
            row["city"],
            row["country"],
            row["distance"],
            row["technique"],
            row["mass_start"],
            row["championship"],
        )
        previous_shape = race_shapes.setdefault(race_key, shape)
        if previous_shape != shape:
            raise EloReplayError(
                f"inconsistent race metadata for {race_key}: {previous_shape} != {shape}"
            )
    return next(iter(seasons))


def replay_sex(
    *,
    sex: str,
    candidates: list[dict[str, str]],
    chrono: Path,
    season: int,
    reference_history: Path | None = None,
    prediction_ledger: Path | None = None,
) -> tuple[list[str], list[dict[str, object]], dict[str, float], dict[str, str]]:
    fields, states, history = load_starting_states(chrono, season)
    if reference_history is not None:
        _, reference_states, _ = load_starting_states(reference_history, season)
        for athlete_id, state in reference_states.items():
            states.setdefault(athlete_id, state)
    prediction_sources = add_prediction_starting_states(
        states=states,
        candidates=candidates,
        sex=sex,
        season=season,
        prediction_ledger=prediction_ledger,
    )
    sex_candidates = [row for row in candidates if row["sex"] == sex]
    k_factors = calculate_k_factors(history, sex_candidates, season)
    existing_races = {
        (int(row["Season"]), int(row["Race"]))
        for row in history
        if row["Event"] != "Offseason"
    }
    grouped: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    for row in sex_candidates:
        race_key = (int(row["season"]), int(row["race"]))
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
        technique = race_rows[0]["technique"]
        places = [int(row["place"]) for row in race_rows]
        previous_by_stream = {
            stream: [states[athlete_id].ratings[stream] for athlete_id in athlete_ids]
            for stream in STREAMS
        }
        updated_by_stream = {
            stream: list(values) for stream, values in previous_by_stream.items()
        }
        for stream, previous in previous_by_stream.items():
            if not stream_includes(stream, distance, technique):
                continue
            updated_by_stream[stream] = update_ratings(
                previous, places, k_factors[stream]
            )

        for index, (row, athlete_id) in enumerate(zip(race_rows, athlete_ids)):
            state = states[athlete_id]
            state.experience += 1
            result: dict[str, object] = {
                "Date": row["date"],
                "City": row["city"],
                "Country": row["country"],
                "Sex": sex,
                "Distance": distance,
                "Event": _event_for(row),
                "MS": int(row["mass_start"]),
                "Technique": technique,
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
    return fields, output, k_factors, prediction_sources


def replay_results(
    *,
    mapped_results: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    output_dir: Path,
    men_reference_history: Path | None = None,
    ladies_reference_history: Path | None = None,
    prediction_ledger: Path | None = None,
) -> dict[str, object]:
    references = tuple(
        path
        for path in (men_reference_history, ladies_reference_history, prediction_ledger)
        if path is not None
    )
    output_dir = _validate_output_dir(
        output_dir, (mapped_results, men_chrono, ladies_chrono, *references)
    )
    source_hashes = {
        "men_chrono": hashlib.sha256(men_chrono.read_bytes()).hexdigest(),
        "ladies_chrono": hashlib.sha256(ladies_chrono.read_bytes()).hexdigest(),
        **(
            {"men_reference_history": hashlib.sha256(men_reference_history.read_bytes()).hexdigest()}
            if men_reference_history is not None else {}
        ),
        **(
            {"ladies_reference_history": hashlib.sha256(ladies_reference_history.read_bytes()).hexdigest()}
            if ladies_reference_history is not None else {}
        ),
        **(
            {"prediction_ledger": hashlib.sha256(prediction_ledger.read_bytes()).hexdigest()}
            if prediction_ledger is not None else {}
        ),
    }
    _, candidates = _read_rows(mapped_results)
    season = _validate_candidates(candidates)
    result_counts: dict[str, int] = {}
    k_report: dict[str, dict[str, float]] = {}
    prediction_seed_sources: Counter[str] = Counter()
    for sex, chrono, reference, filename in (
        ("M", men_chrono, men_reference_history, "men_elo_rows.csv"),
        ("L", ladies_chrono, ladies_reference_history, "ladies_elo_rows.csv"),
    ):
        fields, rows, factors, seeds = replay_sex(
            sex=sex,
            candidates=candidates,
            chrono=chrono,
            season=season,
            reference_history=reference,
            prediction_ledger=prediction_ledger,
        )
        _atomic_text(output_dir / filename, _csv_text(rows, fields))
        result_counts[sex] = len(rows)
        k_report[sex] = dict(sorted(factors.items()))
        prediction_seed_sources.update(seeds.values())

    after_hashes = {
        "men_chrono": hashlib.sha256(men_chrono.read_bytes()).hexdigest(),
        "ladies_chrono": hashlib.sha256(ladies_chrono.read_bytes()).hexdigest(),
        **(
            {"men_reference_history": hashlib.sha256(men_reference_history.read_bytes()).hexdigest()}
            if men_reference_history is not None else {}
        ),
        **(
            {"ladies_reference_history": hashlib.sha256(ladies_reference_history.read_bytes()).hexdigest()}
            if ladies_reference_history is not None else {}
        ),
        **(
            {"prediction_ledger": hashlib.sha256(prediction_ledger.read_bytes()).hexdigest()}
            if prediction_ledger is not None else {}
        ),
    }
    if after_hashes != source_hashes:
        raise EloReplayError("accepted chrono history changed during isolated replay")
    race_count = len(
        {(row["date"], row["sex"], row["race"]) for row in candidates}
    )
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.cross-country-elo-replay/v1",
        "season": season,
        "input_sha256": hashlib.sha256(mapped_results.read_bytes()).hexdigest(),
        "source_sha256": source_hashes,
        "race_count": race_count,
        "result_rows": result_counts,
        "k_factors": k_report,
        "first_a_tier_seed_sources": dict(sorted(prediction_seed_sources.items())),
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
    parser.add_argument("--men-reference-history", type=Path)
    parser.add_argument("--ladies-reference-history", type=Path)
    parser.add_argument("--prediction-ledger", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = replay_results(
        mapped_results=args.mapped_results,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        output_dir=args.output_dir,
        men_reference_history=args.men_reference_history,
        ladies_reference_history=args.ladies_reference_history,
        prediction_ledger=args.prediction_ledger,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
