#!/usr/bin/env python3
"""Replay mapped relay and team-sprint results into isolated Elo candidates.

The legacy Nordic Numbers contract rates every team member at the team's place
and scales K by the team size (one half for team sprint, one quarter for
relay).  This module preserves that behavior, emits athlete-level evidence and
the aggregate rows consumed by relay predictions, and never mutates accepted
chrono histories.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path

from cross_country_results_elo_replay import (
    PREVIOUS_COLUMNS,
    STREAMS,
    _age_on,
    _atomic_text,
    _csv_text,
    _event_for,
    _read_rows,
    _validate_output_dir,
    load_starting_states,
    update_ratings,
)
from cross_country_team_results_identity import (
    EXPECTED_MEMBERS,
    TEAM_KEY_FIELDS,
    TeamIdentityError,
    validate_team_memberships,
)


class TeamEloReplayError(TeamIdentityError):
    """Raised when team Elo cannot be replayed without changing its contract."""


TEAM_OUTPUT_FIELDS = (
    "Nation",
    "Season",
    "Race",
    "Place",
    "Date",
    "City",
    "Country",
    "Event",
    "TeamSize",
    "Distance",
    "MS",
    "Technique",
    "Sex",
    *(f"Team_{stream}" for stream in STREAMS),
    *(f"Team_{column}" for column in PREVIOUS_COLUMNS.values()),
    *(f"Avg_{stream}" for stream in STREAMS),
    *(f"Avg_{column}" for column in PREVIOUS_COLUMNS.values()),
    "TeamMembers",
    "MemberIDs",
    "AvgAge",
    "MaxAge",
    "MinAge",
    "AvgExp",
    "MaxExp",
    "MinExp",
)


def team_stream_includes(stream: str, distance: str, technique: str) -> bool:
    """Return the streams selected by the legacy relay filter pipeline."""
    if distance == "Rel":
        return stream in {"Elo", "Distance_Elo", "Distance_C_Elo", "Classic_Elo"}
    if distance != "Ts":
        raise TeamEloReplayError(f"unsupported team distance: {distance!r}")
    if stream in {"Elo", "Sprint_Elo"}:
        return True
    if technique in {"C", "N/A"}:
        return stream in {"Sprint_C_Elo", "Classic_Elo"}
    if technique == "F":
        return stream in {"Sprint_F_Elo", "Freestyle_Elo"}
    return False


def calculate_team_k_factors(
    history: list[dict[str, str]], candidates: list[dict[str, str]], season: int
) -> dict[str, float]:
    """Reproduce legacy max-season-entry/current-season-entry K factors."""
    factors: dict[str, float] = {}
    for stream in STREAMS:
        counts: Counter[int] = Counter()
        for row in history:
            if row.get("Event") == "Offseason":
                continue
            distance = row.get("Distance", "")
            if distance not in EXPECTED_MEMBERS:
                continue
            if team_stream_includes(stream, distance, row.get("Technique", "")):
                counts[int(row["Season"])] += 1
        candidate_entries = sum(
            1
            for row in candidates
            if int(row["season"]) == season
            and team_stream_includes(stream, row["distance"], row["technique"])
        )
        if not candidate_entries:
            continue
        counts[season] += candidate_entries
        factors[stream] = max(1.0, min(5.0, max(counts.values()) / counts[season]))
    return factors


def _validate_candidates(rows: list[dict[str, str]], mapped_results: Path) -> int:
    validate_team_memberships(mapped_results, mapped=True)
    required = {
        "city",
        "country",
        "technique",
        "championship",
        "canonical_nation",
    }
    missing = sorted(required - set(rows[0]))
    if missing:
        raise TeamEloReplayError(f"mapped team results are missing fields: {missing}")
    seasons = {int(row["season"]) for row in rows}
    if len(seasons) != 1:
        raise TeamEloReplayError(
            f"replay requires exactly one season, found {sorted(seasons)}"
        )
    for row in rows:
        if row["sex"] not in {"M", "L"}:
            raise TeamEloReplayError(f"unsupported sex: {row['sex']!r}")
        if row["technique"] not in {"C", "F", "P", "N/A"}:
            raise TeamEloReplayError(f"unsupported technique: {row['technique']!r}")
        if row["championship"] not in {"0", "1"}:
            raise TeamEloReplayError(
                f"invalid championship value: {row['championship']!r}"
            )
    return next(iter(seasons))


def _mean(values: list[float | int]) -> float:
    return float(sum(values)) / len(values)


def _aggregate_team(
    race_rows: list[dict[str, str]], athlete_rows: list[dict[str, object]]
) -> dict[str, object]:
    first = race_rows[0]
    nations = {str(row["Nation"]) for row in athlete_rows}
    if len(nations) != 1:
        raise TeamEloReplayError(
            f"team {first['team_name']!r} maps to multiple nations: {sorted(nations)}"
        )
    output: dict[str, object] = {
        "Nation": next(iter(nations)),
        "Season": int(first["season"]),
        "Race": int(first["race"]),
        "Place": int(first["place"]),
        "Date": first["date"],
        "City": first["city"],
        "Country": first["country"],
        "Event": athlete_rows[0]["Event"],
        "TeamSize": len(athlete_rows),
        "Distance": first["distance"],
        "MS": 0,
        "Technique": first["technique"],
        "Sex": first["sex"],
        "TeamMembers": ",".join(str(row["Skier"]) for row in athlete_rows),
        "MemberIDs": ",".join(str(row["ID"]) for row in athlete_rows),
    }
    for stream, previous in PREVIOUS_COLUMNS.items():
        current_values = [float(row[stream]) for row in athlete_rows]
        previous_values = [float(row[previous]) for row in athlete_rows]
        output[f"Team_{stream}"] = sum(current_values)
        output[f"Team_{previous}"] = sum(previous_values)
        output[f"Avg_{stream}"] = _mean(current_values)
        output[f"Avg_{previous}"] = _mean(previous_values)
    ages = [float(row["Age"]) for row in athlete_rows]
    experiences = [int(row["Exp"]) for row in athlete_rows]
    output.update(
        {
            "AvgAge": _mean(ages),
            "MaxAge": max(ages),
            "MinAge": min(ages),
            "AvgExp": _mean(experiences),
            "MaxExp": max(experiences),
            "MinExp": min(experiences),
        }
    )
    return output


def replay_sex(
    *, sex: str, candidates: list[dict[str, str]], chrono: Path, season: int
) -> tuple[list[str], list[dict[str, object]], list[dict[str, object]], dict[str, float]]:
    fields, states, history = load_starting_states(chrono, season)
    sex_candidates = [row for row in candidates if row["sex"] == sex]
    k_factors = calculate_team_k_factors(history, sex_candidates, season)
    existing = {
        (int(row["Season"]), int(row["Race"]))
        for row in history
        if row.get("Event") != "Offseason" and row.get("Distance") in EXPECTED_MEMBERS
    }
    grouped: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    for row in sex_candidates:
        key = (int(row["season"]), int(row["race"]))
        if key in existing:
            raise TeamEloReplayError(f"candidate team race already exists in {chrono}: {key}")
        grouped[(row["date"], int(row["race"]))].append(row)

    athlete_output: list[dict[str, object]] = []
    team_output: list[dict[str, object]] = []
    for race_key in sorted(grouped):
        race_rows = sorted(
            grouped[race_key],
            key=lambda row: (int(row["place"]), int(row["team_bib"]), int(row["member_order"])),
        )
        athlete_ids = [row["canonical_athlete_id"] for row in race_rows]
        for row, athlete_id in zip(race_rows, athlete_ids):
            state = states.get(athlete_id)
            if state is None:
                raise TeamEloReplayError(
                    f"canonical athlete {sex}/{athlete_id} has no accepted rating history"
                )
            if state.name != row["canonical_name"]:
                raise TeamEloReplayError(
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
        divisor = EXPECTED_MEMBERS[distance]
        for stream, previous in previous_by_stream.items():
            if team_stream_includes(stream, distance, technique):
                updated_by_stream[stream] = update_ratings(
                    previous, places, k_factors[stream] / divisor
                )

        current_race_athletes: list[dict[str, object]] = []
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
                "MS": 0,
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
            athlete_output.append(result)
            current_race_athletes.append(result)

        teams: dict[tuple[str, ...], list[tuple[dict[str, str], dict[str, object]]]] = defaultdict(list)
        for source, result in zip(race_rows, current_race_athletes):
            key = tuple(source[field] for field in TEAM_KEY_FIELDS)
            teams[key].append((source, result))
        for members in teams.values():
            ordered = sorted(members, key=lambda pair: int(pair[0]["member_order"]))
            team_output.append(
                _aggregate_team(
                    [source for source, _ in ordered],
                    [result for _, result in ordered],
                )
            )
    return fields, athlete_output, team_output, k_factors


def replay_team_results(
    *, mapped_results: Path, men_chrono: Path, ladies_chrono: Path, output_dir: Path
) -> dict[str, object]:
    output_dir = _validate_output_dir(
        output_dir, (mapped_results, men_chrono, ladies_chrono)
    )
    source_hashes = {
        "men_chrono": hashlib.sha256(men_chrono.read_bytes()).hexdigest(),
        "ladies_chrono": hashlib.sha256(ladies_chrono.read_bytes()).hexdigest(),
    }
    _, candidates = _read_rows(mapped_results)
    season = _validate_candidates(candidates, mapped_results)
    athlete_counts: dict[str, int] = {}
    k_report: dict[str, dict[str, float]] = {}
    team_rows: list[dict[str, object]] = []
    for sex, chrono, filename in (
        ("M", men_chrono, "men_athlete_elo_rows.csv"),
        ("L", ladies_chrono, "ladies_athlete_elo_rows.csv"),
    ):
        fields, athlete_rows, sex_team_rows, factors = replay_sex(
            sex=sex, candidates=candidates, chrono=chrono, season=season
        )
        _atomic_text(output_dir / filename, _csv_text(athlete_rows, fields))
        athlete_counts[sex] = len(athlete_rows)
        k_report[sex] = dict(sorted(factors.items()))
        team_rows.extend(sex_team_rows)

    outputs = {
        "men_relay_elo_rows.csv": [
            row for row in team_rows if row["Sex"] == "M" and row["Distance"] == "Rel"
        ],
        "ladies_relay_elo_rows.csv": [
            row for row in team_rows if row["Sex"] == "L" and row["Distance"] == "Rel"
        ],
        "team_sprint_elo_rows.csv": [row for row in team_rows if row["Distance"] == "Ts"],
    }
    for filename, rows in outputs.items():
        rows.sort(key=lambda row: (row["Date"], row["Race"], row["Sex"], row["Place"]))
        _atomic_text(output_dir / filename, _csv_text(rows, list(TEAM_OUTPUT_FIELDS)))

    output_files = (
        "men_athlete_elo_rows.csv",
        "ladies_athlete_elo_rows.csv",
        *outputs,
    )
    output_receipts = {
        filename: {
            "sha256": hashlib.sha256((output_dir / filename).read_bytes()).hexdigest(),
            "rows": (
                athlete_counts["M"]
                if filename == "men_athlete_elo_rows.csv"
                else athlete_counts["L"]
                if filename == "ladies_athlete_elo_rows.csv"
                else len(outputs[filename])
            ),
        }
        for filename in output_files
    }

    after_hashes = {
        "men_chrono": hashlib.sha256(men_chrono.read_bytes()).hexdigest(),
        "ladies_chrono": hashlib.sha256(ladies_chrono.read_bytes()).hexdigest(),
    }
    if after_hashes != source_hashes:
        raise TeamEloReplayError("accepted chrono history changed during isolated replay")
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.cross-country-team-elo-replay/v1",
        "season": season,
        "input_sha256": hashlib.sha256(mapped_results.read_bytes()).hexdigest(),
        "source_sha256": source_hashes,
        "event_count": validate_team_memberships(mapped_results, mapped=True).event_count,
        "athlete_rows": athlete_counts,
        "team_rows": {filename: len(rows) for filename, rows in outputs.items()},
        "outputs": output_receipts,
        "k_factors": k_report,
        "team_k_divisors": {"Rel": 4, "Ts": 2},
        "status": "replayed",
        "source_histories_modified": False,
    }
    _atomic_text(output_dir / "report.json", json.dumps(report, indent=2, sort_keys=True) + "\n")
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
    report = replay_team_results(
        mapped_results=args.mapped_results,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
