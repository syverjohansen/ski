#!/usr/bin/env python3
"""Build an isolated Nordic-combined mixed-team-sprint Elo candidate."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import tempfile
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

BASE_ELO = 1300.0
OFFSEASON_DISCOUNT = 0.85
RATING_STREAMS = ("", "Individual_", "IndividualCompact_", "Sprint_", "MassStart_")


class ReplayError(RuntimeError):
    pass


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def actual_scores(places: list[int]) -> list[float]:
    return [sum(other > place for other in places) + 0.5 * (sum(other == place for other in places) - 1) for place in places]


def expected_scores(ratings: list[float]) -> list[float]:
    return [sum(1 / (1 + 10 ** ((other - rating) / 400)) for j, other in enumerate(ratings) if i != j) for i, rating in enumerate(ratings)]


def update_ratings(ratings: list[float], places: list[int], k: float) -> list[float]:
    actual, expected = actual_scores(places), expected_scores(ratings)
    updated = [rating + k * (score - estimate) for rating, score, estimate in zip(ratings, actual, expected)]
    if not math.isclose(sum(updated), sum(ratings), abs_tol=1e-8):
        raise ReplayError("Elo update is not zero-sum")
    return updated


def season_k(rows: list[dict[str, str]], season: int, inserted_count: int) -> float:
    counts = Counter(int(row["Season"]) for row in rows if row["Race"] != "0")
    counts[season] += inserted_count
    return max(1.0, min(5.0, max(counts.values()) / counts[season]))


def _latest(rows: list[dict[str, str]], athlete_id: str, before: str | None = None) -> dict[str, str] | None:
    candidates = [row for row in rows if row["ID"] == athlete_id and (before is None or row["Date"] <= before)]
    return max(candidates, key=lambda row: (row["Date"], int(row["Race"] or 0)), default=None)


def _age(birthday: str, event_date: str) -> str:
    try:
        born = datetime.fromisoformat(birthday).date()
        raced = date.fromisoformat(event_date)
        return str((raced - born).days / 365.25)
    except (TypeError, ValueError):
        return "0.0"


def _candidate_row(mapped: dict[str, str], metadata: dict[str, str], race: int) -> dict[str, str]:
    row = dict(metadata)
    row.update(
        Date=mapped["date"], City=mapped["city"], Country=mapped["country"],
        Sex=mapped["canonical_sex"], Distance=mapped["distance"],
        RaceType="Team Sprint", MassStart="0", Event="World Cup",
        Place=mapped["place"], Skier=mapped["canonical_name"],
        Nation=mapped["canonical_nation"], ID=mapped["canonical_athlete_id"],
        Season=mapped["season"], Race=str(race), Leg=mapped["member_order"],
        Age=_age(metadata.get("Birthday", ""), mapped["date"]),
        Exp=str(int(float(metadata.get("Exp") or 0)) + 1),
    )
    return row


def replay_history(
    rows: list[dict[str, str]], mapped: list[dict[str, str]], *, season: int, fields: list[str]
) -> tuple[list[dict[str, object]], float, int]:
    required = {"Date", "Sex", "RaceType", "Place", "ID", "Season", "Race", "Pelo", "Elo", "Exp"}
    if missing := sorted(required - set(fields)):
        raise ReplayError(f"chrono missing fields: {missing}")
    sex = next((row["Sex"] for row in rows if row.get("Sex")), "")
    inserted = [row for row in mapped if row["canonical_sex"] == sex]
    if any(row["event_kind"] != "team_sprint" or row["sex"] != "Mixed" for row in inserted):
        raise ReplayError("this replay accepts only mixed team-sprint evidence")
    previous = [row for row in rows if int(row["Season"]) < season]
    current = [row for row in rows if int(row["Season"]) == season and row["Race"] != "0"]
    offseason = {row["ID"]: row for row in rows if int(row["Season"]) == season and row["Race"] == "0"}
    if not current:
        raise ReplayError(f"no accepted {sex} rows for season {season}")
    state: dict[str, float] = {}
    for row in previous:
        if row.get("Elo", "").strip():
            state[row["ID"]] = float(row["Elo"])
    groups: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    for row in current:
        groups[(row["Date"], int(row["Race"]))].append(row)
    event_specs: list[tuple[str, bool, list[dict[str, str]]]] = [
        (key[0], False, groups[key]) for key in sorted(groups, key=lambda item: (item[0], item[1]))
    ]
    if inserted:
        event_specs.append((inserted[0]["date"], True, inserted))
    event_specs.sort(key=lambda item: (item[0], item[1]))
    k = season_k(rows, season, len(inserted))
    inserted_ids = {row["canonical_athlete_id"] for row in inserted}
    inserted_date = inserted[0]["date"] if inserted else ""
    rebuilt: list[dict[str, object]] = []
    changed = 0
    for race_number, (event_date, is_inserted, source_rows) in enumerate(event_specs, 1):
        event_rows: list[dict[str, str]] = []
        for source in source_rows:
            if is_inserted:
                athlete_id = source["canonical_athlete_id"]
                metadata = _latest(previous + current, athlete_id, event_date)
                if metadata is None:
                    raise ReplayError(f"no canonical metadata for athlete {athlete_id}")
                row = _candidate_row(source, metadata, race_number)
            else:
                row = dict(source)
                row["Race"] = str(race_number)
                if row["ID"] in inserted_ids and row["Date"] > inserted_date:
                    row["Exp"] = str(int(float(row["Exp"])) + 1)
            event_rows.append(row)
        athlete_ids = [row["ID"] for row in event_rows]
        if len(athlete_ids) != len(set(athlete_ids)):
            raise ReplayError(f"duplicate athlete in event {event_date}")
        ratings = [state.get(athlete_id, BASE_ELO) for athlete_id in athlete_ids]
        places = [int(row["Place"]) for row in event_rows]
        race_type = event_rows[0]["RaceType"]
        divisor = 2.0 if race_type == "Team Sprint" else 4.0 if race_type == "Team" else 1.0
        updated = update_ratings(ratings, places, k / divisor)
        for row, before, after in zip(event_rows, ratings, updated):
            accepted_elo = row.get("Elo", "")
            row["Pelo"], row["Elo"] = before, after
            if not is_inserted and accepted_elo and not math.isclose(float(accepted_elo), after, abs_tol=1e-10):
                changed += 1
            rebuilt.append(row)
            state[row["ID"]] = after
    race_zero: list[dict[str, object]] = []
    for athlete_id in sorted({row["ID"] for row in rebuilt}):
        if athlete_id not in offseason:
            raise ReplayError(f"missing accepted Race 0 row for athlete {athlete_id}")
        row: dict[str, object] = dict(offseason[athlete_id])
        rating = state[athlete_id]
        row["Pelo"], row["Elo"] = rating, rating * OFFSEASON_DISCOUNT + BASE_ELO * (1 - OFFSEASON_DISCOUNT)
        if athlete_id in inserted_ids:
            row["Exp"] = str(int(float(str(row["Exp"]))) + 1)
        race_zero.append(row)
    prefix = [row for row in rows if int(row["Season"]) < season]
    return [*prefix, *rebuilt, *race_zero], k, changed


def aggregate_team_sprint(
    accepted: list[dict[str, str]], mapped: list[dict[str, str]],
    men_candidate: list[dict[str, object]], ladies_candidate: list[dict[str, object]],
    *, fields: list[str], season: int,
) -> list[dict[str, object]]:
    mapped_keys = {(row["date"], row["canonical_athlete_id"]) for row in mapped}
    candidate_lookup: dict[tuple[str, str], dict[str, object]] = {}
    for row in [*men_candidate, *ladies_candidate]:
        key = (str(row["Date"]), str(row["ID"]))
        if key not in mapped_keys or str(row["Season"]) != str(season) or row["Race"] == "0":
            continue
        if key in candidate_lookup:
            raise ReplayError(f"multiple candidate rows for mixed-team member {key}")
        if row["RaceType"] != "Team Sprint":
            raise ReplayError(f"candidate row for mixed-team member {key} is not Team Sprint")
        candidate_lookup[key] = row
    if candidate_lookup.keys() != mapped_keys:
        missing = sorted(mapped_keys - candidate_lookup.keys())
        raise ReplayError(f"missing replayed mixed-team members: {missing}")
    teams: dict[tuple[int, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in mapped:
        teams[(int(row["place"]), row["team_name"], row["team_nation"])].append(row)
    output: list[dict[str, object]] = list(accepted)
    for (place, team_name, _), members in sorted(teams.items()):
        if len(members) != 2 or {row["canonical_sex"] for row in members} != {"M", "L"}:
            raise ReplayError(f"mixed team {team_name} is not one man and one woman")
        athlete_rows = [candidate_lookup[(row["date"], row["canonical_athlete_id"])] for row in sorted(members, key=lambda item: int(item["member_order"]))]
        first = members[0]
        result: dict[str, object] = {
            "Nation": athlete_rows[0]["Nation"], "Season": str(season), "Race": first["race"],
            "Place": str(place), "Date": first["date"], "City": first["city"],
            "Country": first["country"], "Event": "World Cup", "Sex": "Mixed",
            "TeamSize": "2", "RaceType": "Team Sprint", "Distance": first["distance"],
            "MassStart": "0", "TeamMembers": ",".join(str(row["Skier"]) for row in athlete_rows),
            "MemberIDs": ",".join(str(row["ID"]) for row in athlete_rows),
        }
        for stream in RATING_STREAMS:
            for label in ("Elo", "Pelo"):
                column = f"{stream}{label}"
                values = [float(row[column]) for row in athlete_rows if str(row.get(column, "")).strip()]
                team_column = f"Team_{column}"
                avg_column = f"Avg_{column}"
                result[team_column] = sum(values) if len(values) == 2 else ""
                result[avg_column] = sum(values) / 2 if len(values) == 2 else ""
        for column in ("Age", "Exp"):
            values = [float(row[column]) for row in athlete_rows]
            result[f"Avg{column}"] = sum(values) / 2
            result[f"Max{column}"] = max(values)
            result[f"Min{column}"] = min(values)
        output.append({field: result.get(field, "") for field in fields})
    return sorted(output, key=lambda row: (row["Date"], int(row["Place"])))


def _atomic_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader(); writer.writerows(rows)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False) as handle:
        handle.write(output.getvalue()); temporary = Path(handle.name)
    temporary.replace(path)


def run_replay(
    *, mapped_results: Path, men_history: Path, ladies_history: Path,
    team_sprint_history: Path, season: int, output_dir: Path,
) -> dict[str, object]:
    sources = [mapped_results, men_history, ladies_history, team_sprint_history]
    before = {path: _sha(path) for path in sources}
    resolved = output_dir.resolve()
    if any(path.resolve().parent == resolved or path.resolve().parent in resolved.parents for path in sources):
        raise ReplayError("candidate output cannot be inside accepted data")
    mapped_fields, mapped = _read(mapped_results)
    if not mapped or any(not row.get("canonical_athlete_id") for row in mapped):
        raise ReplayError("mapped results are empty or unresolved")
    men_fields, men = _read(men_history); ladies_fields, ladies = _read(ladies_history)
    team_fields, team_sprint = _read(team_sprint_history)
    men_candidate, men_k, men_changed = replay_history(men, mapped, season=season, fields=men_fields)
    ladies_candidate, ladies_k, ladies_changed = replay_history(ladies, mapped, season=season, fields=ladies_fields)
    team_candidate = aggregate_team_sprint(team_sprint, mapped, men_candidate, ladies_candidate, fields=team_fields, season=season)
    outputs = {
        "candidate_men_chrono.csv": (men_fields, men_candidate),
        "candidate_ladies_chrono.csv": (ladies_fields, ladies_candidate),
        "candidate_team_sprint_chrono.csv": (team_fields, team_candidate),
    }
    for name, (fields, rows) in outputs.items(): _atomic_csv(output_dir / name, fields, rows)
    if before != {path: _sha(path) for path in sources}:
        raise ReplayError("an accepted input changed during replay")
    report = {
        "schema_version": "nordicnumbers.nordic-combined-results-replay/v1",
        "season": season, "inserted_member_rows": len(mapped),
        "inserted_team_rows": len(team_candidate) - len(team_sprint),
        "k_factors": {"M": men_k, "L": ladies_k},
        "changed_existing_rating_rows": {"M": men_changed, "L": ladies_changed},
        "accepted_inputs_modified": False,
        "promotion": {"status": "candidate", "performed": False},
        "inputs": {str(path): digest for path, digest in before.items()},
        "outputs": {name: {"rows": len(rows), "sha256": _sha(output_dir / name)} for name, (_, rows) in outputs.items()},
    }
    report_text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output_dir, delete=False) as handle:
        handle.write(report_text)
        temporary_report = Path(handle.name)
    temporary_report.replace(output_dir / "replay_report.json")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapped-results", required=True, type=Path)
    parser.add_argument("--men-history", required=True, type=Path)
    parser.add_argument("--ladies-history", required=True, type=Path)
    parser.add_argument("--team-sprint-history", required=True, type=Path)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(); report = run_replay(**vars(args))
    print(json.dumps(report, indent=2, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
