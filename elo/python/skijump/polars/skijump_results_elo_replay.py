#!/usr/bin/env python3
"""Build an isolated ski-jumping results and Elo replay candidate.

The accepted CSV files are inputs only.  The command classifies acquired
results before replay so schedule numbering cannot duplicate an event already
present under a different filtered-history race number, and non-major events
remain excluded from the established World Cup/championship Elo model.
Individual, men's team, women's team, and mixed-team results are rebuilt into
one atomic five-history candidate set.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

BASE_ELO = 1300.0
OFFSEASON_DISCOUNT = 0.85
TEAM_DIVISOR = 4.0
STREAMS = ("Overall", "Small", "Medium", "Normal", "Large", "Flying")
MAJOR_EVENT_WORDS = ("olympic", "world cup", "championship")


class ReplayError(RuntimeError):
    """Raised when replay inputs violate the deterministic contract."""


@dataclass(frozen=True)
class EventIdentity:
    event_date: str
    sex: str
    city: str
    race_type: str
    event_kind: str


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_output_dir(output_dir: Path, accepted_inputs: Iterable[Path]) -> None:
    resolved_output = output_dir.expanduser().resolve()
    for source in accepted_inputs:
        accepted_dir = source.expanduser().resolve().parent
        if resolved_output == accepted_dir or accepted_dir in resolved_output.parents:
            raise ReplayError(
                f"candidate output {resolved_output} cannot be inside accepted "
                f"input directory {accepted_dir}"
            )


def _atomic_csv(
    path: Path, fields: list[str], rows: Iterable[dict[str, object]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
        temporary = Path(handle.name)
    temporary.replace(path)


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _event_kind(race_type: str) -> str:
    return "team" if race_type.casefold().startswith("team ") else "individual"


def _is_canonical_mixed_team(row: dict[str, str]) -> bool:
    members = [
        item.strip() for item in row.get("MemberIDs", "").split(",") if item.strip()
    ]
    sexes = [
        item.strip() for item in row.get("MemberSex", "").split(",") if item.strip()
    ]
    return (
        row.get("Sex") == "Mixed"
        and row.get("TeamSize") == "4"
        and len(members) == 4
        and len(set(members)) == 4
        and sorted(sexes) == ["L", "L", "M", "M"]
    )


def _identity(row: dict[str, str], *, sex: str | None = None) -> EventIdentity:
    return EventIdentity(
        event_date=(row.get("date") or row.get("Date") or "").strip(),
        sex=(sex or row.get("sex") or row.get("Sex") or "").strip(),
        city=(row.get("city") or row.get("City") or "").strip().casefold(),
        race_type=(row.get("race_type") or row.get("RaceType") or "")
        .strip()
        .casefold(),
        event_kind=(
            row.get("event_kind")
            or _event_kind(row.get("race_type") or row.get("RaceType") or "")
        ).strip(),
    )


def actual_scores(places: list[int]) -> list[float]:
    """Return pairwise win/draw scores for a multi-competitor event."""
    return [
        sum(other > place for other in places)
        + 0.5 * (sum(other == place for other in places) - 1)
        for place in places
    ]


def expected_scores(ratings: list[float]) -> list[float]:
    """Return the legacy pairwise Elo expected scores (400-point scale)."""
    return [
        sum(
            1.0 / (1.0 + 10.0 ** ((other - rating) / 400.0))
            for index, other in enumerate(ratings)
            if index != athlete_index
        )
        for athlete_index, rating in enumerate(ratings)
    ]


def update_ratings(
    ratings: list[float], places: list[int], *, k_factor: float
) -> list[float]:
    if len(ratings) != len(places) or not ratings:
        raise ReplayError("ratings and places must be non-empty and equally sized")
    actual = actual_scores(places)
    expected = expected_scores(ratings)
    updated = [
        rating + k_factor * (score - expectation)
        for rating, score, expectation in zip(ratings, actual, expected)
    ]
    if not math.isclose(sum(updated), sum(ratings), abs_tol=1e-8):
        raise ReplayError("Elo update is not zero-sum")
    return updated


def offseason_rating(rating: float) -> float:
    """Return the legacy 85/15 regression used at ski-jumping season close."""
    return rating * OFFSEASON_DISCOUNT + BASE_ELO * (1.0 - OFFSEASON_DISCOUNT)


def season_k(entry_counts: dict[int, int], season: int) -> float:
    current = entry_counts.get(season, 0)
    if current <= 0:
        raise ReplayError(f"season {season} has no entries")
    return max(1.0, min(5.0, max(entry_counts.values()) / current))


def _stream_columns(stream: str) -> tuple[str, str]:
    return (
        ("Pelo", "Elo") if stream == "Overall" else (f"{stream}_Pelo", f"{stream}_Elo")
    )


def _hill_stream(race_type: str) -> str | None:
    value = race_type.removeprefix("Team ").strip().title()
    return value if value in STREAMS[1:] else None


def _major_event(event: str) -> bool:
    folded = event.casefold()
    return any(word in folded for word in MAJOR_EVENT_WORDS)


def classify_acquired_events(
    mapped_rows: list[dict[str, str]],
    *,
    men_chrono: list[dict[str, str]],
    ladies_chrono: list[dict[str, str]],
    men_team_chrono: list[dict[str, str]],
    ladies_team_chrono: list[dict[str, str]],
    mixed_team_chrono: list[dict[str, str]],
    men_raw: list[dict[str, str]],
    ladies_raw: list[dict[str, str]],
) -> tuple[list[dict[str, object]], set[EventIdentity]]:
    """Classify evidence as existing, policy-excluded, or replay-eligible."""
    accepted = {
        _identity(row)
        for row in men_chrono + ladies_chrono
        if (row.get("Race") or "") != "0"
    }
    accepted.update(
        _identity(row)
        for row in men_team_chrono + ladies_team_chrono
        if (row.get("Race") or "") != "0"
    )
    accepted.update(
        _identity(row, sex="Mixed")
        for row in mixed_team_chrono
        if (row.get("Race") or "") != "0" and (row.get("Sex") or "") == "Mixed"
    )
    raw_events: dict[EventIdentity, set[str]] = defaultdict(set)
    for row in men_raw + ladies_raw:
        raw_events[_identity(row)].add((row.get("Event") or "").strip())

    grouped: dict[EventIdentity, list[dict[str, str]]] = defaultdict(list)
    for row in mapped_rows:
        grouped[_identity(row)].append(row)

    classifications: list[dict[str, object]] = []
    eligible: set[EventIdentity] = set()
    for identity, rows in sorted(
        grouped.items(),
        key=lambda item: (
            item[0].event_date,
            item[0].sex,
            item[0].city,
            item[0].race_type,
            item[0].event_kind,
        ),
    ):
        if identity in accepted:
            status, reason = "already_accepted", "semantic event already exists"
        elif identity.event_kind == "individual":
            events = raw_events.get(identity, set())
            if events and not any(_major_event(event) for event in events):
                status = "policy_excluded"
                reason = "event is outside the established major-event Elo model"
            else:
                status, reason = "eligible", "missing major individual event"
                eligible.add(identity)
        elif identity.sex == "Mixed":
            status, reason = "eligible", "missing mixed-team event"
            eligible.add(identity)
        else:
            status, reason = "eligible", "missing team event"
            eligible.add(identity)
        classifications.append(
            {
                "date": identity.event_date,
                "sex": identity.sex,
                "city": identity.city,
                "race_type": identity.race_type,
                "event_kind": identity.event_kind,
                "rows": len(rows),
                "status": status,
                "reason": reason,
            }
        )
    return classifications, eligible


def validate_mapped_results(rows: list[dict[str, str]]) -> None:
    """Fail closed on duplicate athletes or malformed mixed-team membership."""
    events: dict[EventIdentity, list[dict[str, str]]] = defaultdict(list)
    for row_number, row in enumerate(rows, start=2):
        athlete_id = (row.get("canonical_athlete_id") or "").strip()
        if not athlete_id:
            raise ReplayError(f"mapped results row {row_number} is unresolved")
        events[_identity(row)].append(row)
    for identity, event_rows in events.items():
        athlete_ids = [row["canonical_athlete_id"] for row in event_rows]
        if len(athlete_ids) != len(set(athlete_ids)):
            raise ReplayError(f"duplicate athlete in mapped event {identity}")
        if identity.event_kind != "team":
            continue
        teams: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        for row in event_rows:
            team_name = (row.get("team_name") or "").strip()
            team_nation = (row.get("team_nation") or "").strip()
            if not team_name or not team_nation:
                raise ReplayError(f"team member is missing team identity in {identity}")
            teams[(team_name, team_nation)].append(row)
        expected_size = 4 if identity.sex == "Mixed" else None
        for team, members in teams.items():
            orders = sorted(int(row["member_order"]) for row in members)
            if expected_size is not None and orders != list(
                range(1, expected_size + 1)
            ):
                raise ReplayError(
                    f"mixed team {team} in {identity} has member orders {orders}"
                )


def _event_groups(rows: list[dict[str, str]]) -> list[list[dict[str, str]]]:
    grouped: dict[tuple[str, str, str, str, str], list[dict[str, str]]] = defaultdict(
        list
    )
    for row in rows:
        key = (row["Date"], row["Race"], row["City"], row["RaceType"], row["TeamEvent"])
        grouped[key].append(row)
    return [
        grouped[key]
        for key in sorted(
            grouped, key=lambda value: (value[0], int(value[1]), value[2])
        )
    ]


def _latest_metadata(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    latest: dict[str, dict[str, str]] = {}
    for row in sorted(
        rows, key=lambda item: (item.get("Date", ""), int(item.get("Race") or 0))
    ):
        latest[row["ID"]] = row
    return latest


def _earliest_metadata(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    earliest: dict[str, dict[str, str]] = {}
    for row in sorted(
        rows, key=lambda item: (item.get("Date", ""), int(item.get("Race") or 0))
    ):
        earliest.setdefault(row["ID"], row)
    return earliest


def _metadata_at(
    rows: list[dict[str, str]], athlete_id: str, event_date: str
) -> dict[str, str] | None:
    eligible = [
        row
        for row in rows
        if row.get("ID") == athlete_id and (row.get("Date") or "") <= event_date
    ]
    if not eligible:
        eligible = [row for row in rows if row.get("ID") == athlete_id]
    if not eligible:
        return None
    return max(
        eligible, key=lambda row: (row.get("Date", ""), int(row.get("Race") or 0))
    )


def _candidate_row(
    mapped: dict[str, str], metadata: dict[str, str] | None, *, race: int, sex: str
) -> dict[str, str]:
    base = dict(metadata or {})
    is_team = mapped["event_kind"] == "team"
    base.update(
        {
            "Date": mapped["date"],
            "City": mapped["city"],
            "Country": mapped["country"],
            "Sex": sex,
            "HillSize": mapped["hill_size"],
            "RaceType": mapped["race_type"],
            "TeamEvent": "1" if is_team else "0",
            "Event": "Olympic Winter Games"
            if mapped["championship"] == "1"
            else "World Cup",
            "Place": mapped["place"],
            "Skier": mapped["canonical_name"],
            "Nation": mapped["canonical_nation"],
            "ID": mapped["canonical_athlete_id"],
            "Season": mapped["season"],
            "Race": str(race),
            "Leg": mapped["member_order"] if is_team else "0",
        }
    )
    base.setdefault("Birthday", f"{mapped.get('birth_year') or '1700'}-01-01 00:00:00")
    base.setdefault("Age", "0")
    base.setdefault("Exp", "0")
    return base


def replay_sex(
    history_rows: list[dict[str, str]],
    mapped_rows: list[dict[str, str]],
    *,
    season: int,
    fields: list[str],
    stream_histories: dict[str, list[dict[str, str]]] | None = None,
) -> tuple[list[dict[str, object]], dict[str, float], int]:
    """Rebuild one sex's complete season, inserting mapped eligible rows."""
    required_fields = {
        "Date",
        "City",
        "Sex",
        "RaceType",
        "TeamEvent",
        "Place",
        "Skier",
        "Nation",
        "ID",
        "Season",
        "Race",
        "Pelo",
        "Elo",
    }
    if not required_fields.issubset(fields):
        raise ReplayError(
            f"chrono is missing required fields: {sorted(required_fields - set(fields))}"
        )
    sex = (history_rows[0].get("Sex") or "") if history_rows else ""
    previous = [row for row in history_rows if int(row["Season"]) < season]
    existing = [
        row
        for row in history_rows
        if int(row["Season"]) == season and row["Race"] != "0"
    ]
    if not existing:
        raise ReplayError(f"no accepted {sex} rows for season {season}")

    latest = _latest_metadata(previous + existing)
    stream_histories = stream_histories or {}
    start_states: dict[str, dict[str, float]] = defaultdict(dict)
    start_pairs: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
    first_current_rows = _earliest_metadata(existing)
    for stream in STREAMS:
        source_rows = stream_histories.get(stream)
        if source_rows is not None:
            prior_source = [row for row in source_rows if int(row["Season"]) < season]
            pelo_column, elo_column = "Pelo", "Elo"
        else:
            prior_source = previous
            pelo_column, elo_column = _stream_columns(stream)
        state_rows = _latest_metadata(prior_source)
        for athlete_id, row in state_rows.items():
            raw_elo = (row.get(elo_column) or "").strip()
            if raw_elo:
                elo_value = float(raw_elo)
                start_states[stream][athlete_id] = elo_value
                if source_rows is not None and stream != "Overall":
                    display = first_current_rows.get(athlete_id, row)
                    display_pelo_column, display_elo_column = _stream_columns(stream)
                else:
                    display = row
                    display_pelo_column, display_elo_column = pelo_column, elo_column
                display_pelo = (display.get(display_pelo_column) or "").strip()
                display_elo = (display.get(display_elo_column) or "").strip()
                start_pairs[stream][athlete_id] = (
                    float(display_pelo) if display_pelo else elo_value,
                    float(display_elo) if display_elo else elo_value,
                )

    candidate_events: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(
        list
    )
    for row in mapped_rows:
        candidate_events[(row["date"], row["city"], row["race_type"])].append(row)

    event_specs: list[tuple[str, str, str, list[dict[str, str]], bool]] = []
    for group in _event_groups(existing):
        event_specs.append(
            (group[0]["Date"], group[0]["City"], group[0]["RaceType"], group, False)
        )
    for (event_date, city, race_type), group in candidate_events.items():
        event_specs.append((event_date, city, race_type, group, True))
    event_specs.sort(key=lambda item: (item[0], item[1], item[2], item[4]))

    counts: dict[str, dict[int, int]] = {stream: defaultdict(int) for stream in STREAMS}
    if stream_histories:
        for stream, source_rows in stream_histories.items():
            for row in source_rows:
                if row["Race"] != "0" and int(row["Season"]) != season:
                    counts[stream][int(row["Season"])] += 1
        # The combined accepted history is authoritative for the season being
        # rebuilt. After mixed-team promotion it contains those member rows,
        # while the legacy per-discipline source files do not.
        for row in existing:
            counts["Overall"][season] += 1
            hill = _hill_stream(row["RaceType"])
            if hill:
                counts[hill][season] += 1
    else:
        for row in history_rows:
            if row["Race"] == "0":
                continue
            row_season = int(row["Season"])
            counts["Overall"][row_season] += 1
            hill = _hill_stream(row["RaceType"])
            if hill:
                counts[hill][row_season] += 1
    for row in mapped_rows:
        counts["Overall"][season] += 1
        hill = _hill_stream(row["race_type"])
        if hill:
            counts[hill][season] += 1
    k_factors = {
        stream: season_k(dict(stream_counts), season)
        for stream, stream_counts in counts.items()
        if stream_counts.get(season)
    }

    states: dict[str, dict[str, float]] = defaultdict(
        dict, {stream: dict(values) for stream, values in start_states.items()}
    )
    display_pairs: dict[str, dict[str, tuple[float, float]]] = defaultdict(
        dict, {stream: dict(values) for stream, values in start_pairs.items()}
    )
    rebuilt: list[dict[str, object]] = []
    candidate_keys: set[tuple[str, str]] = set()
    for race_number, (_, _, _, source_rows, is_candidate) in enumerate(
        event_specs, start=1
    ):
        rows = []
        for source in source_rows:
            if is_candidate:
                athlete_id = source["canonical_athlete_id"]
                row = _candidate_row(
                    source,
                    _metadata_at(previous + existing, athlete_id, source["date"]),
                    race=race_number,
                    sex=sex,
                )
                candidate_keys.add((row["Date"], athlete_id))
            else:
                row = dict(source)
                row["Race"] = str(race_number)
            rows.append(row)

        athlete_ids = [row["ID"] for row in rows]
        if len(athlete_ids) != len(set(athlete_ids)):
            raise ReplayError(
                f"duplicate athlete in {rows[0]['Date']} {rows[0]['City']}"
            )
        places = [int(row["Place"]) for row in rows]
        team_factor = TEAM_DIVISOR if rows[0]["TeamEvent"] == "1" else 1.0
        update_streams = ["Overall"]
        hill = _hill_stream(rows[0]["RaceType"])
        if hill:
            update_streams.append(hill)

        event_values: dict[str, tuple[list[float], list[float]]] = {}
        for stream in update_streams:
            before = [
                states[stream].get(athlete_id, BASE_ELO) for athlete_id in athlete_ids
            ]
            after = update_ratings(
                before, places, k_factor=k_factors[stream] / team_factor
            )
            event_values[stream] = (before, after)
            states[stream].update(zip(athlete_ids, after))
            display_pairs[stream].update(zip(athlete_ids, zip(before, after)))

        for index, row in enumerate(rows):
            output: dict[str, object] = dict(row)
            for stream in STREAMS:
                pelo_column, elo_column = _stream_columns(stream)
                if stream in event_values:
                    output[pelo_column] = event_values[stream][0][index]
                    output[elo_column] = event_values[stream][1][index]
                else:
                    pair = display_pairs[stream].get(row["ID"], ("", ""))
                    output[pelo_column], output[elo_column] = pair
            rebuilt.append(output)
            latest[row["ID"]] = {key: str(value) for key, value in output.items()}

    final_date = f"{season}-05-01"
    for athlete_id in sorted({row["ID"] for row in rebuilt}):
        output: dict[str, object] = dict(latest[athlete_id])
        output.update(
            Date=final_date,
            City="Summer",
            Country="Break",
            HillSize="0",
            RaceType="Offseason",
            TeamEvent="0",
            Event="Offseason",
            Place="0",
            Season=str(season),
            Race="0",
            Leg="0",
        )
        for stream in STREAMS:
            pelo_column, elo_column = _stream_columns(stream)
            value = states[stream].get(athlete_id, "")
            output[pelo_column] = value
            output[elo_column] = offseason_rating(value) if value != "" else ""
        rebuilt.append(output)

    before_index = {
        (row["Date"], row["ID"], row["City"], row["RaceType"]): row for row in existing
    }
    changed = 0
    for row in rebuilt:
        key = (str(row["Date"]), str(row["ID"]), str(row["City"]), str(row["RaceType"]))
        accepted = before_index.get(key)
        if accepted:
            for stream in STREAMS:
                for column in _stream_columns(stream):
                    candidate_value = str(row.get(column) or "").strip()
                    accepted_value = str(accepted.get(column) or "").strip()
                    if not candidate_value and not accepted_value:
                        continue
                    if (
                        not candidate_value
                        or not accepted_value
                        or not math.isclose(
                            float(candidate_value), float(accepted_value), abs_tol=1e-10
                        )
                    ):
                        changed += 1
                        break
                else:
                    continue
                break
    prefix = [row for row in history_rows if int(row["Season"]) < season]
    return prefix + rebuilt, k_factors, changed


def aggregate_mixed_teams(
    men_rows: list[dict[str, object]],
    ladies_rows: list[dict[str, object]],
    *,
    season: int,
    eligible_dates: set[str],
) -> tuple[list[str], list[dict[str, object]], int]:
    candidate = [
        row
        for row in men_rows + ladies_rows
        if int(row["Season"]) == season
        and row["TeamEvent"] == "1"
        and row["Sex"] in {"M", "L"}
        and str(row["Date"]) in eligible_dates
    ]
    # Only true mixed events have both sexes for the same date/race/nation.
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(
        list
    )
    for row in candidate:
        grouped[
            (
                str(row["Date"]),
                str(row["RaceType"]),
                str(row["Nation"]),
                str(row["Place"]),
            )
        ].append(row)
    fields = [
        "Nation",
        "Season",
        "Race",
        "Place",
        "Date",
        "City",
        "Country",
        "Event",
        "HillSize",
        "TeamSize",
        "RaceType",
        "Sex",
        "Total_Elo",
        "Total_Normal_Elo",
        "Total_Large_Elo",
        "Total_Flying_Elo",
        "Total_Pelo",
        "Total_Normal_Pelo",
        "Total_Large_Pelo",
        "Total_Flying_Pelo",
        "Avg_Elo",
        "Avg_Normal_Elo",
        "Avg_Large_Elo",
        "Avg_Flying_Elo",
        "Avg_Pelo",
        "Avg_Normal_Pelo",
        "Avg_Large_Pelo",
        "Avg_Flying_Pelo",
        "TeamMembers",
        "MemberIDs",
        "MemberSex",
        "AvgAge",
        "MaxAge",
        "MinAge",
        "AvgExp",
        "MaxExp",
        "MinExp",
    ]
    rating_columns = [
        column
        for stream in ("Overall", "Normal", "Large", "Flying")
        for column in _stream_columns(stream)
    ]
    quartiles: dict[str, float] = {}
    for column in rating_columns:
        values = sorted(
            float(row[column])
            for row in candidate
            if str(row.get(column) or "").strip()
        )
        if values:
            nearest_index = int(0.25 * (len(values) - 1) + 0.5)
            quartiles[column] = values[nearest_index]
        else:
            quartiles[column] = 1200.0
    imputed_rating_cells = sum(
        not str(row.get(column) or "").strip()
        for row in candidate
        for column in rating_columns
    )
    output = []
    mixed_race_by_date = {
        event_date: index
        for index, event_date in enumerate(sorted(eligible_dates), start=1)
    }
    for key, members in sorted(grouped.items()):
        sexes = {str(row["Sex"]) for row in members}
        if sexes != {"M", "L"}:
            continue
        first = members[0]
        row: dict[str, object] = {
            "Nation": key[2],
            "Season": season,
            "Race": mixed_race_by_date[key[0]],
            "Place": key[3],
            "Date": key[0],
            "City": first["City"],
            "Country": first["Country"],
            "Event": first["Event"],
            "HillSize": first["HillSize"],
            "TeamSize": len(members),
            "RaceType": key[1],
            "Sex": "Mixed",
            "TeamMembers": ",".join(str(item["Skier"]) for item in members),
            "MemberIDs": ",".join(str(item["ID"]) for item in members),
            "MemberSex": ",".join(str(item["Sex"]) for item in members),
        }
        for stream in ("Overall", "Normal", "Large", "Flying"):
            pelo, elo = _stream_columns(stream)
            label = "" if stream == "Overall" else f"{stream}_"
            for source, suffix in ((elo, "Elo"), (pelo, "Pelo")):
                values = [
                    float(item[source])
                    if str(item.get(source) or "").strip()
                    else quartiles[source]
                    for item in members
                ]
                row[f"Total_{label}{suffix}"] = sum(values)
                row[f"Avg_{label}{suffix}"] = sum(values) / len(values)
        ages = [float(item.get("Age") or 0) for item in members]
        exps = [float(item.get("Exp") or 0) for item in members]
        row.update(
            AvgAge=sum(ages) / len(ages),
            MaxAge=max(ages),
            MinAge=min(ages),
            AvgExp=sum(exps) / len(exps),
            MaxExp=max(exps),
            MinExp=min(exps),
        )
        output.append(row)
    return fields, output, imputed_rating_cells


def aggregate_same_sex_teams(
    rows: list[dict[str, object]],
    *,
    season: int,
    sex: str,
    fields: list[str],
) -> tuple[list[dict[str, object]], int]:
    """Rebuild one sex's current-season team summaries from member rows."""
    if sex not in {"M", "L"}:
        raise ReplayError(f"unsupported same-sex team value: {sex}")
    candidate = [
        row
        for row in rows
        if int(row["Season"]) == season
        and row["TeamEvent"] == "1"
        and row["Sex"] == sex
    ]
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(
        list
    )
    for row in candidate:
        grouped[
            (
                str(row["Date"]),
                str(row["RaceType"]),
                str(row["Nation"]),
                str(row["Place"]),
            )
        ].append(row)

    rating_columns = [
        column
        for stream in ("Overall", "Normal", "Large", "Flying")
        for column in _stream_columns(stream)
    ]
    quartiles: dict[str, float] = {}
    for column in rating_columns:
        values = sorted(
            float(row[column])
            for row in candidate
            if str(row.get(column) or "").strip()
        )
        nearest_index = int(0.25 * (len(values) - 1) + 0.5) if values else 0
        quartiles[column] = values[nearest_index] if values else 1200.0
    imputed = sum(
        not str(row.get(column) or "").strip()
        for row in candidate
        for column in rating_columns
    )

    output: list[dict[str, object]] = []
    for key, members in sorted(
        grouped.items(), key=lambda item: (item[0][0], int(item[0][3]), item[0][2])
    ):
        first = members[0]
        row: dict[str, object] = {
            "Nation": key[2],
            "Season": season,
            "Race": first["Race"],
            "Place": key[3],
            "Date": key[0],
            "City": first["City"],
            "Country": first["Country"],
            "Event": first["Event"],
            "HillSize": first["HillSize"],
            "TeamSize": len(members),
            "RaceType": key[1],
            "Sex": sex,
            "TeamMembers": ",".join(str(item["Skier"]) for item in members),
            "MemberIDs": ",".join(str(item["ID"]) for item in members),
        }
        for stream in ("Overall", "Normal", "Large", "Flying"):
            pelo, elo = _stream_columns(stream)
            label = "" if stream == "Overall" else f"{stream}_"
            for source, suffix in ((elo, "Elo"), (pelo, "Pelo")):
                values = [
                    float(item[source])
                    if str(item.get(source) or "").strip()
                    else quartiles[source]
                    for item in members
                ]
                row[f"Total_{label}{suffix}"] = sum(values)
                row[f"Avg_{label}{suffix}"] = sum(values) / len(values)
        ages = [float(item.get("Age") or 0) for item in members]
        exps = [float(item.get("Exp") or 0) for item in members]
        row.update(
            AvgAge=sum(ages) / len(ages),
            MaxAge=max(ages),
            MinAge=min(ages),
            AvgExp=sum(exps) / len(exps),
            MaxExp=max(exps),
            MinExp=min(exps),
        )
        missing = set(fields) - set(row)
        if missing:
            raise ReplayError(f"team summary is missing fields: {sorted(missing)}")
        output.append(row)
    return output, imputed


def run_replay(
    *,
    mapped_results: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_raw: Path,
    ladies_raw: Path,
    men_team_chrono: Path,
    ladies_team_chrono: Path,
    mixed_team_chrono: Path,
    output_dir: Path,
    season: int,
) -> dict[str, object]:
    accepted_inputs = [
        men_chrono,
        ladies_chrono,
        men_raw,
        ladies_raw,
        men_team_chrono,
        ladies_team_chrono,
        mixed_team_chrono,
    ]
    validate_output_dir(output_dir, accepted_inputs)

    def load_streams(
        chrono_path: Path, prefix: str
    ) -> tuple[dict[str, list[dict[str, str]]], list[Path]]:
        paths = {
            stream: chrono_path.parent
            / (f"{prefix}.csv" if stream == "Overall" else f"{prefix}_{stream}.csv")
            for stream in STREAMS
        }
        missing = [path for path in paths.values() if not path.is_file()]
        if missing:
            raise ReplayError(f"missing accepted Elo stream files: {missing}")
        return (
            {stream: _read(path)[1] for stream, path in paths.items()},
            list(paths.values()),
        )

    men_streams, men_stream_paths = load_streams(men_chrono, "M")
    ladies_streams, ladies_stream_paths = load_streams(ladies_chrono, "L")
    inputs = [
        mapped_results,
        men_chrono,
        ladies_chrono,
        men_raw,
        ladies_raw,
        men_team_chrono,
        ladies_team_chrono,
        mixed_team_chrono,
        *men_stream_paths,
        *ladies_stream_paths,
    ]
    before_hashes = {str(path): _sha256(path) for path in inputs}
    mapped_fields, mapped = _read(mapped_results)
    men_fields, men_history = _read(men_chrono)
    ladies_fields, ladies_history = _read(ladies_chrono)
    _, men_raw_rows = _read(men_raw)
    _, ladies_raw_rows = _read(ladies_raw)
    men_team_fields, men_team_history = _read(men_team_chrono)
    ladies_team_fields, ladies_team_history = _read(ladies_team_chrono)
    mixed_fields, mixed_history = _read(mixed_team_chrono)
    required_mapped = {
        "canonical_athlete_id",
        "canonical_name",
        "canonical_sex",
        "identity_match_method",
    }
    if not required_mapped.issubset(mapped_fields):
        raise ReplayError("mapped results are missing canonical identity fields")
    validate_mapped_results(mapped)

    classifications, eligible = classify_acquired_events(
        mapped,
        men_chrono=men_history,
        ladies_chrono=ladies_history,
        men_team_chrono=men_team_history,
        ladies_team_chrono=ladies_team_history,
        mixed_team_chrono=mixed_history,
        men_raw=men_raw_rows,
        ladies_raw=ladies_raw_rows,
    )
    replay_rows = [row for row in mapped if _identity(row) in eligible]
    men_insert = [row for row in replay_rows if row["canonical_sex"] == "M"]
    ladies_insert = [row for row in replay_rows if row["canonical_sex"] == "L"]
    men_candidate, men_k, men_changed = replay_sex(
        men_history,
        men_insert,
        season=season,
        fields=men_fields,
        stream_histories=men_streams,
    )
    ladies_candidate, ladies_k, ladies_changed = replay_sex(
        ladies_history,
        ladies_insert,
        season=season,
        fields=ladies_fields,
        stream_histories=ladies_streams,
    )
    men_team_rows, men_team_imputed = aggregate_same_sex_teams(
        men_candidate,
        season=season,
        sex="M",
        fields=men_team_fields,
    )
    ladies_team_rows, ladies_team_imputed = aggregate_same_sex_teams(
        ladies_candidate,
        season=season,
        sex="L",
        fields=ladies_team_fields,
    )
    men_team_candidate = [
        row for row in men_team_history if int(row["Season"]) < season
    ] + men_team_rows
    ladies_team_candidate = [
        row for row in ladies_team_history if int(row["Season"]) < season
    ] + ladies_team_rows
    for sex, aggregated in (("M", men_team_rows), ("L", ladies_team_rows)):
        expected_groups: dict[tuple[str, str, str], set[str]] = defaultdict(set)
        for row in replay_rows:
            if row["event_kind"] == "team" and row["sex"] == sex:
                expected_groups[
                    (row["date"], row["team_name"], row["team_nation"])
                ].add(row["canonical_athlete_id"])
        expected_members = {
            (event_date, frozenset(member_ids))
            for (event_date, _, _), member_ids in expected_groups.items()
        }
        actual_members = {
            (
                str(row["Date"]),
                frozenset(str(row["MemberIDs"]).split(",")),
            )
            for row in aggregated
        }
        if not expected_members.issubset(actual_members):
            raise ReplayError(
                f"{sex} team aggregation did not preserve every acquired team"
            )
    eligible_dates = {
        identity.event_date for identity in eligible if identity.sex == "Mixed"
    }
    team_fields = mixed_fields
    canonical_mixed_history = [
        row for row in mixed_history if _is_canonical_mixed_team(row)
    ]
    team_candidate = canonical_mixed_history
    imputed_rating_cells = 0
    new_team_rows: list[dict[str, object]] = []
    if eligible_dates:
        team_fields, new_team_rows, imputed_rating_cells = aggregate_mixed_teams(
            men_candidate,
            ladies_candidate,
            season=season,
            eligible_dates=eligible_dates,
        )
        team_candidate = [
            row
            for row in canonical_mixed_history
            if row.get("Date", "") not in eligible_dates
        ] + new_team_rows
    expected_teams = len(
        {
            (row["date"], row["team_name"], row["team_nation"])
            for row in mapped
            if row["date"] in eligible_dates
        }
    )
    if len(new_team_rows) != expected_teams or any(
        int(row["TeamSize"]) != 4 for row in new_team_rows
    ):
        raise ReplayError(
            "mixed-team aggregation did not preserve every four-member team"
        )

    _atomic_csv(output_dir / "candidate_men_chrono.csv", men_fields, men_candidate)
    _atomic_csv(
        output_dir / "candidate_ladies_chrono.csv", ladies_fields, ladies_candidate
    )
    _atomic_csv(
        output_dir / "candidate_men_team_chrono.csv",
        men_team_fields,
        men_team_candidate,
    )
    _atomic_csv(
        output_dir / "candidate_ladies_team_chrono.csv",
        ladies_team_fields,
        ladies_team_candidate,
    )
    _atomic_csv(
        output_dir / "candidate_mixed_team_chrono.csv", team_fields, team_candidate
    )
    after_hashes = {str(path): _sha256(path) for path in inputs}
    if before_hashes != after_hashes:
        raise ReplayError("an accepted input changed during replay")
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.skijump-elo-replay/v1",
        "season": season,
        "accepted_histories_modified": False,
        "promotion_performed": False,
        "input_sha256": before_hashes,
        "event_classifications": classifications,
        "eligible_mixed_team_events": len(eligible_dates),
        "eligible_individual_events": sum(
            identity.event_kind == "individual" for identity in eligible
        ),
        "eligible_same_sex_team_events": {
            sex: sum(
                identity.event_kind == "team" and identity.sex == sex
                for identity in eligible
            )
            for sex in ("M", "L")
        },
        "inserted_member_rows": len(men_insert) + len(ladies_insert),
        "candidate_same_sex_team_rows": {
            "M": len(men_team_candidate),
            "L": len(ladies_team_candidate),
        },
        "candidate_mixed_team_rows": len(team_candidate),
        "imputed_team_rating_cells": {
            "M": men_team_imputed,
            "L": ladies_team_imputed,
            "Mixed": imputed_rating_cells,
        },
        "changed_existing_downstream_rows": {
            "men": men_changed,
            "ladies": ladies_changed,
        },
        "k_factors": {"men": men_k, "ladies": ladies_k},
        "outputs": {
            name: {"path": str(output_dir / name), "sha256": _sha256(output_dir / name)}
            for name in (
                "candidate_men_chrono.csv",
                "candidate_ladies_chrono.csv",
                "candidate_men_team_chrono.csv",
                "candidate_ladies_team_chrono.csv",
                "candidate_mixed_team_chrono.csv",
            )
        },
    }
    _atomic_json(output_dir / "replay_report.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapped-results", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-raw", required=True, type=Path)
    parser.add_argument("--ladies-raw", required=True, type=Path)
    parser.add_argument("--men-team-chrono", required=True, type=Path)
    parser.add_argument("--ladies-team-chrono", required=True, type=Path)
    parser.add_argument("--mixed-team-chrono", required=True, type=Path)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_replay(**vars(args))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
