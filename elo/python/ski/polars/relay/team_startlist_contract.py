#!/usr/bin/env python3
"""Validate relay and team-sprint startlists before prediction."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path

EVENT_MEMBERS = {"relay": 4, "team_sprint": 2}
EVENT_LABELS = {"relay": "Relay", "team_sprint": "Team Sprint"}
TRUE_VALUES = {"1", "true", "yes"}


class TeamStartlistContractError(RuntimeError):
    """Raised when a team-event startlist is unsafe for prediction."""


@dataclass(frozen=True)
class TeamStartlistReport:
    event_type: str
    team_rows: int
    individual_rows: int
    race_dates: tuple[str, ...]
    teams: tuple[str, ...]


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _require_columns(path: Path, fields: list[str], required: set[str]) -> None:
    missing = sorted(required - set(fields))
    if missing:
        raise TeamStartlistContractError(f"{path} is missing columns: {missing}")


def _required(row: dict[str, str], field: str, path: Path, row_number: int) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise TeamStartlistContractError(
            f"{path}:{row_number}: missing required {field}"
        )
    return value


def _positive_number(
    row: dict[str, str], field: str, path: Path, row_number: int
) -> float:
    raw = _required(row, field, path, row_number)
    try:
        value = float(raw)
    except ValueError as exc:
        raise TeamStartlistContractError(
            f"{path}:{row_number}: {field} is not numeric: {raw!r}"
        ) from exc
    if not math.isfinite(value) or value <= 0:
        raise TeamStartlistContractError(
            f"{path}:{row_number}: {field} must be a positive finite number"
        )
    return value


def validate_team_startlists(
    *, teams: Path, individuals: Path, event_type: str
) -> TeamStartlistReport:
    try:
        member_count = EVENT_MEMBERS[event_type]
        event_label = EVENT_LABELS[event_type]
    except KeyError as exc:
        raise TeamStartlistContractError(
            f"unsupported team event type: {event_type}"
        ) from exc

    team_fields, team_rows = _read_csv(teams)
    individual_fields, individual_rows = _read_csv(individuals)
    member_fields = {
        field
        for number in range(1, member_count + 1)
        for field in (
            f"Member_{number}",
            f"Member_{number}_ID",
            f"Member_{number}_Elo",
        )
    }
    _require_columns(
        teams,
        team_fields,
        {
            "Team_Name",
            "Nation",
            "Race_Date",
            "Is_Present",
            "Race_Type",
            "Elo",
            *member_fields,
        },
    )
    _require_columns(
        individuals,
        individual_fields,
        {
            "Skier",
            "ID",
            "Team_Name",
            "Race_Date",
            "In_FIS_List",
            "Elo",
        },
    )
    if not team_rows or not individual_rows:
        raise TeamStartlistContractError("team startlists must not be empty")

    individual_index: dict[tuple[str, str, str], dict[str, str]] = {}
    athlete_dates: set[tuple[str, str]] = set()
    for row_number, row in enumerate(individual_rows, start=2):
        race_date = _required(row, "Race_Date", individuals, row_number)
        team_name = _required(row, "Team_Name", individuals, row_number)
        athlete_id = _required(row, "ID", individuals, row_number)
        _required(row, "Skier", individuals, row_number)
        _positive_number(row, "Elo", individuals, row_number)
        if (row.get("In_FIS_List") or "").strip().lower() not in TRUE_VALUES:
            raise TeamStartlistContractError(
                f"{individuals}:{row_number}: athlete is not confirmed on FIS startlist"
            )
        key = (race_date, team_name, athlete_id)
        if key in individual_index:
            raise TeamStartlistContractError(f"duplicate individual startlist row: {key}")
        athlete_date = (race_date, athlete_id)
        if athlete_date in athlete_dates:
            raise TeamStartlistContractError(
                f"athlete {athlete_id} appears on multiple teams for {race_date}"
            )
        individual_index[key] = row
        athlete_dates.add(athlete_date)

    expected_individuals: set[tuple[str, str, str]] = set()
    seen_teams: set[tuple[str, str]] = set()
    dates: set[str] = set()
    names: set[str] = set()
    for row_number, row in enumerate(team_rows, start=2):
        race_date = _required(row, "Race_Date", teams, row_number)
        team_name = _required(row, "Team_Name", teams, row_number)
        _required(row, "Nation", teams, row_number)
        if (row.get("Is_Present") or "").strip().lower() not in TRUE_VALUES:
            raise TeamStartlistContractError(
                f"{teams}:{row_number}: team is not confirmed on FIS startlist"
            )
        if _required(row, "Race_Type", teams, row_number) != event_label:
            raise TeamStartlistContractError(
                f"{teams}:{row_number}: expected Race_Type {event_label!r}"
            )
        team_key = (race_date, team_name)
        if team_key in seen_teams:
            raise TeamStartlistContractError(f"duplicate team startlist row: {team_key}")
        seen_teams.add(team_key)
        member_ids: set[str] = set()
        member_elos: list[float] = []
        for number in range(1, member_count + 1):
            member_name = _required(row, f"Member_{number}", teams, row_number)
            member_id = _required(row, f"Member_{number}_ID", teams, row_number)
            member_elos.append(
                _positive_number(row, f"Member_{number}_Elo", teams, row_number)
            )
            if member_id in member_ids:
                raise TeamStartlistContractError(
                    f"{teams}:{row_number}: duplicate member ID {member_id}"
                )
            member_ids.add(member_id)
            individual_key = (race_date, team_name, member_id)
            individual = individual_index.get(individual_key)
            if individual is None:
                raise TeamStartlistContractError(
                    f"{teams}:{row_number}: member {member_id} has no matching individual row"
                )
            if individual["Skier"].strip() != member_name:
                raise TeamStartlistContractError(
                    f"{teams}:{row_number}: member name mismatch for ID {member_id}"
                )
            expected_individuals.add(individual_key)
        team_elo = _positive_number(row, "Elo", teams, row_number)
        if not math.isclose(team_elo, sum(member_elos), rel_tol=0.0, abs_tol=1e-6):
            raise TeamStartlistContractError(
                f"{teams}:{row_number}: team Elo does not equal member Elo sum"
            )
        dates.add(race_date)
        names.add(team_name)

    extras = sorted(set(individual_index) - expected_individuals)
    if extras:
        raise TeamStartlistContractError(
            f"individual startlist contains {len(extras)} athletes not assigned to a team"
        )
    return TeamStartlistReport(
        event_type=event_type,
        team_rows=len(team_rows),
        individual_rows=len(individual_rows),
        race_dates=tuple(sorted(dates)),
        teams=tuple(sorted(names)),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--teams", required=True, type=Path)
    parser.add_argument("--individuals", required=True, type=Path)
    parser.add_argument("--event-type", required=True, choices=sorted(EVENT_MEMBERS))
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = validate_team_startlists(
        teams=args.teams, individuals=args.individuals, event_type=args.event_type
    )
    rendered = json.dumps(asdict(report), indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
