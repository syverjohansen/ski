#!/usr/bin/env python3
"""Map acquired team-result members to canonical Nordic Numbers athletes.

The ordinary cross-country identity mapper remains the single implementation
for name normalization and reviewed aliases.  This wrapper adds team-specific
integrity checks before and after mapping so malformed memberships cannot
reach the Elo replay stage.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from cross_country_results_identity import IdentityError, map_identities

EXPECTED_MEMBERS = {"Rel": 4, "Ts": 2}
TEAM_KEY_FIELDS = (
    "date",
    "sex",
    "season",
    "race",
    "distance",
    "place",
    "team_name",
    "nation",
    "team_bib",
)
REQUIRED_FIELDS = {
    *TEAM_KEY_FIELDS,
    "member_order",
    "fis_competitor_id",
    "fis_code",
    "name",
}


class TeamIdentityError(IdentityError):
    """Raised when team membership is not safe to map or replay."""


@dataclass(frozen=True)
class TeamIdentityContract:
    event_count: int
    team_count: int
    member_rows: int
    relay_teams: int
    team_sprint_teams: int


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(rendered)
        temporary = Path(handle.name)
    temporary.replace(path)


def _positive_integer(
    row: dict[str, str], field: str, *, path: Path, row_number: int
) -> int:
    raw = (row.get(field) or "").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise TeamIdentityError(
            f"{path}:{row_number}: {field} is not an integer: {raw!r}"
        ) from exc
    if value <= 0:
        raise TeamIdentityError(
            f"{path}:{row_number}: {field} must be a positive integer"
        )
    return value


def validate_team_memberships(
    path: Path, *, mapped: bool = False
) -> TeamIdentityContract:
    fields, rows = _read_rows(path)
    required = set(REQUIRED_FIELDS)
    if mapped:
        required |= {"canonical_athlete_id", "canonical_name"}
    missing = sorted(required - set(fields))
    if missing:
        raise TeamIdentityError(f"{path} is missing fields: {missing}")
    if not rows:
        raise TeamIdentityError(f"team results candidate is empty: {path}")

    groups: dict[tuple[str, ...], list[tuple[int, dict[str, str]]]] = defaultdict(list)
    event_athletes: dict[tuple[str, ...], set[str]] = defaultdict(set)
    for row_number, row in enumerate(rows, start=2):
        values = []
        for field in TEAM_KEY_FIELDS:
            value = (row.get(field) or "").strip()
            if not value:
                raise TeamIdentityError(f"{path}:{row_number}: missing {field}")
            values.append(value)
        distance = row["distance"].strip()
        if distance not in EXPECTED_MEMBERS:
            raise TeamIdentityError(
                f"{path}:{row_number}: unsupported team distance {distance!r}"
            )
        _positive_integer(row, "place", path=path, row_number=row_number)
        _positive_integer(row, "team_bib", path=path, row_number=row_number)
        _positive_integer(row, "member_order", path=path, row_number=row_number)
        athlete_field = "canonical_athlete_id" if mapped else "fis_competitor_id"
        athlete_id = (row.get(athlete_field) or "").strip()
        if not athlete_id:
            raise TeamIdentityError(
                f"{path}:{row_number}: missing {athlete_field}"
            )
        key = tuple(values)
        groups[key].append((row_number, row))
        event_key = tuple(row[field].strip() for field in TEAM_KEY_FIELDS[:5])
        if athlete_id in event_athletes[event_key]:
            raise TeamIdentityError(
                f"{path}:{row_number}: athlete {athlete_id} appears more than once "
                f"in event {event_key}"
            )
        event_athletes[event_key].add(athlete_id)

    relay_teams = 0
    team_sprint_teams = 0
    event_teams: dict[tuple[str, ...], int] = defaultdict(int)
    for key, members in groups.items():
        distance = key[4]
        expected = EXPECTED_MEMBERS[distance]
        if len(members) != expected:
            raise TeamIdentityError(
                f"{path}: team {key} has {len(members)} members; expected {expected}"
            )
        orders = sorted(
            _positive_integer(row, "member_order", path=path, row_number=row_number)
            for row_number, row in members
        )
        if orders != list(range(1, expected + 1)):
            raise TeamIdentityError(
                f"{path}: team {key} has invalid member order {orders}"
            )
        event_key = key[:5]
        event_teams[event_key] += 1
        if distance == "Rel":
            relay_teams += 1
        else:
            team_sprint_teams += 1

    for event_key, team_count in event_teams.items():
        if team_count < 3:
            raise TeamIdentityError(
                f"{path}: event {event_key} has only {team_count} classified teams"
            )
    return TeamIdentityContract(
        event_count=len(event_teams),
        team_count=len(groups),
        member_rows=len(rows),
        relay_teams=relay_teams,
        team_sprint_teams=team_sprint_teams,
    )


def map_team_identities(
    *,
    results: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    aliases_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    raw_contract = validate_team_memberships(results)
    identity_report = map_identities(
        results=results,
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        aliases_path=aliases_path,
        output_dir=output_dir,
    )
    mapped_path = output_dir / "mapped_results.csv"
    mapped_contract = validate_team_memberships(mapped_path, mapped=True)
    if raw_contract != mapped_contract:
        raise TeamIdentityError("team membership changed during identity mapping")

    report: dict[str, object] = {
        **identity_report,
        "schema_version": "nordicnumbers.cross-country-team-results-identity/v1",
        "input_sha256": hashlib.sha256(results.read_bytes()).hexdigest(),
        "event_count": mapped_contract.event_count,
        "team_count": mapped_contract.team_count,
        "member_rows": mapped_contract.member_rows,
        "relay_teams": mapped_contract.relay_teams,
        "team_sprint_teams": mapped_contract.team_sprint_teams,
        "team_membership_status": "validated",
    }
    _atomic_json(output_dir / "report.json", report)
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
    report = map_team_identities(
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
