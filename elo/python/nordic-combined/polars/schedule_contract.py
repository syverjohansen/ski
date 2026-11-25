#!/usr/bin/env python3
"""Validate Nordic-combined schedules and derive legacy prediction weekends.

This module is deliberately local and read-only with respect to accepted data.
It defines the compatibility boundary that future FIS acquisition and guarded
season promotion must pass.
"""

from __future__ import annotations

import csv
import os
import tempfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from schedule_sources import championship_category

RACES_COLUMNS = (
    "Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
    "Race",
)
WEEKENDS_COLUMNS = ("Date", "Race_Date", *RACES_COLUMNS[1:])
ALLOWED_SEXES = {"M", "L", "Mixed"}
ALLOWED_RACE_TYPES = {"Individual", "IndividualCompact", "Mass Start", "Team", "Team Sprint"}
ALLOWED_PERIODS = {"1", "2", "3", "4", "5"}


class ScheduleContractError(RuntimeError):
    """A Nordic-combined schedule violates its compatibility contract."""


def parse_date(value: str, *, field: str, row_number: int) -> date:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as exc:
        raise ScheduleContractError(
            f"invalid {field} at row {row_number}: {value!r}"
        ) from exc


def read_csv(path: Path, expected_columns: tuple[str, ...]) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            observed = tuple(reader.fieldnames or ())
            if observed != expected_columns:
                raise ScheduleContractError(
                    f"unexpected columns in {path}: {observed}; expected {expected_columns}"
                )
            rows = [dict(row) for row in reader]
    except OSError as exc:
        raise ScheduleContractError(f"cannot read {path}: {exc}") from exc
    if not rows:
        raise ScheduleContractError(f"schedule contains no rows: {path}")
    return rows


def write_csv_atomic(
    path: Path,
    columns: tuple[str, ...],
    rows: list[dict[str, str]],
    *,
    lineterminator: str = "\r\n",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator=lineterminator)
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def race_key(row: dict[str, str]) -> str:
    race_ids = parse_qs(urlparse(row["Startlist"]).query).get("raceid", [])
    if race_ids and race_ids[0].strip():
        return f"fis:{race_ids[0].strip()}"
    return "|".join(row[field].strip() for field in ("Date", "Sex", "City", "RaceType", "Race"))


def _positive_integer(value: str, *, field: str, row_number: int) -> None:
    try:
        if int(value) < 1:
            raise ValueError
    except ValueError as exc:
        raise ScheduleContractError(
            f"invalid {field} at row {row_number}: {value!r}"
        ) from exc


def _distance(value: str, *, row_number: int) -> None:
    if value == "N/A":
        return
    try:
        if float(value) < 0:
            raise ValueError
    except ValueError as exc:
        raise ScheduleContractError(
            f"invalid Distance at row {row_number}: {value!r}"
        ) from exc


def validate_races(rows: list[dict[str, str]]) -> dict[str, Any]:
    identities: set[str] = set()
    sex_races: set[tuple[str, str]] = set()
    dates: list[date] = []
    for row_number, row in enumerate(rows, start=2):
        missing = [field for field in RACES_COLUMNS if not row[field].strip()]
        if missing:
            raise ScheduleContractError(
                f"blank required fields at race row {row_number}: {missing}"
            )
        dates.append(parse_date(row["Date"], field="Date", row_number=row_number))
        if row["Sex"] not in ALLOWED_SEXES:
            raise ScheduleContractError(f"invalid Sex at race row {row_number}: {row['Sex']!r}")
        if row["RaceType"] not in ALLOWED_RACE_TYPES:
            raise ScheduleContractError(
                f"invalid RaceType at race row {row_number}: {row['RaceType']!r}"
            )
        if row["Period"] not in ALLOWED_PERIODS:
            raise ScheduleContractError(f"invalid Period at race row {row_number}")
        if row["Championship"] not in {"0", "1"}:
            raise ScheduleContractError(f"invalid Championship at race row {row_number}")
        _distance(row["Distance"], row_number=row_number)
        _positive_integer(row["Elevation"], field="Elevation", row_number=row_number)
        _positive_integer(row["Race"], field="Race", row_number=row_number)
        parsed = urlparse(row["Startlist"])
        query = parse_qs(parsed.query)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "www.fis-ski.com"
            or query.get("sectorcode") != ["NK"]
            or not query.get("raceid", [""])[0].strip()
        ):
            raise ScheduleContractError(
                f"invalid FIS Nordic-combined Startlist URL at race row {row_number}"
            )
        identity = race_key(row)
        if identity in identities:
            raise ScheduleContractError(f"duplicate race identity: {identity}")
        identities.add(identity)
        sex_race = (row["Sex"], row["Race"])
        if sex_race in sex_races:
            raise ScheduleContractError(f"duplicate Race number within Sex: {sex_race}")
        sex_races.add(sex_race)
    if dates != sorted(dates):
        raise ScheduleContractError("races are not sorted by Date")
    return {
        "status": "passed",
        "rows": len(rows),
        "first_date": min(dates).isoformat(),
        "last_date": max(dates).isoformat(),
        "identities": sorted(identities),
    }


def infer_fis_season(rows: list[dict[str, str]]) -> int:
    dates = [parse_date(row["Date"], field="Date", row_number=i) for i, row in enumerate(rows, 2)]
    season = max(value.year for value in dates)
    if any(not date(season - 1, 7, 1) <= value <= date(season, 6, 30) for value in dates):
        raise ScheduleContractError(f"race dates fall outside inferred FIS season {season}")
    return season


def validate_season_structure(
    rows: list[dict[str, str]], *, expected_season: int | None = None
) -> dict[str, Any]:
    validate_races(rows)
    season = infer_fis_season(rows) if expected_season is None else expected_season
    dated = [(parse_date(row["Date"], field="Date", row_number=i), row) for i, row in enumerate(rows, 2)]
    if any(not date(season - 1, 7, 1) <= value <= date(season, 6, 30) for value, _ in dated):
        raise ScheduleContractError(f"race dates fall outside requested FIS season {season}")
    counts = {sex: sum(row["Sex"] == sex for _, row in dated) for sex in ALLOWED_SEXES}
    if not counts["M"] or not counts["L"]:
        raise ScheduleContractError(f"season {season} must contain men and ladies")
    championships = [(value, row) for value, row in dated if row["Championship"] == "1"]
    expected = championship_category(season)
    if bool(expected) != bool(championships):
        raise ScheduleContractError(f"season {season} championship structure is incomplete")
    if any(row["Period"] != "4" for _, row in championships):
        raise ScheduleContractError("championship races must use Period 4")
    championship_end_by_sex = {
        sex: max(
            (value for value, row in championships if row["Sex"] == sex),
            default=None,
        )
        for sex in ALLOWED_SEXES
    }
    for value, row in dated:
        if row["Championship"] == "1":
            continue
        championship_end = championship_end_by_sex[row["Sex"]]
        if championship_end is None:
            allowed = ALLOWED_PERIODS
        else:
            allowed = {"5"} if value > championship_end else {"1", "2", "3"}
        if row["Period"] not in allowed:
            raise ScheduleContractError(f"invalid period around championship at {row['Date']}")
    return {
        "status": "passed",
        "season": season,
        "sex_counts": counts,
        "championship_rows": len(championships),
        "championship_category": expected[0] if expected else None,
    }


def _event_family(row: dict[str, str]) -> str:
    return "team" if "Team" in row["RaceType"] else "individual"


def _weekend_distance(value: str) -> str:
    """Reproduce the accepted pandas round trip used by legacy automation."""
    return "" if value == "N/A" else str(float(value))


def derive_weekends(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    validate_races(rows)
    parsed = {race_key(row): parse_date(row["Date"], field="Date", row_number=i) for i, row in enumerate(rows, 2)}
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        family = _event_family(row)
        championship_type = row["RaceType"] if row["Championship"] == "1" else ""
        groups[(row["City"], family, championship_type)].append(row)
    starts: dict[str, date] = {}
    for group in groups.values():
        ordered = sorted(group, key=lambda row: parsed[race_key(row)])
        chunks: list[list[dict[str, str]]] = []
        current: list[dict[str, str]] = []
        for row in ordered:
            if current and (parsed[race_key(row)] - parsed[race_key(current[-1])]).days > 14:
                chunks.append(current)
                current = []
            current.append(row)
        if current:
            chunks.append(current)
        for chunk in chunks:
            start = min(parsed[race_key(row)] for row in chunk)
            for row in chunk:
                starts[race_key(row)] = start
    result = []
    for row in rows:
        output = {
            "Date": starts[race_key(row)].strftime("%m/%d/%Y"),
            "Race_Date": row["Date"],
            **{field: row[field] for field in RACES_COLUMNS[1:]},
        }
        output["Distance"] = _weekend_distance(row["Distance"])
        result.append(output)
    return sorted(
        result,
        key=lambda row: (
            parse_date(row["Date"], field="Date", row_number=0),
            parse_date(row["Race_Date"], field="Race_Date", row_number=0),
        ),
    )


def validate_relationship(
    races: list[dict[str, str]], weekends: list[dict[str, str]]
) -> dict[str, Any]:
    if weekends != derive_weekends(races):
        raise ScheduleContractError("weekends do not match the deterministic view derived from races")
    return {"status": "passed", "rows": len(weekends)}
