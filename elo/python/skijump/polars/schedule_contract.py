#!/usr/bin/env python3
"""Validate ski-jumping schedules and derive prediction-weekend rows.

``races.csv`` stores the qualification/start-list date in ``Date`` and the
competition date in ``Final_Date``. ``weekends.csv`` preserves both dates and
adds a leading ``Date`` that identifies the prediction window. This module is
purely local: it validates and derives files but never scrapes or promotes a
candidate into the accepted ``excel365`` directory.
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
    "Final_Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "HillSize",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
    "Race",
)
WEEKENDS_COLUMNS = ("Date", "Race_Date", *RACES_COLUMNS[1:])
ALLOWED_SEXES = {"M", "L", "Mixed"}
ALLOWED_RACE_TYPES = {
    "Normal",
    "Large",
    "Flying",
    "Team",
    "Team Normal",
    "Team Large",
}
ALLOWED_PERIODS = {"1", "2", "3", "4", "5"}
ALLOWED_CHAMPIONSHIP = {"0", "1"}


class ScheduleContractError(RuntimeError):
    """A ski-jumping schedule violates its compatibility contract."""


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
        mode="w",
        encoding="utf-8",
        newline="",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            lineterminator=lineterminator,
        )
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def race_key(row: dict[str, str]) -> str:
    query = parse_qs(urlparse(row["Startlist"]).query)
    race_ids = query.get("raceid", [])
    if race_ids and race_ids[0].strip():
        return f"fis:{race_ids[0].strip()}"
    return "|".join(
        row[field].strip()
        for field in ("Date", "Sex", "City", "HillSize", "RaceType", "Race")
    )


def _positive_integer(value: str, *, field: str, row_number: int) -> None:
    try:
        if int(value) < 1:
            raise ValueError
    except ValueError as exc:
        raise ScheduleContractError(
            f"invalid {field} at row {row_number}: {value!r}"
        ) from exc


def validate_races(rows: list[dict[str, str]]) -> dict[str, Any]:
    identities: set[str] = set()
    sex_race_numbers: set[tuple[str, str]] = set()
    dates: list[date] = []
    for row_number, row in enumerate(rows, start=2):
        missing = [field for field in RACES_COLUMNS if not row[field].strip()]
        if missing:
            raise ScheduleContractError(
                f"blank required fields at race row {row_number}: {missing}"
            )
        qualification_date = parse_date(
            row["Date"], field="Date", row_number=row_number
        )
        final_date = parse_date(
            row["Final_Date"], field="Final_Date", row_number=row_number
        )
        if final_date < qualification_date:
            raise ScheduleContractError(
                f"Final_Date precedes Date at race row {row_number}"
            )
        dates.append(qualification_date)
        if row["Sex"] not in ALLOWED_SEXES:
            raise ScheduleContractError(
                f"invalid Sex at race row {row_number}: {row['Sex']!r}"
            )
        if row["RaceType"] not in ALLOWED_RACE_TYPES:
            raise ScheduleContractError(
                f"invalid RaceType at race row {row_number}: {row['RaceType']!r}"
            )
        if row["Period"] not in ALLOWED_PERIODS:
            raise ScheduleContractError(
                f"invalid Period at race row {row_number}: {row['Period']!r}"
            )
        if row["Championship"] not in ALLOWED_CHAMPIONSHIP:
            raise ScheduleContractError(
                f"invalid Championship at race row {row_number}: "
                f"{row['Championship']!r}"
            )
        _positive_integer(row["HillSize"], field="HillSize", row_number=row_number)
        _positive_integer(row["Elevation"], field="Elevation", row_number=row_number)
        _positive_integer(row["Race"], field="Race", row_number=row_number)
        parsed_url = urlparse(row["Startlist"])
        query = parse_qs(parsed_url.query)
        if (
            parsed_url.scheme != "https"
            or parsed_url.netloc != "www.fis-ski.com"
            or query.get("sectorcode") != ["JP"]
            or not query.get("raceid", [""])[0].strip()
        ):
            raise ScheduleContractError(
                f"invalid FIS ski-jumping Startlist URL at race row {row_number}"
            )
        identity = race_key(row)
        if identity in identities:
            raise ScheduleContractError(f"duplicate race identity: {identity}")
        identities.add(identity)
        sex_race = (row["Sex"], row["Race"])
        if sex_race in sex_race_numbers:
            raise ScheduleContractError(
                f"duplicate Race number within Sex: {sex_race}"
            )
        sex_race_numbers.add(sex_race)
    if dates != sorted(dates):
        raise ScheduleContractError("races are not sorted by qualification Date")
    return {
        "status": "passed",
        "rows": len(rows),
        "first_date": min(dates).isoformat(),
        "last_date": max(dates).isoformat(),
        "identities": sorted(identities),
    }


def infer_fis_season(rows: list[dict[str, str]]) -> int:
    """Infer the July-through-June FIS season from qualification dates."""
    dates = [
        parse_date(row["Date"], field="Date", row_number=index)
        for index, row in enumerate(rows, start=2)
    ]
    season = max(value.year for value in dates)
    season_start = date(season - 1, 7, 1)
    season_end = date(season, 6, 30)
    outside = [value for value in dates if not season_start <= value <= season_end]
    if outside:
        raise ScheduleContractError(
            f"race dates fall outside inferred FIS season {season}: {outside[:3]}"
        )
    return season


def validate_season_structure(
    rows: list[dict[str, str]],
    *,
    expected_season: int | None = None,
) -> dict[str, Any]:
    """Validate the accepted season boundary and championship period rules."""
    validate_races(rows)
    season = infer_fis_season(rows) if expected_season is None else expected_season
    season_start = date(season - 1, 7, 1)
    season_end = date(season, 6, 30)
    dated_rows = [
        (
            parse_date(row["Date"], field="Date", row_number=index),
            row,
        )
        for index, row in enumerate(rows, start=2)
    ]
    outside = [
        race_date
        for race_date, _ in dated_rows
        if not season_start <= race_date <= season_end
    ]
    if outside:
        raise ScheduleContractError(
            f"race dates fall outside requested FIS season {season}: {outside[:3]}"
        )

    sex_counts = {
        sex: sum(1 for row in rows if row["Sex"] == sex)
        for sex in ("M", "L", "Mixed")
    }
    missing = [sex for sex in ("M", "L") if sex_counts[sex] == 0]
    if missing:
        raise ScheduleContractError(
            f"season {season} is missing individual fields for sexes: {missing}"
        )

    championship_rows = [
        (race_date, row)
        for race_date, row in dated_rows
        if row["Championship"] == "1"
    ]
    expects_championship = championship_category(season) is not None
    if expects_championship and not championship_rows:
        raise ScheduleContractError(
            f"season {season} is missing its championship races"
        )
    if not expects_championship and championship_rows:
        raise ScheduleContractError(
            f"season {season} unexpectedly contains championship races"
        )
    if any(row["Period"] != "4" for _, row in championship_rows):
        raise ScheduleContractError("championship races must use Period 4")

    championship_end = (
        max(race_date for race_date, _ in championship_rows)
        if championship_rows
        else None
    )
    for race_date, row in dated_rows:
        if row["Championship"] == "1":
            continue
        allowed = (
            {"5"}
            if championship_end and race_date > championship_end
            else {"1", "2", "3"}
        )
        if row["Period"] not in allowed:
            raise ScheduleContractError(
                f"invalid period around championship at {row['Date']}: "
                f"{row['Period']}"
            )

    return {
        "status": "passed",
        "season": season,
        "first_date": min(value for value, _ in dated_rows).isoformat(),
        "last_date": max(value for value, _ in dated_rows).isoformat(),
        "sex_counts": sex_counts,
        "championship_rows": len(championship_rows),
        "championship_category": (
            championship_category(season)[0] if expects_championship else None
        ),
    }


def _event_family(row: dict[str, str]) -> str:
    return "team" if "Team" in row["RaceType"] else "individual"


def derive_weekends(race_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Derive the legacy prediction-window view from canonical race rows."""
    validate_races(race_rows)
    parsed_dates = {
        race_key(row): parse_date(row["Date"], field="Date", row_number=index)
        for index, row in enumerate(race_rows, start=2)
    }
    window_start: dict[str, date] = {}
    regular_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    championship_groups: dict[
        tuple[str, str, str], list[dict[str, str]]
    ] = defaultdict(list)

    for row in race_rows:
        family = _event_family(row)
        if row["Championship"] == "1":
            race_type = row["RaceType"] if family == "individual" else "team"
            championship_groups[(row["City"], family, race_type)].append(row)
        else:
            regular_groups[(row["City"], family)].append(row)

    for rows in regular_groups.values():
        ordered = sorted(rows, key=lambda row: parsed_dates[race_key(row)])
        groups: list[list[dict[str, str]]] = []
        current: list[dict[str, str]] = []
        for row in ordered:
            if current:
                previous = parsed_dates[race_key(current[-1])]
                current_date = parsed_dates[race_key(row)]
                if (current_date - previous).days > 14:
                    groups.append(current)
                    current = []
            current.append(row)
        if current:
            groups.append(current)
        for group in groups:
            start = min(parsed_dates[race_key(row)] for row in group)
            for row in group:
                window_start[race_key(row)] = start

    for rows in championship_groups.values():
        start = min(parsed_dates[race_key(row)] for row in rows)
        for row in rows:
            window_start[race_key(row)] = start

    result = [
        {
            "Date": window_start[race_key(row)].strftime("%m/%d/%Y"),
            "Race_Date": row["Date"],
            **{field: row[field] for field in RACES_COLUMNS[1:]},
        }
        for row in race_rows
    ]
    return sorted(
        result,
        key=lambda row: (
            parse_date(row["Date"], field="Date", row_number=0),
            parse_date(row["Race_Date"], field="Race_Date", row_number=0),
        ),
    )


def validate_relationship(
    race_rows: list[dict[str, str]],
    weekend_rows: list[dict[str, str]],
) -> dict[str, Any]:
    if len(race_rows) != len(weekend_rows):
        raise ScheduleContractError(
            "weekends must contain exactly one row per race: "
            f"{len(weekend_rows)} != {len(race_rows)}"
        )
    expected = derive_weekends(race_rows)
    if weekend_rows != expected:
        raise ScheduleContractError(
            "weekends do not match the deterministic view derived from races"
        )
    return {"status": "passed", "rows": len(weekend_rows)}
