#!/usr/bin/env python3
"""Validate ski-jumping schedules and derive prediction-weekend rows.

``races.csv`` stores the qualification/start-list date in ``Date`` and the
competition date in ``Final_Date``. ``weekends.csv`` preserves both dates and
adds a leading ``Date`` that identifies the prediction window. This module is
purely local: it validates and derives files but never scrapes or promotes a
candidate into the accepted ``excel365`` directory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
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
REPORT_SCHEMA = "nordicnumbers.skijump-schedule-shadow/v1"
MIN_ROLLOVER_ROW_FRACTION = 0.75
MIN_ROLLOVER_SPAN_DAYS = 100


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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        "span_days": (
            max(value for value, _ in dated_rows)
            - min(value for value, _ in dated_rows)
        ).days,
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


def compare_races(
    candidate_rows: list[dict[str, str]],
    accepted_rows: list[dict[str, str]],
    *,
    max_removed_fraction: float,
) -> dict[str, Any]:
    candidate = {race_key(row): row for row in candidate_rows}
    accepted = {race_key(row): row for row in accepted_rows}
    added = sorted(candidate.keys() - accepted.keys())
    removed = sorted(accepted.keys() - candidate.keys())
    changed = []
    for identity in sorted(candidate.keys() & accepted.keys()):
        fields = [
            field
            for field in RACES_COLUMNS
            if candidate[identity][field] != accepted[identity][field]
        ]
        if fields:
            changed.append({"identity": identity, "fields": fields})
    removed_fraction = len(removed) / len(accepted) if accepted else 0.0
    return {
        "accepted_rows": len(accepted),
        "candidate_rows": len(candidate),
        "added": added,
        "removed": removed,
        "changed": changed,
        "removed_fraction": round(removed_fraction, 6),
        "max_removed_fraction": max_removed_fraction,
        "within_removal_limit": removed_fraction <= max_removed_fraction,
    }


def assess_season_transition(
    candidate_rows: list[dict[str, str]],
    accepted_rows: list[dict[str, str]],
    candidate_structure: dict[str, Any],
    accepted_structure: dict[str, Any],
) -> dict[str, Any]:
    """Classify same-season updates separately from guarded rollovers."""
    candidate_season = candidate_structure["season"]
    accepted_season = accepted_structure["season"]
    if candidate_season == accepted_season:
        return {
            "kind": "same_season",
            "accepted_season": accepted_season,
            "candidate_season": candidate_season,
            "valid": True,
            "approval_required": False,
            "checks": {},
        }

    candidate_keys = {race_key(row) for row in candidate_rows}
    accepted_keys = {race_key(row) for row in accepted_rows}
    row_fraction = len(candidate_rows) / len(accepted_rows)
    checks = {
        "is_next_season": candidate_season == accepted_season + 1,
        "row_fraction": round(row_fraction, 6),
        "minimum_row_fraction": MIN_ROLLOVER_ROW_FRACTION,
        "row_count_sufficient": row_fraction >= MIN_ROLLOVER_ROW_FRACTION,
        "span_days": candidate_structure["span_days"],
        "minimum_span_days": MIN_ROLLOVER_SPAN_DAYS,
        "span_sufficient": (
            candidate_structure["span_days"] >= MIN_ROLLOVER_SPAN_DAYS
        ),
        "identity_overlap": len(candidate_keys & accepted_keys),
        "identities_are_new": not (candidate_keys & accepted_keys),
    }
    valid = all(
        checks[name]
        for name in (
            "is_next_season",
            "row_count_sufficient",
            "span_sufficient",
            "identities_are_new",
        )
    )
    return {
        "kind": "next_season" if checks["is_next_season"] else "unsupported",
        "accepted_season": accepted_season,
        "candidate_season": candidate_season,
        "valid": valid,
        "approval_required": valid,
        "checks": checks,
    }


def write_json_atomic(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def build_shadow(
    candidate_races: Path,
    accepted_races: Path,
    output_root: Path,
    *,
    accepted_weekends: Path | None = None,
    max_removed_fraction: float = 0.1,
    expected_candidate_season: int | None = None,
) -> dict[str, Any]:
    """Build an isolated candidate pair and comparison report; never promote."""
    if not 0 <= max_removed_fraction <= 1:
        raise ScheduleContractError("max_removed_fraction must be between 0 and 1")
    candidate_rows = read_csv(candidate_races, RACES_COLUMNS)
    accepted_rows = read_csv(accepted_races, RACES_COLUMNS)
    accepted_weekends = accepted_weekends or accepted_races.with_name("weekends.csv")
    accepted_weekend_rows = read_csv(accepted_weekends, WEEKENDS_COLUMNS)

    candidate_validation = validate_races(candidate_rows)
    accepted_validation = validate_races(accepted_rows)
    accepted_relationship = validate_relationship(
        accepted_rows,
        accepted_weekend_rows,
    )
    candidate_structure = validate_season_structure(
        candidate_rows,
        expected_season=expected_candidate_season,
    )
    accepted_structure = validate_season_structure(accepted_rows)

    candidate_dir = output_root / "candidate"
    isolated_races = candidate_dir / "races.csv"
    write_csv_atomic(isolated_races, RACES_COLUMNS, candidate_rows)
    weekend_rows = derive_weekends(candidate_rows)
    isolated_weekends = candidate_dir / "weekends.csv"
    write_csv_atomic(
        isolated_weekends,
        WEEKENDS_COLUMNS,
        weekend_rows,
        lineterminator="\n",
    )
    relationship = validate_relationship(candidate_rows, weekend_rows)
    comparison = compare_races(
        candidate_rows,
        accepted_rows,
        max_removed_fraction=max_removed_fraction,
    )
    transition = assess_season_transition(
        candidate_rows,
        accepted_rows,
        candidate_structure,
        accepted_structure,
    )
    if transition["kind"] == "same_season":
        candidate_status = (
            "eligible" if comparison["within_removal_limit"] else "blocked"
        )
        reason = "same-season candidate passed shadow validation"
    elif transition["valid"]:
        candidate_status = "approval_required"
        reason = "valid season rollover requires a separate promotion action"
    else:
        candidate_status = "blocked"
        reason = "candidate does not satisfy season rollover gates"

    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "promotion": {
            "status": candidate_status,
            "performed": False,
            "reason": reason,
        },
        "candidate": {
            "races": {
                **candidate_validation,
                "season_structure": candidate_structure,
                "file": str(isolated_races),
                "sha256": file_sha256(isolated_races),
            },
            "weekends": {
                **relationship,
                "file": str(isolated_weekends),
                "sha256": file_sha256(isolated_weekends),
                "derived_from_races_sha256": file_sha256(isolated_races),
            },
        },
        "accepted": {
            "races": {
                **accepted_validation,
                "file": str(accepted_races),
                "sha256": file_sha256(accepted_races),
            },
            "weekends": {
                **accepted_relationship,
                "file": str(accepted_weekends),
                "sha256": file_sha256(accepted_weekends),
                "derived_from_races_sha256": file_sha256(accepted_races),
            },
            "season_structure": accepted_structure,
        },
        "comparison": comparison,
        "season_transition": transition,
    }
    report_path = output_root / "reports/schedule-shadow.json"
    write_json_atomic(report_path, report)
    return {**report, "report": str(report_path)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    shadow = subparsers.add_parser("shadow", help="build a candidate pair")
    shadow.add_argument("--candidate-races", required=True, type=Path)
    shadow.add_argument("--accepted-races", required=True, type=Path)
    shadow.add_argument("--output-root", required=True, type=Path)
    shadow.add_argument("--max-removed-fraction", type=float, default=0.1)
    shadow.add_argument("--candidate-season", type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = build_shadow(
            args.candidate_races.expanduser().resolve(),
            args.accepted_races.expanduser().resolve(),
            args.output_root.expanduser().resolve(),
            max_removed_fraction=args.max_removed_fraction,
            expected_candidate_season=args.candidate_season,
        )
    except ScheduleContractError as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
