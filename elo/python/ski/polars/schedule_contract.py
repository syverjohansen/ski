#!/usr/bin/env python3
"""Build and validate cross-country race and prediction-window schedules.

``races.csv`` is the canonical one-row-per-race schedule. ``weekends.csv`` is
a deterministic compatibility view: ``Race_Date`` retains the race date while
``Date`` identifies the start of the prediction window. Tour de Ski stages use
their own race dates; regular events group individual, relay, and team-sprint
races separately; championships group individual races by discipline.

This module writes only to an isolated output root. Promotion into ``excel365``
is deliberately left to a separate authorized step.
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


RACES_COLUMNS = (
    "Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "Technique",
    "MS",
    "Period",
    "Pursuit",
    "Stage",
    "Final_Climb",
    "Elevation",
    "Race",
    "Championship",
)
WEEKENDS_COLUMNS = ("Date", "Race_Date", *RACES_COLUMNS[1:])
ALLOWED_SEXES = {"M", "L"}
ALLOWED_TECHNIQUES = {"C", "F", "P", "N/A"}
BINARY_FIELDS = ("MS", "Pursuit", "Stage", "Final_Climb", "Championship")
ALLOWED_PERIODS = {"1", "2", "3", "4", "5"}
REPORT_SCHEMA = "nordicnumbers.cross-country-schedule-shadow/v1"
MIN_ROLLOVER_ROW_FRACTION = 0.70
MIN_ROLLOVER_SPAN_DAYS = 90


class ScheduleContractError(RuntimeError):
    """A schedule cannot be safely built or validated."""


def parse_date(value: str, *, field: str, row_number: int) -> date:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as exc:
        raise ScheduleContractError(
            f"invalid {field} at row {row_number}: {value!r}"
        ) from exc


def read_csv(path: Path, expected_columns: tuple[str, ...]) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
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
    lineterminator: str,
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
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator=lineterminator)
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_json_atomic(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
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
    race_ids = parse_qs(urlparse(row["Startlist"]).query).get("raceid", [])
    if race_ids and race_ids[0].strip():
        return f"fis:{race_ids[0].strip()}"
    return "|".join(row[field].strip() for field in ("Date", "Sex", "City", "Race"))


def championship_category(season: int) -> str | None:
    if season % 4 == 2:
        return "OWG"
    if season % 2 == 1:
        return "WSC"
    return None


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
        race_date = parse_date(row["Date"], field="Date", row_number=row_number)
        dates.append(race_date)
        if row["Sex"] not in ALLOWED_SEXES:
            raise ScheduleContractError(f"invalid Sex at row {row_number}: {row['Sex']!r}")
        if row["Technique"] not in ALLOWED_TECHNIQUES:
            raise ScheduleContractError(
                f"invalid Technique at row {row_number}: {row['Technique']!r}"
            )
        if row["Period"] not in ALLOWED_PERIODS:
            raise ScheduleContractError(
                f"invalid Period at row {row_number}: {row['Period']!r}"
            )
        for field in BINARY_FIELDS:
            if row[field] not in {"0", "1"}:
                raise ScheduleContractError(
                    f"invalid {field} at row {row_number}: {row[field]!r}"
                )
        try:
            if int(row["Race"]) < 1 or float(row["Elevation"]) < 0:
                raise ValueError
        except ValueError as exc:
            raise ScheduleContractError(
                f"invalid numeric field at row {row_number}"
            ) from exc
        parsed = urlparse(row["Startlist"])
        query = parse_qs(parsed.query)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "www.fis-ski.com"
            or query.get("sectorcode") != ["CC"]
            or not query.get("raceid")
        ):
            raise ScheduleContractError(f"invalid FIS Startlist URL at row {row_number}")
        identity = race_key(row)
        if identity in identities:
            raise ScheduleContractError(f"duplicate race identity: {identity}")
        identities.add(identity)
        sex_race = (row["Sex"], row["Race"])
        if sex_race in sex_races:
            raise ScheduleContractError(f"duplicate Race within Sex: {sex_race}")
        sex_races.add(sex_race)
        if row["Distance"] == "Rel" and row["Technique"] != "N/A":
            raise ScheduleContractError("relay races must use Technique N/A")
        if row["Stage"] == "1" and row["Period"] != "2":
            raise ScheduleContractError("stage races must use Period 2")
        if row["Final_Climb"] == "1" and not (
            row["Stage"] == "1" and row["MS"] == "1" and row["Technique"] == "F"
        ):
            raise ScheduleContractError("Final Climb flags are inconsistent")
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
    dates = [
        parse_date(row["Date"], field="Date", row_number=index)
        for index, row in enumerate(rows, start=2)
    ]
    # FIS names a winter season for the calendar year in which it ends. This
    # also works for partial schedules containing only autumn races.
    season = max(value.year + 1 if value.month >= 7 else value.year for value in dates)
    outside = [value for value in dates if not date(season - 1, 7, 1) <= value <= date(season, 6, 30)]
    if outside:
        raise ScheduleContractError(
            f"race dates fall outside inferred FIS season {season}: {outside[:3]}"
        )
    return season


def validate_season_structure(
    rows: list[dict[str, str]], *, expected_season: int | None = None
) -> dict[str, Any]:
    dated = [
        (parse_date(row["Date"], field="Date", row_number=index), row)
        for index, row in enumerate(rows, start=2)
    ]
    season = expected_season if expected_season is not None else infer_fis_season(rows)
    outside = [
        value for value, _ in dated if not date(season - 1, 7, 1) <= value <= date(season, 6, 30)
    ]
    if outside:
        raise ScheduleContractError(
            f"race dates fall outside requested FIS season {season}: {outside[:3]}"
        )
    sex_counts = {sex: sum(row["Sex"] == sex for row in rows) for sex in sorted(ALLOWED_SEXES)}
    if any(count == 0 for count in sex_counts.values()):
        raise ScheduleContractError(f"season {season} must contain both sexes")
    championships = [(value, row) for value, row in dated if row["Championship"] == "1"]
    expected_championship = championship_category(season)
    if bool(expected_championship) != bool(championships):
        raise ScheduleContractError(
            f"season {season} championship coverage does not match {expected_championship}"
        )
    if any(row["Period"] != "4" for _, row in championships):
        raise ScheduleContractError("championship races must use Period 4")
    first = min(value for value, _ in dated)
    last = max(value for value, _ in dated)
    return {
        "status": "passed",
        "season": season,
        "first_date": first.isoformat(),
        "last_date": last.isoformat(),
        "span_days": (last - first).days,
        "sex_counts": sex_counts,
        "championship_rows": len(championships),
        "championship_category": expected_championship,
    }


def _event_type(row: dict[str, str]) -> str:
    return "relay" if row["Distance"] == "Rel" else "team_sprint" if row["Distance"] == "Ts" else "individual"


def derive_weekends(race_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Reproduce the legacy cross-country prediction-window view."""
    validate_races(race_rows)
    indexed = list(enumerate(race_rows))
    dates = {race_key(row): parse_date(row["Date"], field="Date", row_number=i + 2) for i, row in indexed}
    starts: dict[str, date] = {}

    for _, row in indexed:
        if row["Period"] == "2":
            starts[race_key(row)] = dates[race_key(row)]

    regular: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    championships: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for _, row in indexed:
        if row["Championship"] == "1":
            event = _event_type(row)
            key = (row["City"], event)
            if event == "individual":
                key += (row["Distance"], row["Technique"])
            championships[key].append(row)
        elif row["Period"] != "2":
            regular[(row["City"], _event_type(row))].append(row)

    for rows in regular.values():
        ordered = sorted(rows, key=lambda row: dates[race_key(row)])
        groups: list[list[dict[str, str]]] = []
        current: list[dict[str, str]] = []
        for row in ordered:
            if current and (dates[race_key(row)] - dates[race_key(current[-1])]).days > 14:
                groups.append(current)
                current = []
            current.append(row)
        if current:
            groups.append(current)
        for group in groups:
            start = min(dates[race_key(row)] for row in group)
            for row in group:
                starts[race_key(row)] = start

    for rows in championships.values():
        start = min(dates[race_key(row)] for row in rows)
        for row in rows:
            starts[race_key(row)] = start

    result = []
    for order, row in indexed:
        compatibility = {field: row[field] for field in RACES_COLUMNS[1:]}
        if compatibility["Technique"] == "N/A":
            compatibility["Technique"] = ""
        result.append(
            {
                "Date": starts[race_key(row)].strftime("%m/%d/%Y"),
                "Race_Date": row["Date"],
                **compatibility,
                "_order": str(order),
            }
        )
    result.sort(
        key=lambda row: (
            parse_date(row["Date"], field="Date", row_number=0),
            parse_date(row["Race_Date"], field="Race_Date", row_number=0),
            int(row["_order"]),
        )
    )
    for row in result:
        row.pop("_order")
    return result


def validate_relationship(
    race_rows: list[dict[str, str]], weekend_rows: list[dict[str, str]]
) -> dict[str, Any]:
    if len(race_rows) != len(weekend_rows):
        raise ScheduleContractError("weekends must contain exactly one row per race")
    if weekend_rows != derive_weekends(race_rows):
        raise ScheduleContractError("weekends do not match the deterministic view derived from races")
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
    changed = [
        {"identity": identity, "fields": [field for field in RACES_COLUMNS if candidate[identity][field] != accepted[identity][field]]}
        for identity in sorted(candidate.keys() & accepted.keys())
        if candidate[identity] != accepted[identity]
    ]
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


def assess_transition(
    candidate_rows: list[dict[str, str]],
    accepted_rows: list[dict[str, str]],
    candidate_structure: dict[str, Any],
    accepted_structure: dict[str, Any],
) -> dict[str, Any]:
    candidate_season = candidate_structure["season"]
    accepted_season = accepted_structure["season"]
    if candidate_season == accepted_season:
        return {"kind": "same_season", "accepted_season": accepted_season, "candidate_season": candidate_season, "valid": True, "approval_required": False, "checks": {}}
    candidate_keys = {race_key(row) for row in candidate_rows}
    accepted_keys = {race_key(row) for row in accepted_rows}
    row_fraction = len(candidate_rows) / len(accepted_rows)
    checks = {
        "is_next_season": candidate_season == accepted_season + 1,
        "row_fraction": round(row_fraction, 6),
        "row_count_sufficient": row_fraction >= MIN_ROLLOVER_ROW_FRACTION,
        "span_sufficient": candidate_structure["span_days"] >= MIN_ROLLOVER_SPAN_DAYS,
        "identities_are_new": not bool(candidate_keys & accepted_keys),
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
    return {"kind": "next_season" if checks["is_next_season"] else "unsupported", "accepted_season": accepted_season, "candidate_season": candidate_season, "valid": valid, "approval_required": valid, "checks": checks}


def build_shadow(
    candidate_races: Path,
    accepted_races: Path,
    output_root: Path,
    *,
    accepted_weekends: Path | None = None,
    max_removed_fraction: float = 0.1,
    expected_candidate_season: int | None = None,
) -> dict[str, Any]:
    if not 0 <= max_removed_fraction <= 1:
        raise ScheduleContractError("max_removed_fraction must be between 0 and 1")
    candidate_rows = read_csv(candidate_races, RACES_COLUMNS)
    accepted_rows = read_csv(accepted_races, RACES_COLUMNS)
    accepted_weekends = accepted_weekends or accepted_races.with_name("weekends.csv")
    accepted_weekend_rows = read_csv(accepted_weekends, WEEKENDS_COLUMNS)
    candidate_validation = validate_races(candidate_rows)
    accepted_validation = validate_races(accepted_rows)
    accepted_relationship = validate_relationship(accepted_rows, accepted_weekend_rows)
    candidate_structure = validate_season_structure(candidate_rows, expected_season=expected_candidate_season)
    accepted_structure = validate_season_structure(accepted_rows)

    candidate_dir = output_root / "candidate"
    isolated_races = candidate_dir / "races.csv"
    isolated_weekends = candidate_dir / "weekends.csv"
    write_csv_atomic(isolated_races, RACES_COLUMNS, candidate_rows, lineterminator="\r\n")
    weekend_rows = derive_weekends(candidate_rows)
    write_csv_atomic(isolated_weekends, WEEKENDS_COLUMNS, weekend_rows, lineterminator="\n")
    relationship = validate_relationship(candidate_rows, weekend_rows)
    comparison = compare_races(candidate_rows, accepted_rows, max_removed_fraction=max_removed_fraction)
    transition = assess_transition(candidate_rows, accepted_rows, candidate_structure, accepted_structure)
    if transition["kind"] == "same_season":
        promotion_status = "eligible" if comparison["within_removal_limit"] else "blocked"
    elif transition["valid"]:
        promotion_status = "approval_required"
    else:
        promotion_status = "blocked"
    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "promotion": {"status": promotion_status, "performed": False},
        "candidate": {
            "races": {**candidate_validation, "season_structure": candidate_structure, "file": str(isolated_races), "sha256": file_sha256(isolated_races)},
            "weekends": {**relationship, "file": str(isolated_weekends), "sha256": file_sha256(isolated_weekends), "derived_from_races_sha256": file_sha256(isolated_races)},
        },
        "accepted": {
            "races": {**accepted_validation, "file": str(accepted_races), "sha256": file_sha256(accepted_races)},
            "weekends": {**accepted_relationship, "file": str(accepted_weekends), "sha256": file_sha256(accepted_weekends)},
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
    shadow = subparsers.add_parser("shadow")
    shadow.add_argument("--candidate-races", required=True, type=Path)
    shadow.add_argument("--accepted-races", required=True, type=Path)
    shadow.add_argument("--output-root", required=True, type=Path)
    shadow.add_argument("--candidate-season", type=int)
    shadow.add_argument("--max-removed-fraction", type=float, default=0.1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = build_shadow(
            args.candidate_races.resolve(),
            args.accepted_races.resolve(),
            args.output_root.resolve(),
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
