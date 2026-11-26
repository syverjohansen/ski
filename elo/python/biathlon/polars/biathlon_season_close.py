#!/usr/bin/env python3
"""Verify the guarded May 1 close for accepted Biathlon histories.

Biathlon's proven rating rebuild already writes the current season's synthetic
Race=0 snapshot after every accepted result.  Closing the season therefore
does not invent or rewrite ratings: the daily official-result audit owns final
competition completeness, while this close gate proves the accepted schedule
has ended, every current-season athlete has one undiscounted Race=0 row, and
team histories contain no synthetic close rows. The next season's first
accepted race applies the established 85/15 model regression internally while
the archived Race=0 values remain stable.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import tempfile
from datetime import date, datetime
from pathlib import Path


class SeasonCloseError(RuntimeError):
    """Raised when accepted Biathlon data cannot close safely."""


RATING_PAIRS = (
    ("Pelo", "Elo"),
    ("Individual_Pelo", "Individual_Elo"),
    ("Sprint_Pelo", "Sprint_Elo"),
    ("Pursuit_Pelo", "Pursuit_Elo"),
    ("MassStart_Pelo", "MassStart_Elo"),
)


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            return list(reader.fieldnames or []), list(reader)
    except OSError as exc:
        raise SeasonCloseError(f"cannot read {path}: {exc}") from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _schedule_date(value: str) -> date:
    for pattern in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            continue
    raise SeasonCloseError(f"unsupported schedule date: {value!r}")


def _validate_chrono(
    path: Path, *, season: int, sex: str
) -> dict[str, int]:
    fields, rows = _read(path)
    required = {
        "Date", "City", "Sex", "RaceType", "Event", "ID", "Season", "Race",
        *(column for pair in RATING_PAIRS for column in pair),
    }
    missing = sorted(required - set(fields))
    if missing:
        raise SeasonCloseError(f"{path} is missing columns: {missing}")
    season_rows = [row for row in rows if int(row["Season"]) == season]
    race_rows = [row for row in season_rows if row["Race"] != "0"]
    close_rows = [row for row in season_rows if row["Race"] == "0"]
    if not race_rows:
        raise SeasonCloseError(f"{path} has no race rows for season {season}")
    racers = {row["ID"].strip() for row in race_rows}
    closed: dict[str, dict[str, str]] = {}
    close_date = date(season, 5, 1).isoformat()
    for row in close_rows:
        athlete_id = row["ID"].strip()
        if athlete_id in closed:
            raise SeasonCloseError(
                f"duplicate Race=0 row for {sex}/{season}/{athlete_id}"
            )
        closed[athlete_id] = row
        expected = {
            "Date": close_date,
            "Sex": sex,
            "RaceType": "Offseason",
            "Event": "Offseason",
            "Race": "0",
        }
        for field, value in expected.items():
            if row[field] != value:
                raise SeasonCloseError(
                    f"invalid {field} for {sex}/{season}/{athlete_id}: "
                    f"{row[field]!r} != {value!r}"
                )
        for pelo, elo in RATING_PAIRS:
            previous = row[pelo].strip()
            current = row[elo].strip()
            if bool(previous) != bool(current):
                raise SeasonCloseError(
                    f"partial {pelo}/{elo} close rating for {athlete_id}"
                )
            if previous and abs(float(previous) - float(current)) > 1e-9:
                raise SeasonCloseError(
                    f"discounted public Race=0 rating for {athlete_id}: "
                    f"{pelo}={previous}, {elo}={current}"
                )
    if set(closed) != racers:
        raise SeasonCloseError(
            f"partial Race=0 population for {sex}/{season}: "
            f"missing={sorted(racers - set(closed))}, "
            f"unexpected={sorted(set(closed) - racers)}"
        )
    return {"race_rows": len(race_rows), "athletes": len(racers)}


def _validate_team_history(path: Path, season: int) -> int:
    fields, rows = _read(path)
    missing = sorted({"Season", "Race"} - set(fields))
    if missing:
        raise SeasonCloseError(f"{path} is missing columns: {missing}")
    if any(row["Race"] == "0" for row in rows):
        raise SeasonCloseError(f"team history contains synthetic Race=0 rows: {path}")
    return sum(int(row["Season"]) == season for row in rows)


def verify_season_close(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    team_histories: tuple[Path, ...],
    season: int,
    as_of: date,
    output_root: Path,
) -> dict[str, object]:
    close_date = date(season, 5, 1)
    if as_of < close_date:
        raise SeasonCloseError(
            f"season {season} cannot close before {close_date.isoformat()}"
        )
    schedule_fields, schedule_rows = _read(schedule)
    required = {"Date", "City", "Sex", "RaceType"}
    missing = sorted(required - set(schedule_fields))
    if missing:
        raise SeasonCloseError(f"{schedule} is missing columns: {missing}")
    if not schedule_rows:
        raise SeasonCloseError("accepted Biathlon schedule is empty")
    future = [row for row in schedule_rows if _schedule_date(row["Date"]) > as_of]
    if future:
        raise SeasonCloseError(
            f"accepted schedule still has {len(future)} races after {as_of.isoformat()}"
        )

    men_counts = _validate_chrono(
        men_chrono, season=season, sex="M"
    )
    ladies_counts = _validate_chrono(
        ladies_chrono, season=season, sex="L"
    )
    team_counts = {
        path.name: _validate_team_history(path, season) for path in team_histories
    }
    sources = (schedule, men_chrono, ladies_chrono, *team_histories)
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.biathlon-season-close/v1",
        "status": "verified",
        "season": season,
        "as_of": as_of.isoformat(),
        "close_date": close_date.isoformat(),
        "schedule_events": len(schedule_rows),
        "official_result_completeness_owner": "biathlon-results-update",
        "chronos": {"M": men_counts, "L": ladies_counts},
        "team_rows": team_counts,
        "source_sha256": {str(path): _sha256(path) for path in sources},
        "ratings_mutated": False,
        "public_race_zero_discounted": False,
    }
    output_root = output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / "report.json"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=output_root, delete=False
    ) as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(destination)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--team-history", action="append", default=[], type=Path)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    verify_season_close(
        schedule=args.schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        team_histories=tuple(args.team_history),
        season=args.season,
        as_of=args.as_of,
        output_root=args.output_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
