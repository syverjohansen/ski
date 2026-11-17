#!/usr/bin/env python3
"""Report completed Alpine schedule races missing from chrono history.

This command is deliberately read-only.  It gives acquisition/backfill jobs an
explicit, reviewable list of races to fetch instead of allowing them to mutate
the accepted CSVs while discovering work.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


SCHEDULE_DATE_FORMAT = "%m/%d/%Y"
CHRONO_DATE_FORMAT = "%Y-%m-%d"


@dataclass(frozen=True, order=True)
class RaceKey:
    sex: str
    season: int
    race: int
    date: date


@dataclass(frozen=True)
class MissingRace:
    date: str
    sex: str
    season: int
    race: int
    city: str
    country: str
    distance: str
    results_url: str


def season_for_race(race_date: date) -> int:
    """Return the FIS season ending year for a calendar date."""
    return race_date.year + 1 if race_date.month >= 7 else race_date.year


def _required(row: dict[str, str], field: str, source: Path) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{source}: missing required {field!r} value")
    return value


def load_schedule(path: Path, as_of: date) -> list[tuple[RaceKey, MissingRace]]:
    completed: list[tuple[RaceKey, MissingRace]] = []
    seen: set[RaceKey] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                race_date = datetime.strptime(
                    _required(row, "Date", path), SCHEDULE_DATE_FORMAT
                ).date()
                sex = _required(row, "Sex", path)
                if sex not in {"M", "L"}:
                    raise ValueError(f"unsupported Sex {sex!r}")
                key = RaceKey(
                    sex=sex,
                    season=season_for_race(race_date),
                    race=int(_required(row, "Race", path)),
                    date=race_date,
                )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc

            if key in seen:
                raise ValueError(f"{path}:{row_number}: duplicate schedule race {key}")
            seen.add(key)
            if race_date > as_of:
                continue

            completed.append(
                (
                    key,
                    MissingRace(
                        date=race_date.isoformat(),
                        sex=sex,
                        season=key.season,
                        race=key.race,
                        city=(row.get("City") or "").strip(),
                        country=(row.get("Country") or "").strip(),
                        distance=(row.get("Distance") or "").strip(),
                        results_url=(row.get("Startlist") or "").strip(),
                    ),
                )
            )
    return completed


def load_chrono_keys(path: Path, sex: str) -> set[RaceKey]:
    keys: set[RaceKey] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                race = int(_required(row, "Race", path))
                if race == 0:  # The offseason rating row is not a scheduled race.
                    continue
                keys.add(
                    RaceKey(
                        sex=sex,
                        season=int(_required(row, "Season", path)),
                        race=race,
                        date=datetime.strptime(
                            _required(row, "Date", path), CHRONO_DATE_FORMAT
                        ).date(),
                    )
                )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc
    return keys


def find_missing_races(
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    as_of: date,
) -> tuple[list[MissingRace], int]:
    completed = load_schedule(schedule, as_of)
    accepted = load_chrono_keys(men_chrono, "M") | load_chrono_keys(
        ladies_chrono, "L"
    )
    missing = [details for key, details in completed if key not in accepted]
    missing.sort(key=lambda item: (item.date, item.sex, item.race))
    return missing, len(completed)


def build_report(
    missing: Iterable[MissingRace], *, as_of: date, completed_count: int
) -> dict[str, object]:
    gaps = [asdict(item) for item in missing]
    return {
        "schema_version": "nordicnumbers.alpine-results-gap/v1",
        "as_of": as_of.isoformat(),
        "completed_schedule_races": completed_count,
        "missing_race_count": len(gaps),
        "missing_races": gaps,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        help="Exit 2 when at least one completed race is missing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    missing, completed_count = find_missing_races(
        args.schedule, args.men_chrono, args.ladies_chrono, args.as_of
    )
    report = build_report(
        missing, as_of=args.as_of, completed_count=completed_count
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 2 if args.fail_on_gaps and missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
