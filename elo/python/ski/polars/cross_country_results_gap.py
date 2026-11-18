#!/usr/bin/env python3
"""Audit completed cross-country schedule races against accepted chrono data.

The audit is read-only. Future schedule rows are reported but never treated as
missing results. Relay and team-sprint rows are also reported separately because
the individual chrono tables intentionally do not contain team-event results.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

SCHEDULE_DATE_FORMAT = "%m/%d/%Y"
CHRONO_DATE_FORMAT = "%Y-%m-%d"
TEAM_DISTANCES = {"Rel", "Ts"}


@dataclass(frozen=True, order=True)
class RaceKey:
    sex: str
    season: int
    race: int
    date: date


@dataclass(frozen=True)
class ScheduleRace:
    date: str
    sex: str
    season: int
    race: int
    city: str
    country: str
    distance: str
    technique: str
    mass_start: str
    period: str
    pursuit: str
    stage: str
    final_climb: str
    elevation: str
    championship: str
    results_url: str


@dataclass(frozen=True)
class ScheduleAudit:
    total: int
    future: int
    completed_team_events: int
    completed_individual: int
    completed_individual_metadata_incomplete: int
    missing: tuple[ScheduleRace, ...]


def season_for_race(race_date: date) -> int:
    """Return the FIS season ending year for a calendar date."""
    return race_date.year + 1 if race_date.month >= 7 else race_date.year


def _required(row: dict[str, str], field: str, source: Path) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{source}: missing required {field!r} value")
    return value


def load_chrono_keys(path: Path, sex: str) -> set[RaceKey]:
    keys: set[RaceKey] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                race = int(_required(row, "Race", path))
                if race == 0:
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


def audit_results(
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    as_of: date,
) -> ScheduleAudit:
    accepted = load_chrono_keys(men_chrono, "M") | load_chrono_keys(
        ladies_chrono, "L"
    )
    total = 0
    future = 0
    completed_team_events = 0
    completed_individual = 0
    completed_individual_metadata_incomplete = 0
    missing: list[ScheduleRace] = []
    seen: set[RaceKey] = set()

    with schedule.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            total += 1
            try:
                race_date = datetime.strptime(
                    _required(row, "Date", schedule), SCHEDULE_DATE_FORMAT
                ).date()
                sex = _required(row, "Sex", schedule)
                if sex not in {"M", "L"}:
                    raise ValueError(f"unsupported Sex {sex!r}")
                key = RaceKey(
                    sex=sex,
                    season=season_for_race(race_date),
                    race=int(_required(row, "Race", schedule)),
                    date=race_date,
                )
            except ValueError as exc:
                raise ValueError(f"{schedule}:{row_number}: {exc}") from exc
            if key in seen:
                raise ValueError(f"{schedule}:{row_number}: duplicate race {key}")
            seen.add(key)

            if race_date > as_of:
                future += 1
                continue
            distance = (row.get("Distance") or "").strip()
            if distance in TEAM_DISTANCES:
                completed_team_events += 1
                continue
            completed_individual += 1
            if not distance:
                completed_individual_metadata_incomplete += 1
            if key not in accepted:
                missing.append(
                    ScheduleRace(
                        date=race_date.isoformat(),
                        sex=sex,
                        season=key.season,
                        race=key.race,
                        city=(row.get("City") or "").strip(),
                        country=(row.get("Country") or "").strip(),
                        distance=distance,
                        technique=(row.get("Technique") or "").strip(),
                        mass_start=(row.get("MS") or "").strip(),
                        period=(row.get("Period") or "").strip(),
                        pursuit=(row.get("Pursuit") or "").strip(),
                        stage=(row.get("Stage") or "").strip(),
                        final_climb=(row.get("Final_Climb") or "").strip(),
                        elevation=(row.get("Elevation") or "").strip(),
                        championship=(row.get("Championship") or "").strip(),
                        results_url=(row.get("Startlist") or "").strip(),
                    )
                )

    missing.sort(key=lambda item: (item.date, item.sex, item.race))
    return ScheduleAudit(
        total=total,
        future=future,
        completed_team_events=completed_team_events,
        completed_individual=completed_individual,
        completed_individual_metadata_incomplete=(
            completed_individual_metadata_incomplete
        ),
        missing=tuple(missing),
    )


def build_report(audit: ScheduleAudit, *, as_of: date) -> dict[str, object]:
    return {
        "schema_version": "nordicnumbers.cross-country-results-gap/v1",
        "as_of": as_of.isoformat(),
        "schedule_races": audit.total,
        "future_schedule_races": audit.future,
        "completed_team_events_excluded": audit.completed_team_events,
        "completed_individual_races_expected": audit.completed_individual,
        "completed_individual_metadata_incomplete": (
            audit.completed_individual_metadata_incomplete
        ),
        "missing_race_count": len(audit.missing),
        "missing_races": [asdict(item) for item in audit.missing],
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
        help="Exit 2 when at least one completed individual race is missing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit = audit_results(
        args.schedule, args.men_chrono, args.ladies_chrono, args.as_of
    )
    rendered = json.dumps(
        build_report(audit, as_of=args.as_of), indent=2, sort_keys=True
    ) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 2 if args.fail_on_gaps and audit.missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
