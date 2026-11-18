#!/usr/bin/env python3
"""Audit scheduled cross-country team events against accepted team histories.

The individual results pipeline intentionally excludes relays and team
sprints. This read-only audit verifies that completed team events instead
exist in their dedicated accepted chrono tables. Future races are reported but
never treated as gaps.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

SCHEDULE_DATE_FORMAT = "%m/%d/%Y"
CHRONO_DATE_FORMAT = "%Y-%m-%d"
TEAM_DISTANCES = {"Rel", "Ts"}


@dataclass(frozen=True, order=True)
class TeamEventKey:
    sex: str
    season: int
    race: int
    date: date
    distance: str


@dataclass(frozen=True)
class TeamScheduleEvent:
    date: str
    sex: str
    season: int
    race: int
    city: str
    country: str
    distance: str
    technique: str
    championship: str
    results_url: str


@dataclass(frozen=True)
class TeamEventAudit:
    scheduled_team_events: int
    future_team_events: int
    completed_team_events: int
    covered_relay_events: int
    covered_team_sprint_events: int
    accepted_team_rows: int
    missing: tuple[TeamScheduleEvent, ...]


def season_for_race(race_date: date) -> int:
    return race_date.year + 1 if race_date.month >= 7 else race_date.year


def _required(row: dict[str, str], field: str, source: Path) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{source}: missing required {field!r} value")
    return value


def load_team_events(
    path: Path, *, expected_distance: str, expected_sex: str | None = None
) -> Counter[TeamEventKey]:
    events: Counter[TeamEventKey] = Counter()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                distance = _required(row, "Distance", path)
                if distance != expected_distance:
                    raise ValueError(
                        f"expected Distance {expected_distance!r}, found {distance!r}"
                    )
                sex = _required(row, "Sex", path)
                if sex not in {"M", "L"}:
                    raise ValueError(f"unsupported Sex {sex!r}")
                if expected_sex and sex != expected_sex:
                    raise ValueError(f"expected Sex {expected_sex!r}, found {sex!r}")
                race = int(_required(row, "Race", path))
                if race == 0:
                    continue
                key = TeamEventKey(
                    sex=sex,
                    season=int(_required(row, "Season", path)),
                    race=race,
                    date=datetime.strptime(
                        _required(row, "Date", path), CHRONO_DATE_FORMAT
                    ).date(),
                    distance=distance,
                )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc
            events[key] += 1
    return events


def audit_team_events(
    *,
    schedule: Path,
    men_relay_chrono: Path,
    ladies_relay_chrono: Path,
    team_sprint_chrono: Path,
    as_of: date,
) -> TeamEventAudit:
    accepted = load_team_events(
        men_relay_chrono, expected_distance="Rel", expected_sex="M"
    )
    accepted.update(
        load_team_events(
            ladies_relay_chrono, expected_distance="Rel", expected_sex="L"
        )
    )
    accepted.update(load_team_events(team_sprint_chrono, expected_distance="Ts"))

    scheduled_team_events = 0
    future_team_events = 0
    completed_team_events = 0
    covered_relay_events = 0
    covered_team_sprint_events = 0
    accepted_team_rows = 0
    missing: list[TeamScheduleEvent] = []
    seen: set[TeamEventKey] = set()

    with schedule.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            distance = (row.get("Distance") or "").strip()
            if distance not in TEAM_DISTANCES:
                continue
            scheduled_team_events += 1
            try:
                race_date = datetime.strptime(
                    _required(row, "Date", schedule), SCHEDULE_DATE_FORMAT
                ).date()
                sex = _required(row, "Sex", schedule)
                if sex not in {"M", "L"}:
                    raise ValueError(f"unsupported Sex {sex!r}")
                key = TeamEventKey(
                    sex=sex,
                    season=season_for_race(race_date),
                    race=int(_required(row, "Race", schedule)),
                    date=race_date,
                    distance=distance,
                )
            except ValueError as exc:
                raise ValueError(f"{schedule}:{row_number}: {exc}") from exc
            if key in seen:
                raise ValueError(f"{schedule}:{row_number}: duplicate team event {key}")
            seen.add(key)

            if race_date > as_of:
                future_team_events += 1
                continue
            completed_team_events += 1
            rows = accepted.get(key, 0)
            if rows:
                accepted_team_rows += rows
                if distance == "Rel":
                    covered_relay_events += 1
                else:
                    covered_team_sprint_events += 1
                continue
            missing.append(
                TeamScheduleEvent(
                    date=race_date.isoformat(),
                    sex=sex,
                    season=key.season,
                    race=key.race,
                    city=(row.get("City") or "").strip(),
                    country=(row.get("Country") or "").strip(),
                    distance=distance,
                    technique=(row.get("Technique") or "").strip(),
                    championship=(row.get("Championship") or "").strip(),
                    results_url=(row.get("Startlist") or "").strip(),
                )
            )

    missing.sort(key=lambda item: (item.date, item.sex, item.race))
    return TeamEventAudit(
        scheduled_team_events=scheduled_team_events,
        future_team_events=future_team_events,
        completed_team_events=completed_team_events,
        covered_relay_events=covered_relay_events,
        covered_team_sprint_events=covered_team_sprint_events,
        accepted_team_rows=accepted_team_rows,
        missing=tuple(missing),
    )


def build_report(audit: TeamEventAudit, *, as_of: date) -> dict[str, object]:
    return {
        "schema_version": "nordicnumbers.cross-country-team-event-audit/v1",
        "as_of": as_of.isoformat(),
        "scheduled_team_events": audit.scheduled_team_events,
        "future_team_events": audit.future_team_events,
        "completed_team_events_expected": audit.completed_team_events,
        "covered_relay_events": audit.covered_relay_events,
        "covered_team_sprint_events": audit.covered_team_sprint_events,
        "accepted_team_rows": audit.accepted_team_rows,
        "missing_team_event_count": len(audit.missing),
        "missing_team_events": [asdict(item) for item in audit.missing],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-relay-chrono", required=True, type=Path)
    parser.add_argument("--ladies-relay-chrono", required=True, type=Path)
    parser.add_argument("--team-sprint-chrono", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        help="Exit 2 when a completed relay or team sprint is missing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit = audit_team_events(
        schedule=args.schedule,
        men_relay_chrono=args.men_relay_chrono,
        ladies_relay_chrono=args.ladies_relay_chrono,
        team_sprint_chrono=args.team_sprint_chrono,
        as_of=args.as_of,
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
