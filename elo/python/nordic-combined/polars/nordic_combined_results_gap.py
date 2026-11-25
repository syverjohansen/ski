#!/usr/bin/env python3
"""Audit completed Nordic-combined schedule races against accepted histories.

The audit is read-only. Completion identity is the race date, FIS season, sex,
and semantic event kind. Legacy race numbers are deliberately not identities.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

SCHEDULE_DATE_FORMAT = "%m/%d/%Y"
HISTORY_DATE_FORMAT = "%Y-%m-%d"
SUPPORTED_SEXES = {"M", "L", "Mixed"}
SUPPORTED_EVENT_KINDS = {"individual", "team", "team_sprint"}


@dataclass(frozen=True, order=True)
class EventKey:
    sex: str
    season: int
    date: date
    event_kind: str


@dataclass(frozen=True)
class MissingRace:
    date: str
    sex: str
    season: int
    race: int
    event_kind: str
    city: str
    country: str
    distance: str
    race_type: str
    championship: bool
    results_url: str
    fis_race_id: str


def season_for_race(race_date: date) -> int:
    return race_date.year + 1 if race_date.month >= 7 else race_date.year


def _required(row: dict[str, str], field: str, source: Path) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{source}: missing required {field!r} value")
    return value


def validate_results_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    race_ids = query.get("raceid", [])
    sectors = query.get("sectorcode", [])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.fis-ski.com"
        or parsed.path != "/DB/general/results.html"
        or sectors != ["NK"]
        or len(race_ids) != 1
        or not race_ids[0].isdigit()
    ):
        raise ValueError(f"unsupported FIS Nordic-combined results URL: {url}")
    return race_ids[0]


def event_kind(race_type: str) -> str:
    normalized = " ".join(race_type.casefold().replace("_", " ").split())
    if normalized == "team sprint":
        return "team_sprint"
    if normalized == "team":
        return "team"
    if normalized in {
        "individual",
        "individual compact",
        "individualcompact",
        "mass start",
        "sprint",
    }:
        return "individual"
    raise ValueError(f"unsupported Nordic-combined RaceType {race_type!r}")


def load_schedule(path: Path, as_of: date) -> list[tuple[EventKey, MissingRace]]:
    completed: list[tuple[EventKey, MissingRace]] = []
    seen: set[EventKey] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                race_date = datetime.strptime(
                    _required(row, "Date", path), SCHEDULE_DATE_FORMAT
                ).date()
                sex = _required(row, "Sex", path)
                if sex not in SUPPORTED_SEXES:
                    raise ValueError(f"unsupported Sex {sex!r}")
                race_type = _required(row, "RaceType", path)
                kind = event_kind(race_type)
                if sex == "Mixed" and kind == "individual":
                    raise ValueError("Mixed schedule rows must be team events")
                results_url = _required(row, "Startlist", path)
                fis_race_id = validate_results_url(results_url)
                key = EventKey(
                    sex=sex,
                    season=season_for_race(race_date),
                    date=race_date,
                    event_kind=kind,
                )
                race = int(_required(row, "Race", path))
                championship = _required(row, "Championship", path)
                if championship not in {"0", "1"}:
                    raise ValueError(
                        f"unsupported Championship value {championship!r}"
                    )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc
            if key in seen:
                raise ValueError(f"{path}:{row_number}: duplicate schedule event {key}")
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
                        race=race,
                        event_kind=kind,
                        city=(row.get("City") or "").strip(),
                        country=(row.get("Country") or "").strip(),
                        distance=(row.get("Distance") or "").strip(),
                        race_type=race_type,
                        championship=championship == "1",
                        results_url=results_url,
                        fis_race_id=fis_race_id,
                    ),
                )
            )
    return completed


def load_history_keys(path: Path, *, history_kind: str) -> set[EventKey]:
    if history_kind not in SUPPORTED_EVENT_KINDS:
        raise ValueError(f"unsupported history kind {history_kind!r}")
    keys: set[EventKey] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                race = int(_required(row, "Race", path))
                if race == 0:
                    continue
                sex = _required(row, "Sex", path)
                if sex not in SUPPORTED_SEXES:
                    raise ValueError(f"unsupported Sex {sex!r}")
                keys.add(
                    EventKey(
                        sex=sex,
                        season=int(_required(row, "Season", path)),
                        date=datetime.strptime(
                            _required(row, "Date", path), HISTORY_DATE_FORMAT
                        ).date(),
                        event_kind=history_kind,
                    )
                )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc
    return keys


def audit_races(
    *,
    schedule: Path,
    individual_histories: list[Path],
    team_histories: list[Path],
    team_sprint_histories: list[Path],
    as_of: date,
) -> tuple[list[MissingRace], int]:
    completed = load_schedule(schedule, as_of)
    accepted: set[EventKey] = set()
    for path in individual_histories:
        accepted.update(load_history_keys(path, history_kind="individual"))
    for path in team_histories:
        accepted.update(load_history_keys(path, history_kind="team"))
    for path in team_sprint_histories:
        accepted.update(load_history_keys(path, history_kind="team_sprint"))
    missing = [details for key, details in completed if key not in accepted]
    missing.sort(key=lambda item: (item.date, item.event_kind, item.sex, item.race))
    return missing, len(completed)


def build_report(
    missing: list[MissingRace], *, as_of: date, completed_count: int
) -> dict[str, object]:
    gaps = [asdict(item) for item in missing]
    return {
        "schema_version": "nordicnumbers.nordic-combined-results-gap/v1",
        "as_of": as_of.isoformat(),
        "completed_schedule_races": completed_count,
        "missing_race_count": len(gaps),
        "missing_individual_race_count": sum(
            item["event_kind"] == "individual" for item in gaps
        ),
        "missing_team_race_count": sum(
            item["event_kind"] == "team" for item in gaps
        ),
        "missing_team_sprint_race_count": sum(
            item["event_kind"] == "team_sprint" for item in gaps
        ),
        "missing_races": gaps,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--individual-history", action="append", default=[], type=Path)
    parser.add_argument("--team-history", action="append", default=[], type=Path)
    parser.add_argument("--team-sprint-history", action="append", default=[], type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fail-on-gaps", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    missing, completed_count = audit_races(
        schedule=args.schedule,
        individual_histories=args.individual_history,
        team_histories=args.team_history,
        team_sprint_histories=args.team_sprint_history,
        as_of=args.as_of,
    )
    report = build_report(
        missing, as_of=args.as_of, completed_count=completed_count
    )
    document = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(document, encoding="utf-8")
    print(document, end="")
    return 2 if args.fail_on_gaps and missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
