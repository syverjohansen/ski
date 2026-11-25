#!/usr/bin/env python3
"""Report completed ski-jumping races missing from accepted histories.

This command is deliberately read-only.  It separates individual, men's
team, women's team, and mixed-team events so one history cannot accidentally
satisfy another event's completion gate.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

SCHEDULE_DATE_FORMAT = "%m/%d/%Y"
HISTORY_DATE_FORMAT = "%Y-%m-%d"
SUPPORTED_SEXES = {"M", "L", "Mixed"}
DEFAULT_POLICY_EXCLUSIONS = Path(__file__).with_name(
    "skijump_results_policy_exclusions.csv"
)


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
    hill_size: str
    race_type: str
    championship: bool
    results_url: str
    fis_race_id: str


@dataclass(frozen=True)
class PolicyExclusion:
    fis_race_id: str
    season: int
    sex: str
    date: str
    event_kind: str
    reason: str


@dataclass(frozen=True)
class PolicyExcludedRace:
    race: MissingRace
    reason: str


def season_for_race(race_date: date) -> int:
    """Return the FIS season ending year for a calendar date."""
    return race_date.year + 1 if race_date.month >= 7 else race_date.year


def _required(row: dict[str, str], field: str, source: Path) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{source}: missing required {field!r} value")
    return value


def validate_results_url(url: str) -> str:
    """Validate the immutable FIS race identity embedded in a results URL."""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    race_ids = query.get("raceid", [])
    sectors = query.get("sectorcode", [])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.fis-ski.com"
        or parsed.path != "/DB/general/results.html"
        or sectors != ["JP"]
        or len(race_ids) != 1
        or not race_ids[0].isdigit()
    ):
        raise ValueError(f"unsupported FIS ski-jumping results URL: {url}")
    return race_ids[0]


def _event_kind(sex: str, race_type: str) -> str:
    is_team = race_type.casefold().startswith("team ")
    if sex == "Mixed" and not is_team:
        raise ValueError("Mixed schedule rows must be team events")
    return "team" if is_team else "individual"


def load_schedule(path: Path, as_of: date) -> list[tuple[EventKey, MissingRace]]:
    completed: list[tuple[EventKey, MissingRace]] = []
    seen: set[EventKey] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                final_date = datetime.strptime(
                    _required(row, "Final_Date", path), SCHEDULE_DATE_FORMAT
                ).date()
                sex = _required(row, "Sex", path)
                if sex not in SUPPORTED_SEXES:
                    raise ValueError(f"unsupported Sex {sex!r}")
                race_type = _required(row, "RaceType", path)
                event_kind = _event_kind(sex, race_type)
                if event_kind == "individual" and sex == "Mixed":
                    raise ValueError("individual Mixed events are unsupported")
                results_url = _required(row, "Startlist", path)
                fis_race_id = validate_results_url(results_url)
                key = EventKey(
                    sex=sex,
                    season=season_for_race(final_date),
                    date=final_date,
                    event_kind=event_kind,
                )
                race = int(_required(row, "Race", path))
                championship_text = _required(row, "Championship", path)
                if championship_text not in {"0", "1"}:
                    raise ValueError(
                        f"unsupported Championship value {championship_text!r}"
                    )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc

            if key in seen:
                raise ValueError(f"{path}:{row_number}: duplicate schedule event {key}")
            seen.add(key)
            if final_date > as_of:
                continue

            completed.append(
                (
                    key,
                    MissingRace(
                        date=final_date.isoformat(),
                        sex=sex,
                        season=key.season,
                        race=race,
                        event_kind=event_kind,
                        city=(row.get("City") or "").strip(),
                        country=(row.get("Country") or "").strip(),
                        hill_size=(row.get("HillSize") or "").strip(),
                        race_type=race_type,
                        championship=championship_text == "1",
                        results_url=results_url,
                        fis_race_id=fis_race_id,
                    ),
                )
            )
    return completed


def load_history_keys(
    path: Path, *, expected_sex: str, event_kind: str
) -> set[EventKey]:
    keys: set[EventKey] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                race = int(_required(row, "Race", path))
                if race == 0:
                    continue
                row_sex = _required(row, "Sex", path)
                # A combined or corrupted history must not satisfy another
                # sex/event's completion gate.
                if row_sex != expected_sex:
                    continue
                if event_kind == "individual":
                    team_event = _required(row, "TeamEvent", path)
                    if team_event not in {"0", "False", "false"}:
                        continue
                keys.add(
                    EventKey(
                        sex=expected_sex,
                        season=int(_required(row, "Season", path)),
                        date=datetime.strptime(
                            _required(row, "Date", path), HISTORY_DATE_FORMAT
                        ).date(),
                        event_kind=event_kind,
                    )
                )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc
    return keys


def load_policy_exclusions(path: Path) -> dict[str, PolicyExclusion]:
    exclusions: dict[str, PolicyExclusion] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                fis_race_id = _required(row, "fis_race_id", path)
                if not fis_race_id.isdigit():
                    raise ValueError("fis_race_id must contain only digits")
                sex = _required(row, "sex", path)
                if sex not in SUPPORTED_SEXES:
                    raise ValueError(f"unsupported sex {sex!r}")
                event_kind = _required(row, "event_kind", path)
                if event_kind not in {"individual", "team"}:
                    raise ValueError(f"unsupported event_kind {event_kind!r}")
                event_date = date.fromisoformat(_required(row, "date", path))
                exclusion = PolicyExclusion(
                    fis_race_id=fis_race_id,
                    season=int(_required(row, "season", path)),
                    sex=sex,
                    date=event_date.isoformat(),
                    event_kind=event_kind,
                    reason=_required(row, "reason", path),
                )
            except ValueError as exc:
                raise ValueError(f"{path}:{row_number}: {exc}") from exc
            if fis_race_id in exclusions:
                raise ValueError(
                    f"{path}:{row_number}: duplicate policy exclusion {fis_race_id}"
                )
            exclusions[fis_race_id] = exclusion
    return exclusions


def _apply_policy_exclusions(
    missing: Iterable[MissingRace], exclusions: dict[str, PolicyExclusion]
) -> tuple[list[MissingRace], list[PolicyExcludedRace]]:
    remaining: list[MissingRace] = []
    excluded: list[PolicyExcludedRace] = []
    for race in missing:
        exclusion = exclusions.get(race.fis_race_id)
        if exclusion is None:
            remaining.append(race)
            continue
        observed = (race.season, race.sex, race.date, race.event_kind)
        expected = (
            exclusion.season,
            exclusion.sex,
            exclusion.date,
            exclusion.event_kind,
        )
        if observed != expected:
            raise ValueError(
                f"policy exclusion {race.fis_race_id} metadata mismatch: "
                f"expected {expected}, observed {observed}"
            )
        excluded.append(PolicyExcludedRace(race=race, reason=exclusion.reason))
    return remaining, excluded


def audit_races(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_team_chrono: Path,
    ladies_team_chrono: Path,
    mixed_team_chrono: Path,
    as_of: date,
    policy_exclusions: Path | None = None,
) -> tuple[list[MissingRace], list[PolicyExcludedRace], int]:
    completed = load_schedule(schedule, as_of)
    accepted = set().union(
        load_history_keys(men_chrono, expected_sex="M", event_kind="individual"),
        load_history_keys(ladies_chrono, expected_sex="L", event_kind="individual"),
        load_history_keys(men_team_chrono, expected_sex="M", event_kind="team"),
        load_history_keys(ladies_team_chrono, expected_sex="L", event_kind="team"),
        load_history_keys(mixed_team_chrono, expected_sex="Mixed", event_kind="team"),
    )
    candidates = [details for key, details in completed if key not in accepted]
    exclusions = load_policy_exclusions(policy_exclusions) if policy_exclusions else {}
    missing, excluded = _apply_policy_exclusions(candidates, exclusions)
    missing.sort(key=lambda item: (item.date, item.event_kind, item.sex, item.race))
    excluded.sort(
        key=lambda item: (
            item.race.date,
            item.race.event_kind,
            item.race.sex,
            item.race.race,
        )
    )
    return missing, excluded, len(completed)


def find_missing_races(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_team_chrono: Path,
    ladies_team_chrono: Path,
    mixed_team_chrono: Path,
    as_of: date,
    policy_exclusions: Path | None = None,
) -> tuple[list[MissingRace], int]:
    missing, _, completed_count = audit_races(
        schedule=schedule,
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        men_team_chrono=men_team_chrono,
        ladies_team_chrono=ladies_team_chrono,
        mixed_team_chrono=mixed_team_chrono,
        as_of=as_of,
        policy_exclusions=policy_exclusions,
    )
    return missing, completed_count


def build_report(
    missing: Iterable[MissingRace],
    *,
    policy_excluded: Iterable[PolicyExcludedRace] = (),
    as_of: date,
    completed_count: int,
) -> dict[str, object]:
    gaps = [asdict(item) for item in missing]
    excluded = [
        {**asdict(item.race), "reason": item.reason} for item in policy_excluded
    ]
    return {
        "schema_version": "nordicnumbers.skijump-results-gap/v1",
        "as_of": as_of.isoformat(),
        "completed_schedule_races": completed_count,
        "missing_race_count": len(gaps),
        "missing_individual_race_count": sum(
            item["event_kind"] == "individual" for item in gaps
        ),
        "missing_team_race_count": sum(item["event_kind"] == "team" for item in gaps),
        "policy_excluded_race_count": len(excluded),
        "policy_excluded_races": excluded,
        "missing_races": gaps,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-team-chrono", required=True, type=Path)
    parser.add_argument("--ladies-team-chrono", required=True, type=Path)
    parser.add_argument("--mixed-team-chrono", required=True, type=Path)
    parser.add_argument(
        "--policy-exclusions", type=Path, default=DEFAULT_POLICY_EXCLUSIONS
    )
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fail-on-gaps", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    missing, policy_excluded, completed_count = audit_races(
        schedule=args.schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        men_team_chrono=args.men_team_chrono,
        ladies_team_chrono=args.ladies_team_chrono,
        mixed_team_chrono=args.mixed_team_chrono,
        as_of=args.as_of,
        policy_exclusions=args.policy_exclusions,
    )
    report = build_report(
        missing,
        policy_excluded=policy_excluded,
        as_of=args.as_of,
        completed_count=completed_count,
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
