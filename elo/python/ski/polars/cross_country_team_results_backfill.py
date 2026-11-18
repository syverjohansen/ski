#!/usr/bin/env python3
"""Fetch missing cross-country team results into an isolated candidate artifact.

The accepted relay and team-sprint histories remain read-only.  This command
uses the team-event audit to find completed gaps, downloads their official FIS
result pages, and preserves each classified team plus its ordered members for
the later identity-resolution and Elo-replay steps.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path

from cross_country_results_backfill import (
    BackfillError,
    fetch_html,
    validate_output_dir,
    validate_results_url,
)
from cross_country_team_event_audit import TeamScheduleEvent, audit_team_events

TEAM_RESULT_FIELDS = (
    "date",
    "sex",
    "season",
    "race",
    "city",
    "country",
    "distance",
    "technique",
    "championship",
    "place",
    "team_name",
    "nation",
    "team_bib",
    "team_fis_code",
    "member_order",
    "fis_competitor_id",
    "fis_code",
    "name",
    "source_url",
    "source_sha256",
)
EXPECTED_MEMBERS = {"Rel": 4, "Ts": 2}


@dataclass(frozen=True)
class TeamMember:
    order: int
    fis_competitor_id: str
    fis_code: str
    name: str


@dataclass(frozen=True)
class TeamResult:
    place: int
    team_name: str
    nation: str
    bib: str
    fis_code: str
    members: tuple[TeamMember, ...]


@dataclass(frozen=True)
class TeamRaceEvidence:
    date: str
    sex: str
    season: int
    race: int
    distance: str
    source_url: str
    source_sha256: str
    classified_teams: int
    member_rows: int
    output_file: str


class FISTeamResultsParser(HTMLParser):
    """Extract main team rows and their following athlete rows from FIS HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[dict[str, str]] = []
        self.section = ""
        self.event_label_parts: list[str] = []
        self._row: dict[str, str] | None = None
        self._capture: tuple[str, str, list[str]] | None = None
        self._heading: tuple[str, list[str]] | None = None
        self._in_selected_option = False

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = dict(attrs)
        classes = set((attributes.get("class") or "").split())
        if tag == "option" and "selected" in attributes:
            self._in_selected_option = True
        if tag == "div" and {"g-xs-24", "bold"} <= classes:
            self._heading = (tag, [])
        if tag == "a" and "table-row" in classes:
            kind = "member" if "table-row_theme_additional" in classes else "team"
            self._row = {
                "kind": kind,
                "href": attributes.get("href") or "",
                "section": self.section,
            }
            return
        if self._row is None:
            return
        field = None
        if self._row["kind"] == "team":
            if {"justify-right", "bold", "pr-1"} <= classes and "gray" not in classes:
                field = "place"
            elif {"justify-center", "hidden-sm-down", "gray"} <= classes:
                field = "bib"
        elif tag == "span" and "bib" in classes:
            field = "bib"
        if {"hidden-xs", "justify-right", "gray", "pr-1"} <= classes:
            field = "fis_code"
        elif {"justify-left", "bold"} <= classes:
            field = "name"
        elif "country__name-short" in classes:
            field = "nation"
        if field:
            self._capture = (tag, field, [])

    def handle_data(self, data: str) -> None:
        if self._in_selected_option:
            self.event_label_parts.append(data)
        if self._heading:
            self._heading[1].append(data)
        if self._capture:
            self._capture[2].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "option":
            self._in_selected_option = False
        if self._heading and tag == self._heading[0]:
            heading = " ".join("".join(self._heading[1]).split()).lower()
            if heading in {"final", "qualification"}:
                self.section = heading
            self._heading = None
        if self._capture and tag == self._capture[0]:
            _, field, parts = self._capture
            if self._row is not None:
                self._row[field] = " ".join("".join(parts).split())
            self._capture = None
        if tag == "a" and self._row is not None:
            self.rows.append(self._row)
            self._row = None

    @property
    def event_label(self) -> str:
        return " ".join("".join(self.event_label_parts).split())


def validate_team_page_identity(
    html: bytes, event: TeamScheduleEvent
) -> FISTeamResultsParser:
    parser = FISTeamResultsParser()
    parser.feed(html.decode("utf-8", errors="replace"))
    parser.close()
    expected_date = datetime.strptime(event.date, "%Y-%m-%d").strftime("%d.%m.%Y")
    expected_sex = "Men's" if event.sex == "M" else "Women's"
    expected_event = "Relay" if event.distance == "Rel" else "Team Sprint"
    label = parser.event_label
    if (
        expected_date not in label
        or expected_sex not in label
        or expected_event not in label
        or (event.distance == "Ts" and "Qualification" in label)
    ):
        raise BackfillError(
            f"FIS team page identity mismatch for {event.sex} race {event.race}: "
            f"{label!r}"
        )
    return parser


def parse_team_results(
    html: bytes,
    *,
    distance: str,
    parser: FISTeamResultsParser | None = None,
) -> list[TeamResult]:
    try:
        expected_members = EXPECTED_MEMBERS[distance]
    except KeyError as exc:
        raise BackfillError(f"unsupported team-event distance: {distance!r}") from exc

    if parser is None:
        parser = FISTeamResultsParser()
        parser.feed(html.decode("utf-8", errors="replace"))
        parser.close()
    teams: list[TeamResult] = []
    current: dict[str, object] | None = None

    def finish() -> None:
        nonlocal current
        if current is None:
            return
        members = tuple(current.pop("members"))
        if len(members) != expected_members:
            raise BackfillError(
                f"team {current['team_name']!r} has {len(members)} members; "
                f"expected {expected_members} for {distance}"
            )
        orders = [member.order for member in members]
        if orders != list(range(1, expected_members + 1)):
            raise BackfillError(
                f"team {current['team_name']!r} has invalid member order {orders}"
            )
        teams.append(TeamResult(members=members, **current))
        current = None

    seen_athletes: set[str] = set()
    for row in parser.rows:
        if distance == "Ts" and row.get("section") != "final":
            continue
        if row["kind"] == "team":
            finish()
            place = row.get("place", "")
            if not place.isdigit():
                current = None
                continue
            current = {
                "place": int(place),
                "team_name": row.get("name", "").strip(),
                "nation": row.get("nation", "").strip(),
                "bib": row.get("bib", "").strip(),
                "fis_code": row.get("fis_code", "").strip(),
                "members": [],
            }
            if not current["team_name"] or not current["nation"] or not current["bib"]:
                raise BackfillError(f"incomplete classified team row: {current}")
            continue
        if current is None:
            continue
        id_match = re.search(r"competitorid=(\d+)", row.get("href", ""))
        bib_match = re.fullmatch(rf"{re.escape(str(current['bib']))}-(\d+)", row.get("bib", ""))
        name = row.get("name", "").strip()
        fis_code = row.get("fis_code", "").strip()
        if not id_match or not bib_match or not name or not fis_code:
            raise BackfillError(f"incomplete member row for team {current['team_name']!r}")
        competitor_id = id_match.group(1)
        if competitor_id in seen_athletes:
            raise BackfillError(f"duplicate FIS competitor ID: {competitor_id}")
        seen_athletes.add(competitor_id)
        current["members"].append(
            TeamMember(
                order=int(bib_match.group(1)),
                fis_competitor_id=competitor_id,
                fis_code=fis_code,
                name=name,
            )
        )
    finish()
    if len(teams) < 3:
        raise BackfillError(f"FIS page yielded only {len(teams)} classified teams")
    return teams


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=TEAM_RESULT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _event_rows(
    event: TeamScheduleEvent,
    teams: list[TeamResult],
    *,
    source_sha256: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for team in teams:
        for member in team.members:
            rows.append(
                {
                    "date": event.date,
                    "sex": event.sex,
                    "season": event.season,
                    "race": event.race,
                    "city": event.city,
                    "country": event.country,
                    "distance": event.distance,
                    "technique": event.technique,
                    "championship": event.championship,
                    "place": team.place,
                    "team_name": team.team_name,
                    "nation": team.nation,
                    "team_bib": team.bib,
                    "team_fis_code": team.fis_code,
                    "member_order": member.order,
                    "fis_competitor_id": member.fis_competitor_id,
                    "fis_code": member.fis_code,
                    "name": member.name,
                    "source_url": event.results_url,
                    "source_sha256": source_sha256,
                }
            )
    return rows


def acquire_team_backfill(
    *,
    schedule: Path,
    men_relay_chrono: Path,
    ladies_relay_chrono: Path,
    team_sprint_chrono: Path,
    as_of: date,
    output_dir: Path,
    fetcher: Callable[[str], bytes] = fetch_html,
) -> dict[str, object]:
    output_dir = validate_output_dir(
        output_dir,
        (schedule, men_relay_chrono, ladies_relay_chrono, team_sprint_chrono),
    )
    audit = audit_team_events(
        schedule=schedule,
        men_relay_chrono=men_relay_chrono,
        ladies_relay_chrono=ladies_relay_chrono,
        team_sprint_chrono=team_sprint_chrono,
        as_of=as_of,
    )
    if not audit.missing:
        raise BackfillError("no completed team-result gaps were found")

    all_rows: list[dict[str, object]] = []
    evidence: list[TeamRaceEvidence] = []
    for event in audit.missing:
        validate_results_url(event.results_url)
        html = fetcher(event.results_url)
        digest = hashlib.sha256(html).hexdigest()
        parser = validate_team_page_identity(html, event)
        teams = parse_team_results(html, distance=event.distance, parser=parser)
        rows = _event_rows(event, teams, source_sha256=digest)
        relative_file = (
            f"races/{event.sex}-{event.season}-{event.race:03d}-{event.distance}.csv"
        )
        _atomic_text(output_dir / relative_file, _csv_text(rows))
        all_rows.extend(rows)
        evidence.append(
            TeamRaceEvidence(
                date=event.date,
                sex=event.sex,
                season=event.season,
                race=event.race,
                distance=event.distance,
                source_url=event.results_url,
                source_sha256=digest,
                classified_teams=len(teams),
                member_rows=len(rows),
                output_file=relative_file,
            )
        )

    _atomic_text(output_dir / "team_results.csv", _csv_text(all_rows))
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.cross-country-team-results-backfill/v1",
        "as_of": as_of.isoformat(),
        "acquired_team_event_count": len(evidence),
        "acquired_classified_teams": sum(item.classified_teams for item in evidence),
        "acquired_member_rows": len(all_rows),
        "identity_status": "fis-competitor-ids-captured",
        "accepted_histories_mutated": False,
        "events": [asdict(item) for item in evidence],
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-relay-chrono", required=True, type=Path)
    parser.add_argument("--ladies-relay-chrono", required=True, type=Path)
    parser.add_argument("--team-sprint-chrono", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--allow-network", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.allow_network:
        raise BackfillError("network acquisition requires --allow-network")
    report = acquire_team_backfill(
        schedule=args.schedule,
        men_relay_chrono=args.men_relay_chrono,
        ladies_relay_chrono=args.ladies_relay_chrono,
        team_sprint_chrono=args.team_sprint_chrono,
        as_of=args.as_of,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
