#!/usr/bin/env python3
"""Acquire exact-date official IBU startlists into an isolated candidate bundle."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ibu_source_contract import (
    IBUClient,
    IBUContractError,
    discover_public_api_key,
    validate_competitions,
    validate_events,
    validate_reports,
    validate_results,
)


SCHEMA_VERSION = "nordicnumbers.biathlon-startlist-acquisition/v1"
RACE_ID = re.compile(r"(?:startlist|results)/([A-Z0-9_]+)", re.IGNORECASE)


class BiathlonStartlistError(RuntimeError):
    """Official evidence is missing, premature, or internally inconsistent."""


def _atomic_json(path: Path, document: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(document, handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _source_date(value: str) -> date:
    for pattern in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            pass
    raise BiathlonStartlistError(f"unsupported schedule date: {value!r}")


def _season_id(day: date) -> str:
    start = day.year if day.month >= 7 else day.year - 1
    return f"{start % 100:02d}{(start + 1) % 100:02d}"


def _race_id(url: str) -> str:
    match = RACE_ID.search(url.strip())
    if not match:
        raise BiathlonStartlistError(f"schedule does not contain an official IBU race URL: {url!r}")
    return match.group(1).upper()


def _ready(competition: dict[str, Any]) -> bool:
    return (
        str(competition.get("status_text", "")).casefold() == "start list"
        and str(competition.get("result_status", "")).upper() == "START_LIST"
        and str(competition.get("schedule_status", "")).upper() == "SCHEDULED"
    )


def _sha256(document: Any) -> str:
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


def acquire_startlists(
    *, client: IBUClient, schedule: Path, logical_date: date, output_dir: Path
) -> dict[str, Any]:
    output_dir = output_dir.expanduser().resolve()
    if "excel365" in output_dir.parts:
        raise BiathlonStartlistError("startlist evidence cannot be written inside accepted data")
    with schedule.open(newline="", encoding="utf-8-sig") as handle:
        selected = [
            row for row in csv.DictReader(handle)
            if _source_date(row.get("Date", "")) == logical_date
        ]
    if not selected:
        report = {
            "schema_version": SCHEMA_VERSION,
            "status": "no_work",
            "logical_date": logical_date.isoformat(),
            "season_id": _season_id(logical_date),
            "races": [],
            "accepted_data_mutated": False,
            "publication": {"status": "disabled"},
        }
        _atomic_json(output_dir / "report.json", report)
        return report

    season_id = _season_id(logical_date)
    events_payload = client.events(season_id, level=1)
    events = validate_events(events_payload, season_id=season_id, level=1)
    event_ids = {event["event_id"] for event in events}
    requested = {_race_id(row.get("Startlist", "")): row for row in selected}
    if len(requested) != len(selected):
        raise BiathlonStartlistError("schedule contains duplicate race IDs for the target date")

    by_race: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    competition_payloads: dict[str, list[dict[str, Any]]] = {}
    for race_id in requested:
        matches = sorted(
            (event_id for event_id in event_ids if race_id.startswith(event_id)),
            key=len,
            reverse=True,
        )
        if len(matches) != 1:
            raise BiathlonStartlistError(f"cannot identify one official event for {race_id}")
        event_id = matches[0]
        if event_id not in competition_payloads:
            competition_payloads[event_id] = client.competitions(event_id)
        competitions = validate_competitions(
            competition_payloads[event_id], event_id=event_id
        )
        competition = next((item for item in competitions if item["race_id"] == race_id), None)
        if competition is None:
            raise BiathlonStartlistError(f"official event does not contain {race_id}")
        if datetime.fromisoformat(competition["start_time"].replace("Z", "+00:00")).date() != logical_date:
            raise BiathlonStartlistError(f"official start date does not match schedule for {race_id}")
        by_race[race_id] = (competition, requested[race_id])

    raw = output_dir / "raw"
    _atomic_json(raw / "events.json", events_payload)
    for event_id, payload in competition_payloads.items():
        _atomic_json(raw / f"{event_id}-competitions.json", payload)

    race_reports: list[dict[str, Any]] = []
    all_ready = True
    for race_id, (competition, schedule_row) in by_race.items():
        record: dict[str, Any] = {
            **competition,
            "schedule": {
                key: schedule_row.get(key, "")
                for key in ("Date", "Sex", "City", "Country", "Distance", "RaceType", "Race")
            },
        }
        if not _ready(competition):
            all_ready = False
            race_reports.append({**record, "status": "waiting", "reason": "official_start_list_not_ready"})
            continue
        results_payload = client.results(race_id)
        reports_payload = client.reports(race_id)
        try:
            summary = validate_results(results_payload, competition)
            start_list = validate_reports(reports_payload, race_id=race_id)
        except IBUContractError as error:
            all_ready = False
            race_reports.append({**record, "status": "waiting", "reason": str(error)})
            continue
        _atomic_json(raw / f"{race_id}-results.json", results_payload)
        _atomic_json(raw / f"{race_id}-reports.json", reports_payload)
        race_reports.append({
            **record,
            "status": "ready",
            "summary": summary,
            "start_list": start_list,
            "results_sha256": _sha256(results_payload),
            "reports_sha256": _sha256(reports_payload),
        })

    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "ready" if all_ready else "waiting",
        "logical_date": logical_date.isoformat(),
        "season_id": season_id,
        "races": race_reports,
        "accepted_data_mutated": False,
        "publication": {"status": "disabled"},
    }
    _atomic_json(output_dir / "report.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", type=Path, required=True)
    parser.add_argument("--logical-date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fail-if-waiting", action="store_true")
    args = parser.parse_args()
    report = acquire_startlists(
        client=IBUClient(discover_public_api_key()),
        schedule=args.schedule,
        logical_date=args.logical_date,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if args.fail_if_waiting and report["status"] == "waiting" else 0


if __name__ == "__main__":
    raise SystemExit(main())
