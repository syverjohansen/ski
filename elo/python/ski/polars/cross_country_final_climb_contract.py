#!/usr/bin/env python3
"""Plan the official inputs for one Tour de Ski Final Climb prediction.

The accepted ``weekends.csv`` schedule is authoritative.  Final Climb events
must be the validated men's and ladies' last-stage Tour events.  FIS normally
publishes startlists close to the race, so the planner may run on both the day
before and the race date.  Both official lists are required before downstream
model work is allowed.

This module is deliberately side-effect free apart from an optional atomic
JSON report.  It does not fetch FIS, run the model, or publish a post.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

from cross_country_tour_contract import build_tour_plan, read_schedule

SCHEMA_VERSION = "nordicnumbers.cross-country-final-climb-contract/v1"
PRODUCT = "tour_de_ski_final_climb"


class FinalClimbContractError(RuntimeError):
    """The accepted schedule cannot produce an unambiguous Final Climb plan."""


@dataclass(frozen=True)
class FinalClimbPlan:
    schema_version: str
    logical_date: str
    status: str
    product: str
    trigger: str
    final_climb_date: str | None
    publication_window_start: str | None
    event_count: int
    sexes: tuple[str, ...]
    startlist_race_ids: tuple[str, ...]
    missing_startlist_race_ids: tuple[str, ...]
    startlist_mode: str
    acquisition_owner: str
    publication_owner: str
    publication_enabled: bool
    reason: str
    events: tuple[dict[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _write_report(path: Path, rendered: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def build_final_climb_plan(
    *,
    schedule_rows: list[dict[str, str]],
    logical_date: date,
    available_startlist_race_ids: Iterable[str] = (),
) -> FinalClimbPlan:
    """Return a deterministic plan without reading or mutating external state."""
    tour = build_tour_plan(
        schedule_rows=schedule_rows,
        logical_date=logical_date,
    )
    common: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "logical_date": logical_date.isoformat(),
        "product": PRODUCT,
        "trigger": "day_before_or_final_climb_date",
        "acquisition_owner": "ski",
        "publication_owner": "blog",
        "publication_enabled": False,
    }
    events = tuple(event for event in tour.stage_events if event["final_climb"] == "1")
    if not events:
        return FinalClimbPlan(
            **common,
            status="no_work",
            final_climb_date=None,
            publication_window_start=None,
            event_count=0,
            sexes=(),
            startlist_race_ids=(),
            missing_startlist_race_ids=(),
            startlist_mode="not_applicable",
            reason="accepted schedule contains no Tour de Ski Final Climb",
            events=(),
        )

    if len(events) != 2 or {event["sex"] for event in events} != {"M", "L"}:
        raise FinalClimbContractError(
            "Final Climb must contain exactly one men's and one ladies' event"
        )
    event_dates = {date.fromisoformat(event["race_date"]) for event in events}
    if len(event_dates) != 1:
        raise FinalClimbContractError(
            "men's and ladies' Final Climb events must share one race date"
        )
    final_date = event_dates.pop()
    window_start = final_date - timedelta(days=1)
    required = tuple(event["fis_race_id"] for event in events)

    if logical_date not in {window_start, final_date}:
        return FinalClimbPlan(
            **common,
            status="no_work",
            final_climb_date=final_date.isoformat(),
            publication_window_start=window_start.isoformat(),
            event_count=len(events),
            sexes=("M", "L"),
            startlist_race_ids=required,
            missing_startlist_race_ids=(),
            startlist_mode="not_applicable",
            reason="Final Climb prediction runs only during its two-day publication window",
            events=events,
        )

    available = {str(race_id) for race_id in available_startlist_race_ids}
    missing = tuple(race_id for race_id in required if race_id not in available)
    ready = not missing
    return FinalClimbPlan(
        **common,
        status="ready" if ready else "waiting_for_startlists",
        final_climb_date=final_date.isoformat(),
        publication_window_start=window_start.isoformat(),
        event_count=len(events),
        sexes=("M", "L"),
        startlist_race_ids=required,
        missing_startlist_race_ids=missing,
        startlist_mode="official_final_climb_startlists"
        if ready
        else "not_yet_published",
        reason=(
            "both official Final Climb startlists are available"
            if ready
            else "wait for both official Final Climb startlists and retry within the publication window"
        ),
        events=events,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", type=Path, required=True)
    parser.add_argument("--logical-date", type=date.fromisoformat, required=True)
    parser.add_argument("--available-startlist-race-id", action="append", default=[])
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    plan = build_final_climb_plan(
        schedule_rows=read_schedule(args.schedule),
        logical_date=args.logical_date,
        available_startlist_race_ids=args.available_startlist_race_id,
    )
    rendered = json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n"
    if args.report:
        _write_report(args.report, rendered)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
