#!/usr/bin/env python3
"""Plan the read-only inputs for one Tour de Ski overall prediction run.

The accepted ``weekends.csv`` schedule is authoritative.  A Tour is the block
whose rows are marked both ``Period=2`` and ``Stage=1``.  The overall product
runs once, on the first Tour date.  Only the men's and ladies' opening-stage
startlists are required at that point; future-stage startlists are deliberately
not treated as available inputs before those races are published by FIS.

This module does not fetch startlists, run a model, or publish a post.  It emits
a deterministic plan that the acquisition workflow can execute and the blog
workflow can consume later.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

from schedule_contract import WEEKENDS_COLUMNS

SCHEMA_VERSION = "nordicnumbers.cross-country-tour-contract/v1"
PRODUCT = "tour_de_ski_overall"


class TourContractError(RuntimeError):
    """The accepted schedule cannot produce an unambiguous Tour plan."""


@dataclass(frozen=True)
class TourPlan:
    schema_version: str
    logical_date: str
    status: str
    product: str
    trigger: str
    tour_start_date: str | None
    tour_end_date: str | None
    stage_count: int
    event_count: int
    opening_event_count: int
    sexes: tuple[str, ...]
    opening_startlist_race_ids: tuple[str, ...]
    missing_startlist_race_ids: tuple[str, ...]
    startlist_mode: str
    acquisition_owner: str
    publication_owner: str
    publication_enabled: bool
    reason: str
    stage_events: tuple[dict[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_date(value: str, *, field: str) -> date:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as exc:
        raise TourContractError(f"invalid {field}: {value!r}") from exc


def _race_id(url: str) -> str:
    parsed = urlparse(url)
    race_ids = parse_qs(parsed.query).get("raceid", [])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.fis-ski.com"
        or parsed.path != "/DB/general/results.html"
        or parse_qs(parsed.query).get("sectorcode") != ["CC"]
        or len(race_ids) != 1
        or not race_ids[0].isdigit()
    ):
        raise TourContractError(f"unsupported FIS cross-country URL: {url}")
    return race_ids[0]


def read_schedule(path: Path) -> list[dict[str, str]]:
    import csv

    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            observed = tuple(reader.fieldnames or ())
            if observed != WEEKENDS_COLUMNS:
                raise TourContractError(
                    f"unexpected weekend schedule columns: {observed}; "
                    f"expected {WEEKENDS_COLUMNS}"
                )
            return [dict(row) for row in reader]
    except OSError as exc:
        raise TourContractError(f"cannot read schedule {path}: {exc}") from exc


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


def _validate_tour_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    inconsistent = [
        row for row in rows if (row["Period"] == "2") != (row["Stage"] == "1")
    ]
    if inconsistent:
        raise TourContractError(
            "Tour schedule markers disagree: Period=2 and Stage=1 must be paired"
        )

    tour = [row for row in rows if row["Period"] == "2" and row["Stage"] == "1"]
    if not tour:
        return []

    seen_ids: set[str] = set()
    stages: dict[str, set[str]] = {}
    for row in tour:
        if row["Sex"] not in {"M", "L"}:
            raise TourContractError(f"invalid Tour sex: {row['Sex']!r}")
        race_id = _race_id(row["Startlist"])
        if race_id in seen_ids:
            raise TourContractError(f"duplicate Tour FIS race ID: {race_id}")
        seen_ids.add(race_id)
        _parse_date(row["Date"], field="Date")
        _parse_date(row["Race_Date"], field="Race_Date")
        stages.setdefault(row["Race"], set()).add(row["Sex"])

    incomplete = sorted(stage for stage, sexes in stages.items() if sexes != {"M", "L"})
    if incomplete:
        raise TourContractError(
            f"Tour stages must contain one men's and one ladies' event: {incomplete}"
        )

    ordered = sorted(
        tour,
        key=lambda row: (
            _parse_date(row["Race_Date"], field="Race_Date"),
            int(row["Race"]),
            0 if row["Sex"] == "M" else 1,
        ),
    )
    final_date = _parse_date(ordered[-1]["Race_Date"], field="Race_Date")
    final_climb = [row for row in ordered if row["Final_Climb"] == "1"]
    if {row["Sex"] for row in final_climb} != {"M", "L"} or any(
        _parse_date(row["Race_Date"], field="Race_Date") != final_date
        for row in final_climb
    ):
        raise TourContractError(
            "Tour schedule must mark the men's and ladies' last-stage events as Final_Climb"
        )
    return ordered


def build_tour_plan(
    *,
    schedule_rows: list[dict[str, str]],
    logical_date: date,
    available_startlist_race_ids: Iterable[str] = (),
) -> TourPlan:
    tour = _validate_tour_rows(schedule_rows)
    common: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "logical_date": logical_date.isoformat(),
        "product": PRODUCT,
        "trigger": "tour_opening_date",
        "acquisition_owner": "ski",
        "publication_owner": "blog",
        "publication_enabled": False,
    }
    if not tour:
        return TourPlan(
            **common,
            status="no_work",
            tour_start_date=None,
            tour_end_date=None,
            stage_count=0,
            event_count=0,
            opening_event_count=0,
            sexes=(),
            opening_startlist_race_ids=(),
            missing_startlist_race_ids=(),
            startlist_mode="not_applicable",
            reason="accepted schedule contains no Tour de Ski stage block",
            stage_events=(),
        )

    start = min(_parse_date(row["Race_Date"], field="Race_Date") for row in tour)
    end = max(_parse_date(row["Race_Date"], field="Race_Date") for row in tour)
    stages = {row["Race"] for row in tour}
    events = tuple(
        {
            "race_date": _parse_date(row["Race_Date"], field="Race_Date").isoformat(),
            "race_number": row["Race"],
            "fis_race_id": _race_id(row["Startlist"]),
            "sex": row["Sex"],
            "city": row["City"],
            "country": row["Country"],
            "distance": row["Distance"],
            "technique": row["Technique"],
            "final_climb": row["Final_Climb"],
        }
        for row in tour
    )

    if logical_date != start:
        return TourPlan(
            **common,
            status="no_work",
            tour_start_date=start.isoformat(),
            tour_end_date=end.isoformat(),
            stage_count=len(stages),
            event_count=len(tour),
            opening_event_count=0,
            sexes=("M", "L"),
            opening_startlist_race_ids=(),
            missing_startlist_race_ids=(),
            startlist_mode="not_applicable",
            reason="overall Tour prediction runs only on the opening date",
            stage_events=events,
        )

    opening = [
        row
        for row in tour
        if _parse_date(row["Race_Date"], field="Race_Date") == start
    ]
    if {row["Sex"] for row in opening} != {"M", "L"} or len(opening) != 2:
        raise TourContractError(
            "Tour opening must contain exactly one men's and one ladies' event"
        )
    required = tuple(_race_id(row["Startlist"]) for row in opening)
    available = {str(race_id) for race_id in available_startlist_race_ids}
    missing = tuple(race_id for race_id in required if race_id not in available)
    ready = not missing
    return TourPlan(
        **common,
        status="ready" if ready else "waiting_for_startlists",
        tour_start_date=start.isoformat(),
        tour_end_date=end.isoformat(),
        stage_count=len(stages),
        event_count=len(tour),
        opening_event_count=len(opening),
        sexes=("M", "L"),
        opening_startlist_race_ids=required,
        missing_startlist_race_ids=missing,
        startlist_mode="official_opening_startlists" if ready else "not_yet_published",
        reason=(
            "both official opening-stage startlists are available"
            if ready
            else "wait for both official opening-stage startlists; future stages are not required"
        ),
        stage_events=events,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", type=Path, required=True)
    parser.add_argument("--logical-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--available-startlist-race-id", action="append", default=[]
    )
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    plan = build_tour_plan(
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
