#!/usr/bin/env python3
"""Acquire a Nordic-combined schedule into an isolated validated shadow."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from race_schedule_output import save_nordic_combined_races
from schedule_contract import (
    ScheduleContractError,
    build_shadow,
    file_sha256,
    write_json_atomic,
)
from schedule_sources import calendar_sources

REPORT_SCHEMA = "nordicnumbers.nordic-combined-schedule-acquisition/v1"


def _contains(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def validate_isolated_output(output_root: Path, accepted_races: Path) -> None:
    output = output_root.expanduser().resolve()
    accepted = accepted_races.expanduser().resolve()
    if _contains(accepted.parent, output) or _contains(output, accepted):
        raise ScheduleContractError(
            "output_root must be isolated from the accepted schedule directory"
        )


def run_shadow_acquisition(
    *,
    season: int,
    accepted_races: Path,
    output_root: Path,
    include_championships: bool = True,
    max_removed_fraction: float = 0.1,
    scrape_fn: Callable[..., list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    validate_isolated_output(output_root, accepted_races)
    started_at = datetime.now(timezone.utc)
    if scrape_fn is None:
        try:
            from race_scrape import scrape_nordic_combined
        except ImportError as exc:
            raise ScheduleContractError(
                "scraper dependencies are unavailable; install requests, beautifulsoup4, pandas, and fuzzywuzzy"
            ) from exc
        scrape_fn = scrape_nordic_combined
    races = scrape_fn(season=season, include_championships=include_championships)
    if not races:
        raise ScheduleContractError("FIS acquisition returned no Nordic-combined races")
    acquired = output_root / "acquisition/races.csv"
    save_nordic_combined_races(races, acquired)
    try:
        shadow = build_shadow(
            acquired,
            accepted_races,
            output_root,
            max_removed_fraction=max_removed_fraction,
            expected_candidate_season=season,
        )
    except (OSError, ScheduleContractError, ValueError) as exc:
        failed_report = {
            "schema_version": REPORT_SCHEMA,
            "status": "failed",
            "season": season,
            "include_championships": include_championships,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "rows": len(races),
            "error": str(exc),
            "acquired_races": {"file": str(acquired), "sha256": file_sha256(acquired)},
            "promotion": {"status": "blocked", "performed": False},
        }
        write_json_atomic(output_root / "reports/acquisition.json", failed_report)
        raise
    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "season": season,
        "include_championships": include_championships,
        "sources": [
            {"category": category, "url": url}
            for url, category in calendar_sources(season, include_championships=include_championships)
        ],
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "rows": len(races),
        "acquired_races": {"file": str(acquired), "sha256": file_sha256(acquired)},
        "candidate": shadow["candidate"],
        "accepted": shadow["accepted"],
        "comparison": shadow["comparison"],
        "season_transition": shadow["season_transition"],
        "promotion": shadow["promotion"],
    }
    path = output_root / "reports/acquisition.json"
    write_json_atomic(path, report)
    return {**report, "report": str(path)}


def main() -> int:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--accepted-races", type=Path, default=root / "excel365/races.csv")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--world-cup-only", action="store_true")
    parser.add_argument("--max-removed-fraction", type=float, default=0.1)
    args = parser.parse_args()
    try:
        result = run_shadow_acquisition(
            season=args.season,
            accepted_races=args.accepted_races,
            output_root=args.output_root,
            include_championships=not args.world_cup_only,
            max_removed_fraction=args.max_removed_fraction,
        )
    except (OSError, RuntimeError, ScheduleContractError, ValueError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
