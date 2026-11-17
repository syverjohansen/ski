#!/usr/bin/env python3
"""Acquire an Alpine schedule and validate it without promoting accepted data."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from race_schedule_output import save_alpine_races
from schedule_sources import calendar_sources
from schedule_contract import (
    ScheduleContractError,
    build_shadow,
    file_sha256,
    write_json_atomic,
)


REPORT_SCHEMA = "nordicnumbers.alpine-schedule-acquisition/v1"


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
    """Scrape to an isolated location, then build the validated schedule pair."""

    validate_isolated_output(output_root, accepted_races)
    started_at = datetime.now(timezone.utc)
    if scrape_fn is None:
        try:
            from race_scrape import scrape_alpine_races
        except ImportError as exc:
            raise ScheduleContractError(
                "scraper dependencies are unavailable; install requests and "
                "beautifulsoup4"
            ) from exc

        scrape_fn = scrape_alpine_races
    races = scrape_fn(
        season=season,
        include_championships=include_championships,
    )
    if not races:
        raise ScheduleContractError("FIS acquisition returned no Alpine races")

    acquired_races = output_root / "acquisition" / "races.csv"
    acquired_races.parent.mkdir(parents=True, exist_ok=True)
    save_alpine_races(races, acquired_races)

    shadow = build_shadow(
        acquired_races,
        accepted_races,
        output_root,
        max_removed_fraction=max_removed_fraction,
    )
    acquisition = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "season": season,
        "include_championships": include_championships,
        "sources": [
            {"category": category, "url": url}
            for url, category in calendar_sources(season, include_championships)
        ],
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "rows": len(races),
        "acquired_races": {
            "file": str(acquired_races),
            "sha256": file_sha256(acquired_races),
        },
        "candidate": shadow["candidate"],
        "comparison": shadow["comparison"],
        "promotion": shadow["promotion"],
    }
    report_path = output_root / "reports" / "acquisition.json"
    write_json_atomic(report_path, acquisition)
    return {**acquisition, "report": str(report_path)}


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument(
        "--accepted-races",
        type=Path,
        default=root / "excel365" / "races.csv",
    )
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--world-cup-only", action="store_true")
    parser.add_argument("--max-removed-fraction", type=float, default=0.1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_root = args.output_root.expanduser().resolve()
    try:
        result = run_shadow_acquisition(
            season=args.season,
            accepted_races=args.accepted_races.expanduser().resolve(),
            output_root=output_root,
            include_championships=not args.world_cup_only,
            max_removed_fraction=args.max_removed_fraction,
        )
    except (OSError, RuntimeError, ScheduleContractError, ValueError) as exc:
        failure = {
            "schema_version": REPORT_SCHEMA,
            "status": "failed",
            "season": args.season,
            "error": str(exc),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "promotion": {"status": "blocked", "performed": False},
        }
        try:
            write_json_atomic(output_root / "reports" / "acquisition.json", failure)
        except OSError:
            pass
        print(json.dumps(failure, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
