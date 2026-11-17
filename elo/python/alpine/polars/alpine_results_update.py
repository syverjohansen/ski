#!/usr/bin/env python3
"""Build a reviewable Alpine results update without mutating accepted history."""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import date
from pathlib import Path
from typing import Callable

from alpine_results_backfill import acquire_backfill
from alpine_results_elo_replay import replay_results
from alpine_results_gap import MissingRace, build_report, find_missing_races
from alpine_results_identity import map_identities
from alpine_results_promote import promote_results


class ResultsUpdateError(RuntimeError):
    """Raised when a candidate cannot close all completed result gaps."""


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(rendered)
        temporary = Path(handle.name)
    temporary.replace(path)


def build_results_update(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    aliases: Path,
    as_of: date,
    output_root: Path,
    fetcher: Callable[[str], bytes] | None = None,
) -> dict[str, object]:
    output_root = output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    missing, completed_count = find_missing_races(
        schedule, men_chrono, ladies_chrono, as_of
    )
    gap_report = build_report(
        missing, as_of=as_of, completed_count=completed_count
    )
    _atomic_json(output_root / "gap-report.json", gap_report)
    if not missing:
        report: dict[str, object] = {
            "schema_version": "nordicnumbers.alpine-results-update/v1",
            "as_of": as_of.isoformat(),
            "status": "no_changes",
            "completed_schedule_races": completed_count,
            "missing_race_count": 0,
        }
        _atomic_json(output_root / "report.json", report)
        return report

    acquisition_arguments: dict[str, object] = {
        "schedule": schedule,
        "men_chrono": men_chrono,
        "ladies_chrono": ladies_chrono,
        "as_of": as_of,
        "output_dir": output_root / "acquisition",
    }
    if fetcher is not None:
        acquisition_arguments["fetcher"] = fetcher
    acquisition = acquire_backfill(**acquisition_arguments)
    identity = map_identities(
        results=output_root / "acquisition/results.csv",
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        aliases_path=aliases,
        output_dir=output_root / "identity",
    )
    replay = replay_results(
        mapped_results=output_root / "identity/mapped_results.csv",
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        output_dir=output_root / "replay",
    )
    season = int(replay["season"])
    promotion = promote_results(
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        men_rows=output_root / "replay/men_elo_rows.csv",
        ladies_rows=output_root / "replay/ladies_elo_rows.csv",
        expected_season=season,
        output_dir=output_root / "promotion",
    )
    remaining, promoted_completed_count = find_missing_races(
        schedule,
        output_root / "promotion/men_chrono.csv",
        output_root / "promotion/ladies_chrono.csv",
        as_of,
    )
    if remaining:
        raise ResultsUpdateError(
            "promoted candidate still has completed gaps: "
            + ", ".join(_gap_label(item) for item in remaining)
        )

    report = {
        "schema_version": "nordicnumbers.alpine-results-update/v1",
        "as_of": as_of.isoformat(),
        "status": "candidate",
        "season": season,
        "completed_schedule_races": promoted_completed_count,
        "missing_race_count_before": len(missing),
        "missing_race_count_after": 0,
        "acquired_race_count": acquisition["acquired_race_count"],
        "acquired_result_rows": acquisition["acquired_result_rows"],
        "unique_fis_athletes": identity["unique_fis_athletes"],
        "identity_match_methods": identity["match_methods"],
        "replayed_result_rows": replay["result_rows"],
        "promotion_outputs": promotion["outputs"],
    }
    _atomic_json(output_root / "report.json", report)
    return report


def _gap_label(item: MissingRace) -> str:
    return f"{item.date}/{item.sex}/race-{item.race}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="Required acknowledgement before contacting FIS when gaps exist",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    missing, _ = find_missing_races(
        args.schedule, args.men_chrono, args.ladies_chrono, args.as_of
    )
    if missing and not args.allow_network:
        raise ResultsUpdateError("result acquisition requires --allow-network")
    try:
        report = build_results_update(
            schedule=args.schedule,
            men_chrono=args.men_chrono,
            ladies_chrono=args.ladies_chrono,
            aliases=args.aliases,
            as_of=args.as_of,
            output_root=args.output_root,
        )
    except Exception as exc:
        _atomic_json(
            args.output_root / "report.json",
            {
                "schema_version": "nordicnumbers.alpine-results-update/v1",
                "as_of": args.as_of.isoformat(),
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        raise
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
