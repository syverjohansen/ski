#!/usr/bin/env python3
"""Build a reviewable XC team-results update without mutating history."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from cross_country_team_event_audit import (
    TeamEventAudit,
    TeamScheduleEvent,
    audit_team_events,
    build_report,
)
from cross_country_team_results_backfill import acquire_team_backfill
from cross_country_team_results_elo_replay import replay_team_results
from cross_country_team_results_identity import map_team_identities
from cross_country_team_results_promote import promote_team_results


class TeamResultsUpdateError(RuntimeError):
    """Raised when a candidate cannot close all completed team-event gaps."""


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(rendered)
        temporary = Path(handle.name)
    temporary.replace(path)


def _gap_label(item: TeamScheduleEvent) -> str:
    return f"{item.date}/{item.sex}/race-{item.race}/{item.distance}"


def _no_change_report(audit: TeamEventAudit, *, as_of: date) -> dict[str, object]:
    return {
        "schema_version": "nordicnumbers.cross-country-team-results-update/v1",
        "as_of": as_of.isoformat(),
        "status": "no_changes",
        "completed_team_events": audit.completed_team_events,
        "covered_relay_events": audit.covered_relay_events,
        "covered_team_sprint_events": audit.covered_team_sprint_events,
        "missing_team_event_count": 0,
    }


def build_team_results_update(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_relay_chrono: Path,
    ladies_relay_chrono: Path,
    team_sprint_chrono: Path,
    aliases: Path,
    as_of: date,
    output_root: Path,
    fetcher: Callable[[str], bytes] | None = None,
) -> dict[str, object]:
    """Run every isolated team-results stage and prove the candidate closes gaps."""
    output_root = output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    audit_arguments = {
        "schedule": schedule,
        "men_relay_chrono": men_relay_chrono,
        "ladies_relay_chrono": ladies_relay_chrono,
        "team_sprint_chrono": team_sprint_chrono,
        "as_of": as_of,
    }
    audit = audit_team_events(**audit_arguments)
    _atomic_json(output_root / "gap-report.json", build_report(audit, as_of=as_of))
    if not audit.missing:
        report = _no_change_report(audit, as_of=as_of)
        _atomic_json(output_root / "report.json", report)
        return report

    acquisition_arguments: dict[str, object] = {
        **audit_arguments,
        "output_dir": output_root / "acquisition",
    }
    if fetcher is not None:
        acquisition_arguments["fetcher"] = fetcher
    acquisition = acquire_team_backfill(**acquisition_arguments)
    identity = map_team_identities(
        results=output_root / "acquisition/team_results.csv",
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        aliases_path=aliases,
        output_dir=output_root / "identity",
    )
    replay = replay_team_results(
        mapped_results=output_root / "identity/mapped_results.csv",
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        output_dir=output_root / "replay",
    )
    season = int(replay["season"])
    promotion = promote_team_results(
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        men_relay_chrono=men_relay_chrono,
        ladies_relay_chrono=ladies_relay_chrono,
        team_sprint_chrono=team_sprint_chrono,
        replay_dir=output_root / "replay",
        expected_season=season,
        output_dir=output_root / "promotion",
    )

    promoted_audit = audit_team_events(
        schedule=schedule,
        men_relay_chrono=output_root / "promotion/men_relay_chrono.csv",
        ladies_relay_chrono=output_root / "promotion/ladies_relay_chrono.csv",
        team_sprint_chrono=output_root / "promotion/team_sprint_chrono.csv",
        as_of=as_of,
    )
    if promoted_audit.missing:
        raise TeamResultsUpdateError(
            "promoted candidate still has completed team gaps: "
            + ", ".join(_gap_label(item) for item in promoted_audit.missing)
        )

    report: dict[str, object] = {
        "schema_version": "nordicnumbers.cross-country-team-results-update/v1",
        "as_of": as_of.isoformat(),
        "status": "candidate",
        "season": season,
        "completed_team_events": promoted_audit.completed_team_events,
        "missing_team_event_count_before": len(audit.missing),
        "missing_team_event_count_after": 0,
        "acquired_team_event_count": acquisition["acquired_team_event_count"],
        "acquired_classified_teams": acquisition["acquired_classified_teams"],
        "acquired_member_rows": acquisition["acquired_member_rows"],
        "identity_match_methods": identity["match_methods"],
        "replayed_athlete_rows": replay["athlete_rows"],
        "replayed_team_rows": replay["team_rows"],
        "promotion_outputs": promotion["outputs"],
        "accepted_files_modified": False,
    }
    _atomic_json(output_root / "report.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-relay-chrono", required=True, type=Path)
    parser.add_argument("--ladies-relay-chrono", required=True, type=Path)
    parser.add_argument("--team-sprint-chrono", required=True, type=Path)
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
    audit = audit_team_events(
        schedule=args.schedule,
        men_relay_chrono=args.men_relay_chrono,
        ladies_relay_chrono=args.ladies_relay_chrono,
        team_sprint_chrono=args.team_sprint_chrono,
        as_of=args.as_of,
    )
    if audit.missing and not args.allow_network:
        raise TeamResultsUpdateError("team-result acquisition requires --allow-network")
    try:
        report = build_team_results_update(
            schedule=args.schedule,
            men_chrono=args.men_chrono,
            ladies_chrono=args.ladies_chrono,
            men_relay_chrono=args.men_relay_chrono,
            ladies_relay_chrono=args.ladies_relay_chrono,
            team_sprint_chrono=args.team_sprint_chrono,
            aliases=args.aliases,
            as_of=args.as_of,
            output_root=args.output_root,
        )
    except Exception as exc:
        _atomic_json(
            args.output_root / "report.json",
            {
                "schema_version": "nordicnumbers.cross-country-team-results-update/v1",
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
