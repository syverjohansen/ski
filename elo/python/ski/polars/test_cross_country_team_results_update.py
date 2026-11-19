import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from cross_country_team_event_audit import TeamEventAudit, TeamScheduleEvent
from cross_country_team_results_update import (
    TeamResultsUpdateError,
    build_team_results_update,
)


def missing_event() -> TeamScheduleEvent:
    return TeamScheduleEvent(
        date="2026-12-11",
        sex="M",
        season=2027,
        race=4,
        city="Davos",
        country="Switzerland",
        distance="Ts",
        technique="F",
        championship="0",
        results_url="https://www.fis-ski.com/results",
    )


def audit(*missing: TeamScheduleEvent) -> TeamEventAudit:
    return TeamEventAudit(
        scheduled_team_events=3,
        future_team_events=1,
        completed_team_events=2,
        covered_relay_events=1,
        covered_team_sprint_events=0 if missing else 1,
        accepted_team_rows=10,
        missing=tuple(missing),
    )


class CrossCountryTeamResultsUpdateTests(unittest.TestCase):
    def arguments(self, root: Path) -> dict[str, object]:
        return {
            "schedule": root / "races.csv",
            "men_chrono": root / "men.csv",
            "ladies_chrono": root / "ladies.csv",
            "men_relay_chrono": root / "men_relay.csv",
            "ladies_relay_chrono": root / "ladies_relay.csv",
            "team_sprint_chrono": root / "team_sprint.csv",
            "aliases": root / "aliases.csv",
            "as_of": date(2026, 12, 11),
            "output_root": root / "output",
        }

    @patch("cross_country_team_results_update.acquire_team_backfill")
    @patch("cross_country_team_results_update.audit_team_events")
    def test_no_gaps_writes_report_without_acquisition(
        self, audit_team_events, acquire_team_backfill
    ) -> None:
        audit_team_events.return_value = audit()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = build_team_results_update(**self.arguments(root))
            self.assertTrue((root / "output/gap-report.json").is_file())
            self.assertTrue((root / "output/report.json").is_file())
        self.assertEqual(report["status"], "no_changes")
        self.assertEqual(report["missing_team_event_count"], 0)
        acquire_team_backfill.assert_not_called()

    @patch("cross_country_team_results_update.audit_team_events")
    @patch("cross_country_team_results_update.promote_team_results")
    @patch("cross_country_team_results_update.replay_team_results")
    @patch("cross_country_team_results_update.map_team_identities")
    @patch("cross_country_team_results_update.acquire_team_backfill")
    def test_candidate_runs_every_stage_and_closes_gap(
        self, acquire, identity, replay, promote, audit_team_events
    ) -> None:
        gap = missing_event()
        audit_team_events.side_effect = [audit(gap), audit()]
        acquire.return_value = {
            "acquired_team_event_count": 1,
            "acquired_classified_teams": 12,
            "acquired_member_rows": 24,
        }
        identity.return_value = {"match_methods": {"unique_normalized_name": 24}}
        replay.return_value = {
            "season": 2027,
            "athlete_rows": {"M": 24, "L": 0},
            "team_rows": {"team_sprint_elo_rows.csv": 12},
        }
        promote.return_value = {"outputs": [{"appended_rows": 24}]}
        with tempfile.TemporaryDirectory() as directory:
            report = build_team_results_update(**self.arguments(Path(directory)))
        self.assertEqual(report["status"], "candidate")
        self.assertEqual(report["missing_team_event_count_before"], 1)
        self.assertEqual(report["missing_team_event_count_after"], 0)
        self.assertEqual(report["replayed_athlete_rows"], {"M": 24, "L": 0})
        self.assertFalse(report["accepted_files_modified"])
        acquire.assert_called_once()
        identity.assert_called_once()
        replay.assert_called_once()
        promote.assert_called_once()

    @patch("cross_country_team_results_update.audit_team_events")
    @patch("cross_country_team_results_update.promote_team_results")
    @patch("cross_country_team_results_update.replay_team_results")
    @patch("cross_country_team_results_update.map_team_identities")
    @patch("cross_country_team_results_update.acquire_team_backfill")
    def test_remaining_gap_fails_closed(
        self, acquire, identity, replay, promote, audit_team_events
    ) -> None:
        gap = missing_event()
        audit_team_events.side_effect = [audit(gap), audit(gap)]
        acquire.return_value = {
            "acquired_team_event_count": 1,
            "acquired_classified_teams": 12,
            "acquired_member_rows": 24,
        }
        identity.return_value = {"match_methods": {}}
        replay.return_value = {
            "season": 2027,
            "athlete_rows": {"M": 24, "L": 0},
            "team_rows": {},
        }
        promote.return_value = {"outputs": []}
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(
            TeamResultsUpdateError, "still has completed team gaps"
        ):
            build_team_results_update(**self.arguments(Path(directory)))


if __name__ == "__main__":
    unittest.main()
