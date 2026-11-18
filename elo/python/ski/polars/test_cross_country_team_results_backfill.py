import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_results_backfill import BackfillError
from cross_country_team_results_backfill import (
    acquire_team_backfill,
    parse_team_results,
    validate_team_page_identity,
)
from cross_country_team_event_audit import TeamScheduleEvent

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "tests" / "fixtures" / "cc-team-results.html"
TEAM_SPRINT_FIXTURE = HERE / "tests" / "fixtures" / "cc-team-sprint-results.html"


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryTeamResultsBackfillTests(unittest.TestCase):
    def test_parses_relay_teams_and_ordered_members(self) -> None:
        teams = parse_team_results(FIXTURE.read_bytes(), distance="Rel")
        self.assertEqual(len(teams), 3)
        self.assertEqual(teams[0].place, 1)
        self.assertEqual(teams[0].team_name, "NORWAY")
        self.assertEqual(teams[0].nation, "NOR")
        self.assertEqual(teams[0].bib, "2")
        self.assertEqual([member.order for member in teams[0].members], [1, 2, 3, 4])
        self.assertEqual(teams[0].members[0].fis_competitor_id, "1001")
        self.assertEqual(teams[0].members[0].fis_code, "3420001")

    def test_team_sprint_uses_final_only(self) -> None:
        teams = parse_team_results(TEAM_SPRINT_FIXTURE.read_bytes(), distance="Ts")
        self.assertEqual(len(teams), 3)
        self.assertEqual([member.name for member in teams[0].members], ["SKIER Alpha", "SKIER Beta"])
        self.assertNotIn("EXCLUDED", {team.team_name for team in teams})

    def test_rejects_incomplete_team(self) -> None:
        html = FIXTURE.read_text(encoding="utf-8").replace(
            '<a class="table-row table-row_theme_additional" href="?competitorid=1004"><span class="bib">2-4</span><div class="hidden-xs justify-right gray pr-1">3420004</div><div class="justify-left bold">SKIER Delta</div></a>',
            "",
        )
        with self.assertRaisesRegex(BackfillError, "3 members"):
            parse_team_results(html.encode(), distance="Rel")

    def test_rejects_wrong_team_page_identity(self) -> None:
        event = TeamScheduleEvent(
            date="2026-02-15",
            sex="L",
            season=2026,
            race=10,
            city="Testville",
            country="Norway",
            distance="Rel",
            technique="N/A",
            championship="1",
            results_url="https://www.fis-ski.com/DB/general/results.html?raceid=49731&sectorcode=CC",
        )
        with self.assertRaisesRegex(BackfillError, "identity mismatch"):
            validate_team_page_identity(FIXTURE.read_bytes(), event)

    def test_acquires_missing_team_event_without_mutating_histories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            schedule = sources / "races.csv"
            men_relay = sources / "men_relay.csv"
            ladies_relay = sources / "ladies_relay.csv"
            team_sprint = sources / "team_sprint.csv"
            write_csv(
                schedule,
                ["Date", "Startlist", "Sex", "City", "Country", "Distance", "Technique", "Race", "Championship"],
                [{
                    "Date": "02/15/2026",
                    "Startlist": "https://www.fis-ski.com/DB/general/results.html?raceid=49731&sectorcode=CC",
                    "Sex": "M",
                    "City": "Testville",
                    "Country": "Norway",
                    "Distance": "Rel",
                    "Technique": "N/A",
                    "Race": 10,
                    "Championship": 1,
                }],
            )
            fields = ["Date", "Season", "Race", "Sex", "Distance"]
            for path in (men_relay, ladies_relay, team_sprint):
                write_csv(path, fields, [])
            mtimes = {path: path.stat().st_mtime_ns for path in (schedule, men_relay, ladies_relay, team_sprint)}

            output = root / "candidate"
            report = acquire_team_backfill(
                schedule=schedule,
                men_relay_chrono=men_relay,
                ladies_relay_chrono=ladies_relay,
                team_sprint_chrono=team_sprint,
                as_of=date(2026, 2, 15),
                output_dir=output,
                fetcher=lambda _url: FIXTURE.read_bytes(),
            )

            self.assertEqual(report["acquired_team_event_count"], 1)
            self.assertEqual(report["acquired_classified_teams"], 3)
            self.assertEqual(report["acquired_member_rows"], 12)
            self.assertFalse(report["accepted_histories_mutated"])
            with (output / "team_results.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 12)
            self.assertEqual(rows[0]["fis_competitor_id"], "1001")
            self.assertEqual(rows[0]["distance"], "Rel")
            self.assertTrue((output / "races" / "M-2026-010-Rel.csv").is_file())
            self.assertEqual(mtimes, {path: path.stat().st_mtime_ns for path in mtimes})


if __name__ == "__main__":
    unittest.main()
