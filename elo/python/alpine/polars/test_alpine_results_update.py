import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from alpine_results_gap import MissingRace
from alpine_results_update import ResultsUpdateError, build_results_update


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class AlpineResultsUpdateTest(unittest.TestCase):
    def test_no_gaps_writes_no_change_report_without_acquisition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "races.csv"
            men = root / "men.csv"
            ladies = root / "ladies.csv"
            write_csv(
                schedule,
                ["Date", "Startlist", "Sex", "City", "Country", "Distance", "Race"],
                [{
                    "Date": "10/25/2026", "Startlist": "https://www.fis-ski.com/results",
                    "Sex": "M", "City": "Soelden", "Country": "Austria",
                    "Distance": "Giant Slalom", "Race": 1,
                }],
            )
            write_csv(men, ["Date", "Season", "Race"], [
                {"Date": "2026-10-25", "Season": 2027, "Race": 1}
            ])
            write_csv(ladies, ["Date", "Season", "Race"], [])

            with patch("alpine_results_update.acquire_backfill") as acquire:
                report = build_results_update(
                    schedule=schedule, men_chrono=men, ladies_chrono=ladies,
                    aliases=root / "aliases.csv", as_of=date(2026, 10, 25),
                    output_root=root / "output",
                )

            self.assertEqual(report["status"], "no_changes")
            acquire.assert_not_called()
            self.assertTrue((root / "output/report.json").is_file())

    @patch("alpine_results_update.find_missing_races")
    @patch("alpine_results_update.promote_results")
    @patch("alpine_results_update.replay_results")
    @patch("alpine_results_update.map_identities")
    @patch("alpine_results_update.acquire_backfill")
    def test_candidate_runs_all_stages_and_closes_gap(
        self, acquire, identity, replay, promote, find_missing
    ) -> None:
        gap = MissingRace(
            date="2026-11-15", sex="M", season=2027, race=2, city="Levi",
            country="Finland", distance="Slalom",
            results_url="https://www.fis-ski.com/results",
        )
        find_missing.side_effect = [([gap], 1), ([], 1)]
        acquire.return_value = {"acquired_race_count": 1, "acquired_result_rows": 22}
        identity.return_value = {
            "unique_fis_athletes": 22,
            "match_methods": {"unique_normalized_name": 22},
        }
        replay.return_value = {"season": 2027, "result_rows": {"M": 22, "L": 0}}
        promote.return_value = {"outputs": [{"sex": "M", "appended_rows": 22}]}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = build_results_update(
                schedule=root / "races.csv", men_chrono=root / "men.csv",
                ladies_chrono=root / "ladies.csv", aliases=root / "aliases.csv",
                as_of=date(2026, 11, 15), output_root=root / "output",
            )

        self.assertEqual(report["status"], "candidate")
        self.assertEqual(report["missing_race_count_before"], 1)
        self.assertEqual(report["missing_race_count_after"], 0)
        self.assertEqual(report["season"], 2027)
        acquire.assert_called_once()
        identity.assert_called_once()
        replay.assert_called_once()
        promote.assert_called_once()

    @patch("alpine_results_update.find_missing_races")
    @patch("alpine_results_update.promote_results")
    @patch("alpine_results_update.replay_results")
    @patch("alpine_results_update.map_identities")
    @patch("alpine_results_update.acquire_backfill")
    def test_remaining_gap_fails_closed(
        self, acquire, identity, replay, promote, find_missing
    ) -> None:
        gap = MissingRace(
            date="2026-11-15", sex="M", season=2027, race=2, city="Levi",
            country="Finland", distance="Slalom",
            results_url="https://www.fis-ski.com/results",
        )
        find_missing.side_effect = [([gap], 1), ([gap], 1)]
        acquire.return_value = {"acquired_race_count": 1, "acquired_result_rows": 22}
        identity.return_value = {"unique_fis_athletes": 22, "match_methods": {}}
        replay.return_value = {"season": 2027, "result_rows": {"M": 22, "L": 0}}
        promote.return_value = {"outputs": []}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ResultsUpdateError, "still has completed gaps"):
                build_results_update(
                    schedule=root / "races.csv", men_chrono=root / "men.csv",
                    ladies_chrono=root / "ladies.csv", aliases=root / "aliases.csv",
                    as_of=date(2026, 11, 15), output_root=root / "output",
                )


if __name__ == "__main__":
    unittest.main()
