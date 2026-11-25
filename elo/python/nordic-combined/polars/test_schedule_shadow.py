from __future__ import annotations

import csv
import json
import tempfile
import types
import unittest
from pathlib import Path

from race_schedule_output import save_nordic_combined_races
from schedule_contract import ScheduleContractError
from scrape_schedule_shadow import run_shadow_acquisition

ROOT = Path(__file__).resolve().parent
ACCEPTED = ROOT / "excel365/races.csv"


def acquired_rows() -> list[dict[str, str]]:
    with ACCEPTED.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        {
            "date": row["Date"],
            "startlist_url": row["Startlist"],
            "sex": row["Sex"],
            "city": row["City"],
            "country": row["Country"],
            "distance": row["Distance"],
            "race_type": row["RaceType"],
            "period": "",
            "elevation": row["Elevation"],
            "championship": row["Championship"],
        }
        for row in rows
    ]


class NordicCombinedScheduleShadowTests(unittest.TestCase):
    def test_fis_ascii_venue_alias_uses_reviewed_elevation(self) -> None:
        from race_scrape import fuzzy_match_elevation, load_elevation_data

        elevations = load_elevation_data()
        self.assertEqual("139", fuzzy_match_elevation("Otepaa", elevations))

    def test_serializer_reproduces_accepted_races(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "races.csv"
            save_nordic_combined_races(acquired_rows(), output)
            self.assertEqual(ACCEPTED.read_bytes(), output.read_bytes())

    def test_mocked_acquisition_builds_isolated_candidate(self) -> None:
        original = ACCEPTED.read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "shadow"
            result = run_shadow_acquisition(
                season=2026,
                accepted_races=ACCEPTED,
                output_root=output,
                scrape_fn=lambda **_: acquired_rows(),
            )
            self.assertEqual(original, ACCEPTED.read_bytes())
            self.assertEqual("eligible", result["promotion"]["status"])
            self.assertFalse(result["promotion"]["performed"])
            self.assertEqual([], result["comparison"]["removed"])
            self.assertTrue((output / "candidate/races.csv").is_file())
            self.assertTrue((output / "candidate/weekends.csv").is_file())
            report = json.loads((output / "reports/acquisition.json").read_text())
            self.assertEqual(["World Cup", "Olympics"], [row["category"] for row in report["sources"]])

    def test_empty_or_nonisolated_acquisition_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ScheduleContractError, "returned no"):
                run_shadow_acquisition(
                    season=2026,
                    accepted_races=ACCEPTED,
                    output_root=Path(directory) / "shadow",
                    scrape_fn=lambda **_: [],
                )
        with self.assertRaisesRegex(ScheduleContractError, "isolated"):
            run_shadow_acquisition(
                season=2026,
                accepted_races=ACCEPTED,
                output_root=ACCEPTED.parent,
                scrape_fn=lambda **_: acquired_rows(),
            )

    def test_incomplete_same_season_candidate_is_blocked(self) -> None:
        rows = acquired_rows()
        incomplete = rows[:10] + [row for row in rows if row["championship"] == "1"]
        with tempfile.TemporaryDirectory() as directory:
            result = run_shadow_acquisition(
                season=2026,
                accepted_races=ACCEPTED,
                output_root=Path(directory) / "shadow",
                scrape_fn=lambda **_: incomplete,
            )
            self.assertEqual("blocked", result["promotion"]["status"])
            self.assertFalse(result["comparison"]["within_removal_limit"])

    def test_contract_failure_leaves_a_failed_nonpromoting_report(self) -> None:
        rows = acquired_rows()
        rows[0]["elevation"] = ""
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "shadow"
            with self.assertRaisesRegex(ScheduleContractError, "blank required fields"):
                run_shadow_acquisition(
                    season=2026,
                    accepted_races=ACCEPTED,
                    output_root=output,
                    scrape_fn=lambda **_: rows,
                )
            report = json.loads((output / "reports/acquisition.json").read_text())
            self.assertEqual("failed", report["status"])
            self.assertEqual({"status": "blocked", "performed": False}, report["promotion"])
            self.assertEqual(35, report["rows"])

    def test_selected_fis_source_without_calendar_data_fails_closed(self) -> None:
        from race_scrape import scrape_nordic_combined

        response = types.SimpleNamespace(content=b"<html></html>")
        with self.assertRaisesRegex(RuntimeError, "World Cup.*no calendar data"):
            scrape_nordic_combined(
                season=2026,
                include_championships=False,
                request_fn=lambda *_: response,
                sleep_fn=lambda _: None,
            )


if __name__ == "__main__":
    unittest.main()
