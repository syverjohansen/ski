from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from schedule_contract import (
    RACES_COLUMNS,
    WEEKENDS_COLUMNS,
    ScheduleContractError,
    derive_weekends,
    infer_fis_season,
    read_csv,
    validate_races,
    validate_relationship,
    validate_season_structure,
    write_csv_atomic,
)
from schedule_sources import calendar_sources, championship_category

ROOT = Path(__file__).resolve().parent
ACCEPTED_RACES = ROOT / "excel365/races.csv"
ACCEPTED_WEEKENDS = ROOT / "excel365/weekends.csv"


class NordicCombinedScheduleContractTests(unittest.TestCase):
    def test_current_schedule_passes_contract(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        weekends = read_csv(ACCEPTED_WEEKENDS, WEEKENDS_COLUMNS)
        self.assertEqual(35, validate_races(races)["rows"])
        self.assertEqual(2026, infer_fis_season(races))
        structure = validate_season_structure(races, expected_season=2026)
        self.assertEqual("OWG", structure["championship_category"])
        self.assertEqual(3, structure["championship_rows"])
        self.assertEqual({"status": "passed", "rows": 35}, validate_relationship(races, weekends))

    def test_current_weekends_are_reproduced_byte_for_byte(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        expected = read_csv(ACCEPTED_WEEKENDS, WEEKENDS_COLUMNS)
        self.assertEqual(expected, derive_weekends(races))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "weekends.csv"
            write_csv_atomic(output, WEEKENDS_COLUMNS, derive_weekends(races), lineterminator="\n")
            self.assertEqual(ACCEPTED_WEEKENDS.read_bytes(), output.read_bytes())

    def test_individual_team_sprint_and_mixed_rows_remain_distinct(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        olympic_team_sprint = next(
            row for row in races if row["Championship"] == "1" and row["RaceType"] == "Team Sprint"
        )
        mixed_team_sprint = next(row for row in races if row["Sex"] == "Mixed")
        self.assertEqual("M", olympic_team_sprint["Sex"])
        self.assertEqual("Mixed", mixed_team_sprint["Sex"])
        self.assertNotEqual(olympic_team_sprint["Race"], mixed_team_sprint["Race"])

    def test_calendar_sources_use_nordic_combined_sector(self) -> None:
        sources = calendar_sources(2027)
        self.assertEqual(["World Cup", "World Championships"], [label for _, label in sources])
        self.assertTrue(all("sectorcode=NK" in url for url, _ in sources))
        self.assertTrue(all("seasoncode=2027" in url for url, _ in sources))
        self.assertEqual(("OWG", "Olympics"), championship_category(2030))

    def test_wrong_sector_and_schema_drift_fail_closed(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        wrong = [dict(row) for row in races]
        wrong[0]["Startlist"] = wrong[0]["Startlist"].replace("sectorcode=NK", "sectorcode=JP")
        with self.assertRaisesRegex(ScheduleContractError, "Nordic-combined"):
            validate_races(wrong)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.csv"
            path.write_text("Date,Unexpected\n11/28/2025,value\n", encoding="utf-8")
            with self.assertRaisesRegex(ScheduleContractError, "unexpected columns"):
                read_csv(path, RACES_COLUMNS)

    def test_rollover_must_match_requested_season(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        with self.assertRaisesRegex(ScheduleContractError, "outside requested FIS season 2027"):
            validate_season_structure(races, expected_season=2027)


if __name__ == "__main__":
    unittest.main()
