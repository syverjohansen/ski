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


class SkiJumpScheduleContractTests(unittest.TestCase):
    def test_current_schedule_passes_contract(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        weekends = read_csv(ACCEPTED_WEEKENDS, WEEKENDS_COLUMNS)
        self.assertEqual(75, validate_races(races)["rows"])
        self.assertEqual(2026, infer_fis_season(races))
        structure = validate_season_structure(races, expected_season=2026)
        self.assertEqual("OWG", structure["championship_category"])
        self.assertEqual(6, structure["championship_rows"])
        self.assertEqual(
            {"status": "passed", "rows": 75},
            validate_relationship(races, weekends),
        )

    def test_current_weekends_are_exactly_derived_from_current_races(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        expected = read_csv(ACCEPTED_WEEKENDS, WEEKENDS_COLUMNS)
        self.assertEqual(expected, derive_weekends(races))
        with tempfile.TemporaryDirectory(prefix="skijump-weekends-") as text:
            generated = Path(text) / "weekends.csv"
            write_csv_atomic(
                generated,
                WEEKENDS_COLUMNS,
                derive_weekends(races),
                lineterminator="\n",
            )
            self.assertEqual(ACCEPTED_WEEKENDS.read_bytes(), generated.read_bytes())

    def test_qualification_and_final_dates_remain_distinct(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        row = next(row for row in derive_weekends(races) if row["Race"] == "1" and row["Sex"] == "M")
        self.assertEqual("11/21/2025", row["Race_Date"])
        self.assertEqual("11/22/2025", row["Final_Date"])
        self.assertEqual("11/21/2025", row["Date"])

    def test_team_and_individual_events_have_separate_prediction_windows(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        team = dict(next(row for row in races if row["RaceType"] == "Team Large"))
        individual = dict(next(row for row in races if row["RaceType"] == "Large"))
        team.update(Date="01/01/2026", Final_Date="01/01/2026", Race="1")
        individual.update(
            Date="01/10/2026",
            Final_Date="01/11/2026",
            City=team["City"],
            Race="1",
        )
        rows = derive_weekends([team, individual])
        self.assertEqual("01/01/2026", rows[0]["Date"])
        self.assertEqual("01/10/2026", rows[1]["Date"])

    def test_rollover_must_match_requested_fis_season(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        with self.assertRaisesRegex(
            ScheduleContractError, "outside requested FIS season 2027"
        ):
            validate_season_structure(races, expected_season=2027)

    def test_calendar_sources_are_parameterized_for_ski_jumping(self) -> None:
        worlds = calendar_sources(2027)
        self.assertEqual(
            ["World Cup", "World Championships"],
            [source[1] for source in worlds],
        )
        self.assertTrue(all("sectorcode=JP" in source[0] for source in worlds))
        self.assertTrue(all("seasoncode=2027" in source[0] for source in worlds))
        self.assertIn("categorycode=WSC", worlds[1][0])
        self.assertEqual(("OWG", "Olympics"), championship_category(2030))
        self.assertEqual(1, len(calendar_sources(2028)))

    def test_invalid_sector_and_reversed_dates_fail_closed(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        wrong_sector = [dict(row) for row in races]
        wrong_sector[0]["Startlist"] = wrong_sector[0]["Startlist"].replace(
            "sectorcode=JP", "sectorcode=CC"
        )
        with self.assertRaisesRegex(ScheduleContractError, "ski-jumping Startlist"):
            validate_races(wrong_sector)
        reversed_dates = [dict(row) for row in races]
        reversed_dates[0]["Final_Date"] = "11/20/2025"
        with self.assertRaisesRegex(ScheduleContractError, "precedes"):
            validate_races(reversed_dates)

    def test_column_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-schema-") as text:
            path = Path(text) / "bad.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Date", "Unexpected"])
                writer.writerow(["11/21/2025", "value"])
            with self.assertRaisesRegex(ScheduleContractError, "unexpected columns"):
                read_csv(path, RACES_COLUMNS)


if __name__ == "__main__":
    unittest.main()
