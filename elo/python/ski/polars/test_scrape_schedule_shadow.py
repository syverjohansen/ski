from __future__ import annotations

import csv
import json
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path

from cross_country_schedule_scraper import (
    canonical_result_url,
    elevation_for_city,
    mark_tour_stages,
    parse_distance,
    parse_technique,
    scrape_cross_country_races,
)
from race_schedule_output import period_for_date, save_cross_country_races
from schedule_contract import (
    RACES_COLUMNS,
    ScheduleContractError,
    infer_fis_season,
    read_csv,
)
from schedule_sources import calendar_sources
from scrape_schedule_shadow import run_shadow_acquisition

ROOT = Path(__file__).resolve().parent
ACCEPTED_RACES = ROOT / "excel365" / "races.csv"


def raw_accepted_races():
    with ACCEPTED_RACES.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        {
            "date": row["Date"],
            "startlist_url": row["Startlist"],
            "sex": row["Sex"],
            "city": row["City"],
            "country": row["Country"],
            "distance": row["Distance"],
            "technique": row["Technique"],
            "ms": row["MS"],
            "pursuit": row["Pursuit"],
            "elevation": row["Elevation"],
            "championship": row["Championship"],
            "tour_stage": row["Stage"],
        }
        for row in rows
    ]


class CrossCountryScheduleShadowTests(unittest.TestCase):
    def test_serializer_reproduces_current_accepted_races(self):
        with tempfile.TemporaryDirectory(prefix="xc-serializer-") as temporary:
            candidate = Path(temporary) / "races.csv"
            save_cross_country_races(raw_accepted_races(), candidate)
            self.assertEqual(ACCEPTED_RACES.read_bytes(), candidate.read_bytes())

    def test_calendar_sources_are_season_parameterized(self):
        olympics = calendar_sources(2030, include_championships=True)
        self.assertEqual(["World Cup", "Olympics"], [item[1] for item in olympics])
        self.assertTrue(all("sectorcode=CC" in item[0] for item in olympics))
        self.assertTrue(all("seasoncode=2030" in item[0] for item in olympics))

        worlds = calendar_sources(2027, include_championships=True)
        self.assertEqual(
            ["World Cup", "World Championships"], [item[1] for item in worlds]
        )
        self.assertIn("categorycode=WSC", worlds[1][0])
        self.assertEqual(1, len(calendar_sources(2028, include_championships=True)))
        self.assertEqual(1, len(calendar_sources(2027, include_championships=False)))

    def test_periods_depend_on_race_date_not_computer_date(self):
        self.assertEqual("1", period_for_date("11/28/2025", championship="0"))
        self.assertEqual("2", period_for_date("01/04/2026", championship="0"))
        self.assertEqual("3", period_for_date("01/17/2026", championship="0"))
        self.assertEqual("4", period_for_date("02/28/2026", championship="0"))
        self.assertEqual("4", period_for_date("03/01/2027", championship="1"))
        self.assertEqual("5", period_for_date("03/14/2026", championship="0"))

    def test_prediction_race_labels_are_parsed_without_finals_duplicates(self):
        self.assertEqual("Sprint", parse_distance("Sprint Qualification Classic"))
        self.assertEqual("Sprint", parse_distance("1.3 km Sprint Qualification Classic"))
        self.assertEqual("", parse_distance("Sprint Final Classic"))
        self.assertEqual("Ts", parse_distance("Team Sprint Qualification Free"))
        self.assertEqual("", parse_distance("Team Sprint Final Free"))
        self.assertEqual("Rel", parse_distance("4x7.5 km Relay"))
        self.assertEqual("20", parse_distance("10km+10km Skiathlon"))
        self.assertEqual("10", parse_distance("10 km Interval Start Classic"))
        self.assertEqual("C", parse_technique("10 km Classic", "10"))
        self.assertEqual("P", parse_technique("10km+10km Skiathlon", "20"))
        self.assertEqual("N/A", parse_technique("4x7.5 km Relay", "Rel"))

    def test_tour_window_comes_from_fis_start_and_final_climb(self):
        races = [
            {
                "date": "01/01/2027",
                "city": "Les Rousses",
                "ms": "0",
                "technique": "C",
            },
            {
                "date": "01/10/2027",
                "city": "Val di Fiemme",
                "ms": "1",
                "technique": "F",
            },
            {
                "date": "01/23/2027",
                "city": "Engadin",
                "ms": "1",
                "technique": "F",
            },
        ]
        marked = mark_tour_stages(
            races, datetime.strptime("01/01/2027", "%m/%d/%Y").date()
        )
        self.assertEqual(["1", "1", "0"], [row["tour_stage"] for row in marked])

    def test_result_urls_are_reduced_to_stable_cross_country_identity(self):
        result = canonical_result_url(
            "/DB/general/results.html?foo=x&raceid=49462&sectorcode=CC"
        )
        self.assertEqual(
            "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=49462",
            result,
        )

    def test_elevation_mapping_is_exact_and_missing_venues_fail_closed(self):
        elevations = {"holmenkollen": "335", "ruka": "337"}
        self.assertEqual("335", elevation_for_city("Oslo", elevations))
        self.assertEqual("337", elevation_for_city("Ruka", elevations))
        with self.assertRaisesRegex(RuntimeError, "add it explicitly"):
            elevation_for_city("Unknown Venue", elevations)

    def test_selected_fis_source_without_calendar_data_fails_closed(self):
        class MissingCalendarSoup:
            def find(self, *args, **kwargs):
                return None

        response = types.SimpleNamespace(content=b"missing calendar")
        with tempfile.TemporaryDirectory(prefix="xc-elevation-") as temporary:
            elevation = Path(temporary) / "elevation.csv"
            elevation.write_text("City,Elevation\nRuka,337\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "World Cup"):
                scrape_cross_country_races(
                    season=2026,
                    include_championships=False,
                    elevation_file=elevation,
                    request_fn=lambda *args, **kwargs: response,
                    soup_factory=lambda *args, **kwargs: MissingCalendarSoup(),
                    sleep_fn=lambda _: None,
                )

    def test_mocked_acquisition_builds_shadow_without_promotion(self):
        original = ACCEPTED_RACES.read_bytes()
        accepted_season = infer_fis_season(
            read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        )
        with tempfile.TemporaryDirectory(prefix="xc-acquisition-") as temporary:
            output = Path(temporary) / "shadow"
            result = run_shadow_acquisition(
                season=accepted_season,
                accepted_races=ACCEPTED_RACES,
                output_root=output,
                scrape_fn=lambda **_: raw_accepted_races(),
            )

            self.assertEqual(original, ACCEPTED_RACES.read_bytes())
            self.assertEqual(
                "nordicnumbers.cross-country-schedule-acquisition/v1",
                result["schema_version"],
            )
            self.assertEqual("eligible", result["promotion"]["status"])
            self.assertFalse(result["promotion"]["performed"])
            self.assertTrue((output / "candidate" / "races.csv").is_file())
            self.assertTrue((output / "candidate" / "weekends.csv").is_file())
            report = json.loads(
                (output / "reports" / "acquisition.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual("passed", report["status"])
            self.assertEqual([], report["comparison"]["removed"])

    def test_empty_acquisition_fails_before_writing_candidate(self):
        with tempfile.TemporaryDirectory(prefix="xc-acquisition-empty-") as temporary:
            output = Path(temporary) / "shadow"
            with self.assertRaisesRegex(ScheduleContractError, "returned no"):
                run_shadow_acquisition(
                    season=2026,
                    accepted_races=ACCEPTED_RACES,
                    output_root=output,
                    scrape_fn=lambda **_: [],
                )
            self.assertFalse((output / "candidate").exists())

    def test_output_cannot_overlap_accepted_schedule(self):
        accepted_dir = ACCEPTED_RACES.parent
        with self.assertRaisesRegex(ScheduleContractError, "isolated"):
            run_shadow_acquisition(
                season=2026,
                accepted_races=ACCEPTED_RACES,
                output_root=accepted_dir,
                scrape_fn=lambda **_: raw_accepted_races(),
            )
        with self.assertRaisesRegex(ScheduleContractError, "isolated"):
            run_shadow_acquisition(
                season=2026,
                accepted_races=ACCEPTED_RACES,
                output_root=accepted_dir / "shadow",
                scrape_fn=lambda **_: raw_accepted_races(),
            )


if __name__ == "__main__":
    unittest.main()
