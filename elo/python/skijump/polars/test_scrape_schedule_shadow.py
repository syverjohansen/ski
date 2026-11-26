from __future__ import annotations

import csv
import importlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from race_schedule_output import save_skijump_races
from schedule_contract import ScheduleContractError
from scrape_schedule_shadow import run_shadow_acquisition


ROOT = Path(__file__).resolve().parent
ACCEPTED_RACES = ROOT / "excel365/races.csv"


def acquired_rows() -> list[dict[str, str]]:
    with ACCEPTED_RACES.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        {
            "date": row["Date"],
            "final_date": row["Final_Date"],
            "startlist_url": row["Startlist"],
            "sex": row["Sex"],
            "city": row["City"],
            "country": row["Country"],
            "hill_size": row["HillSize"],
            "race_type": row["RaceType"],
            "period": "",
            "elevation": row["Elevation"],
            "championship": row["Championship"],
        }
        for row in rows
    ]


class SkiJumpScheduleShadowTests(unittest.TestCase):
    def test_missing_calendar_hill_sizes_use_latest_accepted_venue_history(self) -> None:
        from race_scrape import (
            load_historical_hill_sizes,
            resolve_historical_hill_size,
        )

        lookup = load_historical_hill_sizes(ROOT)
        self.assertEqual(
            "128",
            resolve_historical_hill_size(
                "Lake Placid", "Mixed", "Team Large", lookup
            ),
        )
        self.assertEqual(
            "128",
            resolve_historical_hill_size("Innsbruck", "L", "Large", lookup),
        )
        with self.assertRaisesRegex(RuntimeError, "no unambiguous accepted"):
            resolve_historical_hill_size("Unknown Hill", "M", "Large", lookup)

    def test_fis_tournament_grouping_names_are_not_race_venues(self) -> None:
        from race_scrape import is_tournament_grouping

        self.assertTrue(is_tournament_grouping("Four Hills Tournament"))
        self.assertTrue(is_tournament_grouping("FIS Four Hills Tournament"))
        self.assertTrue(is_tournament_grouping("Two Nights Tour"))
        self.assertFalse(is_tournament_grouping("Bischofshofen"))

    def test_extracted_serializer_reproduces_accepted_races(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-serializer-") as text:
            candidate = Path(text) / "races.csv"
            save_skijump_races(acquired_rows(), candidate)
            self.assertEqual(ACCEPTED_RACES.read_bytes(), candidate.read_bytes())

    def test_mocked_acquisition_builds_shadow_without_promotion(self) -> None:
        original = ACCEPTED_RACES.read_bytes()
        with tempfile.TemporaryDirectory(prefix="skijump-acquisition-") as text:
            output = Path(text) / "shadow"
            result = run_shadow_acquisition(
                season=2026,
                accepted_races=ACCEPTED_RACES,
                output_root=output,
                scrape_fn=lambda **_: acquired_rows(),
            )
            self.assertEqual(original, ACCEPTED_RACES.read_bytes())
            self.assertEqual(
                "nordicnumbers.skijump-schedule-acquisition/v1",
                result["schema_version"],
            )
            self.assertEqual("eligible", result["promotion"]["status"])
            self.assertFalse(result["promotion"]["performed"])
            self.assertEqual([], result["comparison"]["removed"])
            self.assertTrue((output / "candidate/races.csv").is_file())
            self.assertTrue((output / "candidate/weekends.csv").is_file())
            report = json.loads(
                (output / "reports/acquisition.json").read_text(encoding="utf-8")
            )
            self.assertEqual("passed", report["status"])
            self.assertEqual(["World Cup", "Olympics"], [
                source["category"] for source in report["sources"]
            ])

    def test_empty_acquisition_fails_before_candidate_materialization(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-empty-") as text:
            output = Path(text) / "shadow"
            with self.assertRaisesRegex(ScheduleContractError, "returned no"):
                run_shadow_acquisition(
                    season=2026,
                    accepted_races=ACCEPTED_RACES,
                    output_root=output,
                    scrape_fn=lambda **_: [],
                )
            self.assertFalse((output / "candidate").exists())

    def test_selected_fis_source_without_calendar_data_fails_closed(self) -> None:
        fake_requests = types.ModuleType("requests")
        fake_requests.get = lambda *args, **kwargs: None
        fake_requests.exceptions = types.SimpleNamespace(RequestException=Exception)
        fake_bs4 = types.ModuleType("bs4")
        fake_bs4.BeautifulSoup = lambda *args, **kwargs: types.SimpleNamespace(
            find=lambda *args, **kwargs: None
        )
        fake_pandas = types.ModuleType("pandas")
        fake_pandas.read_csv = lambda *args, **kwargs: (_ for _ in ()).throw(
            FileNotFoundError
        )
        fake_pandas.DataFrame = lambda: types.SimpleNamespace(empty=True)
        fake_fuzzy = types.ModuleType("fuzzywuzzy")
        fake_fuzzy.fuzz = types.SimpleNamespace(ratio=lambda *args: 0)
        fake_fuzzy.process = types.SimpleNamespace(extractOne=lambda *args: None)
        response = types.SimpleNamespace(
            content=b"missing calendar",
            raise_for_status=lambda: None,
        )
        with patch.dict(
            sys.modules,
            {
                "requests": fake_requests,
                "bs4": fake_bs4,
                "pandas": fake_pandas,
                "fuzzywuzzy": fake_fuzzy,
            },
        ):
            sys.modules.pop("race_scrape", None)
            scraper = importlib.import_module("race_scrape")
            with self.assertRaisesRegex(RuntimeError, "World Cup"):
                scraper.scrape_skijump_races(
                    season=2026,
                    include_championships=False,
                    request_fn=lambda *args, **kwargs: response,
                    sleep_fn=lambda _: None,
                )
        sys.modules.pop("race_scrape", None)

    def test_incomplete_candidate_is_blocked(self) -> None:
        rows = acquired_rows()
        incomplete = rows[:20] + [
            row for row in rows if row["championship"] == "1"
        ] + rows[-20:]
        with tempfile.TemporaryDirectory(prefix="skijump-incomplete-") as text:
            result = run_shadow_acquisition(
                season=2026,
                accepted_races=ACCEPTED_RACES,
                output_root=Path(text) / "shadow",
                scrape_fn=lambda **_: incomplete,
            )
            self.assertEqual("blocked", result["promotion"]["status"])
            self.assertFalse(result["comparison"]["within_removal_limit"])
            self.assertFalse(result["promotion"]["performed"])

    def test_output_must_be_isolated_from_accepted_schedule(self) -> None:
        accepted_dir = ACCEPTED_RACES.parent.resolve()
        for output in (accepted_dir, accepted_dir / "shadow"):
            with self.subTest(output=output):
                with self.assertRaisesRegex(ScheduleContractError, "isolated"):
                    run_shadow_acquisition(
                        season=2026,
                        accepted_races=ACCEPTED_RACES,
                        output_root=output,
                        scrape_fn=lambda **_: acquired_rows(),
                    )


if __name__ == "__main__":
    unittest.main()
