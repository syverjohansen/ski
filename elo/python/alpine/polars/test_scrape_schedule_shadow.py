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

from scrape_schedule_shadow import run_shadow_acquisition
from race_schedule_output import assign_periods_by_race_count, save_alpine_races
from schedule_contract import ScheduleContractError
from schedule_sources import (
    calendar_sources,
    championship_value,
    prediction_discipline,
)


ROOT = Path(__file__).resolve().parent


def sample_races() -> list[dict[str, str]]:
    return [
        {
            "date": "01/10/2026",
            "startlist_url": (
                "https://www.fis-ski.com/DB/general/results.html?raceid=1001"
            ),
            "sex": "M",
            "city": "Example",
            "country": "Norway",
            "distance": "Slalom",
            "period": "",
            "championship": "0",
        },
        {
            "date": "01/11/2026",
            "startlist_url": (
                "https://www.fis-ski.com/DB/general/results.html?raceid=1002"
            ),
            "sex": "L",
            "city": "Example",
            "country": "Norway",
            "distance": "Giant Slalom",
            "period": "",
            "championship": "1",
        },
    ]


def write_accepted(path: Path) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "Date",
                "Startlist",
                "Sex",
                "City",
                "Country",
                "Distance",
                "Period",
                "Championship",
                "Race",
            ]
        )
        writer.writerow(
            [
                "01/10/2026",
                "https://www.fis-ski.com/DB/general/results.html?raceid=1001",
                "M",
                "Example",
                "Norway",
                "Slalom",
                "1",
                "0",
                "1",
            ]
        )
        writer.writerow(
            [
                "01/11/2026",
                "https://www.fis-ski.com/DB/general/results.html?raceid=1002",
                "L",
                "Example",
                "Norway",
                "Giant Slalom",
                "4",
                "1",
                "1",
            ]
        )
    return path.read_bytes()


class ScrapeScheduleShadowTests(unittest.TestCase):
    def test_extracted_serializer_reproduces_current_accepted_races(self) -> None:
        accepted = ROOT / "excel365" / "races.csv"
        with accepted.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        acquired = [
            {
                "date": row["Date"],
                "startlist_url": row["Startlist"],
                "sex": row["Sex"],
                "city": row["City"],
                "country": row["Country"],
                "distance": row["Distance"],
                "period": "",
                "championship": row["Championship"],
            }
            for row in rows
        ]
        with tempfile.TemporaryDirectory(prefix="alpine-serializer-") as text:
            candidate = Path(text) / "races.csv"
            save_alpine_races(acquired, candidate)
            self.assertEqual(accepted.read_bytes(), candidate.read_bytes())

    def test_calendar_sources_are_parameterized(self) -> None:
        sources = calendar_sources(2030, include_championships=True)
        self.assertEqual(["World Cup", "Olympics"], [item[1] for item in sources])
        self.assertTrue(all("seasoncode=2030" in item[0] for item in sources))
        worlds = calendar_sources(2027, include_championships=True)
        self.assertEqual(
            ["World Cup", "World Championships"],
            [item[1] for item in worlds],
        )
        self.assertIn("categorycode=WSC", worlds[1][0])
        self.assertEqual(1, len(calendar_sources(2028, include_championships=True)))
        self.assertEqual(1, len(calendar_sources(2031, include_championships=False)))

    def test_only_prediction_races_are_selected(self) -> None:
        self.assertEqual("Giant Slalom", prediction_discipline("Giant Slalom"))
        self.assertEqual("", prediction_discipline("Giant Slalom Qualification"))
        self.assertEqual("", prediction_discipline("Slalom Training"))
        self.assertEqual("", prediction_discipline("Team Combined"))

    def test_world_championships_use_championship_periods(self) -> None:
        races = [
            {"date": "01/01/2027", "championship": "0"},
            {"date": "01/02/2027", "championship": "0"},
            {"date": "02/04/2027", "championship": "1"},
            {"date": "02/14/2027", "championship": "1"},
            {"date": "02/20/2027", "championship": "0"},
        ]
        assigned = assign_periods_by_race_count(races)
        periods_by_date = {race["date"]: race["period"] for race in assigned}
        self.assertEqual("4", periods_by_date["02/04/2027"])
        self.assertEqual("4", periods_by_date["02/14/2027"])
        self.assertEqual("5", periods_by_date["02/20/2027"])
        self.assertEqual("1", championship_value("World Championships"))
        self.assertEqual("1", championship_value("Olympics"))
        self.assertEqual("0", championship_value("World Cup"))

    def test_selected_fis_source_without_calendar_data_fails_closed(self) -> None:
        fake_requests = types.ModuleType("requests")
        fake_requests.get = lambda *args, **kwargs: None
        fake_requests.exceptions = types.SimpleNamespace(RequestException=Exception)
        fake_bs4 = types.ModuleType("bs4")

        class MissingCalendarSoup:
            def find(self, *args, **kwargs):
                return None

        fake_bs4.BeautifulSoup = lambda *args, **kwargs: MissingCalendarSoup()
        response = types.SimpleNamespace(content=b"missing calendar")
        with patch.dict(
            sys.modules,
            {"requests": fake_requests, "bs4": fake_bs4},
        ):
            sys.modules.pop("race_scrape", None)
            scraper = importlib.import_module("race_scrape")
            with self.assertRaisesRegex(RuntimeError, "World Cup"):
                scraper.scrape_alpine_races(
                    season=2026,
                    include_championships=False,
                    request_fn=lambda *args, **kwargs: response,
                    sleep_fn=lambda _: None,
                )
        sys.modules.pop("race_scrape", None)

    def test_mocked_acquisition_builds_shadow_without_promotion(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-acquisition-") as text:
            root = Path(text)
            accepted = root / "accepted" / "races.csv"
            original = write_accepted(accepted)
            output = root / "shadow"

            result = run_shadow_acquisition(
                season=2026,
                accepted_races=accepted,
                output_root=output,
                scrape_fn=lambda **_: sample_races(),
            )

            self.assertEqual(original, accepted.read_bytes())
            self.assertEqual(
                "nordicnumbers.alpine-schedule-acquisition/v2",
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

    def test_empty_acquisition_fails_before_writing_candidate(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-acquisition-empty-") as text:
            root = Path(text)
            accepted = root / "accepted" / "races.csv"
            write_accepted(accepted)
            output = root / "shadow"
            with self.assertRaisesRegex(ScheduleContractError, "returned no"):
                run_shadow_acquisition(
                    season=2026,
                    accepted_races=accepted,
                    output_root=output,
                    scrape_fn=lambda **_: [],
                )
            self.assertFalse((output / "candidate").exists())

    def test_output_cannot_contain_accepted_schedule(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-acquisition-path-") as text:
            accepted_dir = Path(text) / "accepted"
            accepted = accepted_dir / "races.csv"
            write_accepted(accepted)
            with self.assertRaisesRegex(ScheduleContractError, "isolated"):
                run_shadow_acquisition(
                    season=2026,
                    accepted_races=accepted,
                    output_root=accepted_dir,
                    scrape_fn=lambda **_: sample_races(),
                )
            with self.assertRaisesRegex(ScheduleContractError, "isolated"):
                run_shadow_acquisition(
                    season=2026,
                    accepted_races=accepted,
                    output_root=accepted_dir / "shadow",
                    scrape_fn=lambda **_: sample_races(),
                )


if __name__ == "__main__":
    unittest.main()
