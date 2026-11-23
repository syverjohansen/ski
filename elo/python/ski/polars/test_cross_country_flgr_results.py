import csv
import json
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from cross_country_flgr_results import (
    FlgrContractError,
    build_race_candidate,
    parse_season_date,
    validate_candidate_set,
    write_candidate,
)

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "tests" / "fixtures" / "flgr"
SOURCE_URL = "https://flgr-results.ru/results/77/88"
RETRIEVED_AT = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)


def candidate_from_fixture(name: str, *, as_of: date = date(2026, 11, 23)):
    return build_race_candidate(
        (FIXTURES / name).read_bytes(),
        source_url=SOURCE_URL,
        season=2027,
        sex="M",
        as_of=as_of,
        retrieved_at=RETRIEVED_AT,
    )


class FlgrResultsContractTests(unittest.TestCase):
    def test_current_flgr_markup_uses_specification_date_and_heading_city(self):
        candidate = build_race_candidate(
            (FIXTURES / "current-markup-race.html").read_bytes(),
            source_url=SOURCE_URL,
            season=2027,
            sex="M",
            as_of=date(2026, 11, 23),
            retrieved_at=datetime(2026, 11, 23, 14, 0, tzinfo=UTC),
        )

        self.assertEqual(candidate["date"], "2026-11-23")
        self.assertEqual(candidate["source_city"], "г. Тюмень")
        self.assertEqual(candidate["distance"], "10")
        self.assertEqual(candidate["technique"], "F")
        self.assertEqual(len(candidate["results"]), 2)

    def test_season_year_maps_autumn_to_previous_calendar_year(self):
        self.assertEqual(parse_season_date("23.11", 2027), date(2026, 11, 23))
        self.assertEqual(parse_season_date("10.02", 2027), date(2027, 2, 10))

    def test_completed_race_has_stable_source_identities_and_checksum(self):
        candidate = candidate_from_fixture("completed-race.html")

        self.assertEqual(candidate["event_id"], "flgr:77:88")
        self.assertEqual(candidate["date"], "2026-11-23")
        self.assertEqual(candidate["distance"], "10")
        self.assertEqual(candidate["technique"], "C")
        self.assertTrue(candidate["mass_start"])
        self.assertEqual(candidate["classified_result_count"], 2)
        self.assertEqual(
            [result["athlete_id"] for result in candidate["results"]],
            ["flgr:101", "flgr:202"],
        )
        self.assertEqual(len(candidate["source_sha256"]), 64)

    def test_incomplete_race_is_rejected(self):
        with self.assertRaisesRegex(FlgrContractError, "classified results"):
            candidate_from_fixture("incomplete-race.html", as_of=date(2026, 11, 24))

    def test_future_dated_race_is_rejected(self):
        with self.assertRaisesRegex(FlgrContractError, "future-dated"):
            candidate_from_fixture("completed-race.html", as_of=date(2026, 11, 22))

    def test_duplicate_event_and_changed_source_are_rejected(self):
        candidate = candidate_from_fixture("completed-race.html")
        with self.assertRaisesRegex(FlgrContractError, "duplicate candidate event"):
            validate_candidate_set([candidate, candidate])
        with self.assertRaisesRegex(FlgrContractError, "source changed"):
            validate_candidate_set(
                [candidate], accepted_checksums={candidate["event_id"]: "0" * 64}
            )

    def test_candidate_writes_only_to_empty_isolated_directory(self):
        candidate = candidate_from_fixture("completed-race.html")
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "candidate"
            manifest_path, results_path = write_candidate(candidate, output_dir)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            with results_path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(manifest["event_id"], "flgr:77:88")
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["athlete_id"], "flgr:101")
            self.assertEqual(rows[0]["source_sha256"], candidate["source_sha256"])
            with self.assertRaisesRegex(FlgrContractError, "must be empty"):
                write_candidate(candidate, output_dir)

    def test_non_flgr_or_non_race_source_url_is_rejected(self):
        html = (FIXTURES / "completed-race.html").read_bytes()
        for source_url in (
            "https://example.com/results/77/88",
            "http://flgr-results.ru/results/77/88",
            "https://flgr-results.ru/results",
        ):
            with (
                self.subTest(source_url=source_url),
                self.assertRaises(FlgrContractError),
            ):
                build_race_candidate(
                    html,
                    source_url=source_url,
                    season=2027,
                    sex="M",
                    as_of=date(2026, 11, 23),
                    retrieved_at=RETRIEVED_AT,
                )


if __name__ == "__main__":
    unittest.main()
