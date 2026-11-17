import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from alpine_results_backfill import (
    BackfillError,
    acquire_backfill,
    parse_classified_results,
    validate_output_dir,
    validate_results_url,
)


HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "tests" / "fixtures" / "131579.html"


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class AlpineResultsBackfillTest(unittest.TestCase):
    def test_parses_classified_fis_results(self) -> None:
        results = parse_classified_results(FIXTURE.read_bytes())
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["place"], 1)
        self.assertEqual(results[0]["fis_competitor_id"], "237841")
        self.assertEqual(results[0]["name"], "AICHER Emma")
        self.assertEqual(results[0]["nation"], "GER")

    def test_rejects_unapproved_source_url(self) -> None:
        with self.assertRaisesRegex(BackfillError, "unsupported FIS"):
            validate_results_url("https://example.com/results?raceid=131579")

    def test_rejects_output_beside_accepted_sources(self) -> None:
        source = HERE / "excel365" / "races.csv"
        with self.assertRaisesRegex(BackfillError, "accepted source"):
            validate_output_dir(HERE / "excel365" / "candidate", [source])

    def test_acquires_fixture_without_mutating_sources(self) -> None:
        fixture = FIXTURE.read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            schedule = sources / "races.csv"
            men = sources / "men.csv"
            ladies = sources / "ladies.csv"
            write_csv(
                schedule,
                ["Date", "Startlist", "Sex", "City", "Country", "Distance", "Race"],
                [{
                    "Date": "11/14/2026",
                    "Startlist": "https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=131579",
                    "Sex": "L",
                    "City": "Levi",
                    "Country": "Finland",
                    "Distance": "Slalom",
                    "Race": 2,
                }],
            )
            write_csv(men, ["Date", "Season", "Race"], [])
            write_csv(ladies, ["Date", "Season", "Race"], [])
            source_mtimes = {
                path: path.stat().st_mtime_ns for path in (schedule, men, ladies)
            }
            output = root / "candidate"
            report = acquire_backfill(
                schedule=schedule,
                men_chrono=men,
                ladies_chrono=ladies,
                as_of=date(2026, 11, 14),
                output_dir=output,
                fetcher=lambda _url: fixture,
            )
            self.assertEqual(report["acquired_race_count"], 1)
            self.assertEqual(report["acquired_result_rows"], 3)
            self.assertEqual(report["identity_status"], "unmapped")
            with (output / "results.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["city"], "Levi")
            self.assertEqual(rows[0]["sex"], "L")
            self.assertEqual(
                source_mtimes,
                {path: path.stat().st_mtime_ns for path in source_mtimes},
            )


if __name__ == "__main__":
    unittest.main()
