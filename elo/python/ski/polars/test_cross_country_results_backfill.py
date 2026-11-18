import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_results_backfill import (
    BackfillError,
    acquire_backfill,
    parse_classified_results,
    validate_output_dir,
    validate_results_url,
)

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "tests" / "fixtures" / "cc-results.html"


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryResultsBackfillTests(unittest.TestCase):
    def test_parses_classified_fis_results(self) -> None:
        results = parse_classified_results(FIXTURE.read_bytes())
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["place"], 1)
        self.assertEqual(results[0]["fis_competitor_id"], "1001")
        self.assertEqual(results[0]["fis_code"], "3100010")
        self.assertEqual(results[0]["name"], "NISKANEN Iivo")
        self.assertEqual(results[0]["nation"], "FIN")

    def test_accepts_only_fis_cross_country_results_urls(self) -> None:
        url = (
            "https://www.fis-ski.com/DB/general/results.html?"
            "sectorcode=CC&raceid=52085"
        )
        self.assertEqual(validate_results_url(url), "52085")
        with self.assertRaisesRegex(BackfillError, "cross-country"):
            validate_results_url(url.replace("sectorcode=CC", "sectorcode=AL"))
        with self.assertRaisesRegex(BackfillError, "cross-country"):
            validate_results_url("https://example.com/results?sectorcode=CC&raceid=52085")

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
            schedule_fields = [
                "Date",
                "Startlist",
                "Sex",
                "City",
                "Country",
                "Distance",
                "Technique",
                "MS",
                "Period",
                "Pursuit",
                "Stage",
                "Final_Climb",
                "Elevation",
                "Race",
                "Championship",
            ]
            write_csv(
                schedule,
                schedule_fields,
                [
                    {
                        "Date": "11/27/2026",
                        "Startlist": (
                            "https://www.fis-ski.com/DB/general/results.html?"
                            "sectorcode=CC&raceid=52085"
                        ),
                        "Sex": "M",
                        "City": "Ruka",
                        "Country": "Finland",
                        "Distance": "10",
                        "Technique": "C",
                        "MS": "0",
                        "Period": "1",
                        "Pursuit": "0",
                        "Stage": "0",
                        "Final_Climb": "0",
                        "Elevation": "337",
                        "Race": "1",
                        "Championship": "0",
                    }
                ],
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
                as_of=date(2026, 11, 27),
                output_dir=output,
                fetcher=lambda _url: fixture,
            )

            self.assertEqual(report["acquired_race_count"], 1)
            self.assertEqual(report["acquired_result_rows"], 3)
            self.assertEqual(report["identity_status"], "unmapped")
            with (output / "results.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["technique"], "C")
            self.assertEqual(rows[0]["elevation"], "337")
            self.assertEqual(rows[0]["fis_competitor_id"], "1001")
            self.assertTrue((output / "races" / "M-2027-001.csv").is_file())
            self.assertEqual(
                source_mtimes,
                {path: path.stat().st_mtime_ns for path in source_mtimes},
            )

    def test_preseason_schedule_does_not_fetch_future_races(self) -> None:
        data = HERE / "excel365"
        fetched: list[str] = []
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaisesRegex(BackfillError, "no completed"),
        ):
            acquire_backfill(
                schedule=data / "races.csv",
                men_chrono=data / "men_chrono.csv",
                ladies_chrono=data / "ladies_chrono.csv",
                as_of=date(2026, 11, 17),
                output_dir=Path(directory) / "candidate",
                fetcher=lambda url: fetched.append(url) or b"",
            )
        self.assertEqual(fetched, [])


if __name__ == "__main__":
    unittest.main()
