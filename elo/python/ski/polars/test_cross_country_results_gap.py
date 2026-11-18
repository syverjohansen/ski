import csv
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_results_gap import (
    audit_results,
    build_report,
    season_for_race,
)

HERE = Path(__file__).resolve().parent


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryResultsGapTests(unittest.TestCase):
    def test_season_uses_ending_year(self):
        self.assertEqual(season_for_race(date(2026, 11, 27)), 2027)
        self.assertEqual(season_for_race(date(2027, 3, 21)), 2027)

    def test_future_and_team_events_are_not_individual_result_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "races.csv"
            men = root / "men.csv"
            ladies = root / "ladies.csv"
            fields = [
                "Date",
                "Startlist",
                "Sex",
                "City",
                "Country",
                "Distance",
                "Race",
            ]
            write_csv(
                schedule,
                fields,
                [
                    {
                        "Date": "11/27/2026",
                        "Startlist": "https://example/1",
                        "Sex": "L",
                        "City": "Ruka",
                        "Country": "Finland",
                        "Distance": "10",
                        "Race": 1,
                    },
                    {
                        "Date": "11/28/2026",
                        "Startlist": "https://example/2",
                        "Sex": "M",
                        "City": "Ruka",
                        "Country": "Finland",
                        "Distance": "Ts",
                        "Race": 2,
                    },
                    {
                        "Date": "12/01/2026",
                        "Startlist": "https://example/3",
                        "Sex": "M",
                        "City": "Trondheim",
                        "Country": "Norway",
                        "Distance": "10",
                        "Race": 3,
                    },
                ],
            )
            chrono_fields = ["Date", "Season", "Race"]
            write_csv(men, chrono_fields, [])
            write_csv(ladies, chrono_fields, [])

            audit = audit_results(schedule, men, ladies, date(2026, 11, 29))

            self.assertEqual(audit.total, 3)
            self.assertEqual(audit.future, 1)
            self.assertEqual(audit.completed_team_events, 1)
            self.assertEqual(audit.completed_individual, 1)
            self.assertEqual(audit.completed_individual_metadata_incomplete, 0)
            self.assertEqual([(item.sex, item.race) for item in audit.missing], [("L", 1)])

    def test_completed_2026_individual_schedule_has_no_gaps(self):
        data = HERE / "excel365"
        audit = audit_results(
            data / "races2026.csv",
            data / "men_chrono.csv",
            data / "ladies_chrono.csv",
            date(2026, 3, 22),
        )

        self.assertEqual(audit.total, 72)
        self.assertEqual(audit.future, 0)
        self.assertEqual(audit.completed_team_events, 8)
        self.assertEqual(audit.completed_individual, 64)
        self.assertEqual(audit.completed_individual_metadata_incomplete, 4)
        self.assertEqual(audit.missing, ())

    def test_current_2027_schedule_is_entirely_future_before_opener(self):
        data = HERE / "excel365"
        audit = audit_results(
            data / "races.csv",
            data / "men_chrono.csv",
            data / "ladies_chrono.csv",
            date(2026, 11, 17),
        )
        report = build_report(audit, as_of=date(2026, 11, 17))

        self.assertEqual(audit.total, 74)
        self.assertEqual(audit.future, 74)
        self.assertEqual(audit.completed_individual, 0)
        self.assertEqual(audit.completed_individual_metadata_incomplete, 0)
        self.assertEqual(audit.missing, ())
        self.assertEqual(report["missing_race_count"], 0)

    def test_cli_writes_report_and_can_fail_on_real_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "races.csv"
            men = root / "men.csv"
            ladies = root / "ladies.csv"
            output = root / "gap.json"
            write_csv(
                schedule,
                [
                    "Date",
                    "Startlist",
                    "Sex",
                    "City",
                    "Country",
                    "Distance",
                    "Race",
                ],
                [
                    {
                        "Date": "11/27/2026",
                        "Startlist": "https://example/1",
                        "Sex": "M",
                        "City": "Ruka",
                        "Country": "Finland",
                        "Distance": "10",
                        "Race": 1,
                    }
                ],
            )
            write_csv(men, ["Date", "Season", "Race"], [])
            write_csv(ladies, ["Date", "Season", "Race"], [])
            completed = subprocess.run(
                [
                    sys.executable,
                    str(HERE / "cross_country_results_gap.py"),
                    "--schedule",
                    str(schedule),
                    "--men-chrono",
                    str(men),
                    "--ladies-chrono",
                    str(ladies),
                    "--as-of",
                    "2026-11-27",
                    "--output",
                    str(output),
                    "--fail-on-gaps",
                ],
                check=False,
            )
            self.assertEqual(completed.returncode, 2)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["missing_race_count"], 1)


if __name__ == "__main__":
    unittest.main()
