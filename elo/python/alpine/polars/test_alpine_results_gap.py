import csv
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

from alpine_results_gap import build_report, find_missing_races, season_for_race


HERE = Path(__file__).resolve().parent


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class AlpineResultsGapTest(unittest.TestCase):
    def test_season_uses_ending_year(self) -> None:
        self.assertEqual(season_for_race(date(2026, 10, 24)), 2027)
        self.assertEqual(season_for_race(date(2027, 3, 25)), 2027)

    def test_reports_only_completed_missing_races(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "races.csv"
            men = root / "men.csv"
            ladies = root / "ladies.csv"
            write_csv(
                schedule,
                ["Date", "Startlist", "Sex", "City", "Country", "Distance", "Race"],
                [
                    {"Date": "10/24/2026", "Startlist": "https://example/1", "Sex": "L", "City": "Soelden", "Country": "Austria", "Distance": "Giant Slalom", "Race": 1},
                    {"Date": "10/25/2026", "Startlist": "https://example/2", "Sex": "M", "City": "Soelden", "Country": "Austria", "Distance": "Giant Slalom", "Race": 1},
                    {"Date": "11/21/2026", "Startlist": "https://example/3", "Sex": "M", "City": "Gurgl", "Country": "Austria", "Distance": "Slalom", "Race": 3},
                ],
            )
            chrono_fields = ["Date", "Season", "Race"]
            write_csv(men, chrono_fields, [{"Date": "2026-10-25", "Season": 2027, "Race": 1}])
            write_csv(ladies, chrono_fields, [])

            missing, completed = find_missing_races(
                schedule, men, ladies, date(2026, 11, 17)
            )

            self.assertEqual(completed, 2)
            self.assertEqual([(race.sex, race.race) for race in missing], [("L", 1)])

    def test_repository_snapshot_has_four_known_gaps(self) -> None:
        data = HERE / "excel365"
        missing, completed = find_missing_races(
            data / "races.csv",
            data / "men_chrono.csv",
            data / "ladies_chrono.csv",
            date(2026, 11, 17),
        )
        self.assertEqual(completed, 4)
        self.assertEqual(
            [(race.date, race.sex, race.city) for race in missing],
            [
                ("2026-10-24", "L", "Soelden"),
                ("2026-10-25", "M", "Soelden"),
                ("2026-11-14", "L", "Levi"),
                ("2026-11-15", "M", "Levi"),
            ],
        )
        report = build_report(missing, as_of=date(2026, 11, 17), completed_count=completed)
        self.assertEqual(report["missing_race_count"], 4)

    def test_cli_writes_report_and_can_fail_on_gaps(self) -> None:
        data = HERE / "excel365"
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "gap.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(HERE / "alpine_results_gap.py"),
                    "--schedule", str(data / "races.csv"),
                    "--men-chrono", str(data / "men_chrono.csv"),
                    "--ladies-chrono", str(data / "ladies_chrono.csv"),
                    "--as-of", "2026-11-17",
                    "--output", str(report_path),
                    "--fail-on-gaps",
                ],
                check=False,
            )
            self.assertEqual(completed.returncode, 2)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["missing_race_count"], 4)


if __name__ == "__main__":
    unittest.main()
