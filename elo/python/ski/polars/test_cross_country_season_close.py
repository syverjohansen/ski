from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from cross_country_results_gap import ScheduleAudit, ScheduleRace
from cross_country_season_close import (
    SeasonCloseError,
    build_closed_history,
    build_season_close,
)

ROOT = Path(__file__).resolve().parent
SOURCE_RACES = ROOT / "excel365/races.csv"
FIELDS = [
    "Elo",
    "Distance_Elo",
    "Distance_C_Elo",
    "Distance_F_Elo",
    "Sprint_Elo",
    "Sprint_C_Elo",
    "Sprint_F_Elo",
    "Classic_Elo",
    "Freestyle_Elo",
    "Date",
    "City",
    "Country",
    "Sex",
    "Distance",
    "Event",
    "MS",
    "Technique",
    "Place",
    "Skier",
    "Nation",
    "ID",
    "Season",
    "Race",
    "Birthday",
    "Age",
    "Exp",
    "Pelo",
    "Distance_Pelo",
    "Distance_C_Pelo",
    "Distance_F_Pelo",
    "Sprint_Pelo",
    "Sprint_C_Pelo",
    "Sprint_F_Pelo",
    "Classic_Pelo",
    "Freestyle_Pelo",
]


def row(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {field: "" for field in FIELDS}
    value.update(
        {
            "Elo": 1410.0,
            "Distance_Elo": 1420.0,
            "Classic_Elo": 1415.0,
            "Date": "2027-03-20",
            "City": "Finale",
            "Country": "Norway",
            "Sex": "M",
            "Distance": "10",
            "Event": "World Cup",
            "MS": 0,
            "Technique": "C",
            "Place": 1,
            "Skier": "Example Athlete",
            "Nation": "Norway",
            "ID": 10,
            "Season": 2027,
            "Race": 40,
            "Birthday": "2000-01-01T00:00:00",
            "Age": 27.2,
            "Exp": 50,
        }
    )
    value.update(changes)
    return value


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def clean_audit() -> ScheduleAudit:
    return ScheduleAudit(
        total=80,
        future=0,
        completed_team_events=8,
        completed_individual=72,
        completed_individual_metadata_incomplete=0,
        missing=(),
    )


class CrossCountrySeasonCloseTest(unittest.TestCase):
    def test_appends_undiscounted_race_zero_for_each_active_athlete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            output = root / "candidate/men.csv"
            write_csv(
                source,
                [
                    row(Date="2027-01-01", Race=1, Elo=1400.0),
                    row(Date="2027-03-20", Race=40, Elo=1410.0),
                    row(ID=20, Skier="Second Athlete", Elo=1500.0),
                ],
            )

            report = build_closed_history(
                source=source, output=output, sex="M", season=2027
            )

            self.assertEqual("candidate", report["status"])
            self.assertEqual(2, report["appended_rows"])
            with output.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            offseason = [item for item in rows if item["Event"] == "Offseason"]
            self.assertEqual(["10", "20"], [item["ID"] for item in offseason])
            self.assertTrue(all(item["Date"] == "2027-05-01" for item in offseason))
            self.assertTrue(all(item["Race"] == "0" for item in offseason))
            self.assertEqual(offseason[0]["Elo"], offseason[0]["Pelo"])
            self.assertEqual("1410.0", offseason[0]["Elo"])
            self.assertEqual(
                offseason[0]["Distance_Elo"], offseason[0]["Distance_Pelo"]
            )

    def test_existing_complete_close_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            first = root / "first/men.csv"
            second = root / "second/men.csv"
            write_csv(source, [row()])
            build_closed_history(source=source, output=first, sex="M", season=2027)
            report = build_closed_history(
                source=first, output=second, sex="M", season=2027
            )
            self.assertEqual("no_changes", report["status"])
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_partial_existing_close_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            existing = row(
                Date="2027-05-01",
                City="Summer",
                Country="Break",
                Distance="0",
                Event="Offseason",
                Place=0,
                Race=0,
                Pelo=1410.0,
                Elo=1410.0,
                Distance_Pelo=1420.0,
                Distance_Elo=1420.0,
                Classic_Pelo=1415.0,
                Classic_Elo=1415.0,
            )
            write_csv(
                source,
                [row(ID=10), row(ID=20, Skier="Second Athlete"), existing],
            )
            with self.assertRaisesRegex(SeasonCloseError, "partial offseason"):
                build_closed_history(
                    source=source,
                    output=root / "output/men.csv",
                    sex="M",
                    season=2027,
                )

    def test_close_before_may_first_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(SeasonCloseError, "cannot close before"):
                build_season_close(
                    schedule=root / "races.csv",
                    men_chrono=root / "men.csv",
                    ladies_chrono=root / "ladies.csv",
                    season=2027,
                    as_of=date(2027, 4, 30),
                    output_root=root / "output",
                )

    @patch("cross_country_season_close.audit_results")
    def test_missing_individual_result_blocks_close(self, audit_results) -> None:
        gap = ScheduleRace(
            date="2027-03-20",
            sex="M",
            season=2027,
            race=40,
            city="Finale",
            country="Norway",
            distance="10",
            technique="C",
            mass_start="0",
            period="1",
            pursuit="0",
            stage="0",
            final_climb="0",
            elevation="0",
            championship="0",
            results_url="https://www.fis-ski.com/results",
        )
        audit_results.return_value = ScheduleAudit(
            total=80,
            future=0,
            completed_team_events=8,
            completed_individual=72,
            completed_individual_metadata_incomplete=0,
            missing=(gap,),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            (sources / "races.csv").write_bytes(SOURCE_RACES.read_bytes())
            for name in ("men.csv", "ladies.csv"):
                (sources / name).write_text("placeholder", encoding="utf-8")
            with self.assertRaisesRegex(SeasonCloseError, "unprocessed"):
                build_season_close(
                    schedule=sources / "races.csv",
                    men_chrono=sources / "men.csv",
                    ladies_chrono=sources / "ladies.csv",
                    season=2027,
                    as_of=date(2027, 5, 1),
                    output_root=root / "output",
                )

    @patch("cross_country_season_close.audit_results", return_value=clean_audit())
    def test_complete_close_records_excluded_team_events(self, _audit) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            schedule = sources / "races.csv"
            men = sources / "men.csv"
            ladies = sources / "ladies.csv"
            schedule.write_bytes(SOURCE_RACES.read_bytes())
            write_csv(men, [row(Sex="M")])
            write_csv(ladies, [row(Sex="L", ID=20, Skier="Second Athlete")])

            report = build_season_close(
                schedule=schedule,
                men_chrono=men,
                ladies_chrono=ladies,
                season=2027,
                as_of=date(2027, 5, 1),
                output_root=root / "output",
            )

            self.assertEqual("candidate", report["status"])
            self.assertEqual(72, report["completed_individual_races"])
            self.assertEqual(8, report["completed_team_events_excluded"])
            self.assertEqual(
                "delayed_until_first_next-season_result",
                report["rollover"]["timing"],
            )
            self.assertTrue((root / "output/report.json").is_file())


if __name__ == "__main__":
    unittest.main()
