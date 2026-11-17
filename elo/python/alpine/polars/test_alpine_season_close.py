from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from alpine_results_gap import MissingRace
from alpine_season_close import (
    SeasonCloseError,
    build_closed_history,
    build_season_close,
)


ROOT = Path(__file__).resolve().parent
SOURCE_RACES = ROOT / "excel365/races.csv"


FIELDS = [
    "Elo", "Downhill_Elo", "Super G_Elo", "Giant Slalom_Elo",
    "Slalom_Elo", "Combined_Elo", "Tech_Elo", "Speed_Elo", "Date",
    "City", "Country", "Sex", "Distance", "Event", "MS", "Technique",
    "Place", "Skier", "Nation", "ID", "Season", "Race", "Birthday",
    "Age", "Exp", "Pelo", "Downhill_Pelo", "Super G_Pelo",
    "Giant Slalom_Pelo", "Slalom_Pelo", "Combined_Pelo", "Tech_Pelo",
    "Speed_Pelo",
]


def row(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {field: "" for field in FIELDS}
    value.update(
        {
            "Elo": 1410.0,
            "Giant Slalom_Elo": 1420.0,
            "Tech_Elo": 1415.0,
            "Date": "2027-03-20",
            "City": "Finale",
            "Country": "Norway",
            "Sex": "M",
            "Distance": "Giant Slalom",
            "Event": "World Cup",
            "MS": 0,
            "Technique": "N/A",
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


class AlpineSeasonCloseTest(unittest.TestCase):
    def test_candidate_appends_one_undiscounted_row_per_season_athlete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            output = root / "candidate" / "men.csv"
            write_csv(
                source,
                [
                    row(Date="2027-01-01", Race=1, Elo=1400.0),
                    row(Date="2027-03-20", Race=40, Elo=1410.0),
                    row(
                        Date="2027-03-20", Race=40, ID=20,
                        Skier="Second Athlete", Elo=1500.0,
                        **{"Giant Slalom_Elo": 1510.0},
                    ),
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
            first = offseason[0]
            self.assertEqual(first["Elo"], first["Pelo"])
            self.assertEqual("1410.0", first["Elo"])
            self.assertEqual(first["Giant Slalom_Elo"], first["Giant Slalom_Pelo"])
            self.assertEqual("1420.0", first["Giant Slalom_Elo"])

    def test_complete_existing_close_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            first = root / "first/men.csv"
            second = root / "second/men.csv"
            write_csv(source, [row()])
            build_closed_history(
                source=source, output=first, sex="M", season=2027
            )

            report = build_closed_history(
                source=first, output=second, sex="M", season=2027
            )

            self.assertEqual("no_changes", report["status"])
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_partial_existing_close_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            existing = row(
                Date="2027-05-01", City="Summer", Country="Break",
                Distance="0", Event="Offseason", Place=0, Race=0,
                Pelo=1410.0, Elo=1410.0, Tech_Pelo=1415.0, Tech_Elo=1415.0,
                **{
                    "Giant Slalom_Pelo": 1420.0,
                    "Giant Slalom_Elo": 1420.0,
                },
            )
            write_csv(
                source,
                [
                    row(ID=10),
                    row(ID=20, Skier="Second Athlete"),
                    existing,
                ],
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

    @patch("alpine_season_close.find_missing_races")
    def test_unprocessed_schedule_result_blocks_close(self, find_missing) -> None:
        gap = MissingRace(
            date="2027-03-20", sex="M", season=2027, race=40,
            city="Finale", country="Norway", distance="Giant Slalom",
            results_url="https://www.fis-ski.com/results",
        )
        find_missing.return_value = ([gap], 40)
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

    @patch("alpine_season_close.find_missing_races", return_value=([], 91))
    def test_complete_close_writes_candidate_pair_and_report(self, _find_missing) -> None:
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
            self.assertEqual(91, report["completed_schedule_races"])
            self.assertEqual(
                "delayed_until_first_next-season_result",
                report["rollover"]["timing"],
            )
            self.assertTrue((root / "output/candidate/men_chrono.csv").is_file())
            self.assertTrue((root / "output/candidate/ladies_chrono.csv").is_file())
            self.assertTrue((root / "output/report.json").is_file())


if __name__ == "__main__":
    unittest.main()
