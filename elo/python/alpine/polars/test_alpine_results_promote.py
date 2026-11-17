import csv
import tempfile
import unittest
from pathlib import Path

from alpine_results_elo_replay import PREVIOUS_COLUMNS, STREAMS
from alpine_results_promote import PromotionError, build_promoted_history


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class AlpineResultsPromoteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fields = [
            *STREAMS,
            "Date", "City", "Country", "Sex", "Distance", "Event", "MS",
            "Technique", "Place", "Skier", "Nation", "ID", "Season", "Race",
            "Birthday", "Age", "Exp",
            *PREVIOUS_COLUMNS.values(),
        ]

    def row(self, **overrides: object) -> dict[str, object]:
        row: dict[str, object] = {
            "Date": "2026-05-01", "City": "Summer", "Country": "Break", "Sex": "L",
            "Distance": "0", "Event": "Offseason", "MS": 0, "Technique": "",
            "Place": 0, "Skier": "One Athlete", "Nation": "Norway", "ID": 1,
            "Season": 2026, "Race": 0, "Birthday": "2000-01-01T00:00:00",
            "Age": 26, "Exp": 10,
        }
        row.update({column: 1400 for column in [*STREAMS, *PREVIOUS_COLUMNS.values()]})
        row.update(overrides)
        return row

    def test_rolls_over_and_appends_without_rewriting_unchanged_line(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            increments = root / "increments.csv"
            output = root / "candidate.csv"
            regular = self.row(Date="2026-03-01", Event="World Cup", Distance="Slalom", Race=40)
            offseason = self.row()
            incoming = self.row(
                Date="2026-10-24", City="Soelden", Country="Austria",
                Distance="Giant Slalom", Event="World Cup", Place=1,
                Season=2027, Race=1, Elo=1387.5, Pelo=1385.0,
            )
            write_csv(source, self.fields, [regular, offseason])
            original_regular_line = source.read_text(encoding="utf-8").splitlines(True)[1]
            write_csv(increments, self.fields, [incoming])
            report = build_promoted_history(
                source=source,
                increments=increments,
                output=output,
                sex="L",
                expected_season=2027,
            )
            lines = output.read_text(encoding="utf-8").splitlines(True)
            self.assertEqual(lines[1], original_regular_line)
            with output.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertAlmostEqual(float(rows[1]["Elo"]), 1385.0)
            self.assertEqual(rows[2]["Season"], "2027")
            self.assertEqual(report["appended_rows"], 1)
            self.assertEqual(report["rollover_rows"], 1)

    def test_rejects_duplicate_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            increments = root / "increments.csv"
            offseason = self.row()
            existing = self.row(
                Date="2026-10-24", Event="World Cup", Distance="Giant Slalom",
                Season=2027, Race=1, Place=1,
            )
            write_csv(source, self.fields, [offseason, existing])
            write_csv(increments, self.fields, [existing])
            with self.assertRaisesRegex(PromotionError, "duplicate promoted result"):
                build_promoted_history(
                    source=source,
                    increments=increments,
                    output=root / "candidate.csv",
                    sex="L",
                    expected_season=2027,
                )


if __name__ == "__main__":
    unittest.main()
