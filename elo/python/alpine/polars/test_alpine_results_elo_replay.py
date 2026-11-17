import csv
import tempfile
import unittest
from pathlib import Path

from alpine_results_elo_replay import (
    STREAMS,
    actual_scores,
    expected_scores,
    replay_results,
    update_ratings,
)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class AlpineResultsEloReplayTest(unittest.TestCase):
    def test_legacy_multi_competitor_math(self) -> None:
        self.assertEqual(actual_scores([1, 2, 3]), [2.0, 1.0, 0.0])
        self.assertEqual(expected_scores([1300.0] * 3), [1.0, 1.0, 1.0])
        self.assertEqual(
            update_ratings([1300.0] * 3, [1, 2, 3], 5.0),
            [1305.0, 1300.0, 1295.0],
        )

    def test_replay_applies_rollover_and_discipline_updates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            mapped = sources / "mapped.csv"
            men = sources / "men.csv"
            ladies = sources / "ladies.csv"
            metadata_fields = [
                "Date", "City", "Country", "Sex", "Distance", "Event", "MS",
                "Technique", "Place", "Skier", "Nation", "ID", "Season", "Race",
                "Birthday", "Age", "Exp",
            ]
            elo_fields = list(STREAMS)
            pelo_fields = [
                "Pelo", "Downhill_Pelo", "Super G_Pelo", "Giant Slalom_Pelo",
                "Slalom_Pelo", "Combined_Pelo", "Tech_Pelo", "Speed_Pelo",
            ]
            history_fields = elo_fields + metadata_fields + pelo_fields
            history_rows = []
            for athlete_id, name, place in ((1, "One Athlete", 1), (2, "Two Athlete", 2)):
                for race in range(1, 6):
                    historical = {
                        "Date": f"2025-01-{race:02d}", "City": "Prior", "Country": "Norway",
                        "Sex": "L", "Distance": "Giant Slalom", "Event": "World Cup", "MS": 0,
                        "Technique": "N/A", "Place": place, "Skier": name, "Nation": "Norway",
                        "ID": athlete_id, "Season": 2025, "Race": race,
                        "Birthday": "2000-01-01T00:00:00", "Age": 25, "Exp": race,
                    }
                    historical.update(
                        {column: 1400 for column in elo_fields + pelo_fields}
                    )
                    history_rows.append(historical)
                row = {
                    "Date": "2026-05-01", "City": "Summer", "Country": "Break",
                    "Sex": "L", "Distance": "0", "Event": "Offseason", "MS": 0,
                    "Technique": "", "Place": 0, "Skier": name, "Nation": "Norway",
                    "ID": athlete_id, "Season": 2026, "Race": 0,
                    "Birthday": "2000-01-01T00:00:00", "Age": 26, "Exp": 10,
                }
                row.update({column: 1400 for column in elo_fields + pelo_fields})
                history_rows.append(row)
            write_csv(ladies, history_fields, history_rows)
            write_csv(men, history_fields, [])
            candidate_fields = [
                "date", "sex", "season", "race", "city", "country", "distance",
                "place", "canonical_athlete_id", "canonical_name", "canonical_nation",
            ]
            write_csv(
                mapped,
                candidate_fields,
                [
                    {"date": "2026-10-24", "sex": "L", "season": 2027, "race": 1,
                     "city": "Soelden", "country": "Austria", "distance": "Giant Slalom",
                     "place": 1, "canonical_athlete_id": 1,
                     "canonical_name": "One Athlete", "canonical_nation": "Norway"},
                    {"date": "2026-10-24", "sex": "L", "season": 2027, "race": 1,
                     "city": "Soelden", "country": "Austria", "distance": "Giant Slalom",
                     "place": 2, "canonical_athlete_id": 2,
                     "canonical_name": "Two Athlete", "canonical_nation": "Norway"},
                ],
            )
            report = replay_results(
                mapped_results=mapped,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
            )
            self.assertEqual(report["result_rows"], {"M": 0, "L": 2})
            with (root / "output" / "ladies_elo_rows.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            # 1400 rolls to 1385, then equal ratings move by +/-2.5 at K=5.
            self.assertAlmostEqual(float(rows[0]["Pelo"]), 1385.0)
            self.assertAlmostEqual(float(rows[0]["Elo"]), 1387.5)
            self.assertAlmostEqual(float(rows[1]["Elo"]), 1382.5)
            self.assertAlmostEqual(float(rows[0]["Giant Slalom_Elo"]), 1387.5)
            self.assertAlmostEqual(float(rows[0]["Tech_Elo"]), 1387.5)
            self.assertAlmostEqual(float(rows[0]["Slalom_Elo"]), 1385.0)
            self.assertEqual(rows[0]["Exp"], "11")


if __name__ == "__main__":
    unittest.main()
