import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_results_elo_replay import (
    PREVIOUS_COLUMNS,
    STREAMS,
    _event_for,
    actual_scores,
    expected_scores,
    replay_results,
    stream_includes,
    update_ratings,
)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryResultsEloReplayTests(unittest.TestCase):
    def test_legacy_multi_competitor_math(self) -> None:
        self.assertEqual(actual_scores([1, 2, 3]), [2.0, 1.0, 0.0])
        self.assertEqual(expected_scores([1300.0] * 3), [1.0, 1.0, 1.0])
        self.assertEqual(
            update_ratings([1300.0] * 3, [1, 2, 3], 5.0),
            [1305.0, 1300.0, 1295.0],
        )

    def test_stream_rules_match_cross_country_contract(self) -> None:
        self.assertTrue(stream_includes("Distance_C_Elo", "10", "C"))
        self.assertTrue(stream_includes("Sprint_F_Elo", "Sprint", "F"))
        self.assertTrue(stream_includes("Classic_Elo", "10", "N/A"))
        self.assertFalse(stream_includes("Distance_Elo", "Sprint", "C"))
        self.assertFalse(stream_includes("Classic_Elo", "20", "P"))
        self.assertFalse(stream_includes("Freestyle_Elo", "20", "P"))

    def test_championship_event_uses_season(self) -> None:
        self.assertEqual(
            _event_for({"season": "2026", "championship": "1"}),
            "Olympic Winter Games",
        )
        self.assertEqual(
            _event_for({"season": "2027", "championship": "1"}),
            "World Championship",
        )
        self.assertEqual(
            _event_for({"season": "2027", "championship": "0"}), "World Cup"
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
            ]
            history_fields = list(STREAMS) + metadata_fields + list(
                PREVIOUS_COLUMNS.values()
            )
            history_rows: list[dict[str, object]] = []
            for athlete_id, name, place in (
                (1, "One Athlete", 1),
                (2, "Two Athlete", 2),
            ):
                for race in range(1, 6):
                    historical: dict[str, object] = {
                        "Date": f"2025-01-{race:02d}",
                        "City": "Prior",
                        "Country": "Norway",
                        "Sex": "M",
                        "Distance": "10",
                        "Event": "World Cup",
                        "MS": 0,
                        "Technique": "C",
                        "Place": place,
                        "Skier": name,
                        "Nation": "Norway",
                        "ID": athlete_id,
                        "Season": 2025,
                        "Race": race,
                        "Birthday": "2000-01-01T00:00:00",
                        "Age": 25,
                        "Exp": race,
                    }
                    historical.update(
                        {
                            column: 1400
                            for column in (*STREAMS, *PREVIOUS_COLUMNS.values())
                        }
                    )
                    history_rows.append(historical)
                offseason: dict[str, object] = {
                    "Date": "2026-05-01",
                    "City": "Summer",
                    "Country": "Break",
                    "Sex": "M",
                    "Distance": "0",
                    "Event": "Offseason",
                    "MS": 0,
                    "Technique": "",
                    "Place": 0,
                    "Skier": name,
                    "Nation": "Norway",
                    "ID": athlete_id,
                    "Season": 2026,
                    "Race": 0,
                    "Birthday": "2000-01-01T00:00:00",
                    "Age": 26,
                    "Exp": 10,
                }
                offseason.update(
                    {
                        column: 1400
                        for column in (*STREAMS, *PREVIOUS_COLUMNS.values())
                    }
                )
                history_rows.append(offseason)
            write_csv(men, history_fields, history_rows)
            write_csv(ladies, history_fields, [])
            candidate_fields = [
                "date",
                "sex",
                "season",
                "race",
                "city",
                "country",
                "distance",
                "technique",
                "mass_start",
                "championship",
                "place",
                "canonical_athlete_id",
                "canonical_name",
                "canonical_nation",
            ]
            write_csv(
                mapped,
                candidate_fields,
                [
                    {
                        "date": "2026-11-27",
                        "sex": "M",
                        "season": 2027,
                        "race": 1,
                        "city": "Ruka",
                        "country": "Finland",
                        "distance": "10",
                        "technique": "C",
                        "mass_start": 0,
                        "championship": 0,
                        "place": 1,
                        "canonical_athlete_id": 1,
                        "canonical_name": "One Athlete",
                        "canonical_nation": "Norway",
                    },
                    {
                        "date": "2026-11-27",
                        "sex": "M",
                        "season": 2027,
                        "race": 1,
                        "city": "Ruka",
                        "country": "Finland",
                        "distance": "10",
                        "technique": "C",
                        "mass_start": 0,
                        "championship": 0,
                        "place": 2,
                        "canonical_athlete_id": 2,
                        "canonical_name": "Two Athlete",
                        "canonical_nation": "Norway",
                    },
                ],
            )
            source_bytes = {path: path.read_bytes() for path in (men, ladies)}

            report = replay_results(
                mapped_results=mapped,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
            )

            self.assertEqual(report["result_rows"], {"M": 2, "L": 0})
            self.assertFalse(report["source_histories_modified"])
            with (root / "output" / "men_elo_rows.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            # 1400 rolls to 1385, then equal ratings move by +/-2.5 at K=5.
            self.assertAlmostEqual(float(rows[0]["Pelo"]), 1385.0)
            self.assertAlmostEqual(float(rows[0]["Elo"]), 1387.5)
            self.assertAlmostEqual(float(rows[1]["Elo"]), 1382.5)
            self.assertAlmostEqual(float(rows[0]["Distance_Elo"]), 1387.5)
            self.assertAlmostEqual(float(rows[0]["Distance_C_Elo"]), 1387.5)
            self.assertAlmostEqual(float(rows[0]["Classic_Elo"]), 1387.5)
            self.assertAlmostEqual(float(rows[0]["Sprint_Elo"]), 1385.0)
            self.assertAlmostEqual(float(rows[0]["Freestyle_Elo"]), 1385.0)
            self.assertEqual(rows[0]["MS"], "0")
            self.assertEqual(rows[0]["Technique"], "C")
            self.assertEqual(rows[0]["Exp"], "11")
            self.assertEqual(
                source_bytes, {path: path.read_bytes() for path in source_bytes}
            )


if __name__ == "__main__":
    unittest.main()
