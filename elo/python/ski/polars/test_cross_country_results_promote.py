import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS
from cross_country_results_promote import (
    PromotionError,
    build_candidate_history,
)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryResultsPromoteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fields = [
            *STREAMS,
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
            *PREVIOUS_COLUMNS.values(),
        ]

    def row(self, **overrides: object) -> dict[str, object]:
        row: dict[str, object] = {
            "Date": "2026-05-01",
            "City": "Summer",
            "Country": "Break",
            "Sex": "M",
            "Distance": "0",
            "Event": "Offseason",
            "MS": 0,
            "Technique": "",
            "Place": 0,
            "Skier": "One Athlete",
            "Nation": "Norway",
            "ID": 1,
            "Season": 2026,
            "Race": 0,
            "Birthday": "2000-01-01T00:00:00",
            "Age": 26,
            "Exp": 10,
        }
        row.update(
            {column: 1400 for column in (*STREAMS, *PREVIOUS_COLUMNS.values())}
        )
        row.update(overrides)
        return row

    def test_appends_without_changing_history_or_race_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            increments = root / "increments.csv"
            output = root / "candidate.csv"
            regular = self.row(
                Date="2026-03-01",
                Event="World Cup",
                Distance="10",
                Technique="C",
                Race=40,
            )
            offseason = self.row()
            incoming = self.row(
                Date="2026-11-27",
                City="Ruka",
                Country="Finland",
                Distance="10",
                Technique="C",
                Event="World Cup",
                Place=1,
                Season=2027,
                Race=1,
            )
            incoming.update({stream: 1385.0 for stream in STREAMS})
            incoming.update(
                {
                    "Elo": 1387.5,
                    "Distance_Elo": 1387.5,
                    "Distance_C_Elo": 1387.5,
                    "Classic_Elo": 1387.5,
                }
            )
            incoming.update(
                {previous: 1385.0 for previous in PREVIOUS_COLUMNS.values()}
            )
            write_csv(source, self.fields, [regular, offseason])
            source_bytes = source.read_bytes()
            write_csv(increments, self.fields, [incoming])

            report = build_candidate_history(
                source=source,
                increments=increments,
                output=output,
                sex="M",
                expected_season=2027,
            )

            candidate_bytes = output.read_bytes()
            self.assertTrue(candidate_bytes.startswith(source_bytes))
            with output.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertEqual(float(rows[1]["Elo"]), 1400.0)
            self.assertEqual(float(rows[1]["Pelo"]), 1400.0)
            self.assertEqual(float(rows[2]["Pelo"]), 1385.0)
            self.assertEqual(report["race_zero_rows_modified"], 0)
            self.assertTrue(report["historical_bytes_preserved"])
            self.assertEqual(source.read_bytes(), source_bytes)

    def test_allows_header_only_increment_for_sex_without_a_race(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            increments = root / "increments.csv"
            output = root / "candidate.csv"
            write_csv(source, self.fields, [self.row()])
            write_csv(increments, self.fields, [])

            report = build_candidate_history(
                source=source,
                increments=increments,
                output=output,
                sex="M",
                expected_season=2027,
            )

            self.assertEqual(report["appended_rows"], 0)
            self.assertEqual(output.read_bytes(), source.read_bytes())

    def test_reference_history_validates_first_individual_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            reference = root / "reference.csv"
            increments = root / "increments.csv"
            output = root / "candidate.csv"
            write_csv(source, self.fields, [self.row()])
            write_csv(
                reference,
                self.fields,
                [self.row(ID=2, Skier="Reference Athlete")],
            )
            incoming = self.row(
                ID=2,
                Skier="Reference Athlete",
                Date="2026-11-27",
                City="Ruka",
                Country="Finland",
                Distance="10",
                Technique="C",
                Event="World Cup",
                Place=1,
                Season=2027,
                Race=1,
                Exp=11,
            )
            incoming.update({stream: 1385.0 for stream in STREAMS})
            incoming.update(
                {previous: 1385.0 for previous in PREVIOUS_COLUMNS.values()}
            )
            write_csv(increments, self.fields, [incoming])

            report = build_candidate_history(
                source=source,
                increments=increments,
                output=output,
                sex="M",
                expected_season=2027,
                reference_source=reference,
            )

            self.assertEqual(report["appended_rows"], 1)
            with output.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[-1]["ID"], "2")
            self.assertEqual(rows[-1]["Pelo"], "1385.0")

    def test_prediction_ledger_validates_first_a_tier_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            source = sources / "source.csv"
            increments = sources / "increments.csv"
            mapped = sources / "mapped.csv"
            ledger = sources / "ledger.csv"
            write_csv(source, self.fields, [self.row()])
            incoming = self.row(
                ID=3550888, Skier="New Athlete", Nation="Sweden", Birthday="",
                Date="2026-11-27", City="Ruka", Country="Finland",
                Distance="10", Technique="C", Event="World Cup", Place=1,
                Season=2027, Race=1, Exp=1,
            )
            incoming.update({stream: 1425.0 for stream in STREAMS})
            incoming.update(
                {previous: 1425.0 for previous in PREVIOUS_COLUMNS.values()}
            )
            write_csv(increments, self.fields, [incoming])
            mapped_row = {
                "date": "2026-11-27", "sex": "M", "season": 2027,
                "race": 1, "canonical_athlete_id": 3550888,
                "canonical_name": "New Athlete", "canonical_nation": "Sweden",
                "fis_competitor_id": 888, "fis_code": 3550888,
            }
            write_csv(mapped, list(mapped_row), [mapped_row])
            prediction = {
                "date": "2026-11-20", "season": 2027, "sex": "M",
                "fis_race_id": 99, "prediction_athlete_id": "fis:888",
                "fis_competitor_id": 888, "fis_code": 3550888,
                **{stream: 1425 for stream in STREAMS},
            }
            write_csv(ledger, list(prediction), [prediction])

            report = build_candidate_history(
                source=source,
                increments=increments,
                output=root / "candidate.csv",
                sex="M",
                expected_season=2027,
                mapped_results=mapped,
                prediction_ledger=ledger,
            )
            self.assertEqual(report["appended_rows"], 1)

    def test_rejects_out_of_order_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            increments = root / "increments.csv"
            accepted = self.row(
                Date="2026-11-28",
                Event="World Cup",
                Distance="Sprint",
                Technique="C",
                Season=2027,
                Race=2,
                Place=1,
            )
            incoming = self.row(
                Date="2026-11-27",
                Event="World Cup",
                Distance="10",
                Technique="C",
                Season=2027,
                Race=1,
                Place=1,
            )
            write_csv(source, self.fields, [self.row(), accepted])
            write_csv(increments, self.fields, [incoming])
            with self.assertRaisesRegex(PromotionError, "not newer"):
                build_candidate_history(
                    source=source,
                    increments=increments,
                    output=root / "candidate.csv",
                    sex="M",
                    expected_season=2027,
                )


if __name__ == "__main__":
    unittest.main()
