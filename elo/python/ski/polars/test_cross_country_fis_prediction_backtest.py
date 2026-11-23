import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_fis_prediction_backtest import (
    FisPredictionBacktestError,
    backtest_fis_prediction_elo,
)
from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS, regress_rating

LEDGER_FIELDS = [
    "fis_race_id",
    "date",
    "season",
    "sex",
    "place",
    "prediction_athlete_id",
    "fis_competitor_id",
    "fis_code",
    "name",
    "anchor_comparisons",
    "k_factor",
    "update_status",
    *STREAMS,
]
CROSSWALK_FIELDS = [
    "sex",
    "prediction_athlete_id",
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "canonical_athlete_id",
    "canonical_name",
    "status",
    "evidence",
]
HISTORY_FIELDS = [
    "Date",
    "Event",
    "Season",
    "Race",
    "ID",
    "Skier",
    "Place",
    "Distance",
    "Technique",
    *PREVIOUS_COLUMNS.values(),
]


def write_csv(path: Path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def ledger_row(rating=1700, k_factor=5, **overrides):
    row = {
        "fis_race_id": "500",
        "date": "2026-12-01",
        "season": "2027",
        "sex": "M",
        "place": "1",
        "prediction_athlete_id": "fis:300",
        "fis_competitor_id": "300",
        "fis_code": "3400003",
        "name": "New Athlete",
        "anchor_comparisons": "2",
        "k_factor": str(k_factor),
        "update_status": "updated_from_fixed_anchors",
    }
    row.update({stream: rating for stream in STREAMS})
    row.update(overrides)
    return row


def crosswalk_row(**overrides):
    row = {
        "sex": "M",
        "prediction_athlete_id": "fis:300",
        "fis_competitor_id": "300",
        "fis_code": "3400003",
        "fis_name": "New Athlete",
        "canonical_athlete_id": "10",
        "canonical_name": "New Athlete",
        "status": "reviewed",
        "evidence": "FIS competitor and accepted history reviewed",
    }
    row.update(overrides)
    return row


def history_row(**overrides):
    row = {
        "Date": "2027-01-10",
        "Event": "World Cup",
        "Season": "2027",
        "Race": "1",
        "ID": "10",
        "Skier": "New Athlete",
        "Place": "1",
        "Distance": "10",
        "Technique": "C",
    }
    row.update({column: "1300" for column in PREVIOUS_COLUMNS.values()})
    row.update(overrides)
    return row


def write_sources(
    root: Path,
    *,
    strong_rows=None,
    weak_rows=None,
    crosswalk_rows=None,
    history_rows=None,
):
    sources = root / "sources"
    strong = sources / "k5.csv"
    weak = sources / "k1.csv"
    crosswalk = sources / "crosswalk.csv"
    men = sources / "men.csv"
    ladies = sources / "ladies.csv"
    write_csv(strong, LEDGER_FIELDS, strong_rows or [ledger_row()])
    write_csv(
        weak,
        LEDGER_FIELDS,
        weak_rows or [ledger_row(rating=1200, k_factor=1)],
    )
    write_csv(
        crosswalk,
        CROSSWALK_FIELDS,
        crosswalk_rows or [crosswalk_row()],
    )
    write_csv(
        men,
        HISTORY_FIELDS,
        history_rows
        or [
            history_row(),
            history_row(ID="20", Skier="Opponent A", Place="2", Pelo="1500"),
            history_row(ID="30", Skier="Opponent B", Place="3", Pelo="1400"),
        ],
    )
    write_csv(ladies, HISTORY_FIELDS, [])
    return strong, weak, crosswalk, men, ladies


def read_cases(output: Path):
    with (output / "fis-prediction-backtest-cases.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        return list(csv.DictReader(handle))


class CrossCountryFisPredictionBacktestTests(unittest.TestCase):
    def test_selects_candidate_that_improves_first_a_tier_prediction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, weak, crosswalk, men, ladies = write_sources(root)
            report = backtest_fis_prediction_elo(
                candidate_ledgers={"k1": weak, "k5": strong},
                identity_crosswalk=crosswalk,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=1,
            )
            self.assertEqual(report["best_candidate"], "k5")
            self.assertEqual(report["recommended_candidate"], "k5")
            self.assertEqual(report["recommended_k_factor"], 5.0)
            self.assertEqual(report["decision"], "candidate_improves_official_prior")
            self.assertLess(
                report["metrics"]["k5"]["brier_score"],
                report["metrics"]["official_prior"]["brier_score"],
            )
            self.assertFalse(report["publication_eligible"])
            self.assertEqual(report["unknown_vs_unknown_comparisons"], 0)
            self.assertEqual(report["verified_anchor_updates"], 0)

    def test_scores_only_streams_matching_the_first_a_tier_race(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, _, crosswalk, men, ladies = write_sources(root)
            backtest_fis_prediction_elo(
                candidate_ledgers={"candidate": strong},
                identity_crosswalk=crosswalk,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=1,
            )
            streams = {row["stream"] for row in read_cases(root / "output")}
            self.assertEqual(
                streams,
                {"Elo", "Distance_Elo", "Distance_C_Elo", "Classic_Elo"},
            )

    def test_minimum_athlete_gate_prevents_small_sample_recommendation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, _, crosswalk, men, ladies = write_sources(root)
            report = backtest_fis_prediction_elo(
                candidate_ledgers={"candidate": strong},
                identity_crosswalk=crosswalk,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=2,
            )
            self.assertEqual(report["decision"], "insufficient_evidence")
            self.assertIsNone(report["recommended_candidate"])

    def test_regresses_candidate_across_missed_season_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong_rows = [ledger_row(date="2026-05-01", season="2026", rating=1700)]
            strong, _, crosswalk, men, ladies = write_sources(
                root, strong_rows=strong_rows
            )
            backtest_fis_prediction_elo(
                candidate_ledgers={"candidate": strong},
                identity_crosswalk=crosswalk,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=1,
            )
            candidate = next(
                row
                for row in read_cases(root / "output")
                if row["method"] == "candidate" and row["stream"] == "Elo"
            )
            self.assertAlmostEqual(
                float(candidate["target_rating"]), regress_rating(1700, 1)
            )

    def test_rejects_candidate_with_different_replay_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, weak, crosswalk, men, ladies = write_sources(
                root,
                weak_rows=[ledger_row(rating=1200, k_factor=1, anchor_comparisons="1")],
            )
            with self.assertRaisesRegex(
                FisPredictionBacktestError, "identical replay evidence"
            ):
                backtest_fis_prediction_elo(
                    candidate_ledgers={"strong": strong, "weak": weak},
                    identity_crosswalk=crosswalk,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=root / "output",
                )

    def test_rejects_crosswalk_that_does_not_match_prediction_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, _, crosswalk, men, ladies = write_sources(
                root, crosswalk_rows=[crosswalk_row(fis_code="wrong")]
            )
            with self.assertRaisesRegex(FisPredictionBacktestError, "does not match"):
                backtest_fis_prediction_elo(
                    candidate_ledgers={"candidate": strong},
                    identity_crosswalk=crosswalk,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=root / "output",
                )

    def test_rejects_endpoint_after_an_existing_a_tier_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            history = [
                history_row(Date="2026-11-01", Race="1"),
                history_row(
                    Date="2026-11-01",
                    Race="1",
                    ID="20",
                    Skier="Opponent",
                    Place="2",
                ),
            ]
            strong, _, crosswalk, men, ladies = write_sources(
                root, history_rows=history
            )
            with self.assertRaisesRegex(FisPredictionBacktestError, "already A-tier"):
                backtest_fis_prediction_elo(
                    candidate_ledgers={"candidate": strong},
                    identity_crosswalk=crosswalk,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=root / "output",
                )

    def test_sources_are_immutable_and_output_is_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = write_sources(root)
            before = {path: path.read_bytes() for path in sources}
            for output in (root / "first", root / "second"):
                backtest_fis_prediction_elo(
                    candidate_ledgers={"candidate": sources[0]},
                    identity_crosswalk=sources[2],
                    men_chrono=sources[3],
                    ladies_chrono=sources[4],
                    output_dir=output,
                    minimum_athletes=1,
                )
            self.assertEqual(before, {path: path.read_bytes() for path in sources})
            for filename in (
                "fis-prediction-backtest-cases.csv",
                "fis-prediction-backtest-report.json",
            ):
                self.assertEqual(
                    (root / "first" / filename).read_bytes(),
                    (root / "second" / filename).read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
