import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_flgr_bridge_backtest import (
    FlgrBridgeBacktestError,
    backtest_flgr_bridge,
)
from cross_country_results_elo_replay import regress_rating

BRIDGE_FIELDS = [
    "event_id",
    "date",
    "season",
    "sex",
    "canonical_athlete_id",
    "canonical_name",
    "identity_status",
    "place",
    "source_sha256",
    "Elo",
]
HISTORY_FIELDS = [
    "Date",
    "Event",
    "Season",
    "Race",
    "ID",
    "Skier",
    "Place",
    "Pelo",
]


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def bridge_row(**overrides) -> dict[str, object]:
    row: dict[str, object] = {
        "event_id": "flgr:77:88",
        "date": "2026-12-01",
        "season": 2027,
        "sex": "M",
        "canonical_athlete_id": "10",
        "canonical_name": "Ivan Ivanov",
        "identity_status": "reviewed_crosswalk",
        "place": 1,
        "source_sha256": "a" * 64,
        "Elo": 1700,
    }
    row.update(overrides)
    return row


def history_row(**overrides) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "2027-01-10",
        "Event": "World Cup",
        "Season": 2027,
        "Race": 1,
        "ID": "10",
        "Skier": "Ivan Ivanov",
        "Place": 1,
        "Pelo": 1300,
    }
    row.update(overrides)
    return row


def write_sources(
    root: Path,
    *,
    strong_rows=None,
    weak_rows=None,
    history_rows=None,
    legacy_rows=None,
):
    sources = root / "sources"
    sources.mkdir()
    strong = sources / "strong.csv"
    weak = sources / "weak.csv"
    men = sources / "men.csv"
    ladies = sources / "ladies.csv"
    legacy = sources / "legacy.csv"
    write_csv(strong, BRIDGE_FIELDS, strong_rows or [bridge_row()])
    write_csv(weak, BRIDGE_FIELDS, weak_rows or [bridge_row(Elo=1200)])
    write_csv(
        men,
        HISTORY_FIELDS,
        history_rows
        or [
            history_row(),
            history_row(ID="20", Skier="Opponent A", Place=2, Pelo=1500),
            history_row(ID="30", Skier="Opponent B", Place=3, Pelo=1400),
        ],
    )
    write_csv(ladies, HISTORY_FIELDS, [])
    write_csv(
        legacy,
        ["Date", "Season", "ID", "Elo", "Sex"],
        legacy_rows
        or [
            {"Date": "2026-12-20", "Season": 2027, "ID": "10", "Elo": 1600, "Sex": "M"}
        ],
    )
    return strong, weak, men, ladies, legacy


def read_cases(output: Path) -> list[dict[str, str]]:
    with (output / "flgr-bridge-backtest-cases.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        return list(csv.DictReader(handle))


class FlgrBridgeBacktestTests(unittest.TestCase):
    def test_stronger_bridge_beats_stale_fis_on_first_return_race(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, _, men, ladies, _ = write_sources(root)
            report = backtest_flgr_bridge(
                bridge_ledgers={"k5": strong},
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=1,
            )

            self.assertLess(
                report["metrics"]["bridge:k5"]["brier_score"],
                report["metrics"]["stale_fis"]["brier_score"],
            )
            self.assertEqual(report["decision"], "candidate_improves_stale_fis")
            self.assertEqual(report["recommended_candidate"], "k5")
            self.assertFalse(report["publication_eligible"])
            cases = read_cases(root / "output")
            self.assertEqual({row["return_date"] for row in cases}, {"2027-01-10"})
            self.assertEqual(len(cases), 4)

    def test_selects_best_of_multiple_identically_shaped_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, weak, men, ladies, _ = write_sources(root)
            report = backtest_flgr_bridge(
                bridge_ledgers={"k1": weak, "k5": strong},
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=1,
            )
            self.assertEqual(report["best_bridge_candidate"], "k5")
            self.assertEqual(report["recommended_candidate"], "k5")

    def test_minimum_athlete_threshold_prevents_a_tiny_sample_recommendation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, _, men, ladies, _ = write_sources(root)
            report = backtest_flgr_bridge(
                bridge_ledgers={"k5": strong},
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=2,
            )
            self.assertEqual(report["decision"], "insufficient_evidence")
            self.assertIsNone(report["recommended_candidate"])

    def test_uses_first_return_only_and_applies_missed_offseason_regression(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bridge = [bridge_row(date="2026-06-01", season=2026, Elo=1700)]
            history = [
                history_row(Date="2026-11-20", Season=2027, Race=1),
                history_row(
                    Date="2026-11-20", Season=2027, Race=1, ID="20", Place=2, Pelo=1500
                ),
                history_row(Date="2027-01-10", Season=2027, Race=2, Place=10),
                history_row(
                    Date="2027-01-10", Season=2027, Race=2, ID="20", Place=1, Pelo=1500
                ),
            ]
            strong, _, men, ladies, _ = write_sources(
                root, strong_rows=bridge, history_rows=history
            )
            backtest_flgr_bridge(
                bridge_ledgers={"candidate": strong},
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=1,
            )
            candidate_case = next(
                row
                for row in read_cases(root / "output")
                if row["method"] == "bridge:candidate"
            )
            self.assertEqual(candidate_case["return_date"], "2026-11-20")
            self.assertAlmostEqual(
                float(candidate_case["target_rating"]), regress_rating(1700, 1)
            )

    def test_optional_legacy_is_labeled_quarantined_and_never_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, _, men, ladies, legacy = write_sources(root)
            report = backtest_flgr_bridge(
                bridge_ledgers={"candidate": strong},
                men_chrono=men,
                ladies_chrono=ladies,
                legacy_ledger=legacy,
                output_dir=root / "output",
                minimum_athletes=1,
            )
            self.assertIn("legacy_bridge", report["metrics"])
            self.assertTrue(report["legacy_is_quarantined_benchmark_only"])
            self.assertFalse(report["publication_eligible"])

    def test_reports_reviewed_athletes_without_a_later_fis_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            extra = bridge_row(
                canonical_athlete_id="99",
                canonical_name="No Return",
                event_id="flgr:77:89",
            )
            strong_rows = [bridge_row(), extra]
            weak_rows = [bridge_row(Elo=1200), dict(extra, Elo=1200)]
            strong, weak, men, ladies, _ = write_sources(
                root, strong_rows=strong_rows, weak_rows=weak_rows
            )
            report = backtest_flgr_bridge(
                bridge_ledgers={"strong": strong, "weak": weak},
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                minimum_athletes=1,
            )
            self.assertEqual(
                report["athletes_without_return"][0]["canonical_athlete_id"], "99"
            )

    def test_rejects_noncomparable_candidate_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, weak, men, ladies, _ = write_sources(
                root, weak_rows=[bridge_row(event_id="flgr:changed", Elo=1200)]
            )
            with self.assertRaisesRegex(FlgrBridgeBacktestError, "identical bridge"):
                backtest_flgr_bridge(
                    bridge_ledgers={"strong": strong, "weak": weak},
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=root / "output",
                )

    def test_sources_are_immutable_and_output_must_be_isolated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strong, _, men, ladies, _ = write_sources(root)
            before = {path: path.read_bytes() for path in (strong, men, ladies)}
            backtest_flgr_bridge(
                bridge_ledgers={"candidate": strong},
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
            )
            self.assertEqual(before, {path: path.read_bytes() for path in before})

            existing = root / "existing"
            existing.mkdir()
            (existing / "old").write_text("old", encoding="utf-8")
            with self.assertRaisesRegex(FlgrBridgeBacktestError, "must be empty"):
                backtest_flgr_bridge(
                    bridge_ledgers={"candidate": strong},
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=existing,
                )


if __name__ == "__main__":
    unittest.main()
