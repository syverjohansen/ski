import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_fantasyxc_backtest import (
    FantasyXCBacktestError,
    REPORT_SCHEMA,
    backtest_candidates,
)
from cross_country_fantasyxc_resolve import CANDIDATES_SCHEMA
from cross_country_weekend_inputs import STARTLIST_FIELDS


class CrossCountryFantasyXCBacktestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.logical_date = date(2026, 11, 27)
        self.candidates = self.root / "candidates.json"
        self.ladies = self.root / "ladies-startlist.csv"
        self.men = self.root / "men-startlist.csv"
        self._write_candidates()
        self._write_official(
            self.ladies,
            [
                ("L", "100", "10", "Jasmi Joensuu"),
                ("L", "101", "10", "Jasmi Joensuu"),
            ],
        )
        self._write_official(
            self.men,
            [("M", "200", "2", "Johannes Hoesflot Klaebo")],
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_candidates(self) -> None:
        self.candidates.write_text(
            json.dumps(
                {
                    "schema_version": CANDIDATES_SCHEMA,
                    "status": "candidate_review",
                    "logical_date": self.logical_date.isoformat(),
                    "publication": {"status": "disabled"},
                    "source": {"path": "feed.json", "sha256": "abc"},
                    "candidates": [
                        {
                            "candidate_id": "candidate:jasmi",
                            "sex": "L",
                            "canonical_athlete_id": "10",
                            "canonical_name": "Jasmi Joensuu",
                            "suggested_status": "confirmed",
                            "race_ids": ["100", "101"],
                            "observed_at": "2026-11-23T20:00:00Z",
                        },
                        {
                            "candidate_id": "candidate:astrid",
                            "sex": "L",
                            "canonical_athlete_id": "11",
                            "canonical_name": "Astrid Oeyre Slind",
                            "suggested_status": "excluded",
                            "race_ids": ["*"],
                            "observed_at": "2026-11-23T20:01:00Z",
                        },
                        {
                            "candidate_id": "candidate:iivo",
                            "sex": "M",
                            "canonical_athlete_id": "1",
                            "canonical_name": "Iivo Niskanen",
                            "suggested_status": "confirmed",
                            "race_ids": ["200"],
                            "observed_at": "2026-11-23T20:02:00Z",
                        },
                    ],
                    "review_exceptions": [{"exception_id": "exception:one"}],
                }
            ),
            encoding="utf-8",
        )

    def _write_official(
        self,
        path: Path,
        starters: list[tuple[str, str, str, str]],
        *,
        source_mode: str = "official_fis_startlist",
    ) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=STARTLIST_FIELDS)
            writer.writeheader()
            for sex, race_id, athlete_id, name in starters:
                row = dict.fromkeys(STARTLIST_FIELDS, "")
                row.update(
                    {
                        "group_date": self.logical_date.isoformat(),
                        "race_date": self.logical_date.isoformat(),
                        "race_id": race_id,
                        "sex": sex,
                        "canonical_athlete_id": athlete_id,
                        "canonical_name": name,
                        "selection_status": "confirmed",
                        "source_mode": source_mode,
                    }
                )
                writer.writerow(row)

    def _backtest(self) -> dict[str, object]:
        return backtest_candidates(
            candidates_path=self.candidates,
            official_startlists=(self.men, self.ladies),
            logical_date=self.logical_date,
            output_dir=self.root / "output",
        )

    def test_scores_event_level_confirmations_and_exclusions(self) -> None:
        report = self._backtest()
        metrics = report["metrics"]

        self.assertEqual(REPORT_SCHEMA, report["schema_version"])
        self.assertEqual("research_backtest", report["status"])
        self.assertEqual({"status": "disabled"}, report["publication"])
        self.assertEqual("insufficient_evidence", report["recommendation"])
        self.assertEqual(5, metrics["decision_count"])
        self.assertEqual(4, metrics["correct_decisions"])
        self.assertEqual(0.8, metrics["decision_accuracy"])
        self.assertEqual(2 / 3, metrics["confirmed_precision"])
        self.assertEqual(1.0, metrics["excluded_accuracy"])
        self.assertEqual(2 / 3, metrics["official_field_coverage"])
        self.assertEqual(1, metrics["false_confirmations"])
        self.assertEqual(0, metrics["false_exclusions"])
        self.assertEqual(5, report["outputs"]["fantasyxc-backtest-cases.csv"]["rows"])
        self.assertTrue((self.root / "output/fantasyxc-backtest-cases.csv").is_file())
        self.assertTrue((self.root / "output/fantasyxc-backtest-report.json").is_file())

    def test_non_official_truth_is_rejected(self) -> None:
        self._write_official(
            self.men,
            [("M", "200", "2", "Johannes Hoesflot Klaebo")],
            source_mode="latest_season_fallback",
        )
        with self.assertRaisesRegex(FantasyXCBacktestError, "official FIS"):
            self._backtest()

    def test_unknown_candidate_event_is_rejected(self) -> None:
        document = json.loads(self.candidates.read_text())
        document["candidates"][0]["race_ids"] = ["999"]
        self.candidates.write_text(json.dumps(document))
        with self.assertRaisesRegex(FantasyXCBacktestError, "unknown official event"):
            self._backtest()

    def test_post_weekend_evidence_is_rejected(self) -> None:
        document = json.loads(self.candidates.read_text())
        document["candidates"][0]["observed_at"] = "2026-11-28T10:00:00Z"
        self.candidates.write_text(json.dumps(document))
        with self.assertRaisesRegex(FantasyXCBacktestError, "not observed before"):
            self._backtest()


if __name__ == "__main__":
    unittest.main()
