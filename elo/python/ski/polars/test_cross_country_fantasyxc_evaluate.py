import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from cross_country_fantasyxc_backtest import REPORT_SCHEMA as BACKTEST_SCHEMA
from cross_country_fantasyxc_evaluate import (
    EVALUATION_SCHEMA,
    FantasyXCEvaluationError,
    evaluate_backtests,
)
from cross_country_fantasyxc_resolve import RESOLVER_VERSION

ROOT = Path(__file__).resolve().parent
POLICY = ROOT / "cross_country_fantasyxc_automation_policy.json"


class CrossCountryFantasyXCEvaluationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.output = self.root / "evaluation.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _report(
        self,
        index: int,
        *,
        confirmed_correct: int = 19,
        resolver_version: str = RESOLVER_VERSION,
        logical_date: date | None = None,
    ) -> Path:
        confirmed_decisions = 20
        excluded_decisions = 10
        excluded_correct = 10
        path = self.root / f"report-{index}.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": BACKTEST_SCHEMA,
                    "resolver_version": resolver_version,
                    "status": "research_backtest",
                    "logical_date": (
                        logical_date or date(2026, 11, 1) + timedelta(days=7 * index)
                    ).isoformat(),
                    "publication": {"status": "disabled"},
                    "recommendation": "insufficient_evidence",
                    "metrics": {
                        "decision_count": confirmed_decisions + excluded_decisions,
                        "correct_decisions": confirmed_correct + excluded_correct,
                        "confirmed_decisions": confirmed_decisions,
                        "confirmed_correct": confirmed_correct,
                        "excluded_decisions": excluded_decisions,
                        "excluded_correct": excluded_correct,
                        "official_starter_rows": 40,
                        "official_starter_rows_covered": confirmed_correct,
                        "false_confirmations": confirmed_decisions - confirmed_correct,
                        "false_exclusions": excluded_decisions - excluded_correct,
                    },
                    "unresolved_exception_count": 1,
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_five_accurate_weekends_pass_evidence_gate_but_do_not_enable_automation(self) -> None:
        reports = tuple(self._report(index) for index in range(5))
        evaluation = evaluate_backtests(
            report_paths=reports,
            policy_path=POLICY,
            output_path=self.output,
        )

        self.assertEqual(EVALUATION_SCHEMA, evaluation["schema_version"])
        self.assertTrue(evaluation["evidence_gate_passed"])
        self.assertFalse(evaluation["automatic_promotion_enabled"])
        self.assertFalse(evaluation["automatic_promotion_active"])
        self.assertEqual(
            "eligible_for_operator_approval",
            evaluation["recommendation"],
        )
        self.assertEqual(150, evaluation["metrics"]["decision_count"])
        self.assertEqual(0.95, evaluation["metrics"]["confirmed_precision"])
        self.assertEqual(1.0, evaluation["metrics"]["excluded_accuracy"])
        self.assertEqual({"status": "disabled"}, evaluation["publication"])

    def test_accuracy_below_policy_keeps_gate_closed(self) -> None:
        reports = tuple(
            self._report(index, confirmed_correct=18 if index == 0 else 19)
            for index in range(5)
        )
        evaluation = evaluate_backtests(
            report_paths=reports,
            policy_path=POLICY,
            output_path=self.output,
        )
        self.assertFalse(evaluation["evidence_gate_passed"])
        self.assertFalse(
            evaluation["threshold_checks"]["minimum_confirmed_precision"]
        )
        self.assertEqual("insufficient_evidence", evaluation["recommendation"])

    def test_duplicate_weekend_is_rejected(self) -> None:
        same_date = date(2026, 11, 27)
        reports = (
            self._report(1, logical_date=same_date),
            self._report(2, logical_date=same_date),
        )
        with self.assertRaisesRegex(FantasyXCEvaluationError, "duplicate"):
            evaluate_backtests(
                report_paths=reports,
                policy_path=POLICY,
                output_path=self.output,
            )

    def test_mixed_resolver_versions_are_rejected(self) -> None:
        reports = (
            self._report(1),
            self._report(2, resolver_version="older-resolver"),
        )
        with self.assertRaisesRegex(FantasyXCEvaluationError, "mixed resolver"):
            evaluate_backtests(
                report_paths=reports,
                policy_path=POLICY,
                output_path=self.output,
            )


if __name__ == "__main__":
    unittest.main()
