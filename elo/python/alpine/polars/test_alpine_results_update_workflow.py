import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
WORKFLOW = ROOT / ".github/workflows/alpine-results-update.yml"


class AlpineResultsUpdateWorkflowTest(unittest.TestCase):
    def test_workflow_is_manual_recovery_and_publishes_only_after_validation(self) -> None:
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", source)
        trigger = source.split("on:\n", 1)[1].split("\npermissions:", 1)[0]
        self.assertNotIn("schedule:", trigger)
        self.assertIn("contents: write", source)
        self.assertIn("python -m unittest discover", source)
        self.assertIn("git push origin HEAD:main", source)
        self.assertNotIn("--force", source)
        self.assertNotIn("gh pr create", source)

    def test_workflow_runs_fail_closed_pipeline_and_retains_evidence(self) -> None:
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("python alpine_results_update.py", source)
        self.assertIn("--allow-network", source)
        self.assertIn("python -m unittest discover", source)
        self.assertIn("actions/upload-artifact@v4", source)
        self.assertIn("if: always()", source)
        self.assertIn("promotion/report.json", source)


if __name__ == "__main__":
    unittest.main()
