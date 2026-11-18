import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay.prediction_runner import PredictionRunnerError, run_prediction
from relay.team_event_dispatch import run_startlist_builder


class TeamPredictionRunnerTests(unittest.TestCase):
    def make_blog(self, root: Path, script: str) -> Path:
        path = root / "content" / "post" / "cross-country" / "drafts" / script
        path.parent.mkdir(parents=True)
        path.write_text("# fixture\n", encoding="utf-8")
        return path.resolve()

    def test_prediction_uses_explicit_blog_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = self.make_blog(root, "race-picks-relay.R")
            runner = mock.Mock(
                return_value=subprocess.CompletedProcess([], 0, "ok", "")
            )

            run_prediction(
                "races",
                "relay",
                blog_root=root,
                rscript_bin="fixture-Rscript",
                runner=runner,
            )

            runner.assert_called_once_with(
                ["fixture-Rscript", str(script)],
                check=True,
                capture_output=True,
                text=True,
            )

    def test_prediction_requires_configured_blog_checkout(self) -> None:
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            self.assertRaisesRegex(PredictionRunnerError, "NORDIC_BLOG_ROOT"),
        ):
            run_prediction("races", "team_sprint")

    def test_prediction_failure_is_not_swallowed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_blog(root, "race-picks-team-sprint.R")
            runner = mock.Mock(
                side_effect=subprocess.CalledProcessError(4, ["Rscript"])
            )
            with self.assertRaises(subprocess.CalledProcessError):
                run_prediction(
                    "races", "team_sprint", blog_root=root, runner=runner
                )

    def test_dispatch_uses_packaged_absolute_script_and_python(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            races = Path(directory) / "races.csv"
            races.write_text("Date,Distance\n", encoding="utf-8")
            runner = mock.Mock(
                return_value=subprocess.CompletedProcess([], 0, "ok", "")
            )

            run_startlist_builder("relay", races, runner=runner)

            command = runner.call_args.args[0]
            self.assertEqual(command[0], os.sys.executable)
            self.assertTrue(Path(command[1]).is_absolute())
            self.assertEqual(Path(command[1]).name, "startlist_scrape_races_relay.py")
            self.assertEqual(Path(command[2]), races.resolve())
            self.assertTrue(runner.call_args.kwargs["check"])


if __name__ == "__main__":
    unittest.main()
