import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

POLARS_ROOT = Path(__file__).resolve().parent
PYTHON_ROOT = POLARS_ROOT.parents[1]
CONFIG = PYTHON_ROOT / "pipeline_config.py"
DISPATCHER = POLARS_ROOT / "startlist-scrape-races.py"


class HostedPathContractTests(unittest.TestCase):
    def test_pipeline_config_uses_operator_supplied_checkout_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            elo_root = Path(directory) / "checkout" / "elo"
            environment = os.environ.copy()
            environment["NORDIC_SKI_ELO_ROOT"] = str(elo_root)
            environment["PYTHONPATH"] = str(PYTHON_ROOT)
            command = (
                "from pipeline_config import get_races_file; "
                "print(get_races_file('ski'))"
            )
            result = subprocess.run(
                [sys.executable, "-c", command],
                check=True,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(
                Path(result.stdout.strip()),
                elo_root.resolve() / "python/ski/polars/excel365/races.csv",
            )

    def test_pipeline_config_defaults_to_repository_checkout(self) -> None:
        environment = os.environ.copy()
        environment.pop("NORDIC_SKI_ELO_ROOT", None)
        environment["PYTHONPATH"] = str(PYTHON_ROOT)
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from pipeline_config import SKI_ELO_BASE; print(SKI_ELO_BASE)",
            ],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertEqual(Path(result.stdout.strip()), PYTHON_ROOT.parent.resolve())

    def test_dispatcher_contains_no_checkout_specific_ski_paths(self) -> None:
        source = DISPATCHER.read_text(encoding="utf-8")
        self.assertNotIn("~/ski/elo/python", source)
        self.assertIn('get_excel_dir("ski")', source)
        self.assertIn("Path(__file__).resolve().parents[2]", source)

    def test_python_sources_compile(self) -> None:
        for source in (CONFIG, DISPATCHER):
            result = subprocess.run(
                [sys.executable, "-m", "py_compile", str(source)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
