import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay.team_artifacts import (
    chrono_prediction_path,
    schedule_path,
    startlist_paths,
)


class TeamArtifactPathTests(unittest.TestCase):
    def test_paths_use_configured_roots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ski_data = root / "ski-data"
            team_data = root / "team-data"
            with mock.patch.dict(
                os.environ,
                {
                    "NORDIC_SKI_DATA_ROOT": str(ski_data),
                    "NORDIC_TEAM_DATA_ROOT": str(team_data),
                },
            ):
                self.assertEqual(schedule_path(), ski_data.resolve() / "races.csv")
                self.assertEqual(
                    chrono_prediction_path("men"),
                    ski_data.resolve() / "men_chrono_pred.csv",
                )
                self.assertEqual(
                    startlist_paths("team_sprint", "ladies"),
                    (
                        team_data.resolve()
                        / "startlist_team_sprint_races_teams_ladies.csv",
                        team_data.resolve()
                        / "startlist_team_sprint_races_individuals_ladies.csv",
                    ),
                )

    def test_invalid_event_or_gender_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "event type"):
            startlist_paths("individual", "men")
        with self.assertRaisesRegex(ValueError, "gender"):
            startlist_paths("relay", "mixed")


if __name__ == "__main__":
    unittest.main()
