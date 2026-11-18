import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

RELAY_DIR = Path(__file__).resolve().parent
if str(RELAY_DIR) not in sys.path:
    sys.path.insert(0, str(RELAY_DIR))

import startlist_scrape_races_relay as relay_builder
import startlist_scrape_races_team_sprint as team_sprint_builder


class TeamStartlistEnforcementTests(unittest.TestCase):
    def test_combined_relay_batch_writes_each_gender_separately(self) -> None:
        races = pd.DataFrame(
            [
                {
                    "Startlist": "https://example/men",
                    "City": "Falun",
                    "Date": "03/04/2027",
                    "Sex": "M",
                },
                {
                    "Startlist": "https://example/ladies",
                    "City": "Falun",
                    "Date": "03/05/2027",
                    "Sex": "L",
                },
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            races_file = Path(directory) / "races.csv"
            races.to_csv(races_file, index=False)
            team_paths = {
                gender: Path(directory) / f"{gender}-teams.csv"
                for gender in ("men", "ladies")
            }
            individual_paths = {
                gender: Path(directory) / f"{gender}-individuals.csv"
                for gender in ("men", "ladies")
            }

            with (
                mock.patch.object(
                    relay_builder, "get_relay_teams", return_value=[{"team": 1}]
                ),
                mock.patch.object(
                    relay_builder,
                    "process_relay_teams",
                    side_effect=lambda teams, race, gender: (
                        [{"gender": gender}],
                        [{"gender": gender}],
                    ),
                ),
                mock.patch.object(
                    relay_builder,
                    "save_relay_team_data",
                    side_effect=lambda frame, gender: team_paths[gender],
                ) as save_teams,
                mock.patch.object(
                    relay_builder,
                    "save_relay_individual_data",
                    side_effect=lambda frame, gender: individual_paths[gender],
                ) as save_individuals,
                mock.patch.object(
                    relay_builder, "validate_team_startlists"
                ) as validate,
            ):
                generated = relay_builder.process_relay_races(str(races_file))

            self.assertEqual(generated, ("men", "ladies"))
            self.assertEqual(
                [call.args[1] for call in save_teams.call_args_list],
                ["men", "ladies"],
            )
            self.assertEqual(
                [call.args[1] for call in save_individuals.call_args_list],
                ["men", "ladies"],
            )
            self.assertEqual(validate.call_count, 2)

    def test_team_sprint_contract_runs_after_both_files_are_saved(self) -> None:
        races = pd.DataFrame(
            [
                {
                    "Startlist": "https://example/men",
                    "City": "Davos",
                    "Date": "12/11/2026",
                    "Sex": "M",
                }
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            teams_path = Path(directory) / "teams.csv"
            individuals_path = Path(directory) / "individuals.csv"
            with (
                mock.patch.object(
                    team_sprint_builder,
                    "get_team_sprint_teams",
                    return_value=[{"team": 1}],
                ),
                mock.patch.object(
                    team_sprint_builder,
                    "process_team_sprint_teams",
                    return_value=([{"team": 1}], [{"athlete": 1}]),
                ),
                mock.patch.object(
                    team_sprint_builder,
                    "save_team_sprint_team_data",
                    return_value=teams_path,
                ),
                mock.patch.object(
                    team_sprint_builder,
                    "save_team_sprint_individual_data",
                    return_value=individuals_path,
                ),
                mock.patch.object(
                    team_sprint_builder, "validate_team_startlists"
                ) as validate,
            ):
                generated = team_sprint_builder.process_gender_team_sprint_races(
                    races, "men"
                )

            self.assertTrue(generated)
            validate.assert_called_once_with(
                teams=teams_path,
                individuals=individuals_path,
                event_type="team_sprint",
            )


if __name__ == "__main__":
    unittest.main()
