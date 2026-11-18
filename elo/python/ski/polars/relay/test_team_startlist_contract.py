import csv
import tempfile
import unittest
from pathlib import Path

from relay.team_startlist_contract import (
    TeamStartlistContractError,
    validate_team_startlists,
)

HERE = Path(__file__).resolve().parent


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class TeamStartlistContractTests(unittest.TestCase):
    def test_current_historical_team_startlists_pass(self) -> None:
        data = HERE / "excel365"
        expected = {
            "relay": {"men": (10, 40), "ladies": (19, 76)},
            "team_sprint": {"men": (23, 46), "ladies": (25, 50)},
        }
        for event_type, genders in expected.items():
            for gender, counts in genders.items():
                with self.subTest(event_type=event_type, gender=gender):
                    report = validate_team_startlists(
                        teams=data
                        / f"startlist_{event_type}_races_teams_{gender}.csv",
                        individuals=data
                        / f"startlist_{event_type}_races_individuals_{gender}.csv",
                        event_type=event_type,
                    )
                    self.assertEqual(
                        (report.team_rows, report.individual_rows), counts
                    )

    def test_incomplete_team_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            teams = root / "teams.csv"
            individuals = root / "individuals.csv"
            write_csv(
                teams,
                [
                    "Team_Name",
                    "Nation",
                    "Race_Date",
                    "Is_Present",
                    "Race_Type",
                    "Elo",
                    "Member_1",
                    "Member_1_ID",
                    "Member_1_Elo",
                    "Member_2",
                    "Member_2_ID",
                    "Member_2_Elo",
                ],
                [
                    {
                        "Team_Name": "NORWAY I",
                        "Nation": "NORWAY",
                        "Race_Date": "12/11/2026",
                        "Is_Present": True,
                        "Race_Type": "Team Sprint",
                        "Elo": 1700,
                        "Member_1": "One Athlete",
                        "Member_1_ID": 1,
                        "Member_1_Elo": 1700,
                    }
                ],
            )
            write_csv(
                individuals,
                [
                    "Skier",
                    "ID",
                    "Team_Name",
                    "Race_Date",
                    "In_FIS_List",
                    "Elo",
                ],
                [
                    {
                        "Skier": "One Athlete",
                        "ID": 1,
                        "Team_Name": "NORWAY I",
                        "Race_Date": "12/11/2026",
                        "In_FIS_List": True,
                        "Elo": 1700,
                    }
                ],
            )

            with self.assertRaisesRegex(
                TeamStartlistContractError, "missing required Member_2"
            ):
                validate_team_startlists(
                    teams=teams,
                    individuals=individuals,
                    event_type="team_sprint",
                )

    def test_team_rating_must_equal_member_sum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            teams = root / "teams.csv"
            individuals = root / "individuals.csv"
            team_fields = [
                "Team_Name",
                "Nation",
                "Race_Date",
                "Is_Present",
                "Race_Type",
                "Elo",
                "Member_1",
                "Member_1_ID",
                "Member_1_Elo",
                "Member_2",
                "Member_2_ID",
                "Member_2_Elo",
            ]
            individual_fields = [
                "Skier",
                "ID",
                "Team_Name",
                "Race_Date",
                "In_FIS_List",
                "Elo",
            ]
            write_csv(
                teams,
                team_fields,
                [
                    {
                        "Team_Name": "NORWAY I",
                        "Nation": "NORWAY",
                        "Race_Date": "12/11/2026",
                        "Is_Present": True,
                        "Race_Type": "Team Sprint",
                        "Elo": 3000,
                        "Member_1": "One Athlete",
                        "Member_1_ID": 1,
                        "Member_1_Elo": 1600,
                        "Member_2": "Two Athlete",
                        "Member_2_ID": 2,
                        "Member_2_Elo": 1500,
                    }
                ],
            )
            write_csv(
                individuals,
                individual_fields,
                [
                    {
                        "Skier": name,
                        "ID": athlete_id,
                        "Team_Name": "NORWAY I",
                        "Race_Date": "12/11/2026",
                        "In_FIS_List": True,
                        "Elo": elo,
                    }
                    for name, athlete_id, elo in (
                        ("One Athlete", 1, 1600),
                        ("Two Athlete", 2, 1500),
                    )
                ],
            )

            with self.assertRaisesRegex(TeamStartlistContractError, "Elo does not"):
                validate_team_startlists(
                    teams=teams,
                    individuals=individuals,
                    event_type="team_sprint",
                )


if __name__ == "__main__":
    unittest.main()
