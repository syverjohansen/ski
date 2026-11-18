import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS
from cross_country_team_results_elo_replay import (
    TEAM_OUTPUT_FIELDS,
    TeamEloReplayError,
    replay_team_results,
    team_stream_includes,
)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryTeamResultsEloReplayTests(unittest.TestCase):
    def build_sources(
        self, root: Path, *, distance: str = "Ts", technique: str = "F"
    ) -> tuple[Path, Path, Path]:
        sources = root / "sources"
        sources.mkdir()
        mapped = sources / "mapped.csv"
        men = sources / "men.csv"
        ladies = sources / "ladies.csv"
        metadata_fields = [
            "Date", "City", "Country", "Sex", "Distance", "Event", "MS",
            "Technique", "Place", "Skier", "Nation", "ID", "Season", "Race",
            "Birthday", "Age", "Exp",
        ]
        history_fields = list(STREAMS) + metadata_fields + list(PREVIOUS_COLUMNS.values())
        member_count = 2 if distance == "Ts" else 4
        history_rows = []
        mapped_rows = []
        athlete_id = 1
        for place, (team_name, nation, nation_code) in enumerate(
            (("NORWAY", "Norway", "NOR"), ("SWEDEN", "Sweden", "SWE"), ("FINLAND", "Finland", "FIN")),
            start=1,
        ):
            for member_order in range(1, member_count + 1):
                name = f"Athlete {athlete_id}"
                history = {
                    "Date": "2026-03-01", "City": "Prior", "Country": "Norway",
                    "Sex": "M", "Distance": "10", "Event": "World Cup", "MS": 0,
                    "Technique": "C", "Place": athlete_id, "Skier": name,
                    "Nation": nation, "ID": athlete_id, "Season": 2026, "Race": 1,
                    "Birthday": "2000-01-01T00:00:00", "Age": 26, "Exp": 5,
                }
                history.update({column: 1300 for column in (*STREAMS, *PREVIOUS_COLUMNS.values())})
                history_rows.append(history)
                mapped_rows.append(
                    {
                        "date": "2026-11-27", "sex": "M", "season": 2027, "race": 2,
                        "city": "Ruka", "country": "Finland", "distance": distance,
                        "technique": technique, "championship": 0, "place": place,
                        "team_name": team_name, "nation": nation_code, "team_bib": place,
                        "member_order": member_order, "fis_competitor_id": 1000 + athlete_id,
                        "fis_code": 3000000 + athlete_id, "name": name,
                        "canonical_athlete_id": athlete_id, "canonical_name": name,
                        "canonical_nation": nation,
                    }
                )
                athlete_id += 1
        write_csv(men, history_fields, history_rows)
        write_csv(ladies, history_fields, [])
        write_csv(mapped, list(mapped_rows[0]), mapped_rows)
        return mapped, men, ladies

    def test_team_stream_rules_match_legacy_filters(self) -> None:
        relay = {stream for stream in STREAMS if team_stream_includes(stream, "Rel", "N/A")}
        self.assertEqual(relay, {"Elo", "Distance_Elo", "Distance_C_Elo", "Classic_Elo"})
        classic_sprint = {stream for stream in STREAMS if team_stream_includes(stream, "Ts", "C")}
        self.assertEqual(classic_sprint, {"Elo", "Sprint_Elo", "Sprint_C_Elo", "Classic_Elo"})
        freestyle_sprint = {stream for stream in STREAMS if team_stream_includes(stream, "Ts", "F")}
        self.assertEqual(freestyle_sprint, {"Elo", "Sprint_Elo", "Sprint_F_Elo", "Freestyle_Elo"})

    def test_team_sprint_replay_scales_k_and_aggregates_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mapped, men, ladies = self.build_sources(root)
            original = {path: path.read_bytes() for path in (men, ladies)}
            report = replay_team_results(
                mapped_results=mapped,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
            )
            self.assertEqual(report["athlete_rows"], {"M": 6, "L": 0})
            self.assertEqual(report["team_rows"]["team_sprint_elo_rows.csv"], 3)
            self.assertFalse(report["source_histories_modified"])
            with (root / "output" / "men_athlete_elo_rows.csv").open(newline="", encoding="utf-8") as handle:
                athletes = list(csv.DictReader(handle))
            # Six equal athletes, two per place: top/bottom move +/-1 at K/2.
            self.assertAlmostEqual(float(athletes[0]["Pelo"]), 1300.0)
            self.assertAlmostEqual(float(athletes[0]["Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Sprint_Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Sprint_F_Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Freestyle_Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Distance_Elo"]), 1300.0)
            with (root / "output" / "team_sprint_elo_rows.csv").open(newline="", encoding="utf-8") as handle:
                teams = list(csv.DictReader(handle))
            self.assertEqual(list(teams[0]), list(TEAM_OUTPUT_FIELDS))
            self.assertEqual(teams[0]["Nation"], "Norway")
            self.assertEqual(teams[0]["MemberIDs"], "1,2")
            self.assertEqual(teams[0]["TeamMembers"], "Athlete 1,Athlete 2")
            self.assertAlmostEqual(float(teams[0]["Team_Elo"]), 2602.0)
            self.assertAlmostEqual(float(teams[0]["Team_Pelo"]), 2600.0)
            self.assertAlmostEqual(float(teams[0]["Avg_Elo"]), 1301.0)
            self.assertEqual(original, {path: path.read_bytes() for path in original})

    def test_relay_replay_uses_quarter_k(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mapped, men, ladies = self.build_sources(root, distance="Rel", technique="N/A")
            replay_team_results(
                mapped_results=mapped,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
            )
            with (root / "output" / "men_athlete_elo_rows.csv").open(newline="", encoding="utf-8") as handle:
                athletes = list(csv.DictReader(handle))
            # Twelve equal athletes, four per place: top/bottom move +/-1 at K/4.
            self.assertAlmostEqual(float(athletes[0]["Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Distance_Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Distance_C_Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Classic_Elo"]), 1301.0)
            self.assertAlmostEqual(float(athletes[0]["Sprint_Elo"]), 1300.0)
            with (root / "output" / "men_relay_elo_rows.csv").open(newline="", encoding="utf-8") as handle:
                teams = list(csv.DictReader(handle))
            self.assertEqual(len(teams), 3)
            self.assertAlmostEqual(float(teams[0]["Team_Elo"]), 5204.0)

    def test_rejects_team_race_already_in_accepted_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mapped, men, ladies = self.build_sources(root)
            with men.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields, rows = list(reader.fieldnames or []), list(reader)
            rows[0].update({"Season": 2027, "Race": 2, "Distance": "Ts"})
            write_csv(men, fields, rows)
            with self.assertRaisesRegex(TeamEloReplayError, "already exists"):
                replay_team_results(
                    mapped_results=mapped,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=root / "output",
                )


if __name__ == "__main__":
    unittest.main()
