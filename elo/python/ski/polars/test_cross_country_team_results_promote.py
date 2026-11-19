import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS
from cross_country_team_results_elo_replay import (
    TEAM_OUTPUT_FIELDS,
    replay_team_results,
)
from cross_country_team_results_promote import TeamPromotionError, promote_team_results


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryTeamResultsPromoteTests(unittest.TestCase):
    def build_sources(self, root: Path) -> dict[str, Path]:
        sources = root / "sources"
        sources.mkdir()
        paths = {
            "mapped": sources / "mapped.csv",
            "men": sources / "men.csv",
            "ladies": sources / "ladies.csv",
            "men_relay": sources / "men_relay.csv",
            "ladies_relay": sources / "ladies_relay.csv",
            "team_sprint": sources / "team_sprint.csv",
        }
        metadata = [
            "Date", "City", "Country", "Sex", "Distance", "Event", "MS",
            "Technique", "Place", "Skier", "Nation", "ID", "Season", "Race",
            "Birthday", "Age", "Exp",
        ]
        history_fields = list(STREAMS) + metadata + list(PREVIOUS_COLUMNS.values())
        histories = []
        mapped = []
        athlete_id = 1
        for place, (team, nation, code) in enumerate(
            (("NORWAY", "Norway", "NOR"), ("SWEDEN", "Sweden", "SWE"), ("FINLAND", "Finland", "FIN")),
            start=1,
        ):
            for order in (1, 2):
                name = f"Athlete {athlete_id}"
                history = {
                    "Date": "2026-03-01", "City": "Prior", "Country": "Norway",
                    "Sex": "M", "Distance": "10", "Event": "World Cup", "MS": 0,
                    "Technique": "C", "Place": athlete_id, "Skier": name,
                    "Nation": nation, "ID": athlete_id, "Season": 2026, "Race": 1,
                    "Birthday": "2000-01-01T00:00:00", "Age": 26, "Exp": 5,
                }
                history.update({column: 1300 for column in (*STREAMS, *PREVIOUS_COLUMNS.values())})
                histories.append(history)
                mapped.append(
                    {
                        "date": "2026-11-27", "sex": "M", "season": 2027, "race": 2,
                        "city": "Ruka", "country": "Finland", "distance": "Ts",
                        "technique": "F", "championship": 0, "place": place,
                        "team_name": team, "nation": code, "team_bib": place,
                        "member_order": order, "fis_competitor_id": 1000 + athlete_id,
                        "fis_code": 3000000 + athlete_id, "name": name,
                        "canonical_athlete_id": athlete_id, "canonical_name": name,
                        "canonical_nation": nation,
                    }
                )
                athlete_id += 1
        write_csv(paths["men"], history_fields, histories)
        write_csv(paths["ladies"], history_fields, [])
        write_csv(paths["mapped"], list(mapped[0]), mapped)
        for key in ("men_relay", "ladies_relay", "team_sprint"):
            write_csv(paths[key], list(TEAM_OUTPUT_FIELDS), [])
        return paths

    def replay(self, root: Path, paths: dict[str, Path]) -> Path:
        replay_dir = root / "replay"
        replay_team_results(
            mapped_results=paths["mapped"],
            men_chrono=paths["men"],
            ladies_chrono=paths["ladies"],
            output_dir=replay_dir,
        )
        return replay_dir

    def promote(
        self, root: Path, paths: dict[str, Path], replay_dir: Path
    ) -> dict[str, object]:
        return promote_team_results(
            men_chrono=paths["men"],
            ladies_chrono=paths["ladies"],
            men_relay_chrono=paths["men_relay"],
            ladies_relay_chrono=paths["ladies_relay"],
            team_sprint_chrono=paths["team_sprint"],
            replay_dir=replay_dir,
            expected_season=2027,
            output_dir=root / "candidate",
        )

    def test_builds_atomic_five_history_candidate_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self.build_sources(root)
            originals = {key: path.read_bytes() for key, path in paths.items() if key != "mapped"}
            report = self.promote(root, paths, self.replay(root, paths))
            self.assertTrue(report["atomic_candidate_set"])
            self.assertFalse(report["accepted_files_modified"])
            self.assertEqual(len(report["outputs"]), 5)
            candidate = root / "candidate"
            self.assertEqual(
                {path.name for path in candidate.iterdir()},
                {
                    "men_chrono.csv", "ladies_chrono.csv", "men_relay_chrono.csv",
                    "ladies_relay_chrono.csv", "team_sprint_chrono.csv", "report.json",
                },
            )
            for key, before in originals.items():
                self.assertEqual(paths[key].read_bytes(), before)
            with (candidate / "team_sprint_chrono.csv").open(newline="", encoding="utf-8") as handle:
                teams = list(csv.DictReader(handle))
            self.assertEqual(len(teams), 3)
            self.assertEqual(teams[0]["MemberIDs"], "1,2")

    def test_rejects_stale_replay_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self.build_sources(root)
            replay_dir = self.replay(root, paths)
            paths["men"].write_bytes(paths["men"].read_bytes() + b"\n")
            with self.assertRaisesRegex(TeamPromotionError, "changed after team replay"):
                self.promote(root, paths, replay_dir)
            self.assertFalse((root / "candidate").exists())

    def test_rejects_corrupt_team_aggregate_even_with_updated_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self.build_sources(root)
            replay_dir = self.replay(root, paths)
            team_file = replay_dir / "team_sprint_elo_rows.csv"
            with team_file.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields, rows = list(reader.fieldnames or []), list(reader)
            rows[0]["Team_Elo"] = float(rows[0]["Team_Elo"]) + 10
            write_csv(team_file, fields, rows)
            report_path = replay_dir / "report.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["outputs"][team_file.name]["sha256"] = hashlib.sha256(team_file.read_bytes()).hexdigest()
            report_path.write_text(json.dumps(report), encoding="utf-8")
            originals = {key: path.read_bytes() for key, path in paths.items() if key != "mapped"}
            with self.assertRaisesRegex(TeamPromotionError, "does not equal member sum"):
                self.promote(root, paths, replay_dir)
            self.assertFalse((root / "candidate").exists())
            self.assertEqual(originals, {key: paths[key].read_bytes() for key in originals})

    def test_out_of_order_team_row_rolls_back_partial_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self.build_sources(root)
            replay_dir = self.replay(root, paths)
            with (replay_dir / "team_sprint_elo_rows.csv").open(newline="", encoding="utf-8") as handle:
                incoming = next(csv.DictReader(handle))
            accepted = dict(incoming)
            accepted.update({"Date": "2026-12-01", "Race": 3})
            write_csv(paths["team_sprint"], list(TEAM_OUTPUT_FIELDS), [accepted])
            originals = {key: path.read_bytes() for key, path in paths.items() if key != "mapped"}
            with self.assertRaisesRegex(TeamPromotionError, "not newer"):
                self.promote(root, paths, replay_dir)
            self.assertFalse((root / "candidate").exists())
            self.assertEqual(originals, {key: paths[key].read_bytes() for key in originals})


if __name__ == "__main__":
    unittest.main()
