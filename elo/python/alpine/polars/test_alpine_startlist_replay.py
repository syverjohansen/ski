from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from alpine_startlist_replay import (
    RESULT_ONLY_FIELDS,
    build_replay,
    build_replay_atomic,
    filter_history_before,
    merge_startlists,
    parse_archived_results,
)

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "tests" / "fixtures"


def history_rows() -> list[dict]:
    athletes = [
        ("4194", "Emma Aicher", "GER", 1500),
        ("5268", "Lara Colturi", "ALB", 1490),
        ("1345", "Katharina Truppe", "AUT", 1480),
    ]
    rows = []
    for athlete_id, skier, nation, elo in athletes:
        rows.extend(
            [
                {
                    "ID": athlete_id,
                    "Skier": skier,
                    "Nation": nation,
                    "Date": "2026-11-13",
                    "Season": 2027,
                    "Elo": elo,
                    "Slalom_Elo": elo + 10,
                    "Tech_Elo": elo + 5,
                },
                {
                    "ID": athlete_id,
                    "Skier": skier,
                    "Nation": nation,
                    "Date": "2026-11-14",
                    "Season": 2027,
                    "Elo": 9999,
                    "Slalom_Elo": 9999,
                    "Tech_Elo": 9999,
                },
            ]
        )
    return rows


def write_inputs(root: Path, *, race_id: str = "131579") -> tuple[Path, Path, Path]:
    races = pd.DataFrame(
        [
            {
                "Date": "11/14/2026",
                "Startlist": (
                    "https://www.fis-ski.com/DB/general/results.html"
                    f"?sectorcode=AL&raceid={race_id}"
                ),
                "Sex": "L",
                "City": "Levi",
                "Country": "Finland",
                "Distance": "Slalom",
                "Period": "1",
                "Championship": "0",
                "Race": "2",
            }
        ]
    )
    races_path = root / "races.csv"
    men_path = root / "men.csv"
    ladies_path = root / "ladies.csv"
    races.to_csv(races_path, index=False)
    pd.DataFrame(history_rows()).to_csv(ladies_path, index=False)
    pd.DataFrame(history_rows()).to_csv(men_path, index=False)
    return races_path, men_path, ladies_path


class AlpineStartlistReplayTests(unittest.TestCase):
    def test_archived_results_are_split_from_prediction_participants(self) -> None:
        html = (FIXTURES / "131579.html").read_text(encoding="utf-8")
        participants, results = parse_archived_results(html)

        self.assertEqual(3, len(participants))
        self.assertEqual(3, len({row["ID"] for row in participants}))
        self.assertEqual("237841", participants[0]["ID"])
        self.assertTrue(
            all(field not in participants[0] for field in RESULT_ONLY_FIELDS)
        )
        self.assertEqual("1", results[0]["Rank"])
        self.assertEqual("1:52.62", results[0]["Time"])

    def test_history_cutoff_excludes_target_result_and_future_rows(self) -> None:
        history = pd.DataFrame(
            history_rows()
            + [
                {
                    **history_rows()[0],
                    "Date": "2026-11-15",
                    "Elo": 10000,
                }
            ]
        )
        filtered = filter_history_before(history, "11/14/2026")

        self.assertEqual({"2026-11-13"}, set(filtered["Date"]))
        self.assertNotIn(9999, set(filtered["Elo"]))
        self.assertNotIn(10000, set(filtered["Elo"]))

    def test_fixture_replay_writes_leak_free_prediction_inputs(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-replay-") as text:
            root = Path(text)
            races, men, ladies = write_inputs(root)
            output = root / "output"

            report = build_replay(
                logical_date="11/14/2026",
                races_path=races,
                men_history_path=men,
                ladies_history_path=ladies,
                output_dir=output,
                fixture_dir=FIXTURES,
            )

            startlist = pd.read_csv(output / "startlist_races_ladies.csv")
            filtered_history = pd.read_csv(output / "ladies_chrono.csv")
            actual = json.loads(
                (output / "reports" / "actual-results.json").read_text()
            )
            self.assertEqual("passed", report["status"])
            self.assertEqual("disabled", report["publication"]["status"])
            self.assertEqual("historical_result_proxy", report["races"][0]["source"])
            self.assertEqual({1345, 4194, 5268}, set(startlist["ID"]))
            self.assertEqual({True}, set(startlist["HistoryMatched"]))
            self.assertEqual(1.0, report["races"][0]["history_match_rate"])
            self.assertEqual({1.0}, set(startlist["Race1_Prob"]))
            self.assertFalse(set(RESULT_ONLY_FIELDS) & set(startlist.columns))
            self.assertEqual({"2026-11-13"}, set(filtered_history["Date"]))
            self.assertEqual("1", actual[0]["Rank"])

    def test_missing_archive_builds_nonempty_generated_field(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-fallback-") as text:
            root = Path(text)
            races, men, ladies = write_inputs(root, race_id="999999")
            output = root / "output"

            report = build_replay(
                logical_date="2026-11-14",
                races_path=races,
                men_history_path=men,
                ladies_history_path=ladies,
                output_dir=output,
                fixture_dir=FIXTURES,
                allow_network=False,
            )

            startlist = pd.read_csv(output / "startlist_races_ladies.csv")
            self.assertEqual("generated_pre_race_history", report["races"][0]["source"])
            self.assertEqual(3, len(startlist))
            self.assertEqual({1.0}, set(startlist["Race1_Prob"]))
            self.assertEqual({True}, set(startlist["In_Startlist"]))

    def test_replay_rejects_a_field_with_no_history_overlap(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-unmatched-") as text:
            root = Path(text)
            races, men, ladies = write_inputs(root)
            history = pd.read_csv(ladies)
            history["Skier"] = [f"Unknown {index}" for index in range(len(history))]
            history.to_csv(ladies, index=False)

            with self.assertRaisesRegex(RuntimeError, "history match rate 0.0%"):
                build_replay(
                    logical_date="11/14/2026",
                    races_path=races,
                    men_history_path=men,
                    ladies_history_path=ladies,
                    output_dir=root / "output",
                    fixture_dir=FIXTURES,
                )

    def test_reviewed_alias_is_preferred_over_name_matching(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-alias-") as text:
            root = Path(text)
            races, men, ladies = write_inputs(root)
            aliases = root / "aliases.csv"
            aliases.write_text(
                "sex,fis_competitor_id,fis_code,fis_name,canonical_id,canonical_name\n"
                "L,237841,507168,AICHER Emma,4194,Emma Aicher\n",
                encoding="utf-8",
            )

            report = build_replay(
                logical_date="11/14/2026",
                races_path=races,
                men_history_path=men,
                ladies_history_path=ladies,
                identity_aliases_path=aliases,
                output_dir=root / "output",
                fixture_dir=FIXTURES,
            )

            self.assertEqual(
                1,
                report["races"][0]["identity_match_methods"]["reviewed_alias"],
            )

    def test_multiple_races_preserve_new_athlete_data_and_probability_masks(
        self,
    ) -> None:
        first = pd.DataFrame(
            [
                {
                    "ID": "1",
                    "Skier": "First Athlete",
                    "Race1_Prob": 1.0,
                }
            ]
        )
        second = pd.DataFrame(
            [
                {
                    "ID": "2",
                    "Skier": "Second Athlete",
                    "Race2_Prob": 1.0,
                }
            ]
        )

        merged = merge_startlists(first, second, "Race2_Prob")

        by_id = merged.set_index("ID")
        self.assertEqual("Second Athlete", by_id.loc["2", "Skier"])
        self.assertEqual(0.0, by_id.loc["1", "Race2_Prob"])
        self.assertEqual(0.0, by_id.loc["2", "Race1_Prob"])

    def test_replay_rejects_dates_without_races(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-no-race-") as text:
            root = Path(text)
            races, men, ladies = write_inputs(root)
            with self.assertRaisesRegex(RuntimeError, "no Alpine races"):
                build_replay(
                    logical_date="11/13/2026",
                    races_path=races,
                    men_history_path=men,
                    ladies_history_path=ladies,
                    output_dir=root / "output",
                    fixture_dir=FIXTURES,
                )

    def test_atomic_replay_cleans_staging_after_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-atomic-failure-") as text:
            root = Path(text)
            races, men, ladies = write_inputs(root)
            output = root.parent / f"{root.name}-output"
            with self.assertRaisesRegex(RuntimeError, "no Alpine races"):
                build_replay_atomic(
                    logical_date="11/13/2026",
                    races_path=races,
                    men_history_path=men,
                    ladies_history_path=ladies,
                    output_dir=output,
                    fixture_dir=FIXTURES,
                )
            self.assertFalse(output.exists())
            self.assertEqual([], list(output.parent.glob(f".{output.name}.staging.*")))

    def test_shadow_output_cannot_pollute_source_directory(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-isolation-") as text:
            root = Path(text)
            races, men, ladies = write_inputs(root)
            with self.assertRaisesRegex(RuntimeError, "cannot be inside source"):
                build_replay_atomic(
                    logical_date="11/14/2026",
                    races_path=races,
                    men_history_path=men,
                    ladies_history_path=ladies,
                    output_dir=root / "shadow-output",
                    fixture_dir=FIXTURES,
                )

    def test_live_scraper_shadow_subcommand_preserves_sources(self) -> None:
        spec = importlib.util.spec_from_file_location(
            "alpine_startlist_scraper",
            ROOT / "startlist-scrape-races.py",
        )
        assert spec and spec.loader
        scraper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(scraper)

        with tempfile.TemporaryDirectory(prefix="alpine-live-shadow-") as text:
            root = Path(text)
            source = root / "source"
            source.mkdir()
            races, men, ladies = write_inputs(source)
            aliases = source / "fis_identity_aliases.csv"
            aliases.write_text(
                "sex,fis_competitor_id,fis_code,fis_name,canonical_id,canonical_name\n",
                encoding="utf-8",
            )
            before = {path: path.read_bytes() for path in (races, men, ladies)}
            output = root / "runs" / "replay"

            exit_code = scraper.main(
                [
                    "shadow",
                    "--logical-date",
                    "11/14/2026",
                    "--races",
                    str(races),
                    "--men-history",
                    str(men),
                    "--ladies-history",
                    str(ladies),
                    "--identity-aliases",
                    str(aliases),
                    "--output-dir",
                    str(output),
                    "--fixture-dir",
                    str(FIXTURES),
                ]
            )

            self.assertEqual(0, exit_code)
            self.assertTrue((output / "startlist_races_ladies.csv").is_file())
            self.assertTrue((output / "reports" / "replay.json").is_file())
            self.assertEqual(
                before,
                {path: path.read_bytes() for path in (races, men, ladies)},
            )


if __name__ == "__main__":
    unittest.main()
