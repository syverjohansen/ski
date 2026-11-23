import csv
import json
import tempfile
import unittest
from pathlib import Path

from cross_country_flgr_elo_replay import (
    FlgrEloReplayError,
    replay_flgr_elo,
)
from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS, regress_rating


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


HISTORY_FIELDS = [
    "Date",
    "Event",
    "Season",
    "Race",
    "ID",
    "Skier",
    "Exp",
    *STREAMS,
    *PREVIOUS_COLUMNS.values(),
]

IDENTITY_FIELDS = [
    "event_id",
    "date",
    "season",
    "sex",
    "distance",
    "technique",
    "mass_start",
    "athlete_id",
    "source_competitor_id",
    "source_name",
    "birth_year",
    "place",
    "canonical_athlete_id",
    "canonical_name",
    "identity_status",
    "source_sha256",
]


def history_row(**overrides) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "2026-05-01",
        "Event": "Offseason",
        "Season": 2026,
        "Race": 0,
        "ID": "10",
        "Skier": "Ivan Ivanov",
        "Exp": 50,
    }
    for stream in STREAMS:
        row[stream] = 1300
        row[PREVIOUS_COLUMNS[stream]] = 1500
    row.update(overrides)
    return row


def identity_row(**overrides) -> dict[str, object]:
    row: dict[str, object] = {
        "event_id": "flgr:77:88",
        "date": "2026-11-23",
        "season": 2027,
        "sex": "M",
        "distance": "10",
        "technique": "C",
        "mass_start": True,
        "athlete_id": "flgr:101",
        "source_competitor_id": "101",
        "source_name": "ИВАНОВ Иван",
        "birth_year": 1999,
        "place": 2,
        "canonical_athlete_id": "10",
        "canonical_name": "Ivan Ivanov",
        "identity_status": "reviewed_crosswalk",
        "source_sha256": "a" * 64,
    }
    row.update(overrides)
    return row


def write_sources(root: Path, identity_rows=None, history_rows=None):
    sources = root / "sources"
    sources.mkdir()
    identity = sources / "identity.csv"
    men = sources / "men.csv"
    ladies = sources / "ladies.csv"
    write_csv(
        identity,
        IDENTITY_FIELDS,
        identity_rows
        or [
            identity_row(),
            identity_row(
                athlete_id="flgr:202",
                source_competitor_id="202",
                source_name="НОВИКОВ Алексей",
                birth_year=2003,
                place=1,
                canonical_athlete_id="",
                canonical_name="",
                identity_status="flgr_only",
            ),
        ],
    )
    write_csv(men, HISTORY_FIELDS, history_rows or [history_row()])
    write_csv(ladies, HISTORY_FIELDS, [])
    return identity, men, ladies


def read_ledger(root: Path) -> list[dict[str, str]]:
    with (root / "flgr-bridge-ledger.csv").open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class FlgrEloReplayTests(unittest.TestCase):
    def test_reviewed_anchor_and_newcomer_both_update(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity, men, ladies = write_sources(root)
            report = replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                k_factor=5.0,
            )
            rows = {row["athlete_id"]: row for row in read_ledger(root / "output")}
            anchor = rows["flgr:101"]
            newcomer = rows["flgr:202"]

            self.assertAlmostEqual(float(anchor["Pelo"]), regress_rating(1500, 1))
            self.assertLess(float(anchor["Elo"]), float(anchor["Pelo"]))
            self.assertGreater(float(newcomer["Elo"]), 1300)
            self.assertEqual(anchor["anchor_source"], "accepted_fis_history")
            self.assertEqual(newcomer["anchor_source"], "newcomer_1300")
            self.assertEqual(report["reviewed_anchor_athletes"], 1)
            self.assertEqual(report["flgr_only_athletes"], 1)
            self.assertFalse(report["publication_eligible"])
            self.assertEqual(report["calibration_status"], "pending_backtest")
            self.assertFalse(report["official_histories_modified"])

    def test_classic_distance_updates_only_matching_streams(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity, men, ladies = write_sources(root)
            replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                k_factor=5.0,
            )
            anchor = next(
                row
                for row in read_ledger(root / "output")
                if row["athlete_id"] == "flgr:101"
            )
            for stream in ("Elo", "Distance_Elo", "Distance_C_Elo", "Classic_Elo"):
                self.assertNotEqual(
                    float(anchor[stream]), float(anchor[PREVIOUS_COLUMNS[stream]])
                )
            for stream in (
                "Distance_F_Elo",
                "Sprint_Elo",
                "Sprint_C_Elo",
                "Sprint_F_Elo",
                "Freestyle_Elo",
            ):
                self.assertEqual(
                    float(anchor[stream]), float(anchor[PREVIOUS_COLUMNS[stream]])
                )

    def test_reviewed_newcomer_without_prior_fis_rating_starts_at_1300(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [
                identity_row(
                    date="2025-11-20",
                    season=2026,
                    canonical_athlete_id="99",
                    canonical_name="Reviewed Newcomer",
                    source_name="НОВИЧОК Анна",
                ),
                identity_row(
                    date="2025-11-20",
                    season=2026,
                    athlete_id="flgr:202",
                    source_competitor_id="202",
                    source_name="НОВИКОВ Алексей",
                    place=1,
                    canonical_athlete_id="",
                    canonical_name="",
                    identity_status="flgr_only",
                ),
            ]
            future_history = [
                history_row(
                    Date="2025-12-13",
                    Season=2026,
                    ID="99",
                    Skier="Reviewed Newcomer",
                )
            ]
            identity, men, ladies = write_sources(
                root, identity_rows=rows, history_rows=future_history
            )
            report = replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                k_factor=1.0,
            )
            newcomer = next(
                row
                for row in read_ledger(root / "output")
                if row["athlete_id"] == "flgr:101"
            )
            self.assertEqual(newcomer["canonical_athlete_id"], "99")
            self.assertEqual(newcomer["anchor_source"], "reviewed_newcomer_1300")
            self.assertEqual(float(newcomer["Pelo"]), 1300)
            self.assertEqual(report["reviewed_anchor_athletes"], 0)
            self.assertEqual(report["reviewed_newcomer_athletes"], 1)
            self.assertEqual(report["reviewed_crosswalk_athletes"], 1)

    def test_events_chain_and_each_new_season_regresses_every_athlete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = [
                identity_row(),
                identity_row(
                    athlete_id="flgr:202",
                    source_competitor_id="202",
                    source_name="НОВИКОВ Алексей",
                    place=1,
                    canonical_athlete_id="",
                    canonical_name="",
                    identity_status="flgr_only",
                ),
            ]
            second = [
                identity_row(event_id="flgr:77:89", date="2026-12-01", place=1),
                identity_row(
                    event_id="flgr:77:89",
                    date="2026-12-01",
                    athlete_id="flgr:202",
                    source_competitor_id="202",
                    source_name="НОВИКОВ Алексей",
                    place=2,
                    canonical_athlete_id="",
                    canonical_name="",
                    identity_status="flgr_only",
                ),
            ]
            third = [
                identity_row(
                    event_id="flgr:78:1", date="2027-11-01", season=2028, place=2
                ),
                identity_row(
                    event_id="flgr:78:1",
                    date="2027-11-01",
                    season=2028,
                    athlete_id="flgr:202",
                    source_competitor_id="202",
                    source_name="НОВИКОВ Алексей",
                    place=1,
                    canonical_athlete_id="",
                    canonical_name="",
                    identity_status="flgr_only",
                ),
            ]
            identity, men, ladies = write_sources(root, first + second + third)
            replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                k_factor=5.0,
            )
            rows = read_ledger(root / "output")
            anchor_rows = [row for row in rows if row["athlete_id"] == "flgr:101"]
            self.assertAlmostEqual(
                float(anchor_rows[1]["Pelo"]), float(anchor_rows[0]["Elo"])
            )
            self.assertAlmostEqual(
                float(anchor_rows[2]["Pelo"]),
                regress_rating(float(anchor_rows[1]["Elo"]), 1),
            )

    def test_anchor_ignores_fis_rows_on_or_after_first_bridge_event(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            history = [
                history_row(Event="World Cup", Date="2026-03-01", Elo=1500),
                history_row(
                    Event="World Cup",
                    Date="2026-11-23",
                    Season=2027,
                    Race=1,
                    Elo=1800,
                ),
                history_row(
                    Event="World Cup",
                    Date="2026-12-01",
                    Season=2027,
                    Race=2,
                    Elo=1900,
                ),
            ]
            identity, men, ladies = write_sources(root, history_rows=history)
            replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                k_factor=1.0,
            )
            anchor = next(
                row
                for row in read_ledger(root / "output")
                if row["athlete_id"] == "flgr:101"
            )
            self.assertAlmostEqual(float(anchor["Pelo"]), regress_rating(1500, 1))

    def test_rejects_identity_conflicts_and_underfilled_events(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            conflict = [
                identity_row(),
                identity_row(
                    athlete_id="flgr:202",
                    source_competitor_id="202",
                    source_name="Other",
                    place=1,
                    canonical_athlete_id="10",
                    canonical_name="Ivan Ivanov",
                ),
            ]
            identity, men, ladies = write_sources(root, conflict)
            with self.assertRaisesRegex(FlgrEloReplayError, "claimed more than once"):
                replay_flgr_elo(
                    identity_results=identity,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=root / "conflict-output",
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity, men, ladies = write_sources(root, [identity_row()])
            with self.assertRaisesRegex(FlgrEloReplayError, "at least two"):
                replay_flgr_elo(
                    identity_results=identity,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=root / "small-output",
                )

    def test_sources_are_immutable_outputs_are_isolated_and_replay_is_deterministic(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity, men, ladies = write_sources(root)
            before = {path: path.read_bytes() for path in (identity, men, ladies)}
            replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "first",
                k_factor=5.0,
            )
            replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "second",
                k_factor=5.0,
            )
            self.assertEqual(before, {path: path.read_bytes() for path in before})
            for filename in ("flgr-bridge-ledger.csv", "flgr-bridge-report.json"):
                self.assertEqual(
                    (root / "first" / filename).read_bytes(),
                    (root / "second" / filename).read_bytes(),
                )

            existing = root / "existing"
            existing.mkdir()
            (existing / "old.txt").write_text("old", encoding="utf-8")
            with self.assertRaisesRegex(FlgrEloReplayError, "must be empty"):
                replay_flgr_elo(
                    identity_results=identity,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    output_dir=existing,
                )

    def test_report_is_valid_json_and_uses_requested_k(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity, men, ladies = write_sources(root)
            replay_flgr_elo(
                identity_results=identity,
                men_chrono=men,
                ladies_chrono=ladies,
                output_dir=root / "output",
                k_factor=2.5,
            )
            report = json.loads(
                (root / "output" / "flgr-bridge-report.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(report["k_factor"], 2.5)
            self.assertEqual(report["status"], "replayed_prediction_only")


if __name__ == "__main__":
    unittest.main()
