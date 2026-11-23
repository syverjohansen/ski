import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_fis_prediction_replay import (
    FisPredictionReplayError,
    replay_fis_prediction_elo,
)
from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS, regress_rating


def write_csv(path: Path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


PARTICIPANT_FIELDS = [
    "fis_race_id",
    "date",
    "sex",
    "distance",
    "technique",
    "place",
    "fis_competitor_id",
    "fis_code",
    "name",
    "prediction_athlete_id",
    "a_tier_verified",
]
COMPARISON_FIELDS = [
    "fis_race_id",
    "date",
    "sex",
    "distance",
    "technique",
    "target_prediction_athlete_id",
    "target_fis_competitor_id",
    "target_fis_code",
    "target_name",
    "target_place",
    "anchor_canonical_athlete_id",
    "anchor_name",
    "anchor_place",
    "score",
    *[f"anchor_{stream}" for stream in STREAMS],
]


def participant(**overrides):
    row = {
        "fis_race_id": "500",
        "date": "2026-11-20",
        "sex": "M",
        "distance": "10",
        "technique": "C",
        "place": "59",
        "fis_competitor_id": "300",
        "fis_code": "3400003",
        "name": "Unknown A",
        "prediction_athlete_id": "fis:300",
        "a_tier_verified": "false",
    }
    row.update(overrides)
    return row


def comparison(**overrides):
    row = {
        "fis_race_id": "500",
        "date": "2026-11-20",
        "sex": "M",
        "distance": "10",
        "technique": "C",
        "target_prediction_athlete_id": "fis:300",
        "target_fis_competitor_id": "300",
        "target_fis_code": "3400003",
        "target_name": "Unknown A",
        "target_place": "59",
        "anchor_canonical_athlete_id": "canonical:10",
        "anchor_name": "Verified One",
        "anchor_place": "58",
        "score": "0",
    }
    for stream in STREAMS:
        row[f"anchor_{stream}"] = "1600"
    row.update(overrides)
    return row


def prepare_sources(root: Path):
    participants = root / "sources" / "participants.csv"
    comparisons = root / "sources" / "comparisons.csv"
    participant_rows = [
        participant(
            place="58",
            fis_competitor_id="100",
            fis_code="3400001",
            name="Verified One",
            prediction_athlete_id="canonical:10",
            a_tier_verified="true",
        ),
        participant(),
        participant(
            place="60",
            fis_competitor_id="400",
            fis_code="3400004",
            name="Unknown B",
            prediction_athlete_id="fis:400",
        ),
        participant(
            place="61",
            fis_competitor_id="200",
            fis_code="3400002",
            name="Verified Two",
            prediction_athlete_id="canonical:20",
            a_tier_verified="true",
        ),
        participant(fis_race_id="501", date="2026-12-20", place="1"),
        participant(
            fis_race_id="501",
            date="2026-12-20",
            place="2",
            fis_competitor_id="400",
            fis_code="3400004",
            name="Unknown B",
            prediction_athlete_id="fis:400",
        ),
        participant(fis_race_id="502", date="2027-11-20", place="1", technique="F"),
        participant(
            fis_race_id="502",
            date="2027-11-20",
            place="3",
            technique="F",
            fis_competitor_id="400",
            fis_code="3400004",
            name="Unknown B",
            prediction_athlete_id="fis:400",
        ),
        participant(
            fis_race_id="502",
            date="2027-11-20",
            place="2",
            technique="F",
            fis_competitor_id="100",
            fis_code="3400001",
            name="Verified One",
            prediction_athlete_id="canonical:10",
            a_tier_verified="true",
        ),
    ]
    comparison_rows = [
        comparison(),
        comparison(
            score="1",
            anchor_canonical_athlete_id="canonical:20",
            anchor_name="Verified Two",
            anchor_place="61",
            **{f"anchor_{stream}": "1350" for stream in STREAMS},
        ),
        comparison(
            target_prediction_athlete_id="fis:400",
            target_fis_competitor_id="400",
            target_fis_code="3400004",
            target_name="Unknown B",
            target_place="60",
        ),
        comparison(
            target_prediction_athlete_id="fis:400",
            target_fis_competitor_id="400",
            target_fis_code="3400004",
            target_name="Unknown B",
            target_place="60",
            score="1",
            anchor_canonical_athlete_id="canonical:20",
            anchor_name="Verified Two",
            anchor_place="61",
            **{f"anchor_{stream}": "1350" for stream in STREAMS},
        ),
        comparison(
            fis_race_id="502",
            date="2027-11-20",
            technique="F",
            target_place="1",
            anchor_place="2",
            score="1",
            **{f"anchor_{stream}": "1500" for stream in STREAMS},
        ),
        comparison(
            fis_race_id="502",
            date="2027-11-20",
            target_prediction_athlete_id="fis:400",
            target_fis_competitor_id="400",
            target_fis_code="3400004",
            target_name="Unknown B",
            target_place="3",
            technique="F",
            anchor_place="2",
            score="0",
            **{f"anchor_{stream}": "1500" for stream in STREAMS},
        ),
    ]
    write_csv(participants, PARTICIPANT_FIELDS, participant_rows)
    write_csv(comparisons, COMPARISON_FIELDS, comparison_rows)
    return participants, comparisons


def read_ledger(root: Path):
    with (root / "fis-prediction-ledger.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        return list(csv.DictReader(handle))


class CrossCountryFisPredictionReplayTests(unittest.TestCase):
    def test_same_anchor_evidence_produces_same_rating_despite_unknown_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            participants, comparisons = prepare_sources(root)
            report = replay_fis_prediction_elo(
                participants=participants,
                comparisons=comparisons,
                output_dir=root / "output",
                k_factor=2.0,
            )
            first = [
                row
                for row in read_ledger(root / "output")
                if row["fis_race_id"] == "500"
            ]
            self.assertEqual(len(first), 2)
            self.assertNotEqual(first[0]["place"], first[1]["place"])
            for stream in STREAMS:
                self.assertEqual(first[0][stream], first[1][stream])
            self.assertEqual({row["k_factor"] for row in first}, {"2.0"})
            self.assertEqual(report["unknown_vs_unknown_comparisons"], 0)
            self.assertEqual(report["verified_anchor_updates"], 0)

    def test_anchor_free_event_carries_rating_without_an_update(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = prepare_sources(root)
            replay_fis_prediction_elo(
                participants=sources[0],
                comparisons=sources[1],
                output_dir=root / "output",
                k_factor=2.0,
            )
            rows = [
                row
                for row in read_ledger(root / "output")
                if row["prediction_athlete_id"] == "fis:300"
            ]
            self.assertEqual(rows[1]["fis_race_id"], "501")
            self.assertEqual(rows[1]["update_status"], "anchor_free_no_update")
            for stream in STREAMS:
                self.assertEqual(rows[1][stream], rows[1][PREVIOUS_COLUMNS[stream]])

    def test_only_matching_streams_update_and_new_season_regresses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = prepare_sources(root)
            replay_fis_prediction_elo(
                participants=sources[0],
                comparisons=sources[1],
                output_dir=root / "output",
                k_factor=2.0,
            )
            rows = [
                row
                for row in read_ledger(root / "output")
                if row["prediction_athlete_id"] == "fis:300"
            ]
            final = rows[-1]
            self.assertAlmostEqual(
                float(final["Pelo"]), regress_rating(float(rows[1]["Elo"]), 1)
            )
            for stream in ("Elo", "Distance_Elo", "Distance_F_Elo", "Freestyle_Elo"):
                self.assertNotEqual(
                    float(final[stream]), float(final[PREVIOUS_COLUMNS[stream]])
                )
            for stream in (
                "Distance_C_Elo",
                "Sprint_Elo",
                "Sprint_C_Elo",
                "Sprint_F_Elo",
                "Classic_Elo",
            ):
                self.assertEqual(
                    float(final[stream]), float(final[PREVIOUS_COLUMNS[stream]])
                )

    def test_verified_participants_never_enter_prediction_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = prepare_sources(root)
            report = replay_fis_prediction_elo(
                participants=sources[0],
                comparisons=sources[1],
                output_dir=root / "output",
            )
            ledger = read_ledger(root / "output")
            self.assertEqual(
                {row["prediction_athlete_id"] for row in ledger}, {"fis:300", "fis:400"}
            )
            self.assertEqual(report["never_a_tier_athletes"], 2)
            self.assertFalse(report["publication_eligible"])
            self.assertEqual(
                report["calibration_status"], "pending_fixed_anchor_backtest"
            )

    def test_sources_are_immutable_and_replay_is_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = prepare_sources(root)
            before = {path: path.read_bytes() for path in sources}
            for output in (root / "first", root / "second"):
                replay_fis_prediction_elo(
                    participants=sources[0],
                    comparisons=sources[1],
                    output_dir=output,
                    k_factor=2.0,
                )
            self.assertEqual(before, {path: path.read_bytes() for path in sources})
            for filename in ("fis-prediction-ledger.csv", "report.json"):
                self.assertEqual(
                    (root / "first" / filename).read_bytes(),
                    (root / "second" / filename).read_bytes(),
                )

    def test_rejects_comparison_targeting_a_verified_participant(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            participants, comparisons = prepare_sources(root)
            with comparisons.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            rows.append(
                comparison(
                    target_prediction_athlete_id="canonical:10",
                    target_fis_competitor_id="100",
                )
            )
            write_csv(comparisons, COMPARISON_FIELDS, rows)
            with self.assertRaisesRegex(FisPredictionReplayError, "targets verified"):
                replay_fis_prediction_elo(
                    participants=participants,
                    comparisons=comparisons,
                    output_dir=root / "output",
                )

    def test_rejects_comparison_with_mismatched_target_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            participants, comparisons = prepare_sources(root)
            with comparisons.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            rows[0]["target_fis_competitor_id"] = "wrong"
            write_csv(comparisons, COMPARISON_FIELDS, rows)
            with self.assertRaisesRegex(FisPredictionReplayError, "metadata mismatch"):
                replay_fis_prediction_elo(
                    participants=participants,
                    comparisons=comparisons,
                    output_dir=root / "output",
                )


if __name__ == "__main__":
    unittest.main()
