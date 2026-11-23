import csv
import json
import tempfile
import unittest
from pathlib import Path

from cross_country_prediction_elo_shadow import (
    PredictionEloShadowError,
    enrich_prediction_elo_shadow,
)
from cross_country_results_elo_replay import STREAMS
from cross_country_weekend_inputs import STARTLIST_FIELDS


def write_csv(path: Path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def startlist_row(athlete_id: str, competitor_id: str, fis_code: str, name: str):
    return {
        "group_date": "2026-11-27",
        "race_date": "2026-11-28",
        "race_id": "52085",
        "sex": "M",
        "city": "Ruka",
        "country": "Finland",
        "distance": "10",
        "technique": "C",
        "canonical_athlete_id": athlete_id,
        "canonical_name": name,
        "canonical_nation": "Norway",
        "fis_competitor_id": competitor_id,
        "fis_code": fis_code,
        "identity_match_method": (
            "prediction_only_fis_identity"
            if athlete_id.startswith("fis:")
            else "reviewed_alias"
        ),
        "selection_status": "confirmed",
        "source_mode": "official_fis_startlist",
        "source_url": "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=52085",
        "source_sha256": "a" * 64,
    }


def rating_values(value: float):
    return {stream: str(value + index) for index, stream in enumerate(STREAMS)}


class PredictionEloShadowTests(unittest.TestCase):
    def make_fixture(self, root: Path):
        startlist = root / "inputs" / "men_startlist.csv"
        men = root / "accepted" / "men_chrono.csv"
        ladies = root / "accepted" / "ladies_chrono.csv"
        ledger = root / "private" / "fis-prediction-ledger.csv"
        policy = root / "policy" / "policy.json"
        history_fields = ["Date", "Race", "ID", *STREAMS]
        write_csv(
            men,
            history_fields,
            [
                {"Date": "2026-03-01", "Race": "4", "ID": "11", **rating_values(1600)},
                {"Date": "2026-12-01", "Race": "5", "ID": "11", **rating_values(1900)},
            ],
        )
        write_csv(ladies, history_fields, [])
        ledger_fields = [
            "fis_race_id",
            "date",
            "prediction_athlete_id",
            "fis_competitor_id",
            "fis_code",
            "k_factor",
            *STREAMS,
        ]
        write_csv(
            ledger,
            ledger_fields,
            [
                {
                    "fis_race_id": "1",
                    "date": "2026-10-01",
                    "prediction_athlete_id": "fis:999",
                    "fis_competitor_id": "999",
                    "fis_code": "3999999",
                    "k_factor": "4.0",
                    **rating_values(1410),
                },
                {
                    "fis_race_id": "2",
                    "date": "2026-12-01",
                    "prediction_athlete_id": "fis:999",
                    "fis_competitor_id": "999",
                    "fis_code": "3999999",
                    "k_factor": "4.0",
                    **rating_values(1700),
                },
            ],
        )
        policy.parent.mkdir(parents=True)
        policy.write_text(
            json.dumps(
                {"prediction_elo": {"selected_k_factor": 4.0, "prior_is_private": True}}
            ),
            encoding="utf-8",
        )
        return startlist, men, ladies, ledger, policy

    def test_selects_official_private_and_newcomer_ratings_as_of_race(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            startlist, men, ladies, ledger, policy = self.make_fixture(root)
            write_csv(
                startlist,
                STARTLIST_FIELDS,
                [
                    startlist_row("11", "111", "3111111", "Verified Athlete"),
                    startlist_row("fis:999", "999", "3999999", "Predicted Athlete"),
                    startlist_row("fis:888", "888", "3888888", "New Athlete"),
                ],
            )
            mtimes = {
                path: path.stat().st_mtime_ns
                for path in (startlist, men, ladies, ledger, policy)
            }
            report = enrich_prediction_elo_shadow(
                startlist=startlist,
                men_chrono=men,
                ladies_chrono=ladies,
                prediction_ledger=ledger,
                policy_path=policy,
                output_dir=root / "output",
            )
            with (root / "output" / "startlist_prediction_elo_shadow.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(
                [row["rating_source"] for row in rows],
                [
                    "accepted_official_elo",
                    "fixed_anchor_prediction_elo",
                    "newcomer_prior",
                ],
            )
            self.assertEqual(float(rows[0]["Elo"]), 1600.0)
            self.assertEqual(float(rows[1]["Elo"]), 1410.0)
            self.assertEqual(rows[1]["rating_as_of_date"], "2026-10-01")
            self.assertEqual(float(rows[2]["Elo"]), 1300.0)
            self.assertEqual(report["publication"]["status"], "disabled")
            self.assertEqual(report["selected_k_factor"], 4.0)
            self.assertEqual(mtimes, {path: path.stat().st_mtime_ns for path in mtimes})

    def test_rejects_fis_code_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            startlist, men, ladies, ledger, policy = self.make_fixture(root)
            write_csv(
                startlist,
                STARTLIST_FIELDS,
                [startlist_row("fis:999", "999", "WRONG", "Predicted Athlete")],
            )
            with self.assertRaisesRegex(PredictionEloShadowError, "FIS code changed"):
                enrich_prediction_elo_shadow(
                    startlist=startlist,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    prediction_ledger=ledger,
                    policy_path=policy,
                    output_dir=root / "output",
                )

    def test_rejects_non_calibrated_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            startlist, men, ladies, ledger, policy = self.make_fixture(root)
            text = ledger.read_text(encoding="utf-8").replace(",4.0,", ",5.0,")
            ledger.write_text(text, encoding="utf-8")
            write_csv(
                startlist,
                STARTLIST_FIELDS,
                [startlist_row("fis:999", "999", "3999999", "Predicted Athlete")],
            )
            with self.assertRaisesRegex(PredictionEloShadowError, "ledger K"):
                enrich_prediction_elo_shadow(
                    startlist=startlist,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    prediction_ledger=ledger,
                    policy_path=policy,
                    output_dir=root / "output",
                )


if __name__ == "__main__":
    unittest.main()
