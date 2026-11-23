import csv
import json
import unittest
from pathlib import Path

from cross_country_prediction_sources import (
    A_TIER_FIS_CATEGORIES,
    EXCLUDED_FIS_CATEGORIES,
    PREDICTION_ONLY_FIS_CATEGORIES,
    PredictionParticipant,
    build_fixed_anchor_comparisons,
    classify_fis_category,
    prediction_calendar_url,
)

ROOT = Path(__file__).resolve().parent


class CrossCountryPredictionSourceTests(unittest.TestCase):
    def test_fis_tiers_are_explicit_and_disjoint(self):
        self.assertEqual(set(A_TIER_FIS_CATEGORIES), {"OWG", "SWC", "WC", "WSC"})
        self.assertFalse(
            set(A_TIER_FIS_CATEGORIES) & set(PREDICTION_ONLY_FIS_CATEGORIES)
        )
        self.assertFalse(set(A_TIER_FIS_CATEGORIES) & set(EXCLUDED_FIS_CATEGORIES))
        self.assertEqual(classify_fis_category("scan"), "prediction_only")
        self.assertEqual(classify_fis_category("WC"), "a_tier")
        self.assertEqual(classify_fis_category("MAS"), "excluded")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            classify_fis_category("OTHER")

    def test_prediction_calendar_is_bounded_to_lower_tier_cross_country(self):
        url = prediction_calendar_url(2027, "UST")
        self.assertIn("sectorcode=CC", url)
        self.assertIn("seasoncode=2027", url)
        self.assertIn("categorycode=UST", url)
        with self.assertRaisesRegex(ValueError, "not prediction-only"):
            prediction_calendar_url(2027, "WC")
        with self.assertRaisesRegex(ValueError, "supported range"):
            prediction_calendar_url(1900, "FIS")

    def test_unknowns_in_same_anchor_interval_receive_identical_evidence(self):
        comparisons = build_fixed_anchor_comparisons(
            [
                PredictionParticipant("verified-a", 10, True, 1600),
                PredictionParticipant("verified-b", 58, True, 1450),
                PredictionParticipant("unknown-59", 59, False),
                PredictionParticipant("unknown-60", 60, False),
                PredictionParticipant("verified-c", 61, True, 1350),
            ]
        )
        evidence_59 = [
            (item.anchor_athlete_id, item.anchor_rating, item.score)
            for item in comparisons["unknown-59"]
        ]
        evidence_60 = [
            (item.anchor_athlete_id, item.anchor_rating, item.score)
            for item in comparisons["unknown-60"]
        ]
        self.assertEqual(evidence_59, evidence_60)
        self.assertEqual(
            evidence_59,
            [
                ("verified-a", 1600.0, 0.0),
                ("verified-b", 1450.0, 0.0),
                ("verified-c", 1350.0, 1.0),
            ],
        )

    def test_unknown_order_never_appears_in_another_unknowns_evidence(self):
        comparisons = build_fixed_anchor_comparisons(
            [
                PredictionParticipant("unknown-winner", 1, False),
                PredictionParticipant("unknown-loser", 2, False),
            ]
        )
        self.assertEqual(comparisons["unknown-winner"], ())
        self.assertEqual(comparisons["unknown-loser"], ())

    def test_verified_athletes_are_anchors_not_update_targets(self):
        comparisons = build_fixed_anchor_comparisons(
            [
                PredictionParticipant("verified", 2, True, 1550),
                PredictionParticipant("unknown", 1, False),
            ]
        )
        self.assertNotIn("verified", comparisons)
        self.assertEqual(comparisons["unknown"][0].score, 1.0)
        self.assertEqual(comparisons["unknown"][0].anchor_rating, 1550)

    def test_policy_matches_implemented_source_and_comparison_boundaries(self):
        policy = json.loads(
            (ROOT / "cross_country_prediction_elo_policy.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            set(policy["official_elo"]["fis_categories"]),
            set(A_TIER_FIS_CATEGORIES),
        )
        self.assertEqual(
            set(policy["fis_prediction_sources"]["categories"]),
            set(PREDICTION_ONLY_FIS_CATEGORIES),
        )
        self.assertEqual(
            set(policy["excluded_fis_categories"]), set(EXCLUDED_FIS_CATEGORIES)
        )
        self.assertEqual(policy["prediction_elo"]["unknown_vs_unknown"], "ignored")
        self.assertFalse(policy["prediction_elo"]["verified_anchor_updates"])
        self.assertFalse(policy["fis_prediction_sources"]["accepted_history_target"])
        cohort = policy["calibration_cohort"]
        self.assertEqual(cohort["minimum_reviewed_athletes"], 20)
        self.assertTrue(cohort["unique_name_matches_are_proposals_only"])
        self.assertFalse(cohort["automatic_approvals"])
        self.assertFalse(cohort["accepted_history_target"])

    def test_calibration_crosswalk_has_reviewed_balanced_minimum(self):
        policy = json.loads(
            (ROOT / "cross_country_prediction_elo_policy.json").read_text(
                encoding="utf-8"
            )
        )
        cohort = policy["calibration_cohort"]
        crosswalk_path = ROOT / cohort["reviewed_crosswalk"]
        aliases_path = ROOT / "excel365" / "fis_identity_aliases.csv"
        with crosswalk_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        with aliases_path.open(newline="", encoding="utf-8") as handle:
            aliases = {
                (row["sex"], row["fis_competitor_id"]): row
                for row in csv.DictReader(handle)
            }

        self.assertEqual(len(rows), cohort["reviewed_athletes"])
        self.assertGreaterEqual(len(rows), cohort["minimum_reviewed_athletes"])
        self.assertEqual({row["sex"] for row in rows}, {"M", "L"})
        self.assertEqual({row["status"] for row in rows}, {"reviewed"})
        self.assertEqual(
            len({(row["sex"], row["fis_competitor_id"]) for row in rows}),
            len(rows),
        )
        for row in rows:
            self.assertEqual(
                row["prediction_athlete_id"], f"fis:{row['fis_competitor_id']}"
            )
            self.assertIn("first accepted A-tier race", row["evidence"])
            alias = aliases[(row["sex"], row["fis_competitor_id"])]
            self.assertEqual(alias["fis_code"], row["fis_code"])
            self.assertEqual(alias["canonical_id"], row["canonical_athlete_id"])
            self.assertEqual(alias["canonical_name"], row["canonical_name"])

    def test_selected_k_factor_is_supported_by_calibration_decision(self):
        policy = json.loads(
            (ROOT / "cross_country_prediction_elo_policy.json").read_text(
                encoding="utf-8"
            )
        )
        prediction = policy["prediction_elo"]
        decision = json.loads(
            (ROOT / prediction["calibration_decision"]).read_text(encoding="utf-8")
        )

        self.assertEqual(prediction["selected_k_factor"], 4.0)
        self.assertEqual(decision["selected_k_factor"], 4.0)
        self.assertGreaterEqual(
            decision["evaluated_athletes"],
            policy["calibration_cohort"]["minimum_reviewed_athletes"],
        )
        self.assertLess(
            decision["metrics"]["k4"]["brier_score"],
            decision["metrics"]["official_prior"]["brier_score"],
        )
        self.assertFalse(decision["publication_eligible"])
        self.assertEqual(decision["unknown_vs_unknown_comparisons"], 0)
        self.assertEqual(decision["verified_anchor_updates"], 0)
        for digest in (
            *decision["input_sha256"].values(),
            *decision["candidate_ledger_sha256"].values(),
        ):
            self.assertEqual(len(digest), 64)
            int(digest, 16)


if __name__ == "__main__":
    unittest.main()
