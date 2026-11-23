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


if __name__ == "__main__":
    unittest.main()
