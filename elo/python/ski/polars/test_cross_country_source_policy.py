import csv
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
POLICY_PATH = ROOT / "cross_country_source_policy.json"


class CrossCountrySourcePolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))

    def test_production_authorities_are_explicit(self):
        boundary = self.policy["production_boundary"]

        self.assertEqual(
            self.policy["schema_version"],
            "nordicnumbers.cross-country-source-policy/v2",
        )
        self.assertEqual(boundary["official_history"]["authority"], "FIS")
        self.assertEqual(boundary["participants"]["authority"], "FIS")
        self.assertFalse(
            boundary["participants"][
                "domestic_participation_grants_prediction_eligibility"
            ]
        )
        overlay = boundary["participants"]["pre_startlist_overlay"]
        self.assertEqual("reviewed_evidence_only", overlay["status"])
        self.assertTrue(overlay["unmentioned_athletes_keep_existing_fallback"])
        self.assertTrue(overlay["official_fis_startlist_always_supersedes"])
        self.assertTrue(overlay["raw_feed_is_never_directly_prediction_eligible"])
        self.assertTrue(overlay["resolver_candidates_require_human_review"])
        self.assertTrue(
            boundary["prediction_history"]["must_not_replace_official_history"]
        )
        self.assertEqual(
            boundary["official_history"]["accepted_histories"],
            ["excel365/men_chrono.csv", "excel365/ladies_chrono.csv"],
        )

    def test_russian_domestic_bridge_is_required(self):
        bridge = self.policy["russian_domestic_bridge"]

        self.assertEqual(
            bridge["status"],
            "fixed_anchor_cohort_builder_implemented_pending_evidence_review",
        )
        self.assertEqual(bridge["source_domain"], "flgr-results.ru")
        self.assertGreaterEqual(len(bridge["implementation_requirements"]), 6)

        legacy = bridge["legacy_implementation"]
        self.assertEqual(legacy["status"], "quarantined_pending_replacement")
        self.assertFalse(legacy["publication_eligible"])
        self.assertGreaterEqual(len(legacy["reasons"]), 4)

    def test_new_athletes_accumulate_namespaced_bridge_ratings(self):
        contract = self.policy["russian_domestic_bridge"]["target_contract"]

        self.assertEqual(contract["athlete_identity"], "flgr:<source_competitor_id>")
        self.assertEqual(contract["event_identity"], "flgr:<source_event_id>")
        self.assertEqual(contract["new_athlete_initial_elo"], 1300)
        self.assertEqual(
            contract["crosswalk_path"], "excel365/flgr_identity_crosswalk.csv"
        )
        self.assertTrue((ROOT / contract["crosswalk_path"]).is_file())
        self.assertTrue((ROOT / contract["historical_evidence_manifest"]).is_file())
        self.assertTrue(contract["fuzzy_matches_are_suggestions_only"])
        self.assertTrue(contract["future_dated_results_are_rejected"])
        self.assertFalse(contract["replay_publication_eligible"])
        self.assertEqual(
            contract["replay_calibration_status"],
            "fixed_anchor_cohort_builder_implemented_pending_review",
        )
        self.assertIsNone(contract["replay_k_factor"])
        calibration_path = ROOT / contract["calibration_decision"]
        calibration = json.loads(calibration_path.read_text(encoding="utf-8"))
        self.assertIsNone(calibration["selected_k_factor"])
        self.assertEqual(calibration["historical_all_participant_k_factor"], 2.0)
        self.assertEqual(calibration["evaluated_athletes"], 22)
        self.assertGreaterEqual(calibration["evaluated_athletes"], 20)
        self.assertEqual(calibration["status"], "superseded_by_fixed_anchor_contract")
        self.assertFalse(calibration["publication_eligible"])
        self.assertIn("lower Brier error", contract["backtest_rule"])
        self.assertIn("first A-tier race", contract["fixed_anchor_backtest_rule"])
        self.assertIn("established FIS athletes", contract["new_athlete_rating_rule"])
        self.assertIn(
            "official target-race FIS startlist", contract["prediction_selection"]
        )

    def test_post_ban_holdout_identities_are_reviewed_explicitly(self):
        crosswalk_path = (
            ROOT
            / self.policy["russian_domestic_bridge"]["target_contract"][
                "crosswalk_path"
            ]
        )
        with crosswalk_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(
            {
                (
                    row["sex"],
                    row["flgr_competitor_id"],
                    row["canonical_athlete_id"],
                    row["status"],
                )
                for row in rows
                if row["flgr_competitor_id"] in {"109411", "204470"}
            },
            {
                ("M", "109411", "13188", "reviewed"),
                ("L", "204470", "6555", "reviewed"),
            },
        )

    def test_historical_calibration_has_reviewed_minimum_cohort(self):
        contract = self.policy["russian_domestic_bridge"]["target_contract"]
        with (ROOT / contract["historical_evidence_manifest"]).open(
            newline="", encoding="utf-8"
        ) as handle:
            manifest_rows = [
                row
                for row in csv.DictReader(handle)
                if row["cohort"] == "historical_calibration"
            ]
        with (ROOT / contract["crosswalk_path"]).open(
            newline="", encoding="utf-8"
        ) as handle:
            reviewed = {
                (row["sex"], row["flgr_competitor_id"]): row
                for row in csv.DictReader(handle)
                if row["status"] == "reviewed"
            }

        targets = {
            (row["sex"], row["target_flgr_competitor_id"]) for row in manifest_rows
        }
        self.assertGreaterEqual(len(targets), 20)
        self.assertEqual({row["season"] for row in manifest_rows}, {"2021"})
        self.assertEqual({row["sex"] for row in manifest_rows}, {"M", "L"})
        for row in manifest_rows:
            mapping = reviewed[(row["sex"], row["target_flgr_competitor_id"])]
            self.assertEqual(
                mapping["canonical_athlete_id"],
                row["proposed_canonical_athlete_id"],
            )
            self.assertEqual(mapping["canonical_name"], row["proposed_canonical_name"])

    def test_preserved_legacy_scripts_are_labeled_research_only(self):
        scripts = self.policy["russian_domestic_bridge"]["legacy_implementation"][
            "preserved_research_only_scripts"
        ]

        self.assertIn("russia_scrape.py", scripts)
        self.assertIn("russia_update_scrape.py", scripts)
        self.assertIn("merge_scrape.py", scripts)
        for relative_path in scripts:
            self.assertTrue((ROOT / relative_path).is_file(), relative_path)

    def test_hosted_modules_do_not_read_quarantined_legacy_inputs(self):
        forbidden_inputs = self.policy["forbidden_legacy_hosted_inputs"]

        for relative_path in self.policy["hosted_modules"]:
            source_path = ROOT / relative_path
            self.assertTrue(source_path.is_file(), relative_path)
            source = source_path.read_text(encoding="utf-8")
            for filename in forbidden_inputs:
                self.assertNotIn(filename, source, f"{relative_path}: {filename}")


if __name__ == "__main__":
    unittest.main()
