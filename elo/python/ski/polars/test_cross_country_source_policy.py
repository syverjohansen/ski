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
        self.assertTrue(
            boundary["prediction_history"]["must_not_replace_official_history"]
        )
        self.assertEqual(
            boundary["official_history"]["accepted_histories"],
            ["excel365/men_chrono.csv", "excel365/ladies_chrono.csv"],
        )

    def test_russian_domestic_bridge_is_required(self):
        bridge = self.policy["russian_domestic_bridge"]

        self.assertEqual(bridge["status"], "required_not_yet_implemented")
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
        self.assertTrue(contract["fuzzy_matches_are_suggestions_only"])
        self.assertTrue(contract["future_dated_results_are_rejected"])
        self.assertIn("established FIS athletes", contract["new_athlete_rating_rule"])
        self.assertIn(
            "official target-race FIS startlist", contract["prediction_selection"]
        )

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
