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
            "nordicnumbers.cross-country-source-policy/v1",
        )
        self.assertEqual(boundary["results"]["authority"], "FIS")
        self.assertEqual(boundary["participants"]["authority"], "FIS")
        self.assertEqual(
            boundary["results"]["accepted_histories"],
            ["excel365/men_chrono.csv", "excel365/ladies_chrono.csv"],
        )

    def test_russian_domestic_augmentation_is_not_publishable(self):
        augmentation = self.policy["russian_domestic_augmentation"]

        self.assertEqual(
            augmentation["status"], "retired_from_hosted_predictions"
        )
        self.assertFalse(augmentation["publication_eligible"])
        self.assertEqual(augmentation["source_domain"], "flgr-results.ru")
        self.assertGreaterEqual(len(augmentation["reasons"]), 4)
        self.assertGreaterEqual(len(augmentation["reactivation_requirements"]), 5)

    def test_preserved_legacy_scripts_are_labeled_research_only(self):
        scripts = self.policy["russian_domestic_augmentation"][
            "preserved_research_only_scripts"
        ]

        self.assertIn("russia_scrape.py", scripts)
        self.assertIn("russia_update_scrape.py", scripts)
        self.assertIn("merge_scrape.py", scripts)
        for relative_path in scripts:
            self.assertTrue((ROOT / relative_path).is_file(), relative_path)

    def test_hosted_modules_do_not_read_retired_inputs(self):
        forbidden_inputs = self.policy["forbidden_hosted_inputs"]

        for relative_path in self.policy["hosted_modules"]:
            source_path = ROOT / relative_path
            self.assertTrue(source_path.is_file(), relative_path)
            source = source_path.read_text(encoding="utf-8")
            for filename in forbidden_inputs:
                self.assertNotIn(filename, source, f"{relative_path}: {filename}")


if __name__ == "__main__":
    unittest.main()
