import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_fis_prediction_cohort import (
    FisPredictionCohortError,
    build_fis_prediction_calibration_cohort,
)

RESULT_FIELDS = [
    "date",
    "sex",
    "fis_race_id",
    "fis_competitor_id",
    "fis_code",
    "name",
    "place",
    "source_url",
    "source_sha256",
]
HISTORY_FIELDS = ["Date", "Event", "Season", "Race", "ID", "Skier", "Place"]
ALIAS_FIELDS = [
    "sex",
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "canonical_id",
    "canonical_name",
]


def write_csv(path: Path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def result_row(**overrides):
    row = {
        "date": "2026-12-01",
        "sex": "M",
        "fis_race_id": "500",
        "fis_competitor_id": "300",
        "fis_code": "3400003",
        "name": "ATHLETE New",
        "place": "1",
        "source_url": "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=500",
        "source_sha256": "a" * 64,
    }
    row.update(overrides)
    if "source_url" not in overrides:
        row["source_url"] = (
            "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid="
            f"{row['fis_race_id']}"
        )
    return row


def history_row(**overrides):
    row = {
        "Date": "2027-01-10",
        "Event": "World Cup",
        "Season": "2027",
        "Race": "1",
        "ID": "10",
        "Skier": "New Athlete",
        "Place": "1",
    }
    row.update(overrides)
    return row


def alias_row(**overrides):
    row = {
        "sex": "M",
        "fis_competitor_id": "300",
        "fis_code": "3400003",
        "fis_name": "ATHLETE New",
        "canonical_id": "10",
        "canonical_name": "New Athlete",
    }
    row.update(overrides)
    return row


def write_sources(root: Path, *, results=None, histories=None, aliases=None):
    source = root / "sources"
    result_path = source / "results.csv"
    men = source / "men.csv"
    ladies = source / "ladies.csv"
    alias_path = source / "aliases.csv"
    default_results = [
        result_row(),
        result_row(
            fis_race_id="501",
            fis_competitor_id="400",
            fis_code="3400004",
            name="PROSPECT Fresh",
            place="2",
        ),
    ]
    default_histories = [
        history_row(),
        history_row(ID="20", Skier="Fresh Prospect", Place="2"),
    ]
    write_csv(result_path, RESULT_FIELDS, results or default_results)
    write_csv(men, HISTORY_FIELDS, histories or default_histories)
    write_csv(ladies, HISTORY_FIELDS, [])
    write_csv(alias_path, ALIAS_FIELDS, aliases or [alias_row()])
    return result_path, men, ladies, alias_path


def read_crosswalk(output: Path):
    with (output / "fis-prediction-calibration-crosswalk.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        return list(csv.DictReader(handle))


class CrossCountryFisPredictionCohortTests(unittest.TestCase):
    def test_preserves_reviewed_alias_and_only_proposes_unique_name_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = write_sources(root)
            report = build_fis_prediction_calibration_cohort(
                results=[results],
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                output_dir=root / "output",
                minimum_athletes=2,
            )
            rows = {
                row["fis_competitor_id"]: row for row in read_crosswalk(root / "output")
            }
            self.assertEqual(rows["300"]["status"], "reviewed")
            self.assertEqual(rows["400"]["status"], "proposed_unique_name")
            self.assertIn("first accepted A-tier race", rows["400"]["evidence"])
            self.assertEqual(report["reviewed_athletes"], 1)
            self.assertEqual(report["proposed_athletes"], 1)
            self.assertTrue(report["review_queue_ready"])
            self.assertFalse(report["calibration_ready"])
            self.assertEqual(report["automatic_approvals"], 0)

    def test_reviewed_cohort_reaches_gate_only_with_reviewed_aliases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            aliases = [
                alias_row(),
                alias_row(
                    fis_competitor_id="400",
                    fis_code="3400004",
                    fis_name="PROSPECT Fresh",
                    canonical_id="20",
                    canonical_name="Fresh Prospect",
                ),
            ]
            results, men, ladies, aliases_path = write_sources(root, aliases=aliases)
            report = build_fis_prediction_calibration_cohort(
                results=[results],
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases_path,
                output_dir=root / "output",
                minimum_athletes=2,
            )
            self.assertEqual(report["reviewed_athletes"], 2)
            self.assertTrue(report["calibration_ready"])

    def test_uses_only_lower_tier_evidence_strictly_before_debut(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [result_row(date="2027-02-01")]
            results, men, ladies, aliases = write_sources(root, results=rows)
            with self.assertRaisesRegex(
                FisPredictionCohortError, "no pre-debut calibration"
            ):
                build_fis_prediction_calibration_cohort(
                    results=[results],
                    men_chrono=men,
                    ladies_chrono=ladies,
                    aliases_path=aliases,
                    output_dir=root / "output",
                )

    def test_rejects_reviewed_alias_that_disagrees_with_fis_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = write_sources(
                root, aliases=[alias_row(fis_code="wrong")]
            )
            with self.assertRaisesRegex(FisPredictionCohortError, "does not match"):
                build_fis_prediction_calibration_cohort(
                    results=[results],
                    men_chrono=men,
                    ladies_chrono=ladies,
                    aliases_path=aliases,
                    output_dir=root / "output",
                )

    def test_duplicate_result_across_acquisition_files_is_counted_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = write_sources(root)
            duplicate = root / "other" / "results.csv"
            duplicate.parent.mkdir()
            duplicate.write_bytes(results.read_bytes())
            report = build_fis_prediction_calibration_cohort(
                results=[results, duplicate],
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                output_dir=root / "output",
                minimum_athletes=2,
            )
            self.assertEqual(report["candidate_athletes"], 2)

    def test_sources_are_immutable_and_output_is_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = write_sources(root)
            before = {path: path.read_bytes() for path in sources}
            for output in (root / "first", root / "second"):
                build_fis_prediction_calibration_cohort(
                    results=[sources[0]],
                    men_chrono=sources[1],
                    ladies_chrono=sources[2],
                    aliases_path=sources[3],
                    output_dir=output,
                    minimum_athletes=2,
                )
            self.assertEqual(before, {path: path.read_bytes() for path in sources})
            for filename in (
                "fis-prediction-calibration-crosswalk.csv",
                "report.json",
            ):
                self.assertEqual(
                    (root / "first" / filename).read_bytes(),
                    (root / "second" / filename).read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
