import csv
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from cross_country_flgr_evidence_acquire import (
    MANIFEST_FIELDS,
    acquire_flgr_evidence,
)
from cross_country_flgr_evidence_resolve import (
    FlgrEvidenceResolveError,
    resolve_evidence_bundle,
)
from cross_country_flgr_identity import CROSSWALK_FIELDS

ROOT = Path(__file__).resolve().parent
FIXTURE = ROOT / "tests" / "fixtures" / "flgr" / "current-markup-race.html"


def write_csv(path: Path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def prepare_bundle(
    root: Path, *, reviewed: bool = True, multiple_targets: bool = False
):
    sources = root / "sources"
    sources.mkdir()
    manifest = sources / "manifest.csv"
    write_csv(
        manifest,
        MANIFEST_FIELDS,
        [
            {
                "cohort": "post_ban_holdout",
                "season": 2027,
                "sex": "M",
                "source_url": f"https://flgr-results.ru/results/77/{race_id}",
                "target_flgr_competitor_id": "101",
                "proposed_canonical_athlete_id": "10",
                "proposed_canonical_name": "Ivan Ivanov",
                "evidence_note": "Test review evidence",
            }
            for race_id in (88, 89)
        ]
        + (
            [
                {
                    "cohort": "post_ban_holdout",
                    "season": 2027,
                    "sex": "M",
                    "source_url": "https://flgr-results.ru/results/77/88",
                    "target_flgr_competitor_id": "202",
                    "proposed_canonical_athlete_id": "20",
                    "proposed_canonical_name": "Petr Petrov",
                    "evidence_note": "Second reviewed target on the same event",
                }
            ]
            if multiple_targets
            else []
        ),
    )
    evidence = root / "evidence"
    acquire_flgr_evidence(
        manifest_path=manifest,
        output_dir=evidence,
        as_of=date(2026, 11, 23),
        fetch=lambda _: FIXTURE.read_bytes(),
        retrieved_at=datetime(2026, 11, 23, 19, 0, tzinfo=UTC),
    )
    men = sources / "men.csv"
    ladies = sources / "ladies.csv"
    crosswalk = sources / "crosswalk.csv"
    history_fields = ["Date", "ID", "Skier", "Nation", "Birthday"]
    history_rows = [
        {
            "Date": "2022-01-01",
            "ID": "10",
            "Skier": "Ivan Ivanov",
            "Nation": "Russia",
            "Birthday": "1999-04-10",
        }
    ]
    if multiple_targets:
        history_rows.append(
            {
                "Date": "2022-01-01",
                "ID": "20",
                "Skier": "Petr Petrov",
                "Nation": "Russia",
                "Birthday": "2003-05-11",
            }
        )
    write_csv(men, history_fields, history_rows)
    write_csv(ladies, history_fields, [])
    crosswalk_rows = []
    if reviewed:
        crosswalk_rows.append(
            {
                "sex": "M",
                "flgr_competitor_id": "101",
                "flgr_name": "ИВАНОВ Иван",
                "birth_year": "1999",
                "canonical_athlete_id": "10",
                "canonical_name": "Ivan Ivanov",
                "status": "reviewed",
                "evidence": "Test evidence reviewed",
            }
        )
        if multiple_targets:
            crosswalk_rows.append(
                {
                    "sex": "M",
                    "flgr_competitor_id": "202",
                    "flgr_name": "НОВИКОВ Алексей",
                    "birth_year": "2003",
                    "canonical_athlete_id": "20",
                    "canonical_name": "Petr Petrov",
                    "status": "reviewed",
                    "evidence": "Second test evidence reviewed",
                }
            )
    write_csv(crosswalk, CROSSWALK_FIELDS, crosswalk_rows)
    return evidence, men, ladies, crosswalk


class FlgrEvidenceResolveTests(unittest.TestCase):
    def test_does_not_promote_reviewed_identity_outside_selected_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root)
            with men.open(newline="", encoding="utf-8") as handle:
                history_reader = csv.DictReader(handle)
                history_fields = list(history_reader.fieldnames or ())
                history_rows = list(history_reader)
            history_rows.append(
                {
                    "Date": "2022-01-01",
                    "ID": "20",
                    "Skier": "Petr Petrov",
                    "Nation": "Russia",
                    "Birthday": "2003-05-11",
                }
            )
            write_csv(men, history_fields, history_rows)
            with crosswalk.open(newline="", encoding="utf-8") as handle:
                crosswalk_rows = list(csv.DictReader(handle))
            crosswalk_rows.append(
                {
                    "sex": "M",
                    "flgr_competitor_id": "202",
                    "flgr_name": "НОВИКОВ Алексей",
                    "birth_year": "2003",
                    "canonical_athlete_id": "20",
                    "canonical_name": "Petr Petrov",
                    "status": "reviewed",
                    "evidence": "Reviewed for another evidence cohort",
                }
            )
            write_csv(crosswalk, CROSSWALK_FIELDS, crosswalk_rows)

            report = resolve_evidence_bundle(
                evidence_dir=evidence,
                men_chrono=men,
                ladies_chrono=ladies,
                crosswalk_path=crosswalk,
                output_dir=root / "output",
            )

            self.assertTrue(report["reviewed_mappings_limited_to_evidence_targets"])
            with (root / "output" / "flgr-identity-results.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            untargeted = [row for row in rows if row["athlete_id"] == "flgr:202"]
            self.assertEqual(len(untargeted), 2)
            self.assertEqual(
                {row["identity_status"] for row in untargeted}, {"flgr_only"}
            )
            self.assertEqual({row["canonical_athlete_id"] for row in untargeted}, {""})

    def test_resolves_only_the_requested_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root)
            index = evidence / "evidence-index.csv"
            with index.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or ())
                rows = list(reader)
            rows[1]["cohort"] = "historical_calibration"
            write_csv(index, fields, rows)

            report = resolve_evidence_bundle(
                evidence_dir=evidence,
                men_chrono=men,
                ladies_chrono=ladies,
                crosswalk_path=crosswalk,
                output_dir=root / "output",
                cohort="historical_calibration",
            )

            self.assertEqual(report["cohort_filter"], "historical_calibration")
            self.assertEqual(report["event_count"], 1)
            self.assertEqual(report["result_rows"], 2)

    def test_rejects_unknown_or_empty_cohort_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root)
            with self.assertRaisesRegex(
                FlgrEvidenceResolveError, "unsupported evidence cohort"
            ):
                resolve_evidence_bundle(
                    evidence_dir=evidence,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=root / "unknown",
                    cohort="other",
                )
            with self.assertRaisesRegex(FlgrEvidenceResolveError, "no rows for cohort"):
                resolve_evidence_bundle(
                    evidence_dir=evidence,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=root / "empty",
                    cohort="historical_calibration",
                )

    def test_resolves_multiple_reviewed_targets_from_one_event_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(
                root, multiple_targets=True
            )
            report = resolve_evidence_bundle(
                evidence_dir=evidence,
                men_chrono=men,
                ladies_chrono=ladies,
                crosswalk_path=crosswalk,
                output_dir=root / "output",
            )

            self.assertEqual(report["event_count"], 2)
            self.assertEqual(report["result_rows"], 4)
            event = next(
                item for item in report["events"] if item["reviewed_targets"] == 2
            )
            self.assertEqual(event["reviewed_targets"], 2)
            with (root / "output" / "flgr-identity-results.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            event_rows = [row for row in rows if row["event_id"] == event["event_id"]]
            self.assertEqual(len(event_rows), 2)
            self.assertEqual(
                {row["canonical_athlete_id"] for row in event_rows}, {"10", "20"}
            )

    def test_resolves_every_hashed_candidate_into_chronological_replay_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root)
            sources = {
                path: path.read_bytes()
                for path in (
                    evidence / "evidence-index.csv",
                    men,
                    ladies,
                    crosswalk,
                )
            }
            report = resolve_evidence_bundle(
                evidence_dir=evidence,
                men_chrono=men,
                ladies_chrono=ladies,
                crosswalk_path=crosswalk,
                output_dir=root / "output",
            )

            self.assertEqual(report["event_count"], 2)
            self.assertEqual(report["result_rows"], 4)
            self.assertEqual(
                report["identity_status_rows"],
                {"flgr_only": 2, "reviewed_crosswalk": 2},
            )
            self.assertFalse(report["mapping_suggestions_applied"])
            self.assertFalse(report["publication_eligible"])
            with (root / "output" / "flgr-identity-results.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            target_rows = [row for row in rows if row["athlete_id"] == "flgr:101"]
            self.assertEqual(len(target_rows), 2)
            self.assertEqual(
                {row["canonical_athlete_id"] for row in target_rows}, {"10"}
            )
            self.assertEqual(sources, {path: path.read_bytes() for path in sources})

    def test_rejects_raw_page_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root)
            raw = next((evidence / "raw").iterdir())
            raw.write_bytes(raw.read_bytes() + b"tampered")
            with self.assertRaisesRegex(
                FlgrEvidenceResolveError, "raw source checksum changed"
            ):
                resolve_evidence_bundle(
                    evidence_dir=evidence,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=root / "output",
                )

    def test_proposed_target_requires_exact_reviewed_crosswalk(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root, reviewed=False)
            with self.assertRaisesRegex(
                FlgrEvidenceResolveError, "has not been reviewed exactly"
            ):
                resolve_evidence_bundle(
                    evidence_dir=evidence,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=root / "output",
                )

    def test_rejects_candidate_path_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root)
            index = evidence / "evidence-index.csv"
            with index.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or ())
                rows = list(reader)
            rows[0]["candidate_file"] = "../manifest.csv"
            write_csv(index, fields, rows)
            with self.assertRaisesRegex(
                FlgrEvidenceResolveError, "unsafe or missing candidate_file"
            ):
                resolve_evidence_bundle(
                    evidence_dir=evidence,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=root / "output",
                )

    def test_rejects_conflicting_metadata_for_shared_event(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(
                root, multiple_targets=True
            )
            index = evidence / "evidence-index.csv"
            with index.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or ())
                rows = list(reader)
            duplicate_event = rows[0]["event_id"]
            second = next(row for row in rows[1:] if row["event_id"] == duplicate_event)
            second["source_sha256"] = "0" * 64
            write_csv(index, fields, rows)
            with self.assertRaisesRegex(
                FlgrEvidenceResolveError, "conflicts with .* metadata"
            ):
                resolve_evidence_bundle(
                    evidence_dir=evidence,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=root / "output",
                )

    def test_output_directory_must_be_empty_and_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, men, ladies, crosswalk = prepare_bundle(root)
            existing = root / "existing"
            existing.mkdir()
            (existing / "old").write_text("old", encoding="utf-8")
            with self.assertRaisesRegex(FlgrEvidenceResolveError, "must be empty"):
                resolve_evidence_bundle(
                    evidence_dir=evidence,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=existing,
                )


if __name__ == "__main__":
    unittest.main()
