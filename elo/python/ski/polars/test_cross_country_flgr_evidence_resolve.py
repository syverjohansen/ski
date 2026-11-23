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


def prepare_bundle(root: Path, *, reviewed: bool = True):
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
        ],
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
    write_csv(
        men,
        history_fields,
        [
            {
                "Date": "2022-01-01",
                "ID": "10",
                "Skier": "Ivan Ivanov",
                "Nation": "Russia",
                "Birthday": "1999-04-10",
            }
        ],
    )
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
    write_csv(crosswalk, CROSSWALK_FIELDS, crosswalk_rows)
    return evidence, men, ladies, crosswalk


class FlgrEvidenceResolveTests(unittest.TestCase):
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
