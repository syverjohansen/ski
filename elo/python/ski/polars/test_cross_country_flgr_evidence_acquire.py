import csv
import json
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from cross_country_flgr_evidence_acquire import (
    MANIFEST_FIELDS,
    FlgrEvidenceAcquisitionError,
    acquire_flgr_evidence,
)

ROOT = Path(__file__).resolve().parent
FIXTURE = ROOT / "tests" / "fixtures" / "flgr" / "current-markup-race.html"


def write_manifest(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def manifest_row(**overrides) -> dict[str, object]:
    row: dict[str, object] = {
        "cohort": "post_ban_holdout",
        "season": 2027,
        "sex": "M",
        "source_url": "https://flgr-results.ru/results/77/88",
        "target_flgr_competitor_id": "101",
        "proposed_canonical_athlete_id": "10",
        "proposed_canonical_name": "Ivan Ivanov",
        "evidence_note": "Reviewed return cohort locator",
    }
    row.update(overrides)
    return row


class FlgrEvidenceAcquisitionTests(unittest.TestCase):
    def test_approved_pages_are_hashed_validated_and_left_unmapped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.csv"
            urls = [
                "https://flgr-results.ru/results/77/88",
                "https://flgr-results.ru/results/77/89",
            ]
            write_manifest(
                manifest,
                [
                    manifest_row(source_url=urls[0]),
                    manifest_row(source_url=urls[1]),
                ],
            )
            requested = []

            def fetch(url: str) -> bytes:
                requested.append(url)
                return FIXTURE.read_bytes()

            report = acquire_flgr_evidence(
                manifest_path=manifest,
                output_dir=root / "output",
                as_of=date(2026, 11, 23),
                fetch=fetch,
                retrieved_at=datetime(2026, 11, 23, 19, 0, tzinfo=UTC),
            )

            self.assertEqual(requested, urls)
            self.assertEqual(report["race_pages"], 2)
            self.assertEqual(report["evidence_requests"], 2)
            self.assertEqual(report["target_athletes"], 1)
            self.assertFalse(report["mapping_applied"])
            self.assertFalse(report["publication_eligible"])
            with (root / "output" / "evidence-index.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(
                {row["mapping_status"] for row in rows},
                {"proposed_requires_review"},
            )
            for row in rows:
                self.assertEqual(row["target_athlete_id"], "flgr:101")
                self.assertTrue((root / "output" / row["raw_file"]).is_file())
                candidate_path = root / "output" / row["candidate_file"]
                candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
                self.assertEqual(candidate["source_sha256"], row["source_sha256"])

    def test_target_athlete_must_be_classified_on_every_page(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.csv"
            write_manifest(
                manifest,
                [manifest_row(target_flgr_competitor_id="999")],
            )
            with self.assertRaisesRegex(
                FlgrEvidenceAcquisitionError, "target flgr:999 is absent"
            ):
                acquire_flgr_evidence(
                    manifest_path=manifest,
                    output_dir=root / "output",
                    as_of=date(2026, 11, 23),
                    fetch=lambda _: FIXTURE.read_bytes(),
                    retrieved_at=datetime(2026, 11, 23, 19, 0, tzinfo=UTC),
                )

    def test_one_approved_page_can_request_multiple_reviewed_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.csv"
            source_url = "https://flgr-results.ru/results/77/88"
            write_manifest(
                manifest,
                [
                    manifest_row(source_url=source_url),
                    manifest_row(
                        source_url=source_url,
                        target_flgr_competitor_id="202",
                        proposed_canonical_athlete_id="20",
                        proposed_canonical_name="Petr Petrov",
                    ),
                ],
            )
            requested = []

            def fetch(url: str) -> bytes:
                requested.append(url)
                return FIXTURE.read_bytes()

            report = acquire_flgr_evidence(
                manifest_path=manifest,
                output_dir=root / "output",
                as_of=date(2026, 11, 23),
                fetch=fetch,
                retrieved_at=datetime(2026, 11, 23, 19, 0, tzinfo=UTC),
            )

            self.assertEqual(requested, [source_url])
            self.assertEqual(report["race_pages"], 1)
            self.assertEqual(report["evidence_requests"], 2)
            self.assertEqual(report["target_athletes"], 2)
            with (root / "output" / "evidence-index.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(
                {row["target_athlete_id"] for row in rows},
                {"flgr:101", "flgr:202"},
            )
            self.assertEqual(len({row["raw_file"] for row in rows}), 1)
            self.assertEqual(len({row["candidate_file"] for row in rows}), 1)

    def test_manifest_rejects_duplicate_targets_or_unapproved_urls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.csv"
            row = manifest_row()
            write_manifest(manifest, [row, row])
            with self.assertRaisesRegex(
                FlgrEvidenceAcquisitionError, "duplicate target 101"
            ):
                acquire_flgr_evidence(
                    manifest_path=manifest,
                    output_dir=root / "duplicate-output",
                    as_of=date(2026, 11, 23),
                    fetch=lambda _: FIXTURE.read_bytes(),
                )

            write_manifest(
                manifest,
                [row, manifest_row(target_flgr_competitor_id="202", season=2026)],
            )
            with self.assertRaisesRegex(
                FlgrEvidenceAcquisitionError, "conflicting source metadata"
            ):
                acquire_flgr_evidence(
                    manifest_path=manifest,
                    output_dir=root / "conflicting-output",
                    as_of=date(2026, 11, 23),
                    fetch=lambda _: FIXTURE.read_bytes(),
                )

            write_manifest(
                manifest,
                [manifest_row(source_url="https://example.com/results/77/88")],
            )
            with self.assertRaisesRegex(ValueError, "flgr-results.ru"):
                acquire_flgr_evidence(
                    manifest_path=manifest,
                    output_dir=root / "domain-output",
                    as_of=date(2026, 11, 23),
                    fetch=lambda _: FIXTURE.read_bytes(),
                )

    def test_raw_evidence_cannot_be_written_into_git_or_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.csv"
            write_manifest(manifest, [manifest_row()])

            with self.assertRaisesRegex(
                FlgrEvidenceAcquisitionError, "outside the Git repository"
            ):
                acquire_flgr_evidence(
                    manifest_path=manifest,
                    output_dir=ROOT / "forbidden-evidence-output",
                    as_of=date(2026, 11, 23),
                    fetch=lambda _: FIXTURE.read_bytes(),
                )

            existing = root / "existing"
            existing.mkdir()
            (existing / "old").write_text("old", encoding="utf-8")
            with self.assertRaisesRegex(FlgrEvidenceAcquisitionError, "must be empty"):
                acquire_flgr_evidence(
                    manifest_path=manifest,
                    output_dir=existing,
                    as_of=date(2026, 11, 23),
                    fetch=lambda _: FIXTURE.read_bytes(),
                )

    def test_future_dated_page_is_rejected_by_candidate_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.csv"
            write_manifest(manifest, [manifest_row()])
            with self.assertRaisesRegex(ValueError, "future-dated"):
                acquire_flgr_evidence(
                    manifest_path=manifest,
                    output_dir=root / "output",
                    as_of=date(2026, 11, 22),
                    fetch=lambda _: FIXTURE.read_bytes(),
                    retrieved_at=datetime(2026, 11, 23, 19, 0, tzinfo=UTC),
                )


if __name__ == "__main__":
    unittest.main()
