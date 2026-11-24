import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_announced_rosters import load_announced_rosters
from cross_country_fantasyxc_resolve import CANDIDATES_SCHEMA, RESOLVER_VERSION
from cross_country_fantasyxc_review import (
    FantasyXCReviewError,
    REVIEW_SCHEMA,
    create_review_template,
    promote_review,
)
from cross_country_results_identity import load_history


class CrossCountryFantasyXCReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.logical_date = date(2026, 11, 27)
        self.candidates = self.root / "candidates.json"
        self.review = self.root / "review.json"
        self.output = self.root / "announced-rosters.json"
        self.men = self.root / "men.csv"
        self.ladies = self.root / "ladies.csv"
        self._write_history(self.men, [("1", "Iivo Niskanen", "Finland")])
        self._write_history(
            self.ladies,
            [
                ("10", "Jasmi Joensuu", "Finland"),
                ("11", "Astrid Oeyre Slind", "Norway"),
            ],
        )
        self.candidates.write_text(
            json.dumps(
                {
                    "schema_version": CANDIDATES_SCHEMA,
                    "resolver_version": RESOLVER_VERSION,
                    "status": "candidate_review",
                    "logical_date": self.logical_date.isoformat(),
                    "publication": {"status": "disabled"},
                    "source": {"path": "feed.json", "sha256": "abc"},
                    "candidates": [
                        {
                            "candidate_id": "candidate:one",
                            "sex": "L",
                            "canonical_athlete_id": "10",
                            "canonical_name": "Jasmi Joensuu",
                            "suggested_status": "confirmed",
                            "race_ids": ["100", "101"],
                            "source_url": "https://x.com/FantasyXC/status/1",
                            "observed_at": "2026-11-23T20:00:00Z",
                            "evidence_text": "Jasmi Joensuu (10K+s)",
                            "identity_match_method": "exact_canonical_name_tokens",
                            "confidence": "high",
                        },
                        {
                            "candidate_id": "candidate:two",
                            "sex": "L",
                            "canonical_athlete_id": "11",
                            "canonical_name": "Astrid Oeyre Slind",
                            "suggested_status": "excluded",
                            "race_ids": ["*"],
                            "source_url": "https://x.com/FantasyXC/status/2",
                            "observed_at": "2026-11-23T20:01:00Z",
                            "evidence_text": "Astrid will miss Ruka",
                            "identity_match_method": "exact_canonical_name_tokens",
                            "confidence": "high",
                        },
                    ],
                    "review_exceptions": [
                        {
                            "exception_id": "exception:one",
                            "reason": "unknown athlete line",
                            "evidence_text": "New Skier (10K)",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _write_history(path: Path, athletes: list[tuple[str, str, str]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["ID", "Skier", "Nation", "Date"],
            )
            writer.writeheader()
            for athlete_id, name, nation in athletes:
                writer.writerow(
                    {
                        "ID": athlete_id,
                        "Skier": name,
                        "Nation": nation,
                        "Date": "2026-03-01",
                    }
                )

    def _completed_review(self) -> dict[str, object]:
        review = create_review_template(
            candidates_path=self.candidates,
            logical_date=self.logical_date,
            output_path=self.review,
        )
        review["status"] = "reviewed"
        review["reviewer"] = "operator@example.test"
        review["reviewed_at"] = "2026-11-23T22:00:00Z"
        review["candidate_decisions"][0]["decision"] = "accepted"
        review["candidate_decisions"][1].update(
            {"decision": "rejected", "note": "Conflicting federation report"}
        )
        review["exception_decisions"][0].update(
            {"decision": "deferred", "note": "No canonical identity yet"}
        )
        self.review.write_text(json.dumps(review), encoding="utf-8")
        return review

    def _promote(self) -> dict[str, object]:
        return promote_review(
            candidates_path=self.candidates,
            review_path=self.review,
            men_chrono=self.men,
            ladies_chrono=self.ladies,
            logical_date=self.logical_date,
            output_path=self.output,
        )

    def test_template_contains_every_candidate_and_exception(self) -> None:
        review = create_review_template(
            candidates_path=self.candidates,
            logical_date=self.logical_date,
            output_path=self.review,
        )
        self.assertEqual(REVIEW_SCHEMA, review["schema_version"])
        self.assertEqual("draft_review", review["status"])
        self.assertEqual({"status": "disabled"}, review["publication"])
        self.assertEqual(2, len(review["candidate_decisions"]))
        self.assertEqual(1, len(review["exception_decisions"]))
        self.assertTrue(review["candidate_source"]["sha256"])

    def test_review_output_cannot_overwrite_candidate_input(self) -> None:
        with self.assertRaisesRegex(FantasyXCReviewError, "cannot overwrite"):
            create_review_template(
                candidates_path=self.candidates,
                logical_date=self.logical_date,
                output_path=self.candidates,
            )

    def test_completed_review_emits_existing_reviewed_contract(self) -> None:
        self._completed_review()
        output = self._promote()

        self.assertEqual("reviewed", output["status"])
        self.assertEqual({"status": "disabled"}, output["publication"])
        self.assertEqual(1, output["review"]["accepted"])
        self.assertEqual(1, output["review"]["rejected"])
        self.assertEqual("10", output["entries"][0]["canonical_athlete_id"])
        athletes, _index, _names = load_history(self.men, self.ladies)
        selections = load_announced_rosters(
            self.output,
            logical_date=self.logical_date,
            athletes=athletes,
        )
        self.assertEqual("confirmed", selections[("L", "100", "10")].status)

    def test_candidate_checksum_change_invalidates_review(self) -> None:
        self._completed_review()
        document = json.loads(self.candidates.read_text())
        document["candidates"][0]["race_ids"] = ["999"]
        self.candidates.write_text(json.dumps(document))
        with self.assertRaisesRegex(FantasyXCReviewError, "not bound"):
            self._promote()

    def test_pending_review_cannot_promote(self) -> None:
        create_review_template(
            candidates_path=self.candidates,
            logical_date=self.logical_date,
            output_path=self.review,
        )
        with self.assertRaisesRegex(FantasyXCReviewError, "status must be reviewed"):
            self._promote()

    def test_every_exception_requires_a_review_note(self) -> None:
        review = self._completed_review()
        review["exception_decisions"][0]["note"] = ""
        self.review.write_text(json.dumps(review))
        with self.assertRaisesRegex(FantasyXCReviewError, "requires a note"):
            self._promote()

    def test_rejected_candidate_requires_a_review_note(self) -> None:
        review = self._completed_review()
        review["candidate_decisions"][1]["note"] = ""
        self.review.write_text(json.dumps(review))
        with self.assertRaisesRegex(FantasyXCReviewError, "rejected candidate"):
            self._promote()


if __name__ == "__main__":
    unittest.main()
