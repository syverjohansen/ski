import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_fantasyxc_feed import ITEMS_SCHEMA
from cross_country_fantasyxc_resolve import (
    CANDIDATES_SCHEMA,
    FantasyXCResolveError,
    RESOLVER_VERSION,
    html_lines,
    resolve_feed,
)
from schedule_contract import WEEKENDS_COLUMNS


class CrossCountryFantasyXCResolveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.logical_date = date(2026, 11, 27)
        self.men = self.root / "men.csv"
        self.ladies = self.root / "ladies.csv"
        self.schedule = self.root / "weekends.csv"
        self.feed = self.root / "feed.json"
        self.output = self.root / "candidates.json"
        self.report = self.root / "report.json"
        self._write_history(
            self.men,
            [
                ("1", "Iivo Niskanen", "Finland"),
                ("2", "Johannes Hoesflot Klaebo", "Norway"),
            ],
        )
        self._write_history(
            self.ladies,
            [("10", "Jasmi Joensuu", "Finland"), ("11", "Astrid Oeyre Slind", "Norway")],
        )
        self._write_schedule()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _write_history(path: Path, athletes: list[tuple[str, str, str]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["ID", "Skier", "Nation", "Date"])
            writer.writeheader()
            for athlete_id, name, nation in athletes:
                writer.writerow(
                    {"ID": athlete_id, "Skier": name, "Nation": nation, "Date": "2026-03-01"}
                )

    def _write_schedule(self) -> None:
        rows = []
        for race_id, sex, distance in (
            ("100", "L", "10"),
            ("101", "L", "Sprint"),
            ("102", "M", "10"),
            ("103", "M", "Sprint"),
        ):
            row = dict.fromkeys(WEEKENDS_COLUMNS, "0")
            row.update(
                {
                    "Date": "11/27/2026",
                    "Race_Date": "11/27/2026",
                    "Startlist": "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=" + race_id,
                    "Sex": sex,
                    "City": "Ruka",
                    "Country": "Finland",
                    "Distance": distance,
                    "Technique": "C",
                    "Stage": "0",
                }
            )
            rows.append(row)
        with self.schedule.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=WEEKENDS_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def _write_feed(self, items: list[dict[str, str]]) -> None:
        self.feed.write_text(
            json.dumps(
                {
                    "schema_version": ITEMS_SCHEMA,
                    "status": "candidate_review",
                    "logical_date": self.logical_date.isoformat(),
                    "as_of": "2026-11-23",
                    "publication": {"status": "disabled"},
                    "source": {"url": "https://fxtwitter.com/FantasyXC/feed.xml", "sha256": "abc"},
                    "items": items,
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _item(status_id: str, published: str, title: str, html: str) -> dict[str, str]:
        return {
            "status_id": status_id,
            "source_url": f"https://x.com/FantasyXC/status/{status_id}",
            "published_at": published,
            "title": title,
            "description_html": html,
        }

    def _resolve(self) -> dict[str, object]:
        return resolve_feed(
            feed_items=self.feed,
            men_chrono=self.men,
            ladies_chrono=self.ladies,
            schedule=self.schedule,
            logical_date=self.logical_date,
            output_path=self.output,
            report_path=self.report,
        )

    def test_html_is_split_into_evidence_lines(self) -> None:
        self.assertEqual(
            ["Women:", "Jasmi Joensuu (10K+s)"],
            html_lines("<p>Women:<br />Jasmi Joensuu (10K+s)</p>"),
        )

    def test_exact_names_and_annotations_create_review_candidates(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1000",
                    "2026-11-23T20:00:00Z",
                    "Finland has announced its Ruka team",
                    "<p>Women:<br>Jasmi Joensuu (10K+s)</p>",
                ),
                self._item(
                    "1001",
                    "2026-11-23T20:08:00Z",
                    "Continuation",
                    "<p>Men:<br>Johannes Hoesflot Klaebo (s)</p>",
                ),
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(CANDIDATES_SCHEMA, document["schema_version"])
        self.assertEqual(RESOLVER_VERSION, document["resolver_version"])
        self.assertEqual("candidate_review", document["status"])
        self.assertEqual({"status": "disabled"}, document["publication"])
        self.assertEqual(2, report["candidate_count"])
        self.assertTrue(
            all(
                row["candidate_id"].startswith("candidate:")
                for row in document["candidates"]
            )
        )
        observed = {
            (row["canonical_athlete_id"], tuple(row["race_ids"]), row["suggested_status"])
            for row in document["candidates"]
        }
        self.assertEqual(
            {("10", ("100", "101"), "confirmed"), ("2", ("103",), "confirmed")},
            observed,
        )

    def test_negative_claim_is_excluded_but_injury_only_is_unresolved(self) -> None:
        self._write_feed(
            [
                self._item(
                    "2000",
                    "2026-11-23T20:00:00Z",
                    "Norway roster announced",
                    "<p>Women:<br>Astrid Oeyre Slind will miss Ruka</p>"
                    "<p>Men:<br>Iivo Niskanen is rehabilitating an injury</p>",
                )
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(1, report["candidate_count"])
        self.assertEqual("excluded", document["candidates"][0]["suggested_status"])
        self.assertEqual(["*"], document["candidates"][0]["race_ids"])
        self.assertEqual(1, report["exception_count"])
        self.assertIn("injury prose", document["review_exceptions"][0]["reason"])

    def test_adjacent_annotated_athlete_post_inherits_roster_context(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1500",
                    "2026-11-23T20:00:00Z",
                    "Norway roster announced",
                    "<p>Men:<br>Johannes Hoesflot Klaebo (s)</p>",
                ),
                self._item(
                    "1501",
                    "2026-11-23T20:12:00Z",
                    "Continuation",
                    "<p>Jasmi Joensuu (10K+s)<br>New Skier (10K)</p>",
                ),
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(2, report["candidate_count"])
        jasmi = next(
            row
            for row in document["candidates"]
            if row["canonical_athlete_id"] == "10"
        )
        self.assertEqual("confirmed", jasmi["suggested_status"])
        self.assertEqual(["100", "101"], jasmi["race_ids"])
        self.assertEqual(1, report["exception_count"])
        self.assertEqual("unknown", document["review_exceptions"][0]["sex"])

    def test_unannotated_adjacent_mention_does_not_inherit_roster_context(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1600",
                    "2026-11-23T20:00:00Z",
                    "Norway roster announced",
                    "<p>Men:<br>Johannes Hoesflot Klaebo (s)</p>",
                ),
                self._item(
                    "1601",
                    "2026-11-23T20:12:00Z",
                    "Unrelated update",
                    "<p>Jasmi Joensuu trained today</p>",
                ),
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(1, report["candidate_count"])
        self.assertEqual(1, report["exception_count"])
        self.assertIn(
            "no explicit roster decision",
            document["review_exceptions"][0]["reason"],
        )

    def test_unknown_roster_name_becomes_review_exception(self) -> None:
        self._write_feed(
            [
                self._item(
                    "3000",
                    "2026-11-23T20:00:00Z",
                    "Team selection announced",
                    "<p>Women:<br>Unknown Skier (10K)</p>",
                )
            ]
        )
        report = self._resolve()
        self.assertEqual(0, report["candidate_count"])
        self.assertEqual(1, report["exception_count"])

    def test_conflicting_claims_are_removed_from_candidates(self) -> None:
        self._write_feed(
            [
                self._item(
                    "4000",
                    "2026-11-23T20:00:00Z",
                    "Norway roster announced",
                    "<p>Women:<br>Astrid Oeyre Slind</p>",
                ),
                self._item(
                    "4001",
                    "2026-11-23T21:00:00Z",
                    "Update",
                    "<p>Women:<br>Astrid Oeyre Slind will not travel</p>",
                ),
            ]
        )
        report = self._resolve()
        self.assertEqual(0, report["candidate_count"])
        self.assertEqual(1, report["conflict_count"])

    def test_feed_contract_must_remain_shadow_only(self) -> None:
        self._write_feed([])
        document = json.loads(self.feed.read_text())
        document["publication"]["status"] = "enabled"
        self.feed.write_text(json.dumps(document))
        with self.assertRaisesRegex(FantasyXCResolveError, "publication"):
            self._resolve()


if __name__ == "__main__":
    unittest.main()
