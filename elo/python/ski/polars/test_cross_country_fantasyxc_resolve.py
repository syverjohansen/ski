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

    def _write_schedule(self, *, city: str = "Ruka") -> None:
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
                    "City": city,
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
                    "Norway roster for Ruka announced",
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
                    "Norway roster for Ruka announced",
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

    def test_current_roster_intro_phrases_confirm_exact_names(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1510",
                    "2026-11-23T20:00:00Z",
                    "The Italian squad to Ruka consists of",
                    "<p>Men:<br>Iivo Niskanen</p>",
                ),
                self._item(
                    "1511",
                    "2026-11-23T21:00:00Z",
                    "Representing Team Sweden in Ruka are",
                    "<p>Women:<br>Jasmi Joensuu</p>",
                ),
                self._item(
                    "1512",
                    "2026-11-23T22:00:00Z",
                    "Switzerland is presenting these athletes for Ruka",
                    "<p>Men:<br>Johannes Hoesflot Klaebo</p>",
                ),
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(3, report["candidate_count"])
        self.assertEqual(0, report["exception_count"])
        self.assertEqual(
            {"1", "2", "10"},
            {row["canonical_athlete_id"] for row in document["candidates"]},
        )

    def test_candidates_are_isolated_to_the_scheduled_weekend_venue(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1515",
                    "2026-11-23T20:00:00Z",
                    "Norway roster for Ruka announced",
                    "<p>Men:<br>Iivo Niskanen</p>",
                ),
                self._item(
                    "1516",
                    "2026-11-23T21:00:00Z",
                    "Finland roster for Trondheim announced",
                    "<p>Women:<br>Jasmi Joensuu</p>",
                ),
            ]
        )

        ruka_report = self._resolve()
        ruka = json.loads(self.output.read_text(encoding="utf-8"))
        self.assertEqual(["Ruka"], ruka["weekend_context"]["venues"])
        self.assertEqual(1, ruka_report["candidate_count"])
        self.assertEqual(1, ruka_report["ignored_item_count"])
        self.assertEqual("1", ruka["candidates"][0]["canonical_athlete_id"])

        self._write_schedule(city="Trondheim")
        trondheim_report = self._resolve()
        trondheim = json.loads(self.output.read_text(encoding="utf-8"))
        self.assertEqual(["Trondheim"], trondheim["weekend_context"]["venues"])
        self.assertEqual(1, trondheim_report["candidate_count"])
        self.assertEqual(1, trondheim_report["ignored_item_count"])
        self.assertEqual("10", trondheim["candidates"][0]["canonical_athlete_id"])

    def test_adjacent_name_only_list_inherits_roster_context(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1520",
                    "2026-11-23T20:00:00Z",
                    "Finland team selection for Ruka announced",
                    "<p>Women:<br>Jasmi Joensuu</p>",
                ),
                self._item(
                    "1521",
                    "2026-11-23T20:08:00Z",
                    "Continuation",
                    "<p>Iivo Niskanen</p>",
                ),
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(2, report["candidate_count"])
        self.assertEqual(0, report["exception_count"])
        iivo = next(
            row
            for row in document["candidates"]
            if row["canonical_athlete_id"] == "1"
        )
        self.assertEqual("confirmed", iivo["suggested_status"])
        self.assertEqual(["*"], iivo["race_ids"])

    def test_distinct_names_share_explicit_list_or_negative_decision(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1530",
                    "2026-11-23T20:00:00Z",
                    "Finland roster for Ruka announced",
                    "<p>Men:<br>Iivo Niskanen, Johannes Hoesflot Klaebo</p>",
                ),
                self._item(
                    "1531",
                    "2026-11-23T21:00:00Z",
                    "Ruka update",
                    "<p>Jasmi Joensuu and Astrid Oeyre Slind are out sick "
                    "this weekend</p>",
                ),
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(4, report["candidate_count"])
        self.assertEqual(0, report["exception_count"])
        statuses = {
            row["canonical_athlete_id"]: row["suggested_status"]
            for row in document["candidates"]
        }
        self.assertEqual("confirmed", statuses["1"])
        self.assertEqual("confirmed", statuses["2"])
        self.assertEqual("excluded", statuses["10"])
        self.assertEqual("excluded", statuses["11"])

    def test_athlete_present_as_coaching_staff_is_excluded(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1540",
                    "2026-11-23T20:00:00Z",
                    "Ruka update",
                    "<p>Iivo Niskanen will be present as part of the coaching "
                    "staff.</p>",
                )
            ]
        )
        report = self._resolve()
        document = json.loads(self.output.read_text(encoding="utf-8"))

        self.assertEqual(1, report["candidate_count"])
        self.assertEqual(0, report["exception_count"])
        self.assertEqual("excluded", document["candidates"][0]["suggested_status"])

    def test_unannotated_adjacent_mention_does_not_inherit_roster_context(self) -> None:
        self._write_feed(
            [
                self._item(
                    "1600",
                    "2026-11-23T20:00:00Z",
                    "Norway roster for Ruka announced",
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
                    "Ruka team selection announced",
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
                    "Norway roster for Ruka announced",
                    "<p>Women:<br>Astrid Oeyre Slind</p>",
                ),
                self._item(
                    "4001",
                    "2026-11-23T21:00:00Z",
                    "Ruka update",
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
