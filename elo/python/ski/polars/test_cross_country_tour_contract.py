import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from cross_country_tour_contract import (
    SCHEMA_VERSION,
    TourContractError,
    build_tour_plan,
    main,
    read_schedule,
)

FIXTURE = (
    Path(__file__).parent
    / "tests"
    / "fixtures"
    / "cross_country_tour_contract"
    / "2027-tour.csv"
)


class CrossCountryTourContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = read_schedule(FIXTURE)

    def test_2027_tour_is_ready_with_both_opening_startlists(self) -> None:
        plan = build_tour_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 1),
            available_startlist_race_ids=("52111", "52110"),
        )
        self.assertEqual(SCHEMA_VERSION, plan.schema_version)
        self.assertEqual("ready", plan.status)
        self.assertEqual("official_opening_startlists", plan.startlist_mode)
        self.assertEqual(8, plan.stage_count)
        self.assertEqual(16, plan.event_count)
        self.assertEqual(2, plan.opening_event_count)
        self.assertEqual(("52111", "52110"), plan.opening_startlist_race_ids)
        self.assertEqual((), plan.missing_startlist_race_ids)
        self.assertEqual("2027-01-10", plan.tour_end_date)
        self.assertEqual(2, sum(e["final_climb"] == "1" for e in plan.stage_events))
        self.assertFalse(plan.publication_enabled)

    def test_missing_opening_list_waits_without_requiring_future_stages(self) -> None:
        plan = build_tour_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 1),
            available_startlist_race_ids=("52111",),
        )
        self.assertEqual("waiting_for_startlists", plan.status)
        self.assertEqual("not_yet_published", plan.startlist_mode)
        self.assertEqual(("52110",), plan.missing_startlist_race_ids)
        self.assertNotIn("52115", plan.opening_startlist_race_ids)

    def test_non_opening_date_is_an_idempotent_no_op(self) -> None:
        first = build_tour_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 2),
            available_startlist_race_ids=(),
        )
        second = build_tour_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 2),
            available_startlist_race_ids=("52111", "52110"),
        )
        self.assertEqual("no_work", first.status)
        self.assertEqual("not_applicable", first.startlist_mode)
        self.assertEqual(first.to_dict(), second.to_dict())
        self.assertEqual(
            json.dumps(first.to_dict(), sort_keys=True),
            json.dumps(second.to_dict(), sort_keys=True),
        )

    def test_schedule_without_tour_is_no_work(self) -> None:
        regular = dict(self.rows[0], Period="1", Stage="0", Final_Climb="0")
        plan = build_tour_plan(
            schedule_rows=[regular],
            logical_date=date(2027, 1, 1),
        )
        self.assertEqual("no_work", plan.status)
        self.assertEqual(0, plan.event_count)
        self.assertIsNone(plan.tour_start_date)

    def test_period_and_stage_markers_must_agree(self) -> None:
        rows = [dict(row) for row in self.rows]
        rows[0]["Stage"] = "0"
        with self.assertRaisesRegex(TourContractError, "markers disagree"):
            build_tour_plan(
                schedule_rows=rows,
                logical_date=date(2027, 1, 1),
            )

    def test_duplicate_fis_race_id_fails_closed(self) -> None:
        rows = [dict(row) for row in self.rows]
        rows[1]["Startlist"] = rows[0]["Startlist"]
        with self.assertRaisesRegex(TourContractError, "duplicate"):
            build_tour_plan(
                schedule_rows=rows,
                logical_date=date(2027, 1, 1),
            )

    def test_cli_writes_the_versioned_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "tour-plan.json"
            with patch(
                "sys.argv",
                [
                    "cross_country_tour_contract.py",
                    "--schedule",
                    str(FIXTURE),
                    "--logical-date",
                    "2027-01-01",
                    "--available-startlist-race-id",
                    "52111",
                    "--available-startlist-race-id",
                    "52110",
                    "--report",
                    str(report),
                ],
            ):
                self.assertEqual(0, main())
            loaded = json.loads(report.read_text(encoding="utf-8"))
        self.assertEqual("tour_de_ski_overall", loaded["product"])
        self.assertEqual("ski", loaded["acquisition_owner"])
        self.assertEqual("blog", loaded["publication_owner"])


if __name__ == "__main__":
    unittest.main()
