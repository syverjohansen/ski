import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from cross_country_final_climb_contract import (
    SCHEMA_VERSION,
    build_final_climb_plan,
    main,
)
from cross_country_tour_contract import TourContractError, read_schedule

FIXTURE = (
    Path(__file__).parent
    / "tests"
    / "fixtures"
    / "cross_country_tour_contract"
    / "2027-tour.csv"
)


class CrossCountryFinalClimbContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = read_schedule(FIXTURE)

    def test_day_before_is_ready_with_both_official_startlists(self) -> None:
        plan = build_final_climb_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 9),
            available_startlist_race_ids=("52129", "52128"),
        )
        self.assertEqual(SCHEMA_VERSION, plan.schema_version)
        self.assertEqual("ready", plan.status)
        self.assertEqual("2027-01-10", plan.final_climb_date)
        self.assertEqual("2027-01-09", plan.publication_window_start)
        self.assertEqual(("52129", "52128"), plan.startlist_race_ids)
        self.assertEqual((), plan.missing_startlist_race_ids)
        self.assertEqual("official_final_climb_startlists", plan.startlist_mode)
        self.assertEqual({"M", "L"}, {event["sex"] for event in plan.events})
        self.assertTrue(all(event["final_climb"] == "1" for event in plan.events))
        self.assertFalse(plan.publication_enabled)

    def test_race_date_retries_when_list_was_not_available_day_before(self) -> None:
        waiting = build_final_climb_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 9),
            available_startlist_race_ids=("52129",),
        )
        ready = build_final_climb_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 10),
            available_startlist_race_ids=("52129", "52128"),
        )
        self.assertEqual("waiting_for_startlists", waiting.status)
        self.assertEqual(("52128",), waiting.missing_startlist_race_ids)
        self.assertEqual("ready", ready.status)

    def test_outside_publication_window_is_idempotent_no_work(self) -> None:
        first = build_final_climb_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 8),
        )
        second = build_final_climb_plan(
            schedule_rows=self.rows,
            logical_date=date(2027, 1, 8),
            available_startlist_race_ids=("52129", "52128"),
        )
        self.assertEqual("no_work", first.status)
        self.assertEqual("not_applicable", first.startlist_mode)
        self.assertEqual(first.to_dict(), second.to_dict())

    def test_schedule_without_tour_is_no_work(self) -> None:
        regular = dict(self.rows[0], Period="1", Stage="0", Final_Climb="0")
        plan = build_final_climb_plan(
            schedule_rows=[regular],
            logical_date=date(2027, 1, 9),
        )
        self.assertEqual("no_work", plan.status)
        self.assertIsNone(plan.final_climb_date)
        self.assertEqual(0, plan.event_count)

    def test_invalid_final_climb_markers_fail_closed(self) -> None:
        rows = [dict(row) for row in self.rows]
        ladies = next(
            row for row in rows if row["Final_Climb"] == "1" and row["Sex"] == "L"
        )
        ladies["Final_Climb"] = "0"
        with self.assertRaisesRegex(TourContractError, "last-stage events"):
            build_final_climb_plan(
                schedule_rows=rows,
                logical_date=date(2027, 1, 9),
            )

    def test_cli_writes_versioned_report_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "final-climb-plan.json"
            with patch(
                "sys.argv",
                [
                    "cross_country_final_climb_contract.py",
                    "--schedule",
                    str(FIXTURE),
                    "--logical-date",
                    "2027-01-09",
                    "--available-startlist-race-id",
                    "52129",
                    "--available-startlist-race-id",
                    "52128",
                    "--report",
                    str(report),
                ],
            ):
                self.assertEqual(0, main())
            loaded = json.loads(report.read_text(encoding="utf-8"))
        self.assertEqual("tour_de_ski_final_climb", loaded["product"])
        self.assertEqual("ready", loaded["status"])
        self.assertEqual("ski", loaded["acquisition_owner"])
        self.assertEqual("blog", loaded["publication_owner"])


if __name__ == "__main__":
    unittest.main()
