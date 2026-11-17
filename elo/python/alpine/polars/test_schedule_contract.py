from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from schedule_contract import (
    RACES_COLUMNS,
    WEEKENDS_COLUMNS,
    ScheduleContractError,
    build_shadow,
    derive_weekends,
    read_csv,
    validate_relationship,
    write_csv_atomic,
)


ROOT = Path(__file__).resolve().parent
ACCEPTED_RACES = ROOT / "excel365/races.csv"
ACCEPTED_WEEKENDS = ROOT / "excel365/weekends.csv"


class ScheduleContractTests(unittest.TestCase):
    def test_current_weekends_are_exactly_derived_from_current_races(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        expected = read_csv(ACCEPTED_WEEKENDS, WEEKENDS_COLUMNS)
        self.assertEqual(expected, derive_weekends(races))
        self.assertEqual(
            {"status": "passed", "rows": len(races)},
            validate_relationship(races, expected),
        )

    def test_shadow_weekends_reproduce_accepted_bytes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-weekend-bytes-") as text:
            output_root = Path(text)
            build_shadow(ACCEPTED_RACES, ACCEPTED_RACES, output_root)
            generated = output_root / "candidate" / "weekends.csv"
            self.assertEqual(
                ACCEPTED_WEEKENDS.read_bytes(),
                generated.read_bytes(),
            )

    def test_identical_shadow_pair_is_eligible_without_promotion(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-schedule-shadow-") as text:
            output_root = Path(text)
            result = build_shadow(ACCEPTED_RACES, ACCEPTED_RACES, output_root)
            self.assertEqual("eligible", result["promotion"]["status"])
            self.assertFalse(result["promotion"]["performed"])
            self.assertEqual([], result["comparison"]["added"])
            self.assertEqual([], result["comparison"]["removed"])
            report = json.loads(
                (output_root / "reports/schedule-shadow.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(result["candidate"], report["candidate"])

    def test_large_candidate_shrinkage_blocks_promotion(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        with tempfile.TemporaryDirectory(prefix="alpine-schedule-shrink-") as text:
            root = Path(text)
            candidate = root / "candidate.csv"
            write_csv_atomic(candidate, RACES_COLUMNS, races[:10])
            result = build_shadow(candidate, ACCEPTED_RACES, root / "output")
            self.assertEqual("blocked", result["promotion"]["status"])
            self.assertFalse(result["comparison"]["within_removal_limit"])

    def test_relationship_rejects_changed_canonical_fields(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        weekends = derive_weekends(races)
        weekends[0]["City"] = "Wrong City"
        with self.assertRaisesRegex(
            ScheduleContractError, "no corresponding race|changes canonical"
        ):
            validate_relationship(races, weekends)

    def test_relationship_rejects_arbitrary_window_dates(self) -> None:
        races = read_csv(ACCEPTED_RACES, RACES_COLUMNS)
        weekends = derive_weekends(races)
        weekends[1]["Date"] = weekends[1]["Race_Date"]
        with self.assertRaisesRegex(
            ScheduleContractError, "deterministic view"
        ):
            validate_relationship(races, weekends)

    def test_csv_contract_rejects_column_drift(self) -> None:
        with tempfile.TemporaryDirectory(prefix="alpine-schedule-schema-") as text:
            path = Path(text) / "bad.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Date", "Unexpected"])
                writer.writerow(["01/01/2026", "value"])
            with self.assertRaisesRegex(ScheduleContractError, "unexpected columns"):
                read_csv(path, RACES_COLUMNS)


if __name__ == "__main__":
    unittest.main()
