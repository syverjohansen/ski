from __future__ import annotations

import csv
import json
import re
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from promote_schedule import promote_schedule
from schedule_contract import (
    RACES_COLUMNS,
    WEEKENDS_COLUMNS,
    ScheduleContractError,
    build_shadow,
    derive_weekends,
    file_sha256,
    infer_fis_season,
    read_csv,
    write_csv_atomic,
)


ROOT = Path(__file__).resolve().parent
SOURCE_ACCEPTED = ROOT / "excel365"
ACCEPTED_RACES = SOURCE_ACCEPTED / "races2026.csv"


def accepted_2026_rows() -> list[dict[str, str]]:
    with ACCEPTED_RACES.open(newline="", encoding="utf-8-sig") as handle:
        return [
            {**row, "Final_Date": row["Date"]}
            for row in csv.DictReader(handle)
        ]


def shift_season(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    shifted = []
    for row in rows:
        changed = dict(row)
        for field in ("Date", "Final_Date"):
            value = datetime.strptime(row[field], "%m/%d/%Y")
            changed[field] = value.replace(year=value.year + 1).strftime(
                "%m/%d/%Y"
            )
        changed["Startlist"] = re.sub(
            r"raceid=(\d+)",
            lambda match: f"raceid={int(match.group(1)) + 100000}",
            row["Startlist"],
        )
        shifted.append(changed)
    return shifted


class PromoteSkiJumpScheduleTests(unittest.TestCase):
    source_rows = accepted_2026_rows()
    target_season = infer_fis_season(source_rows) + 1

    def prepare_rollover(self, root: Path) -> tuple[Path, Path, Path]:
        accepted_dir = root / "accepted"
        accepted_dir.mkdir(parents=True)
        write_csv_atomic(
            accepted_dir / "races.csv",
            RACES_COLUMNS,
            self.source_rows,
        )
        write_csv_atomic(
            accepted_dir / "weekends.csv",
            WEEKENDS_COLUMNS,
            derive_weekends(self.source_rows),
            lineterminator="\n",
        )
        candidate = root / "candidate-races.csv"
        write_csv_atomic(
            candidate,
            RACES_COLUMNS,
            shift_season(read_csv(accepted_dir / "races.csv", RACES_COLUMNS)),
        )
        output_root = root / "shadow"
        result = build_shadow(
            candidate,
            accepted_dir / "races.csv",
            output_root,
            expected_candidate_season=self.target_season,
        )
        self.assertEqual("approval_required", result["promotion"]["status"])
        return accepted_dir, Path(result["report"]), output_root

    def test_approved_rollover_replaces_pair_and_writes_receipt(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-promote-") as text:
            root = Path(text)
            accepted_dir, report, output_root = self.prepare_rollover(root)
            old_races = file_sha256(accepted_dir / "races.csv")
            old_weekends = file_sha256(accepted_dir / "weekends.csv")
            receipt_path = root / "receipts/promotion.json"

            result = promote_schedule(
                report,
                expected_report_sha256=file_sha256(report),
                accepted_dir=accepted_dir,
                approve_season=self.target_season,
                receipt_path=receipt_path,
            )

            self.assertTrue(result["performed"])
            self.assertEqual(self.target_season, result["approved_season"])
            self.assertNotEqual(old_races, file_sha256(accepted_dir / "races.csv"))
            self.assertNotEqual(
                old_weekends,
                file_sha256(accepted_dir / "weekends.csv"),
            )
            self.assertEqual(
                (output_root / "candidate/races.csv").read_bytes(),
                (accepted_dir / "races.csv").read_bytes(),
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            backup = Path(receipt["backup"]["directory"])
            self.assertEqual(old_races, file_sha256(backup / "races.csv"))
            self.assertEqual(old_weekends, file_sha256(backup / "weekends.csv"))

    def test_wrong_report_hash_is_rejected_without_changes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-promote-hash-") as text:
            root = Path(text)
            accepted_dir, report, _ = self.prepare_rollover(root)
            before = (accepted_dir / "races.csv").read_bytes()
            with self.assertRaisesRegex(ScheduleContractError, "report SHA-256"):
                promote_schedule(
                    report,
                    expected_report_sha256="0" * 64,
                    accepted_dir=accepted_dir,
                    approve_season=self.target_season,
                    receipt_path=root / "receipt.json",
                )
            self.assertEqual(before, (accepted_dir / "races.csv").read_bytes())

    def test_wrong_season_approval_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-promote-season-") as text:
            root = Path(text)
            accepted_dir, report, _ = self.prepare_rollover(root)
            with self.assertRaisesRegex(ScheduleContractError, "does not match"):
                promote_schedule(
                    report,
                    expected_report_sha256=file_sha256(report),
                    accepted_dir=accepted_dir,
                    approve_season=self.target_season + 1,
                    receipt_path=root / "receipt.json",
                )

    def test_changed_inputs_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-promote-drift-") as text:
            root = Path(text)
            accepted_dir, report, output_root = self.prepare_rollover(root)
            with (accepted_dir / "races.csv").open("ab") as handle:
                handle.write(b"\n")
            with self.assertRaisesRegex(ScheduleContractError, "changed"):
                promote_schedule(
                    report,
                    expected_report_sha256=file_sha256(report),
                    accepted_dir=accepted_dir,
                    approve_season=self.target_season,
                    receipt_path=root / "receipt.json",
                )

            accepted_dir, report, output_root = self.prepare_rollover(
                root / "second"
            )
            with (output_root / "candidate/races.csv").open("ab") as handle:
                handle.write(b"\n")
            with self.assertRaisesRegex(
                ScheduleContractError, "candidate races SHA-256"
            ):
                promote_schedule(
                    report,
                    expected_report_sha256=file_sha256(report),
                    accepted_dir=accepted_dir,
                    approve_season=self.target_season,
                    receipt_path=root / "receipt.json",
                )

    def test_partial_replace_failure_restores_pair(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-promote-rollback-") as text:
            root = Path(text)
            accepted_dir, report, _ = self.prepare_rollover(root)
            before_races = (accepted_dir / "races.csv").read_bytes()
            before_weekends = (accepted_dir / "weekends.csv").read_bytes()
            real_replace = __import__("os").replace
            replace_calls = 0

            def fail_second_replace(source: Path, destination: Path) -> None:
                nonlocal replace_calls
                replace_calls += 1
                if replace_calls == 2:
                    raise OSError("simulated second replacement failure")
                real_replace(source, destination)

            with (
                patch("promote_schedule.os.replace", side_effect=fail_second_replace),
                self.assertRaisesRegex(ScheduleContractError, "restored"),
            ):
                promote_schedule(
                    report,
                    expected_report_sha256=file_sha256(report),
                    accepted_dir=accepted_dir,
                    approve_season=self.target_season,
                    receipt_path=root / "receipt.json",
                )
            self.assertEqual(before_races, (accepted_dir / "races.csv").read_bytes())
            self.assertEqual(
                before_weekends,
                (accepted_dir / "weekends.csv").read_bytes(),
            )

    def test_receipt_failure_restores_pair(self) -> None:
        with tempfile.TemporaryDirectory(prefix="skijump-promote-receipt-") as text:
            root = Path(text)
            accepted_dir, report, _ = self.prepare_rollover(root)
            before_races = (accepted_dir / "races.csv").read_bytes()
            before_weekends = (accepted_dir / "weekends.csv").read_bytes()
            with (
                patch(
                    "promote_schedule.write_json_atomic",
                    side_effect=OSError("simulated receipt failure"),
                ),
                self.assertRaisesRegex(ScheduleContractError, "pair restored"),
            ):
                promote_schedule(
                    report,
                    expected_report_sha256=file_sha256(report),
                    accepted_dir=accepted_dir,
                    approve_season=self.target_season,
                    receipt_path=root / "receipt.json",
                )
            self.assertEqual(before_races, (accepted_dir / "races.csv").read_bytes())
            self.assertEqual(
                before_weekends,
                (accepted_dir / "weekends.csv").read_bytes(),
            )


if __name__ == "__main__":
    unittest.main()
