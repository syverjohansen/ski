from __future__ import annotations

import re
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

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
SOURCE = ROOT / "excel365"
ACCEPTED_RACES = SOURCE / "races2026.csv"
ACCEPTED_SEASON = infer_fis_season(read_csv(ACCEPTED_RACES, RACES_COLUMNS))


def shift_season(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    shifted = []
    for row in rows:
        changed = dict(row)
        value = datetime.strptime(row["Date"], "%m/%d/%Y")
        changed["Date"] = value.replace(year=value.year + 1).strftime("%m/%d/%Y")
        changed["Startlist"] = re.sub(
            r"raceid=(\d+)",
            lambda match: f"raceid={int(match.group(1)) + 100000}",
            row["Startlist"],
        )
        shifted.append(changed)
    return shifted


class PromoteNordicCombinedScheduleTests(unittest.TestCase):
    target_season = ACCEPTED_SEASON + 1

    def prepare(self, root: Path, *, rollover: bool) -> tuple[Path, Path, Path]:
        accepted = root / "accepted"
        accepted.mkdir(parents=True)
        (accepted / "races.csv").write_bytes(ACCEPTED_RACES.read_bytes())
        accepted_rows = read_csv(accepted / "races.csv", RACES_COLUMNS)
        write_csv_atomic(
            accepted / "weekends.csv",
            WEEKENDS_COLUMNS,
            derive_weekends(accepted_rows),
            lineterminator="\n",
        )
        rows = read_csv(accepted / "races.csv", RACES_COLUMNS)
        season = ACCEPTED_SEASON
        if rollover:
            rows = shift_season(rows)
            season = self.target_season
        else:
            rows[0] = {**rows[0], "Country": "FIN"}
        candidate = root / "candidate.csv"
        write_csv_atomic(candidate, RACES_COLUMNS, rows)
        result = build_shadow(
            candidate,
            accepted / "races.csv",
            root / "shadow",
            expected_candidate_season=season,
        )
        expected = "approval_required" if rollover else "eligible"
        self.assertEqual(expected, result["promotion"]["status"])
        return accepted, Path(result["report"]), root / "shadow"

    def test_approved_rollover_replaces_pair_and_writes_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accepted, report, shadow = self.prepare(root, rollover=True)
            before = file_sha256(accepted / "races.csv")
            result = promote_schedule(
                report,
                expected_report_sha256=file_sha256(report),
                accepted_dir=accepted,
                approve_season=self.target_season,
                receipt_path=root / "receipt.json",
            )
            self.assertEqual("next_season", result["transition"])
            self.assertNotEqual(before, file_sha256(accepted / "races.csv"))
            self.assertEqual((shadow / "candidate/races.csv").read_bytes(), (accepted / "races.csv").read_bytes())

    def test_eligible_same_season_update_is_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accepted, report, shadow = self.prepare(root, rollover=False)
            result = promote_schedule(
                report,
                expected_report_sha256=file_sha256(report),
                accepted_dir=accepted,
                approve_season=ACCEPTED_SEASON,
                receipt_path=root / "receipt.json",
            )
            self.assertEqual("same_season", result["transition"])
            self.assertEqual((shadow / "candidate/weekends.csv").read_bytes(), (accepted / "weekends.csv").read_bytes())

    def test_hash_or_accepted_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accepted, report, _ = self.prepare(root, rollover=True)
            with self.assertRaisesRegex(ScheduleContractError, "report SHA-256"):
                promote_schedule(
                    report,
                    expected_report_sha256="0" * 64,
                    accepted_dir=accepted,
                    approve_season=self.target_season,
                    receipt_path=root / "receipt.json",
                )
            with (accepted / "races.csv").open("ab") as handle:
                handle.write(b"\n")
            with self.assertRaisesRegex(ScheduleContractError, "changed"):
                promote_schedule(
                    report,
                    expected_report_sha256=file_sha256(report),
                    accepted_dir=accepted,
                    approve_season=self.target_season,
                    receipt_path=root / "receipt.json",
                )


if __name__ == "__main__":
    unittest.main()
