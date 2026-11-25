from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from skijump_results_apply import PromotionError, apply_promotion, parse_args
from skijump_results_promote import PROMOTION_SCHEMA


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def prepare(root: Path) -> tuple[Path, Path]:
    accepted = root / "accepted"
    candidate = root / "candidate"
    accepted.mkdir()
    candidate.mkdir()
    for name in ("men_chrono.csv", "ladies_chrono.csv"):
        write_csv(accepted / name, ["value"], [{"value": f"old-{name}"}])
        write_csv(candidate / name, ["value"], [{"value": f"new-{name}"}])
    write_csv(accepted / "mixed_team_chrono.csv", ["value"], [{"value": "old"}])
    mixed_fields = [
        "Date",
        "Nation",
        "Place",
        "Sex",
        "TeamSize",
        "MemberIDs",
        "TeamMembers",
        "MemberSex",
    ]
    write_csv(
        candidate / "mixed_team_chrono.csv",
        mixed_fields,
        [
            {
                "Date": "2025-11-21",
                "Nation": "Norway",
                "Place": 1,
                "Sex": "Mixed",
                "TeamSize": 4,
                "MemberIDs": "1,2,3,4",
                "TeamMembers": "A,B,C,D",
                "MemberSex": "M,M,L,L",
            }
        ],
    )
    names = ("men_chrono.csv", "ladies_chrono.csv", "mixed_team_chrono.csv")
    report = {
        "schema_version": PROMOTION_SCHEMA,
        "status": "promotion_candidate",
        "season": 2026,
        "accepted_files_modified": False,
        "atomic_candidate_set": True,
        "eligible_mixed_team_dates": ["2025-11-21"],
        "accepted_before": {
            name: {"file": str(accepted / name), "sha256": digest(accepted / name)}
            for name in names
        },
        "outputs": {
            name: {"file": name, "sha256": digest(candidate / name), "rows": 1}
            for name in names
        },
    }
    report_path = candidate / "report.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return accepted, report_path


def args(root: Path, accepted: Path, report: Path) -> dict[str, object]:
    return {
        "promotion_report": report,
        "expected_report_sha256": digest(report),
        "accepted_dir": accepted,
        "approve_season": 2026,
        "backup_root": root / "backups",
        "receipt_path": root / "receipts" / "application.json",
    }


def test_applies_all_histories_and_writes_receipt(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    before = {
        name: digest(accepted / name)
        for name in ("men_chrono.csv", "ladies_chrono.csv", "mixed_team_chrono.csv")
    }
    result = apply_promotion(**args(tmp_path, accepted, report))
    assert result["performed"] is True
    assert result["before"] == before
    assert result["after"] == {name: digest(report.parent / name) for name in before}
    assert result["promotion_evidence"]["eligible_mixed_team_dates"] == ["2025-11-21"]
    assert Path(result["receipt"]).is_file()
    assert Path(result["backup"]["directory"]).is_dir()


def test_wrong_report_hash_is_rejected(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    options = args(tmp_path, accepted, report)
    options["expected_report_sha256"] = "0" * 64
    with pytest.raises(PromotionError, match="report SHA-256 mismatch"):
        apply_promotion(**options)


def test_cli_receipt_flag_maps_to_function_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "skijump_results_apply.py",
            "--promotion-report",
            "report.json",
            "--expected-report-sha256",
            "0" * 64,
            "--accepted-dir",
            "accepted",
            "--approve-season",
            "2026",
            "--backup-root",
            "backups",
            "--receipt",
            "receipt.json",
        ],
    )
    parsed = vars(parse_args())
    assert parsed["receipt_path"] == Path("receipt.json")
    assert "receipt" not in parsed


def test_accepted_drift_is_rejected(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    (accepted / "men_chrono.csv").write_bytes(b"changed\n")
    with pytest.raises(PromotionError, match="accepted history changed"):
        apply_promotion(**args(tmp_path, accepted, report))


def test_candidate_drift_is_rejected(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    (report.parent / "ladies_chrono.csv").write_bytes(b"changed\n")
    with pytest.raises(PromotionError, match="candidate checksum mismatch"):
        apply_promotion(**args(tmp_path, accepted, report))


def test_partial_replace_failure_restores_all_histories(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    before = {
        name: (accepted / name).read_bytes()
        for name in ("men_chrono.csv", "ladies_chrono.csv", "mixed_team_chrono.csv")
    }
    real_replace = os.replace
    calls = 0

    def fail_second(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated replacement failure")
        real_replace(source, destination)

    with (
        patch("skijump_results_apply.os.replace", side_effect=fail_second),
        pytest.raises(PromotionError, match="histories restored"),
    ):
        apply_promotion(**args(tmp_path, accepted, report))
    assert {name: (accepted / name).read_bytes() for name in before} == before


def test_receipt_failure_restores_all_histories(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    before = {
        name: (accepted / name).read_bytes()
        for name in ("men_chrono.csv", "ladies_chrono.csv", "mixed_team_chrono.csv")
    }
    with (
        patch("skijump_results_apply._atomic_json", side_effect=OSError("receipt")),
        pytest.raises(PromotionError, match="histories restored"),
    ):
        apply_promotion(**args(tmp_path, accepted, report))
    assert {name: (accepted / name).read_bytes() for name in before} == before
