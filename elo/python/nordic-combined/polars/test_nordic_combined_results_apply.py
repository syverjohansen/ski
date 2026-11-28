from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from nordic_combined_results_apply import (
    PromotionError,
    apply_promotion,
    application_lock,
    parse_args,
)
from nordic_combined_results_promote import PROMOTION_SCHEMA


FILES = (
    "men_chrono.csv", "ladies_chrono.csv", "men_team_chrono.csv",
    "team_sprint_chrono.csv",
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def prepare(root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    accepted = root / "accepted"
    candidate = root / "candidate"
    accepted.mkdir()
    candidate.mkdir()
    for name in ("men_chrono.csv", "ladies_chrono.csv"):
        write_csv(accepted / name, ["value"], [{"value": f"old-{name}"}])
        write_csv(candidate / name, ["value"], [
            {"value": f"old-{name}"}, {"value": f"new-{name}"},
        ])
    fields = ["Date", "Nation", "Place", "Sex", "TeamSize", "MemberIDs", "TeamMembers"]
    existing = [
        {
            "Date": "2026-02-01", "Nation": "Norway", "Place": 1, "Sex": "M",
            # Accepted legacy evidence can be incomplete; it must be preserved
            # exactly without weakening validation of newly added teams.
            "TeamSize": 1, "MemberIDs": "1", "TeamMembers": "A",
        },
        {
            "Date": "2026-02-02", "Nation": "Austria", "Place": 1, "Sex": "Mixed",
            "TeamSize": 2, "MemberIDs": "3,4", "TeamMembers": "E,F",
        },
    ]
    write_csv(accepted / "team_sprint_chrono.csv", fields, existing)
    write_csv(candidate / "team_sprint_chrono.csv", fields, [
        *existing,
        {
            "Date": "2026-03-07", "Nation": "Germany", "Place": 1, "Sex": "Mixed",
            "TeamSize": 2, "MemberIDs": "10,20", "TeamMembers": "C,D",
        },
    ])
    write_csv(accepted / "men_team_chrono.csv", fields, existing)
    write_csv(candidate / "men_team_chrono.csv", fields, existing)
    report = {
        "schema_version": PROMOTION_SCHEMA,
        "status": "promotion_candidate", "season": 2026,
        "event": {
            "date": "2026-03-07", "sex": "Mixed", "event_kind": "team_sprint", "race": "1",
            "race_type": "Team Sprint", "championship": "0",
        },
        "row_deltas": {
            "men_chrono.csv": 1, "ladies_chrono.csv": 1,
            "men_team_chrono.csv": 0,
            "team_sprint_chrono.csv": 1,
        },
        "atomic_candidate_set": True, "accepted_files_modified": False,
        "application_performed": False, "mapped_member_rows": 2, "mapped_team_rows": 1,
        "accepted_before": {
            name: {"file": str(accepted / name), "sha256": digest(accepted / name)}
            for name in FILES
        },
        "outputs": {
            name: {
                "file": name,
                "rows": 3 if name == "team_sprint_chrono.csv" else 2,
                "sha256": digest(candidate / name),
            }
            for name in FILES
        },
    }
    report_path = candidate / "report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return accepted, report_path


def args(root: Path, accepted: Path, report: Path) -> dict[str, object]:
    return {
        "promotion_report": report,
        "expected_report_sha256": digest(report),
        "accepted_dir": accepted,
        "approve_season": 2026,
        "backup_root": root / "backups",
        "receipt_path": root / "receipts/application.json",
    }


def prepare_individual(root: Path) -> tuple[Path, Path]:
    accepted, report_path = prepare(root)
    candidate = report_path.parent
    write_csv(candidate / "ladies_chrono.csv", ["value"], [{"value": "old-ladies_chrono.csv"}])
    fields = ["Date", "Nation", "Place", "Sex", "TeamSize", "MemberIDs", "TeamMembers"]
    team_rows = [
        {
            "Date": "2026-02-01", "Nation": "Norway", "Place": 1, "Sex": "M",
            "TeamSize": 1, "MemberIDs": "1", "TeamMembers": "A",
        },
        {
            "Date": "2026-02-02", "Nation": "Austria", "Place": 1, "Sex": "Mixed",
            "TeamSize": 2, "MemberIDs": "3,4", "TeamMembers": "E,F",
        },
    ]
    write_csv(candidate / "team_sprint_chrono.csv", fields, team_rows)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report.update(
        event={
            "date": "2026-03-08", "sex": "M", "event_kind": "individual", "race": "1",
            "race_type": "Individual", "championship": "0",
        },
        mapped_member_rows=1,
        mapped_team_rows=0,
        row_deltas={
            "men_chrono.csv": 1, "ladies_chrono.csv": 0,
            "men_team_chrono.csv": 0,
            "team_sprint_chrono.csv": 0,
        },
    )
    report["outputs"] = {
        name: {"file": name, "rows": sum(1 for _ in csv.DictReader((candidate / name).open())), "sha256": digest(candidate / name)}
        for name in FILES
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return accepted, report_path


def prepare_team(root: Path) -> tuple[Path, Path]:
    accepted, report_path = prepare(root)
    candidate = report_path.parent
    write_csv(
        candidate / "men_chrono.csv", ["value"],
        [{"value": "old-men_chrono.csv"}, *({"value": f"member-{i}"} for i in range(4))],
    )
    write_csv(candidate / "ladies_chrono.csv", ["value"], [{"value": "old-ladies_chrono.csv"}])
    fields = ["Date", "Nation", "Place", "Sex", "TeamSize", "MemberIDs", "TeamMembers"]
    existing = list(csv.DictReader((accepted / "men_team_chrono.csv").open()))
    write_csv(candidate / "men_team_chrono.csv", fields, [
        *existing,
        {
            "Date": "2026-03-08", "Nation": "Austria", "Place": 1, "Sex": "M",
            "TeamSize": 4, "MemberIDs": "10,11,12,13", "TeamMembers": "A,B,C,D",
        },
    ])
    write_csv(
        candidate / "team_sprint_chrono.csv", fields,
        list(csv.DictReader((accepted / "team_sprint_chrono.csv").open())),
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report.update(
        event={
            "date": "2026-03-08", "sex": "M", "event_kind": "team", "race": "1",
            "race_type": "Team", "championship": "0",
        },
        mapped_member_rows=4,
        mapped_team_rows=1,
        row_deltas={
            "men_chrono.csv": 4, "ladies_chrono.csv": 0,
            "men_team_chrono.csv": 1, "team_sprint_chrono.csv": 0,
        },
    )
    report["outputs"] = {
        name: {
            "file": name,
            "rows": sum(1 for _ in csv.DictReader((candidate / name).open())),
            "sha256": digest(candidate / name),
        }
        for name in FILES
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return accepted, report_path


def test_applies_all_histories_with_backup_and_receipt(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    before = {name: digest(accepted / name) for name in FILES}
    result = apply_promotion(**args(tmp_path, accepted, report))
    assert result["performed"] is True
    assert result["before"] == before
    assert result["after"] == {name: digest(report.parent / name) for name in FILES}
    assert Path(result["backup"]["directory"]).is_dir()
    assert Path(result["receipt"]).is_file()


def test_applies_regular_individual_candidate_without_team_changes(tmp_path: Path) -> None:
    accepted, report = prepare_individual(tmp_path)
    team_before = digest(accepted / "team_sprint_chrono.csv")
    ladies_before = digest(accepted / "ladies_chrono.csv")
    result = apply_promotion(**args(tmp_path, accepted, report))
    assert result["performed"] is True
    assert digest(accepted / "team_sprint_chrono.csv") == team_before
    assert digest(accepted / "ladies_chrono.csv") == ladies_before
    assert digest(accepted / "men_chrono.csv") == digest(report.parent / "men_chrono.csv")


def test_applies_same_sex_team_candidate_atomically(tmp_path: Path) -> None:
    accepted, report = prepare_team(tmp_path)
    ladies_before = digest(accepted / "ladies_chrono.csv")
    sprint_before = digest(accepted / "team_sprint_chrono.csv")
    result = apply_promotion(**args(tmp_path, accepted, report))
    assert result["performed"] is True
    assert digest(accepted / "ladies_chrono.csv") == ladies_before
    assert digest(accepted / "team_sprint_chrono.csv") == sprint_before
    assert digest(accepted / "men_team_chrono.csv") == digest(
        report.parent / "men_team_chrono.csv"
    )


def test_rejects_wrong_report_hash_and_season(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    options = args(tmp_path, accepted, report)
    options["expected_report_sha256"] = "0" * 64
    with pytest.raises(PromotionError, match="report SHA-256 mismatch"):
        apply_promotion(**options)
    options = args(tmp_path, accepted, report)
    options["approve_season"] = 2027
    with pytest.raises(PromotionError, match="does not match candidate season"):
        apply_promotion(**options)


def test_rejects_accepted_and_candidate_drift(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path / "accepted-drift")
    (accepted / "men_chrono.csv").write_text("changed\n", encoding="utf-8")
    with pytest.raises(PromotionError, match="accepted history changed"):
        apply_promotion(**args(tmp_path / "accepted-drift", accepted, report))

    other = tmp_path / "candidate-drift"
    other.mkdir()
    accepted, report = prepare(other)
    (report.parent / "ladies_chrono.csv").write_text("changed\n", encoding="utf-8")
    with pytest.raises(PromotionError, match="candidate checksum mismatch"):
        apply_promotion(**args(other, accepted, report))


def test_partial_replace_failure_restores_every_history(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    before = {name: (accepted / name).read_bytes() for name in FILES}
    real_replace = os.replace
    calls = 0

    def fail_second(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated replacement failure")
        real_replace(source, destination)

    with (
        patch("nordic_combined_results_apply.os.replace", side_effect=fail_second),
        pytest.raises(PromotionError, match="histories restored"),
    ):
        apply_promotion(**args(tmp_path, accepted, report))
    assert {name: (accepted / name).read_bytes() for name in FILES} == before


def test_receipt_failure_restores_every_history(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    before = {name: (accepted / name).read_bytes() for name in FILES}
    with (
        patch("nordic_combined_results_apply._atomic_json", side_effect=OSError("receipt")),
        pytest.raises(PromotionError, match="histories restored"),
    ):
        apply_promotion(**args(tmp_path, accepted, report))
    assert {name: (accepted / name).read_bytes() for name in FILES} == before


def test_concurrent_application_lock_is_rejected(tmp_path: Path) -> None:
    accepted, report = prepare(tmp_path)
    with application_lock(accepted), pytest.raises(PromotionError, match="another results application"):
        apply_promotion(**args(tmp_path, accepted, report))


def test_cli_receipt_maps_to_function_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", [
        "nordic_combined_results_apply.py", "--promotion-report", "report.json",
        "--expected-report-sha256", "0" * 64, "--accepted-dir", "accepted",
        "--approve-season", "2026", "--backup-root", "backups",
        "--receipt", "receipt.json",
    ])
    parsed = vars(parse_args())
    assert parsed["receipt_path"] == Path("receipt.json")
    assert "receipt" not in parsed
