from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

import biathlon_schedule_promotion as promotion

SEASON = "2627"
YEAR = 2027


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def candidate_rows() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    definitions = [
        ("11/26/2026", "M", 1, 1, "Individual", "20"),
        ("11/27/2026", "L", 1, 1, "Individual", "15"),
        ("12/04/2026", "M", 2, 2, "Sprint", "10"),
        ("12/05/2026", "L", 2, 2, "Sprint", "7.5"),
        ("01/02/2027", "M", 3, 3, "Mass Start", "15"),
        ("01/03/2027", "L", 3, 3, "Mass Start", "12.5"),
        ("01/03/2027", "Mixed", 3, 3, "Mixed Relay", "N/A"),
    ]
    races: list[dict[str, object]] = []
    weekends: list[dict[str, object]] = []
    counters = {"M": 0, "L": 0, "Mixed": 0}
    for index, (day, sex, event_number, period, race_type, distance) in enumerate(
        definitions, start=1
    ):
        counters[sex] += 1
        race_id = f"BT2627SWRLCP{event_number:02d}{'MXRL' if sex == 'Mixed' else ('SMIN' if sex == 'M' and index == 1 else 'SWIN' if sex == 'L' and index == 2 else 'SMSP' if sex == 'M' and index == 3 else 'SWSP' if sex == 'L' and index == 4 else 'SMMS' if sex == 'M' else 'SWMS')}"
        common: dict[str, object] = {
            "Startlist": f"https://www.biathlonworld.com/startlist/{race_id}",
            "Sex": sex,
            "City": f"Venue {event_number}",
            "Country": "Country",
            "Distance": distance,
            "RaceType": race_type,
            "Period": period,
            "Elevation": 100 + event_number,
            "Championship": 0,
        }
        races.append({"Date": day, **common, "Race": counters[sex]})
        weekends.append({"Date": day, "Race_Date": day, **common})
    first_by_event: dict[str, str] = {}
    for row in weekends:
        event_id = str(row["Startlist"]).rsplit("/", 1)[-1][:-4]
        first_by_event[event_id] = min(
            first_by_event.get(event_id, str(row["Race_Date"])),
            str(row["Race_Date"]),
            key=lambda value: datetime.strptime(value, "%m/%d/%Y"),
        )
    for row in weekends:
        event_id = str(row["Startlist"]).rsplit("/", 1)[-1][:-4]
        row["Date"] = first_by_event[event_id]
    races.sort(key=lambda row: str(row["Date"]))
    weekends.sort(key=lambda row: str(row["Race_Date"]))
    # Sorting by MM/DD/YYYY is not chronological across New Year; restore the
    # explicit season order expected by the contract.
    order = {day: index for index, (day, *_rest) in enumerate(definitions)}
    races.sort(key=lambda row: order[str(row["Date"])])
    weekends.sort(key=lambda row: order[str(row["Race_Date"])])
    return races, weekends


def prepare_candidate(root: Path) -> tuple[Path, Path]:
    accepted = root / "accepted"
    write_csv(
        accepted / "races.csv",
        promotion.RACE_FIELDS,
        [
            {
                "Date": "03/22/2026",
                "Startlist": "https://www.biathlonworld.com/startlist/BT2526SWRLCP09SWMS",
                "Sex": "L",
                "City": "Old",
                "Country": "Norway",
                "Distance": "12.5",
                "RaceType": "Mass Start",
                "Period": 5,
                "Elevation": 327,
                "Championship": 0,
                "Race": 31,
            }
        ],
    )
    write_csv(
        accepted / "weekends.csv",
        promotion.WEEKEND_FIELDS,
        [
            {
                "Date": "03/19/2026",
                "Race_Date": "03/22/2026",
                "Startlist": "https://www.biathlonworld.com/startlist/BT2526SWRLCP09SWMS",
                "Sex": "L",
                "City": "Old",
                "Country": "Norway",
                "Distance": "12.5",
                "RaceType": "Mass Start",
                "Period": 5,
                "Elevation": 327,
                "Championship": 0,
            }
        ],
    )
    candidate = root / "candidate"
    candidate.mkdir()
    races, weekends = candidate_rows()
    races_name = f"races{YEAR}_candidate.csv"
    write_csv(candidate / races_name, promotion.RACE_FIELDS, races)
    write_csv(candidate / "weekends_candidate.csv", promotion.WEEKEND_FIELDS, weekends)
    current_hash = promotion._sha256(accepted / "races.csv")
    report = {
        "schema_version": promotion.CANDIDATE_SCHEMA,
        "status": "passed",
        "season_id": SEASON,
        "accepted_schedule": {
            "sha256_before": current_hash,
            "sha256_after": current_hash,
            "mutated": False,
        },
        "candidate_files": {
            races_name: {
                "rows": len(races),
                "sha256": promotion._sha256(candidate / races_name),
            },
            "weekends_candidate.csv": {
                "rows": len(weekends),
                "sha256": promotion._sha256(candidate / "weekends_candidate.csv"),
            },
        },
        "ready_for_review": True,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    report_path = candidate / "candidate_report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return accepted, report_path


def build_review(root: Path) -> tuple[Path, Path]:
    accepted, candidate_report = prepare_candidate(root)
    review_dir = root / "review"
    promotion.build_review(
        candidate_report=candidate_report,
        accepted_dir=accepted,
        output_dir=review_dir,
    )
    return accepted, review_dir / "review_report.json"


def apply_options(root: Path, accepted: Path, review: Path) -> dict[str, object]:
    return {
        "review_report": review,
        "expected_report_sha256": promotion._sha256(review),
        "accepted_dir": accepted,
        "approve_season_id": SEASON,
        "transaction_root": root / "transactions",
        "backup_root": root / "backups",
        "receipt_path": root / "receipts" / "schedule.json",
    }


def accepted_bytes(accepted: Path) -> dict[str, bytes | None]:
    return {
        name: (accepted / name).read_bytes() if (accepted / name).is_file() else None
        for name in ("races.csv", f"races{YEAR}.csv", "weekends.csv")
    }


def test_builds_checksummed_review_with_exact_target_plan(tmp_path: Path) -> None:
    accepted, candidate_report = prepare_candidate(tmp_path)
    output = tmp_path / "review"
    report = promotion.build_review(
        candidate_report=candidate_report,
        accepted_dir=accepted,
        output_dir=output,
    )

    assert report["status"] == "schedule_review_candidate"
    assert report["apply_ready"] is True
    assert report["target_plan"] == {
        "races2027_candidate.csv": ["races.csv", "races2027.csv"],
        "weekends_candidate.csv": ["weekends.csv"],
    }
    assert report["accepted_before"]["races2027.csv"] == {"exists": False}
    assert set(report["artifacts"]) == {
        "races2027_candidate.csv",
        "weekends_candidate.csv",
    }
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}


def test_rejects_candidate_layout_disagreement(tmp_path: Path) -> None:
    accepted, report_path = prepare_candidate(tmp_path)
    weekends = report_path.parent / "weekends_candidate.csv"
    payload = weekends.read_text(encoding="utf-8").replace("Venue 1", "Wrong", 1)
    weekends.write_text(payload, encoding="utf-8")
    report = json.loads(report_path.read_text())
    report["candidate_files"]["weekends_candidate.csv"]["sha256"] = promotion._sha256(
        weekends
    )
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(promotion.SchedulePromotionError, match="layouts disagree"):
        promotion.build_review(
            candidate_report=report_path,
            accepted_dir=accepted,
            output_dir=tmp_path / "review",
        )


def test_applies_all_schedule_targets_and_writes_receipt(tmp_path: Path) -> None:
    accepted, review = build_review(tmp_path)
    result = promotion.apply_review(**apply_options(tmp_path, accepted, review))

    assert result["performed"] is True
    assert result["publication"] == {"status": "disabled"}
    assert result["backup"]["file_count"] == 2
    assert Path(result["receipt"]).is_file()
    assert (accepted / "races.csv").read_bytes() == (
        accepted / "races2027.csv"
    ).read_bytes()
    assert len(read_csv(accepted / "races.csv")) == 7
    assert len(read_csv(accepted / "weekends.csv")) == 7


def test_rejects_wrong_review_hash_or_season(tmp_path: Path) -> None:
    accepted, review = build_review(tmp_path)
    arguments = apply_options(tmp_path, accepted, review)
    arguments["expected_report_sha256"] = "0" * 64
    with pytest.raises(promotion.SchedulePromotionError, match="SHA-256 mismatch"):
        promotion.apply_review(**arguments)
    arguments = apply_options(tmp_path, accepted, review)
    arguments["approve_season_id"] = "2526"
    with pytest.raises(promotion.SchedulePromotionError, match="season ID"):
        promotion.apply_review(**arguments)


def test_rejects_accepted_or_review_artifact_drift(tmp_path: Path) -> None:
    accepted, review = build_review(tmp_path)
    (accepted / "races.csv").write_text("changed\n", encoding="utf-8")
    with pytest.raises(promotion.SchedulePromotionError, match="changed after review"):
        promotion.apply_review(**apply_options(tmp_path, accepted, review))

    other = tmp_path / "other"
    accepted, review = build_review(other)
    artifact = review.parent / "races2027_candidate.csv"
    artifact.write_text("changed\n", encoding="utf-8")
    with pytest.raises(promotion.SchedulePromotionError, match="artifact drifted"):
        promotion.apply_review(**apply_options(other, accepted, review))


def test_partial_replacement_failure_restores_existing_and_absent_targets(
    tmp_path: Path,
) -> None:
    accepted, review = build_review(tmp_path)
    before = accepted_bytes(accepted)
    real_replace = os.replace
    failed = False

    def fail_new_season_once(source: Path, destination: Path) -> None:
        nonlocal failed
        if Path(destination).name == "races2027.csv" and not failed:
            failed = True
            raise OSError("simulated replacement failure")
        real_replace(source, destination)

    with (
        patch(
            "biathlon_schedule_promotion.os.replace", side_effect=fail_new_season_once
        ),
        pytest.raises(promotion.SchedulePromotionError, match="was restored"),
    ):
        promotion.apply_review(**apply_options(tmp_path, accepted, review))
    assert accepted_bytes(accepted) == before


def test_receipt_failure_restores_schedule(tmp_path: Path) -> None:
    accepted, review = build_review(tmp_path)
    before = accepted_bytes(accepted)
    with (
        patch(
            "biathlon_schedule_promotion._atomic_json", side_effect=OSError("receipt")
        ),
        pytest.raises(promotion.SchedulePromotionError, match="was restored"),
    ):
        promotion.apply_review(**apply_options(tmp_path, accepted, review))
    assert accepted_bytes(accepted) == before


def test_external_outputs_cannot_be_inside_accepted_data(tmp_path: Path) -> None:
    accepted, candidate_report = prepare_candidate(tmp_path)
    with pytest.raises(promotion.SchedulePromotionError, match="protected input"):
        promotion.build_review(
            candidate_report=candidate_report,
            accepted_dir=accepted,
            output_dir=accepted / "review",
        )
    accepted, review = build_review(tmp_path / "apply")
    arguments = apply_options(tmp_path / "apply", accepted, review)
    arguments["transaction_root"] = accepted / "transactions"
    with pytest.raises(promotion.SchedulePromotionError, match="inside accepted"):
        promotion.apply_review(**arguments)


def test_cli_subcommands_map_all_parameters(tmp_path: Path) -> None:
    review = promotion.parser().parse_args(
        [
            "review",
            "--candidate-report",
            str(tmp_path / "candidate.json"),
            "--accepted-dir",
            str(tmp_path / "accepted"),
            "--output-dir",
            str(tmp_path / "review"),
        ]
    )
    assert review.command == "review"
    assert review.candidate_report == tmp_path / "candidate.json"
    apply = promotion.parser().parse_args(
        [
            "apply",
            "--review-report",
            str(tmp_path / "review.json"),
            "--expected-report-sha256",
            "a" * 64,
            "--accepted-dir",
            str(tmp_path / "accepted"),
            "--approve-season-id",
            SEASON,
            "--transaction-root",
            str(tmp_path / "tx"),
            "--backup-root",
            str(tmp_path / "backup"),
            "--receipt",
            str(tmp_path / "receipt.json"),
        ]
    )
    assert apply.command == "apply"
    assert apply.approve_season_id == SEASON
