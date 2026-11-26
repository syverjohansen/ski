from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from unittest.mock import patch

import biathlon_promotion_apply as apply
import pytest
from biathlon_promotion_review import REVIEW_SCHEMA
from biathlon_runtime_shadow import fingerprint_tree

RUN_ID = "hosted-proof"
EVENT_ID = "BT2526SWRLCP01"


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def artifact_receipt(path: Path, root: Path) -> dict[str, object]:
    receipt: dict[str, object] = {
        "file": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": apply._sha256(path),
    }
    if path.suffix == ".csv":
        receipt["rows"] = apply._csv_rows(path)
    return receipt


def prepare(root: Path) -> tuple[Path, Path, Path]:
    accepted_root = root / "accepted"
    individual = accepted_root / "individual"
    relay = accepted_root / "relay"
    review_dir = root / "review"
    individual.mkdir(parents=True)
    relay.mkdir(parents=True)
    (individual / "untouched.txt").write_text("keep\n", encoding="utf-8")
    (relay / "untouched.txt").write_text("keep\n", encoding="utf-8")

    derived_mapping = apply._expected_full_mapping()
    target_plan: list[dict[str, object]] = []
    for artifact, target in sorted(derived_mapping.items()):
        policy, filename = target.split("/", 1)
        accepted = individual if policy == "individual" else relay
        write_csv(
            accepted / filename, ["kind", "value"], [{"kind": "old", "value": filename}]
        )
        write_csv(
            review_dir / artifact,
            ["kind", "value"],
            [{"kind": "new", "value": filename}],
        )
        target_plan.append(
            {
                "artifact": artifact,
                "operation": "replace_full_artifact",
                "targets": [target],
            }
        )

    schedule_fields = ["Date", "RaceType", "Sex", "Race", "City"]
    write_csv(
        individual / "races2026.csv",
        schedule_fields,
        [
            {
                "Date": "12/05/2025",
                "RaceType": "Sprint",
                "Sex": "M",
                "Race": 1,
                "City": "Old",
            },
            {
                "Date": "12/12/2025",
                "RaceType": "Sprint",
                "Sex": "M",
                "Race": 2,
                "City": "Keep",
            },
        ],
    )
    schedule_artifact = "runtime_evidence/candidates/schedule_candidate.csv"
    write_csv(
        review_dir / schedule_artifact,
        schedule_fields,
        [
            {
                "Date": "12/05/2025",
                "RaceType": "Sprint",
                "Sex": "M",
                "Race": 1,
                "City": "New",
            }
        ],
    )
    target_plan.append(
        {
            "artifact": "candidates/schedule_candidate.csv",
            "operation": "merge_event_rows",
            "targets": ["individual/races2026.csv"],
        }
    )

    result_fields = ["Date", "RaceType", "Sex", "Race", "ID", "Leg", "Age"]
    for sex, label in (("M", "men"), ("L", "ladies")):
        for accepted, race in ((individual, 1), (relay, 9)):
            write_csv(
                accepted / f"{label}_scrape_update.csv",
                result_fields,
                [
                    {
                        "Date": "2025-12-05",
                        "RaceType": "Sprint",
                        "Sex": sex,
                        "Race": race,
                        "ID": f"{sex}1",
                        "Leg": 0,
                        "Age": 24,
                    },
                    {
                        "Date": "2025-12-12",
                        "RaceType": "Sprint",
                        "Sex": sex,
                        "Race": race + 1,
                        "ID": f"{sex}2",
                        "Leg": 0,
                        "Age": 25,
                    },
                ],
            )
        artifact = f"runtime_evidence/candidates/{label}_results_candidate.csv"
        write_csv(
            review_dir / artifact,
            result_fields,
            [
                {
                    "Date": "2025-12-05",
                    "RaceType": "Sprint",
                    "Sex": sex,
                    "Race": 1,
                    "ID": f"{sex}1",
                    "Leg": 0,
                    "Age": 24.5,
                }
            ],
        )
        target_plan.append(
            {
                "artifact": f"candidates/{label}_results_candidate.csv",
                "operation": "merge_event_rows",
                "targets": [
                    f"individual/{label}_scrape_update.csv",
                    f"relay/{label}_scrape_update.csv",
                ],
            }
        )

    team_fields = ["Date", "RaceType", "Sex", "Race", "Nation", "Place"]
    write_csv(
        relay / "men_relay_chrono.csv",
        team_fields,
        [
            {
                "Date": "2025-12-05",
                "RaceType": "Relay",
                "Sex": "M",
                "Race": 7,
                "Nation": "Norway",
                "Place": 2,
            },
            {
                "Date": "2025-12-20",
                "RaceType": "Relay",
                "Sex": "M",
                "Race": 8,
                "Nation": "Sweden",
                "Place": 1,
            },
        ],
    )
    team_artifact = "runtime_evidence/relay_team_parity/team_candidates/men_relay.csv"
    write_csv(
        review_dir / team_artifact,
        team_fields,
        [
            {
                "Date": "2025-12-05",
                "RaceType": "Relay",
                "Sex": "M",
                "Race": 1,
                "Nation": "Norway",
                "Place": 1,
            }
        ],
    )
    target_plan.append(
        {
            "artifact": "relay_team_parity/team_candidates/men_relay.csv",
            "operation": "merge_event_rows",
            "targets": ["relay/men_relay_chrono.csv"],
        }
    )

    identity = review_dir / "runtime_evidence/candidates/identity_map.csv"
    write_csv(identity, ["status"], [{"status": "resolved"}])
    rating = (
        review_dir / "runtime_evidence/elo_parity/event_ratings/relay_M_overall.csv"
    )
    write_csv(rating, ["ID", "Elo"], [{"ID": "M1", "Elo": 1400}])
    target_plan.extend(
        [
            {
                "artifact": "candidates/identity_map.csv",
                "operation": "evidence_only",
                "targets": [],
            },
            {
                "artifact": "elo_parity/event_ratings/relay_M_overall.csv",
                "operation": "evidence_only",
                "targets": [],
            },
        ]
    )
    report = {
        "schema_version": REVIEW_SCHEMA,
        "status": "promotion_review_candidate",
        "run_id": RUN_ID,
        "event_id": EVENT_ID,
        "season_id": "2526",
        "atomic_candidate_set_complete": True,
        "pending_materialization": [],
        "remaining_blockers": [],
        "apply_ready": True,
        "accepted_files_modified": False,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
        "accepted_current": {
            "individual": fingerprint_tree(individual),
            "relay": fingerprint_tree(relay),
        },
        "target_plan": target_plan,
        "artifacts": [
            artifact_receipt(path, review_dir)
            for path in sorted(review_dir.rglob("*"))
            if path.is_file()
        ],
    }
    report_path = review_dir / "review_report.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return individual, relay, report_path


def options(
    root: Path, individual: Path, relay: Path, report: Path
) -> dict[str, object]:
    return {
        "review_report": report,
        "expected_report_sha256": apply._sha256(report),
        "individual_dir": individual,
        "relay_dir": relay,
        "approve_run_id": RUN_ID,
        "approve_event_id": EVENT_ID,
        "transaction_root": root / "transactions",
        "backup_root": root / "backups",
        "receipt_path": root / "receipts" / "application.json",
    }


def accepted_bytes(individual: Path, relay: Path) -> dict[str, bytes]:
    return {
        f"individual/{path.name}": path.read_bytes()
        for path in individual.iterdir()
        if path.is_file()
    } | {
        f"relay/{path.name}": path.read_bytes()
        for path in relay.iterdir()
        if path.is_file()
    }


def test_applies_complete_review_and_writes_receipt(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    result = apply.apply_review(**options(tmp_path, individual, relay, report))

    assert result["performed"] is True
    assert result["publication"] == {"status": "disabled"}
    assert len(result["targets"]) == 34
    assert Path(result["receipt"]).is_file()
    assert result["backup"]["file_count"] == 34
    assert (individual / "untouched.txt").read_text() == "keep\n"
    assert rows(individual / "races2026.csv")[0]["City"] == "New"
    assert rows(individual / "men_scrape_update.csv")[0]["Age"] == "24.5"
    assert rows(relay / "men_scrape_update.csv")[0]["Race"] == "9"
    assert rows(relay / "men_relay_chrono.csv")[0]["Race"] == "7"
    assert all(
        rows((individual if policy == "individual" else relay) / filename)[0]["kind"]
        == "new"
        for policy, filename in (target.split("/", 1) for target in apply.FULL_TARGETS)
    )


def test_rejects_wrong_review_hash(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    arguments = options(tmp_path, individual, relay, report)
    arguments["expected_report_sha256"] = "0" * 64
    with pytest.raises(apply.ApplyError, match="review report SHA-256 mismatch"):
        apply.apply_review(**arguments)


def test_rejects_approval_identity_mismatch(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    arguments = options(tmp_path, individual, relay, report)
    arguments["approve_event_id"] = "wrong"
    with pytest.raises(apply.ApplyError, match="approved event ID"):
        apply.apply_review(**arguments)


def test_rejects_accepted_tree_drift(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    (individual / "untouched.txt").write_text("changed\n", encoding="utf-8")
    with pytest.raises(apply.ApplyError, match="trees changed after review"):
        apply.apply_review(**options(tmp_path, individual, relay, report))


def test_rejects_candidate_artifact_drift(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    candidate = report.parent / "derived_candidates/full_ratings/individual/M.csv"
    candidate.write_text("kind,value\ntampered,M.csv\n", encoding="utf-8")
    with pytest.raises(apply.ApplyError, match="artifact receipt drifted"):
        apply.apply_review(**options(tmp_path, individual, relay, report))


def test_rejects_incomplete_target_plan(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    payload = json.loads(report.read_text())
    payload["target_plan"] = payload["target_plan"][1:]
    report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    with pytest.raises(apply.ApplyError, match="full-replacement plan"):
        apply.apply_review(**options(tmp_path, individual, relay, report))


def test_partial_replace_failure_restores_both_trees(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    before = accepted_bytes(individual, relay)
    real_replace = os.replace
    calls = 0

    def fail_once(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated replacement failure")
        real_replace(source, destination)

    with (
        patch("biathlon_promotion_apply.os.replace", side_effect=fail_once),
        pytest.raises(apply.ApplyError, match="trees restored"),
    ):
        apply.apply_review(**options(tmp_path, individual, relay, report))
    assert accepted_bytes(individual, relay) == before


def test_receipt_failure_restores_both_trees(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    before = accepted_bytes(individual, relay)
    with (
        patch("biathlon_promotion_apply._atomic_json", side_effect=OSError("receipt")),
        pytest.raises(apply.ApplyError, match="trees restored"),
    ):
        apply.apply_review(**options(tmp_path, individual, relay, report))
    assert accepted_bytes(individual, relay) == before


def test_external_work_paths_cannot_be_inside_accepted_data(tmp_path: Path) -> None:
    individual, relay, report = prepare(tmp_path)
    arguments = options(tmp_path, individual, relay, report)
    arguments["transaction_root"] = individual / "transactions"
    with pytest.raises(apply.ApplyError, match="transaction root cannot be inside"):
        apply.apply_review(**arguments)


def test_mixed_team_merge_does_not_require_sex_column(tmp_path: Path) -> None:
    fields = ["Date", "RaceType", "Race", "Nation", "Place"]
    target = tmp_path / "mixed_relay_chrono.csv"
    source = tmp_path / "mixed_relay.csv"
    write_csv(
        target,
        fields,
        [
            {
                "Date": "2025-12-05",
                "RaceType": "Mixed Relay",
                "Race": 7,
                "Nation": "Norway",
                "Place": 2,
            }
        ],
    )
    write_csv(
        source,
        fields,
        [
            {
                "Date": "2025-12-05",
                "RaceType": "Mixed Relay",
                "Race": 1,
                "Nation": "Norway",
                "Place": 1,
            }
        ],
    )

    apply._merge_event_rows(source, target, "relay/mixed_relay_chrono.csv")

    assert rows(target) == [
        {
            "Date": "2025-12-05",
            "RaceType": "Mixed Relay",
            "Race": "7",
            "Nation": "Norway",
            "Place": "1",
        }
    ]
