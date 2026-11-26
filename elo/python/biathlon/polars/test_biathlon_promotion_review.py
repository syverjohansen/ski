from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import biathlon_promotion_review as review
import pytest
from biathlon_runtime_shadow import fingerprint_tree

COMMIT = "a" * 40
RACE_ID = "BT2526SWRLCP01SWSP"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def canonical_sha(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def fixture(tmp_path: Path) -> tuple[Path, Path, Path, str]:
    individual = tmp_path / "accepted" / "individual"
    relay = tmp_path / "accepted" / "relay"
    individual.mkdir(parents=True)
    relay.mkdir(parents=True)
    (individual / "accepted.csv").write_text("id,elo\n1,1400\n", encoding="utf-8")
    (relay / "accepted.csv").write_text("id,elo\n1,1400\n", encoding="utf-8")
    runtime_dir = tmp_path / "runtime"

    results = {"Results": [{"IBUId": "BTNOR1"}]}
    reports = {"Reports": [{"Description": "Start List"}]}
    write_json(runtime_dir / "evidence" / "raw" / f"{RACE_ID}-results.json", results)
    write_json(runtime_dir / "evidence" / "raw" / f"{RACE_ID}-reports.json", reports)
    write_json(
        runtime_dir / "evidence" / "report.json",
        {
            "schema_version": "nordicnumbers.biathlon-ibu-source/v1",
            "status": "passed",
            "season_id": "2526",
            "event_id": "BT2526SWRLCP01",
            "competition_count": 1,
            "races": [
                {
                    "race_id": RACE_ID,
                    "results_sha256": canonical_sha(results),
                    "reports_sha256": canonical_sha(reports),
                }
            ],
            "accepted_data_mutated": False,
            "publication": {"status": "disabled"},
        },
    )

    candidate_row = {"Date": "2025-12-05", "Race": "1", "ID": "1"}
    schedule_row = {"Date": "12/05/2025", "Race": "1"}
    identity_row = {"race_id": RACE_ID, "local_id": "1", "status": "resolved"}
    write_csv(runtime_dir / "candidates" / "schedule_candidate.csv", [schedule_row])
    write_csv(runtime_dir / "candidates" / "men_results_candidate.csv", [candidate_row])
    write_csv(
        runtime_dir / "candidates" / "ladies_results_candidate.csv", [candidate_row]
    )
    write_csv(
        runtime_dir / "candidates" / "identity_map.csv", [identity_row, identity_row]
    )
    write_json(
        runtime_dir / "candidates" / "parity_report.json",
        {
            "schema_version": "nordicnumbers.biathlon-official-adapter/v1",
            "status": "passed",
            "season_id": "2526",
            "event_id": "BT2526SWRLCP01",
            "schedule_candidate_rows": 1,
            "result_parity": {"M": {"candidate_rows": 1}, "L": {"candidate_rows": 1}},
            "identity": {"resolved": 2, "unresolved": 0},
            "accepted_data_mutated": False,
            "publication": {"status": "disabled"},
        },
    )

    rating_row = {"Date": "2025-12-05", "ID": "1", "Elo": "1400"}
    for name in (
        "relay_M_overall.csv",
        "relay_L_overall.csv",
        "individual_M_sprint.csv",
    ):
        write_csv(runtime_dir / "elo_parity" / "event_ratings" / name, [rating_row])
    write_json(
        runtime_dir / "elo_parity" / "elo_parity_report.json",
        {
            "schema_version": "nordicnumbers.biathlon-elo-parity/v1",
            "status": "passed",
            "accepted_data_mutated": False,
            "publication": {"status": "disabled"},
        },
    )

    write_csv(
        runtime_dir / "relay_team_parity" / "team_candidates" / "mixed_relay.csv",
        [{"Date": "2025-12-05", "Nation": "Norway", "Team_Elo": "2800"}],
    )
    write_json(
        runtime_dir / "relay_team_parity" / "relay_team_parity_report.json",
        {
            "schema_version": "nordicnumbers.biathlon-relay-team-parity/v1",
            "status": "passed",
            "accepted_data_mutated": False,
            "publication": {"status": "disabled"},
        },
    )

    fingerprints = {
        "individual": fingerprint_tree(individual),
        "relay": fingerprint_tree(relay),
    }
    runtime = {
        "schema_version": "nordicnumbers.biathlon-runtime-shadow/v1",
        "status": "passed",
        "run_id": "hosted-proof",
        "season_id": "2526",
        "event_id": "BT2526SWRLCP01",
        "race_ids": [RACE_ID],
        "code_commit": COMMIT,
        "stages": [{"name": name, "status": "passed"} for name, _, _ in review.STAGES],
        "accepted_fingerprints": {"before": fingerprints, "after": fingerprints},
        "accepted_data_mutated": False,
        "promotion": {"status": "disabled"},
        "publication": {"status": "disabled"},
    }
    report_path = runtime_dir / "runtime_report.json"
    write_json(report_path, runtime)
    return runtime_dir, individual, relay, review._sha256(report_path)


def derived_fixture(
    tmp_path: Path, runtime_dir: Path, individual: Path, relay: Path
) -> tuple[Path, str]:
    derived = tmp_path / "derived"
    policies: dict[str, object] = {}
    for policy in ("individual", "relay"):
        policy_report: dict[str, object] = {}
        for sex in ("M", "L"):
            streams: dict[str, object] = {}
            names = {
                "overall": f"{sex}.csv",
                "individual": f"{sex}_Individual.csv",
                "sprint": f"{sex}_Sprint.csv",
                "pursuit": f"{sex}_Pursuit.csv",
                "mass_start": f"{sex}_Mass_Start.csv",
            }
            for stream, name in names.items():
                path = derived / "full_ratings" / policy / name
                write_csv(path, [{"ID": "1", "Elo": "1400"}])
                streams[stream] = {
                    "status": "passed",
                    "candidate": {
                        "file": path.relative_to(derived).as_posix(),
                        "rows": 1,
                        "bytes": path.stat().st_size,
                        "sha256": review._sha256(path),
                    },
                }
            policy_report[sex] = {"streams": streams}
        for section, suffix in (
            ("chronos", "chrono.csv"),
            ("elevation_chronos", "chrono_elevation.csv"),
        ):
            comparisons: dict[str, object] = {}
            root = "full_chronos" if section == "chronos" else "full_elevation_chronos"
            for sex, label in (("M", "men"), ("L", "ladies")):
                path = derived / root / policy / f"{label}_{suffix}"
                write_csv(path, [{"ID": "1", "Elo": "1400"}])
                comparisons[sex] = {
                    "status": "passed",
                    "candidate": {
                        "file": path.relative_to(derived).as_posix(),
                        "rows": 1,
                        "bytes": path.stat().st_size,
                        "sha256": review._sha256(path),
                    },
                }
            policy_report[section] = comparisons
        policies[policy] = policy_report

    accepted = {
        "individual": fingerprint_tree(individual),
        "relay": fingerprint_tree(relay),
    }
    source = fingerprint_tree(runtime_dir / "candidates")
    report = {
        "schema_version": review.DERIVED_SCHEMA,
        "status": "passed",
        "policies": policies,
        "source_candidate_fingerprints": {"before": source, "after": source},
        "source_candidates_mutated": False,
        "accepted_fingerprints": {"before": accepted, "after": accepted},
        "accepted_data_mutated": False,
        "apply_ready": False,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    report_path = derived / review.DERIVED_REPORT
    write_json(report_path, report)
    return derived, review._sha256(report_path)


def build(tmp_path: Path, **overrides: object) -> dict[str, object]:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    arguments: dict[str, object] = {
        "runtime_dir": runtime_dir,
        "expected_report_sha256": report_sha,
        "expected_code_commit": COMMIT,
        "individual_dir": individual,
        "relay_dir": relay,
        "output_dir": tmp_path / "review",
    }
    arguments.update(overrides)
    return review.build_review_candidate(**arguments)  # type: ignore[arg-type]


def test_builds_isolated_checksummed_review_bundle(tmp_path: Path) -> None:
    report = build(tmp_path)

    assert report["status"] == "promotion_review_candidate"
    assert report["apply_ready"] is False
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}
    assert report["pending_materialization"]
    assert all(item["sha256"] for item in report["artifacts"])
    targets = {target for item in report["target_plan"] for target in item["targets"]}
    assert "individual/races2026.csv" in targets
    assert "relay/mixed_relay_chrono.csv" in targets
    assert (tmp_path / "review" / "review_report.json").is_file()


def test_integrates_complete_derived_atomic_candidate_set(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    derived, derived_sha = derived_fixture(tmp_path, runtime_dir, individual, relay)

    report = review.build_review_candidate(
        runtime_dir=runtime_dir,
        expected_report_sha256=report_sha,
        expected_code_commit=COMMIT,
        individual_dir=individual,
        relay_dir=relay,
        output_dir=tmp_path / "review",
        derived_candidate_dir=derived,
        expected_derived_report_sha256=derived_sha,
    )

    assert report["atomic_candidate_set_complete"] is True
    assert report["apply_ready"] is True
    assert report["pending_materialization"] == []
    assert report["remaining_blockers"] == []
    derived_plan = [
        item
        for item in report["target_plan"]
        if item["operation"] == "replace_full_artifact"
    ]
    assert len(derived_plan) == 28
    assert len({item["targets"][0] for item in derived_plan}) == 28
    assert (
        len(
            [
                item
                for item in report["artifacts"]
                if item["file"].startswith("derived_candidates/")
            ]
        )
        == 29
    )
    event_ratings = [
        item for item in report["target_plan"] if "/event_ratings/" in item["artifact"]
    ]
    assert event_ratings
    assert all(item["operation"] == "evidence_only" for item in event_ratings)
    assert (
        tmp_path
        / "review"
        / "derived_candidates"
        / "full_elevation_chronos"
        / "relay"
        / "ladies_chrono_elevation.csv"
    ).is_file()


def test_rejects_wrong_derived_report_hash(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    derived, _ = derived_fixture(tmp_path, runtime_dir, individual, relay)
    with pytest.raises(review.ReviewError, match="derived report SHA-256 mismatch"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
            derived_candidate_dir=derived,
            expected_derived_report_sha256="0" * 64,
        )


def test_rejects_missing_derived_report(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    derived = tmp_path / "derived"
    derived.mkdir()
    with pytest.raises(review.ReviewError, match="derived candidate report is missing"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
            derived_candidate_dir=derived,
            expected_derived_report_sha256="0" * 64,
        )


def test_rejects_derived_artifact_tampering(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    derived, derived_sha = derived_fixture(tmp_path, runtime_dir, individual, relay)
    (derived / "full_ratings" / "individual" / "M.csv").write_text(
        "ID,Elo\n1,9999\n", encoding="utf-8"
    )
    with pytest.raises(review.ReviewError, match="artifact receipt drifted"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
            derived_candidate_dir=derived,
            expected_derived_report_sha256=derived_sha,
        )


def test_rejects_derived_source_candidate_mismatch(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    derived, derived_sha = derived_fixture(tmp_path, runtime_dir, individual, relay)
    write_csv(runtime_dir / "candidates" / "extra.csv", [{"unexpected": "candidate"}])
    with pytest.raises(review.ReviewError, match="do not match the retained runtime"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
            derived_candidate_dir=derived,
            expected_derived_report_sha256=derived_sha,
        )


def test_rejects_incomplete_derived_arguments(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    with pytest.raises(review.ReviewError, match="required together"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
            derived_candidate_dir=tmp_path / "derived",
        )


def test_rejects_wrong_runtime_report_hash(tmp_path: Path) -> None:
    runtime_dir, individual, relay, _ = fixture(tmp_path)
    with pytest.raises(review.ReviewError, match="SHA-256 mismatch"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256="0" * 64,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
        )


def test_rejects_accepted_data_drift(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    (individual / "accepted.csv").write_text("id,elo\n1,9999\n", encoding="utf-8")
    with pytest.raises(review.ReviewError, match="accepted individual data drifted"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
        )


def test_rejects_failed_stage_report(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    path = runtime_dir / "elo_parity" / "elo_parity_report.json"
    payload = json.loads(path.read_text())
    payload["status"] = "failed"
    write_json(path, payload)
    with pytest.raises(
        review.ReviewError, match="replay_athlete_ratings report did not pass"
    ):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
        )


def test_rejects_unresolved_identity(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    path = runtime_dir / "candidates" / "parity_report.json"
    payload = json.loads(path.read_text())
    payload["identity"]["unresolved"] = 1
    write_json(path, payload)
    with pytest.raises(review.ReviewError, match="unresolved athlete identities"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
        )


def test_rejects_candidate_row_tampering(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    write_csv(
        runtime_dir / "candidates" / "schedule_candidate.csv",
        [{"Date": "12/05/2025", "Race": "1"}, {"Date": "12/06/2025", "Race": "2"}],
    )
    with pytest.raises(review.ReviewError, match="candidate evidence drifted"):
        review.build_review_candidate(
            runtime_dir=runtime_dir,
            expected_report_sha256=report_sha,
            expected_code_commit=COMMIT,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "review",
        )


def test_output_cannot_be_inside_runtime_or_accepted_tree(tmp_path: Path) -> None:
    runtime_dir, individual, relay, report_sha = fixture(tmp_path)
    for output in (runtime_dir / "review", individual / "review"):
        with pytest.raises(review.ReviewError, match="protected directory"):
            review.build_review_candidate(
                runtime_dir=runtime_dir,
                expected_report_sha256=report_sha,
                expected_code_commit=COMMIT,
                individual_dir=individual,
                relay_dir=relay,
                output_dir=output,
            )
