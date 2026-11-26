from __future__ import annotations

import csv
import hashlib
import json
import shutil
from pathlib import Path

import pytest

import biathlon_additive_result_adapter as adapter
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError


HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "tests/fixtures/ibu"
EVENT = "BT2526SWRLCP01"
RACE = f"{EVENT}SWSP"


def load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def semantic_hash(payload) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def build_bundle(tmp_path: Path, *, unknown: bool = False):
    accepted = tmp_path / "accepted"
    source_individual = HERE / "excel365"
    source_relay = HERE / "relay/excel365"
    individual = accepted / "individual"
    relay = accepted / "relay"
    individual.mkdir(parents=True)
    relay.mkdir(parents=True)
    for source, target in ((source_individual, individual), (source_relay, relay)):
        for label in ("men", "ladies"):
            rows = read_rows(source / f"{label}_scrape_update.csv")
            rows = [
                row for row in rows
                if not (row["Date"] == "2025-12-05" and row["RaceType"] == "Sprint")
            ]
            write_rows(target / f"{label}_scrape_update.csv", rows)

    gap = tmp_path / "gap"
    raw = gap / "raw"
    raw.mkdir(parents=True)
    events = [
        row for row in load("events-2526-level1.json")
        if row["EventId"] == EVENT
    ]
    competitions = load("competitions-cp01.json")
    (raw / "events.json").write_text(json.dumps(events), encoding="utf-8")
    (raw / f"{EVENT}-competitions.json").write_text(
        json.dumps(competitions), encoding="utf-8"
    )
    gap_report = {
        "schema_version": "nordicnumbers.biathlon-results-gap/v1",
        "status": "passed",
        "season_id": "2526",
        "source": {
            "event_payload_sha256": semantic_hash(events),
            "competition_payload_sha256": {EVENT: semantic_hash(competitions)},
        },
        "accepted_data_mutated": False,
    }
    (gap / "gap_report.json").write_text(json.dumps(gap_report), encoding="utf-8")

    capture = tmp_path / "capture"
    capture_raw = capture / "raw"
    capture_raw.mkdir(parents=True)
    results = load("results-women-sprint.json")
    if unknown:
        results["Results"][0]["IBUId"] = "BTZZZ10101199901"
        results["Results"][0]["Name"] = "Completely Unknown Athlete"
    (capture_raw / f"{RACE}-results.json").write_text(
        json.dumps(results), encoding="utf-8"
    )
    fingerprints = {
        "schedule": {
            "file": str((source_individual / "races2026.csv").resolve()),
            "sha256": adapter._sha256(source_individual / "races2026.csv"),
        },
        "individual": fingerprint_tree(individual),
        "relay": fingerprint_tree(relay),
    }
    gap_path = gap / "gap_report.json"
    report = {
        "schema_version": adapter.CAPTURE_SCHEMA,
        "status": "passed",
        "season_id": "2526",
        "source": {
            "gap_report": str(gap_path),
            "gap_report_sha256": adapter._sha256(gap_path),
            "api_key_persisted": False,
        },
        "races": [
            {"race_id": RACE, "results_sha256": semantic_hash(results)}
        ],
        "accepted_fingerprints": {
            "before": fingerprints,
            "after": fingerprints,
        },
        "accepted_data_mutated": False,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    (capture / "capture_report.json").write_text(
        json.dumps(report), encoding="utf-8"
    )
    return capture, individual, relay, source_individual / "races2026.csv"


def run(tmp_path: Path, *, unknown: bool = False):
    capture, individual, relay, schedule = build_bundle(tmp_path, unknown=unknown)
    output = tmp_path / "candidate"
    report = adapter.adapt_additive_results(
        capture_dir=capture,
        individual_dir=individual,
        relay_dir=relay,
        schedule_path=schedule,
        output_dir=output,
        aliases_path=HERE / "ibu_identity_aliases.csv",
    )
    return report, output, individual, relay


def test_builds_policy_specific_additive_candidates(tmp_path: Path) -> None:
    report, output, *_ = run(tmp_path)

    assert report["status"] == "passed"
    assert report["identity"]["unresolved"] == 0
    individual = read_rows(output / "individual/ladies_results_candidate.csv")
    relay = read_rows(output / "relay/ladies_results_candidate.csv")
    assert len(individual) == len(relay) == 5
    assert {row["Race"] for row in individual} == {"3"}
    assert {row["Race"] for row in relay} == {"5"}
    assert {row["Date"] for row in individual} == {"2025-12-05"}
    assert report["ready_for_derived_rehearsal"] is True


def test_preserves_accepted_histories_and_disables_application(tmp_path: Path) -> None:
    report, output, individual, relay = run(tmp_path)

    assert report["accepted_data_mutated"] is False
    assert report["accepted_fingerprints"]["before"] == report["accepted_fingerprints"]["after"]
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}
    assert report["accepted_fingerprints"]["after"] == {
        "individual": fingerprint_tree(individual),
        "relay": fingerprint_tree(relay),
    }
    assert b"\r\n" not in (output / "individual/ladies_results_candidate.csv").read_bytes()


def test_unknown_identity_fails_closed_with_review_evidence(tmp_path: Path) -> None:
    report, output, *_ = run(tmp_path, unknown=True)

    assert report["status"] == "failed"
    assert report["ready_for_derived_rehearsal"] is False
    assert report["identity"]["unresolved"] == 1
    identities = read_rows(output / "identity_map.csv")
    assert any(row["status"] == "unresolved" for row in identities)


def test_official_biography_creates_deterministic_debut_identity(
    tmp_path: Path,
) -> None:
    capture, individual, relay, schedule = build_bundle(tmp_path)
    ibu_id = "BTFIN20101200401"
    result_path = capture / "raw" / f"{RACE}-results.json"
    results = json.loads(result_path.read_text(encoding="utf-8"))
    results["Results"][0].update(
        {"IBUId": ibu_id, "Name": "ROOKIE Rita", "Nat": "FIN"}
    )
    result_path.write_text(json.dumps(results), encoding="utf-8")
    bio = {
        "IBUId": ibu_id,
        "GivenName": "Rita",
        "FamilyName": "ROOKIE",
        "NAT": "FIN",
        "Birthdate": "2004-01-01T00:00:00",
        "GenderId": "L",
    }
    bio_path = capture / "raw" / f"{ibu_id}-bio.json"
    bio_path.write_text(json.dumps(bio), encoding="utf-8")
    report_path = capture / "capture_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["races"][0]["results_sha256"] = semantic_hash(results)
    report["biographies"] = {
        "count": 1,
        "sha256": {ibu_id: semantic_hash(bio)},
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")

    output = tmp_path / "candidate"
    adapted = adapter.adapt_additive_results(
        capture_dir=capture,
        individual_dir=individual,
        relay_dir=relay,
        schedule_path=schedule,
        output_dir=output,
        aliases_path=HERE / "ibu_identity_aliases.csv",
    )

    assert adapted["status"] == "passed"
    assert adapted["identity"]["new_official_identities"] == 1
    identity = next(
        row for row in read_rows(output / "identity_map.csv")
        if row["IBUId"] == ibu_id
    )
    assert identity["method"] == "new_official_ibu_identity"
    assert identity["accepted_name"] == "Rita Rookie"
    aliases = read_rows(output / "identity_aliases_candidate.csv")
    assert any(
        row["IBUId"] == ibu_id and row["ID"] == identity["local_id"]
        for row in aliases
    )


def test_rejects_accepted_history_drift(tmp_path: Path) -> None:
    capture, individual, relay, schedule = build_bundle(tmp_path)
    with (individual / "men_scrape_update.csv").open("a", encoding="utf-8") as handle:
        handle.write("drift\n")
    with pytest.raises(IBUContractError, match="drifted"):
        adapter.adapt_additive_results(
            capture_dir=capture,
            individual_dir=individual,
            relay_dir=relay,
            schedule_path=schedule,
            output_dir=tmp_path / "candidate",
            aliases_path=HERE / "ibu_identity_aliases.csv",
        )


def test_explicit_gap_directory_keeps_retained_bundle_relocatable(tmp_path: Path) -> None:
    capture, individual, relay, schedule = build_bundle(tmp_path)
    relocated = tmp_path / "relocated-gap"
    (tmp_path / "gap").replace(relocated)

    report = adapter.adapt_additive_results(
        capture_dir=capture,
        individual_dir=individual,
        relay_dir=relay,
        schedule_path=schedule,
        output_dir=tmp_path / "candidate",
        aliases_path=HERE / "ibu_identity_aliases.csv",
        gap_evidence_dir=relocated,
    )

    assert report["status"] == "passed"


def test_explicit_race_number_policies() -> None:
    competitions = [
        {"race_id": "M1", "sex": "M"},
        {"race_id": "L1", "sex": "L"},
        {"race_id": "X1", "sex": "Mixed"},
        {"race_id": "L2", "sex": "L"},
    ]
    individual = adapter._race_numbers(competitions, "individual")
    relay = adapter._race_numbers(competitions, "relay")
    assert individual[("X1", "M")] == individual[("X1", "L")] == 2
    assert individual[("L2", "L")] == 2
    assert relay[("X1", "M")] == relay[("X1", "L")] == 2
    assert relay[("L2", "L")] == 3
