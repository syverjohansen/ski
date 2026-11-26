from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import biathlon_runtime_shadow as runtime
from ibu_source_contract import IBUContractError


COMMIT = "a" * 40


def accepted_roots(tmp_path: Path) -> tuple[Path, Path]:
    individual = tmp_path / "accepted" / "individual"
    relay = tmp_path / "accepted" / "relay"
    individual.mkdir(parents=True)
    relay.mkdir(parents=True)
    (individual / "history.csv").write_text("id,elo\n1,1400\n", encoding="utf-8")
    (relay / "history.csv").write_text("id,elo\n1,1400\n", encoding="utf-8")
    return individual, relay


def patch_passing_stages(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    monkeypatch.setattr(runtime, "_resolve_commit", lambda *_: COMMIT)

    def stage(name: str):
        def operation(**_: object) -> dict[str, str]:
            calls.append(name)
            return {"status": "passed"}
        return operation

    monkeypatch.setattr(runtime, "capture_contract", stage("capture"))
    monkeypatch.setattr(runtime, "adapt_evidence", stage("adapt"))
    monkeypatch.setattr(runtime, "run_elo_parity", stage("elo"))
    monkeypatch.setattr(runtime, "run_relay_team_parity", stage("relay_team"))
    return calls


def run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    output_root: Path | None = None,
    run_id: str = "test-run",
) -> tuple[dict[str, object], Path, Path]:
    individual, relay = accepted_roots(tmp_path)
    report = runtime.run_shadow(
        season_id="2526",
        event_id="BT2526SWRLCP01",
        race_ids=["BT2526SWRLCP01SWSP"],
        output_root=output_root or tmp_path / "output",
        run_id=run_id,
        individual_dir=individual,
        relay_dir=relay,
        aliases_path=tmp_path / "aliases.csv",
        engine_path=tmp_path / "elo.py",
        aggregator_path=tmp_path / "relay_chrono.py",
        repo_root=tmp_path,
        code_commit=COMMIT,
        client=object(),  # stage is mocked; no network client is used
    )
    return report, individual, relay


def test_shadow_runs_all_gates_and_preserves_accepted_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = patch_passing_stages(monkeypatch)
    report, _, _ = run(tmp_path, monkeypatch)

    assert calls == ["capture", "adapt", "elo", "relay_team"]
    assert report["status"] == "passed"
    assert report["accepted_data_mutated"] is False
    assert report["promotion"] == {"status": "disabled"}
    assert report["publication"] == {"status": "disabled"}
    assert [stage["status"] for stage in report["stages"]] == ["passed"] * 4
    persisted = json.loads(
        (tmp_path / "output" / "test-run" / "runtime_report.json").read_text()
    )
    assert persisted == report


def test_failed_stage_is_recorded_and_blocks_downstream_stages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = patch_passing_stages(monkeypatch)

    def fail_adapter(**_: object) -> dict[str, str]:
        calls.append("adapt_failed")
        return {"status": "failed"}

    monkeypatch.setattr(runtime, "adapt_evidence", fail_adapter)
    report, _, _ = run(tmp_path, monkeypatch)

    assert calls == ["capture", "adapt_failed"]
    assert report["status"] == "failed"
    assert [stage["name"] for stage in report["stages"]] == [
        "capture_official_evidence",
        "adapt_compatibility_candidates",
    ]
    assert report["publication"] == {"status": "disabled"}


def test_stage_exception_survives_in_final_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    patch_passing_stages(monkeypatch)

    def explode(**_: object) -> dict[str, str]:
        raise RuntimeError("fixture source unavailable")

    monkeypatch.setattr(runtime, "capture_contract", explode)
    report, _, _ = run(tmp_path, monkeypatch)

    assert report["status"] == "failed"
    assert report["stages"][0]["error"] == {
        "type": "RuntimeError",
        "message": "fixture source unavailable",
    }
    assert (tmp_path / "output" / "test-run" / "runtime_report.json").is_file()


def test_accepted_mutation_forces_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    patch_passing_stages(monkeypatch)

    def mutate(**_: object) -> dict[str, str]:
        accepted_file = tmp_path / "accepted" / "individual" / "history.csv"
        accepted_file.write_text("id,elo\n1,9999\n", encoding="utf-8")
        return {"status": "passed"}

    monkeypatch.setattr(runtime, "capture_contract", mutate)
    report, _, _ = run(tmp_path, monkeypatch)

    assert report["status"] == "failed"
    assert report["accepted_data_mutated"] is True
    assert report["error"]["type"] == "AcceptedDataMutation"


def test_output_cannot_be_inside_accepted_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    patch_passing_stages(monkeypatch)
    individual, relay = accepted_roots(tmp_path)
    with pytest.raises(IBUContractError, match="outside accepted"):
        runtime.run_shadow(
            season_id="2526",
            event_id="BT2526SWRLCP01",
            race_ids=["BT2526SWRLCP01SWSP"],
            output_root=individual,
            run_id="unsafe",
            individual_dir=individual,
            relay_dir=relay,
            aliases_path=tmp_path / "aliases.csv",
            engine_path=tmp_path / "elo.py",
            aggregator_path=tmp_path / "relay_chrono.py",
            repo_root=tmp_path,
            code_commit=COMMIT,
            client=object(),
        )


def test_run_directory_must_be_unique(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    patch_passing_stages(monkeypatch)
    run(tmp_path, monkeypatch)
    with pytest.raises(IBUContractError, match="already exists"):
        runtime.run_shadow(
            season_id="2526",
            event_id="BT2526SWRLCP01",
            race_ids=["BT2526SWRLCP01SWSP"],
            output_root=tmp_path / "output",
            run_id="test-run",
            individual_dir=tmp_path / "accepted" / "individual",
            relay_dir=tmp_path / "accepted" / "relay",
            aliases_path=tmp_path / "aliases.csv",
            engine_path=tmp_path / "elo.py",
            aggregator_path=tmp_path / "relay_chrono.py",
            repo_root=tmp_path,
            code_commit=COMMIT,
            client=object(),
        )


def test_fingerprint_changes_with_file_content(tmp_path: Path) -> None:
    root = tmp_path / "accepted"
    root.mkdir()
    target = root / "history.csv"
    target.write_text("one", encoding="utf-8")
    first = runtime.fingerprint_tree(root)
    target.write_text("two", encoding="utf-8")
    second = runtime.fingerprint_tree(root)
    assert first["sha256"] != second["sha256"]


def test_declared_commit_must_match_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        lambda *_, **__: SimpleNamespace(stdout="b" * 40 + "\n"),
    )
    with pytest.raises(IBUContractError, match="does not match checkout"):
        runtime._resolve_commit(COMMIT, tmp_path)
