from __future__ import annotations

import json
from pathlib import Path

import pytest

import biathlon_additive_promotion as promotion
import biathlon_additive_result_adapter as adapter
import biathlon_rating_chrono_candidate as derived
from biathlon_elo_parity import HISTORY_FIELDS
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError
from test_biathlon_rating_chrono_candidate import (
    FakeEngine,
    fixture,
    row,
    write_rows,
)


def _candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    adapter_dir, individual, relay = fixture(tmp_path)
    aliases_path = tmp_path / "ibu_identity_aliases.csv"
    aliases_path.write_text("IBUId,ID,reason\n", encoding="utf-8")
    additive = row("L", "Sprint")
    additive.update(
        {"Date": "2026-11-26", "Season": "2027", "Race": "1", "Exp": "2"}
    )
    candidate_files = {}
    for policy in ("individual", "relay"):
        for sex, prefix in (("M", "men"), ("L", "ladies")):
            path = adapter_dir / policy / f"{prefix}_results_candidate.csv"
            write_rows(path, [additive] if sex == "L" else [], HISTORY_FIELDS)
            candidate_files[path.relative_to(adapter_dir).as_posix()] = {
                "rows": 1 if sex == "L" else 0,
                "sha256": adapter._sha256(path),
            }
    (adapter_dir / "men_results_candidate.csv").unlink()
    (adapter_dir / "ladies_results_candidate.csv").unlink()
    fingerprints = {
        "individual": fingerprint_tree(individual),
        "relay": fingerprint_tree(relay),
    }
    adapter_report = {
        "schema_version": adapter.REPORT_SCHEMA,
        "status": "passed",
        "season_id": "2627",
        "race_ids": ["TEST-RACE"],
        "candidate_files": candidate_files,
        "accepted_fingerprints": {
            "before": fingerprints,
            "after": fingerprints,
        },
        "accepted_data_mutated": False,
        "ready_for_derived_rehearsal": True,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    (adapter_dir / "adapter_report.json").write_text(
        json.dumps(adapter_report), encoding="utf-8"
    )
    alias_candidate = adapter_dir / "identity_aliases_candidate.csv"
    alias_candidate.write_text("IBUId,ID,reason\n", encoding="utf-8")
    adapter_report["candidate_files"]["identity_aliases_candidate.csv"] = {
        "rows": 0,
        "sha256": adapter._sha256(alias_candidate),
    }
    adapter_report["aliases"] = {
        "source_file": str(aliases_path),
        "source_sha256": adapter._sha256(aliases_path),
        "candidate_rows": 0,
        "new_rows": 0,
    }
    (adapter_dir / "adapter_report.json").write_text(
        json.dumps(adapter_report), encoding="utf-8"
    )
    monkeypatch.setattr(derived, "load_engine", lambda _: FakeEngine())
    derived_dir = tmp_path / "derived"
    derived_report = derived.build_candidates(
        candidate_dir=adapter_dir,
        individual_dir=individual,
        relay_dir=relay,
        engine_path=tmp_path / "elo.py",
        output_dir=derived_dir,
    )
    assert derived_report["status"] == "passed"
    bundle = tmp_path / "promotion"
    report = promotion.build_promotion(
        adapter_dir=adapter_dir,
        derived_dir=derived_dir,
        individual_dir=individual,
        relay_dir=relay,
        aliases_path=aliases_path,
        output_dir=bundle,
    )
    return report, bundle, individual, relay, aliases_path


def test_additive_bundle_builds_and_applies_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, bundle, individual, relay, aliases_path = _candidate(tmp_path, monkeypatch)

    assert report["status"] == "promotion_candidate"
    assert report["apply_ready"] is True
    assert report["publication"] == {"status": "disabled"}
    assert all(item["file"].startswith("promotion/") for item in report["artifacts"])

    receipt = promotion.apply_promotion(
        bundle_dir=bundle,
        individual_dir=individual,
        relay_dir=relay,
        aliases_path=aliases_path,
    )

    assert receipt["status"] == "passed"
    assert receipt["performed"] is True
    assert receipt["publication"] == {"status": "disabled"}
    for root in (individual, relay):
        text = (root / "ladies_scrape_update.csv").read_text(encoding="utf-8")
        assert "2026-11-26" in text


def test_apply_rejects_accepted_tree_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle, individual, relay, aliases_path = _candidate(tmp_path, monkeypatch)
    with (individual / "men_scrape_update.csv").open("a", encoding="utf-8") as handle:
        handle.write("drift\n")

    with pytest.raises(IBUContractError, match="drifted"):
        promotion.apply_promotion(
            bundle_dir=bundle,
            individual_dir=individual,
            relay_dir=relay,
            aliases_path=aliases_path,
        )
