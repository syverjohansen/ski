from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import biathlon_results_update as update


class FakeClient:
    pass


def test_season_id_rolls_over_in_july() -> None:
    assert update.season_id_for(date(2027, 1, 10)) == "2627"
    assert update.season_id_for(date(2027, 7, 1)) == "2728"


def test_no_final_results_is_clean_no_op(
    tmp_path: Path, monkeypatch
) -> None:
    def fake_audit(**kwargs):
        output = kwargs["output_dir"]
        output.mkdir(parents=True)
        (output / "gap_report.json").write_text("{}", encoding="utf-8")
        return {
            "status": "passed",
            "summary": {"schedule_missing": 0, "final_results_missing": 0},
        }

    monkeypatch.setattr(update, "audit_gaps", fake_audit)
    output = tmp_path / "run"
    report = update.run_update(
        client=FakeClient(),
        season_id="2627",
        schedule_path=tmp_path / "races.csv",
        individual_dir=tmp_path / "individual",
        relay_dir=tmp_path / "relay",
        aliases_path=tmp_path / "aliases.csv",
        engine_path=tmp_path / "elo.py",
        aggregator_path=tmp_path / "relay.py",
        output_root=output,
        apply=True,
    )

    assert report["status"] == "no_op"
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}
    assert json.loads((output / "report.json").read_text())["status"] == "no_op"


def test_schedule_gap_blocks_result_application(
    tmp_path: Path, monkeypatch
) -> None:
    def fake_audit(**kwargs):
        output = kwargs["output_dir"]
        output.mkdir(parents=True)
        return {
            "status": "passed",
            "summary": {"schedule_missing": 1, "final_results_missing": 1},
        }

    monkeypatch.setattr(update, "audit_gaps", fake_audit)
    report = update.run_update(
        client=FakeClient(),
        season_id="2627",
        schedule_path=tmp_path / "races.csv",
        individual_dir=tmp_path / "individual",
        relay_dir=tmp_path / "relay",
        aliases_path=tmp_path / "aliases.csv",
        engine_path=tmp_path / "elo.py",
        aggregator_path=tmp_path / "relay.py",
        output_root=tmp_path / "run",
        apply=True,
    )

    assert report["status"] == "blocked"
    assert report["application"]["performed"] is False
