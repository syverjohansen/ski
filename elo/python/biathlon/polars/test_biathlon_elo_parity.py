from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from biathlon_elo_parity import (
    HISTORY_FIELDS,
    compare_ratings,
    run_parity,
    splice_candidates,
)
from ibu_source_contract import IBUContractError


def row(**updates: str) -> dict[str, str]:
    result = {
        "Date": "2025-12-02",
        "City": "Östersund",
        "Country": "Sweden",
        "Event": "World Cup",
        "Distance": "15",
        "RaceType": "Individual",
        "MassStart": "0",
        "Season": "2026",
        "Race": "4",
        "Place": "1",
        "Skier": "Test Athlete",
        "Nation": "Sweden",
        "ID": "10",
        "Sex": "L",
        "Birthday": "2000-01-01T00:00:00.000000",
        "Leg": "0",
        "Age": "25.91786447638604",
        "Exp": "3",
    }
    result.update(updates)
    return result


def test_splice_aligns_target_race_number_and_ignores_float_rendering() -> None:
    accepted = [row(), row(Date="2024-01-01", Race="1", ID="20")]
    candidate = [row(Race="2", Age="25.917864476386042")]
    spliced, report = splice_candidates(accepted, candidate)
    assert report["status"] == "passed"
    assert report["age_excluded_from_identity"] is True
    replacement = next(item for item in spliced if item["ID"] == "10")
    assert replacement["Race"] == "4"
    assert replacement["Age"] == "25.917864476386042"


def test_splice_fails_closed_on_result_identity_difference() -> None:
    spliced, report = splice_candidates([row()], [row(ID="999")])
    assert spliced == []
    assert report["status"] == "failed"
    assert report["missing_rows"] == 1
    assert report["unexpected_rows"] == 1


def rating_frame(elo: float, pelo: float, *, athlete_id: str = "10") -> pl.DataFrame:
    return pl.DataFrame({
        "Date": ["2025-12-02"],
        "Season": [2026],
        "Race": [4],
        "Place": [1],
        "ID": [athlete_id],
        "RaceType": ["Individual"],
        "Leg": [0],
        "Pelo": [pelo],
        "Elo": [elo],
    })


def test_rating_comparison_accepts_only_bounded_numeric_noise() -> None:
    passed = compare_ratings(
        rating_frame(1400.0 + 2e-13, 1390.0),
        rating_frame(1400.0, 1390.0),
        tolerance=1e-9,
    )
    assert passed["status"] == "passed"
    assert passed["maximum_absolute_rating_difference"] < 1e-9

    failed = compare_ratings(
        rating_frame(1400.01, 1390.0),
        rating_frame(1400.0, 1390.0),
        tolerance=1e-9,
    )
    assert failed["status"] == "failed"
    assert failed["rating_mismatches"] == 1


def test_rating_comparison_rejects_missing_athlete() -> None:
    report = compare_ratings(
        rating_frame(1400.0, 1390.0, athlete_id="11"),
        rating_frame(1400.0, 1390.0, athlete_id="10"),
        tolerance=1e-9,
    )
    assert report["status"] == "failed"
    assert report["missing_rows"] == 1
    assert report["unexpected_rows"] == 1


def test_parity_output_cannot_enter_accepted_tree(tmp_path: Path) -> None:
    accepted = tmp_path / "accepted"
    with pytest.raises(IBUContractError, match="outside accepted"):
        run_parity(
            candidate_dir=tmp_path / "candidate",
            individual_dir=accepted,
            relay_dir=tmp_path / "relay",
            output_dir=accepted / "shadow",
            engine_path=tmp_path / "elo.py",
        )


def test_history_contract_remains_complete() -> None:
    assert len(HISTORY_FIELDS) == 18
    assert HISTORY_FIELDS[-3:] == ["Leg", "Age", "Exp"]
