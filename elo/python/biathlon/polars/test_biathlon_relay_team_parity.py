from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from biathlon_relay_team_parity import (
    compare_team_artifact,
    inject_event_ratings,
    run_parity,
)
from ibu_source_contract import IBUContractError


def athlete_frame(*, elo: float = 1400.0, athlete_id: str = "10") -> pl.DataFrame:
    return pl.DataFrame({
        "Date": [date(2025, 12, 2)],
        "Season": [2026],
        "Race": [4],
        "Place": [1],
        "ID": [athlete_id],
        "RaceType": ["Relay"],
        "Leg": [1],
        "Pelo": [1390.0],
        "Elo": [elo],
        "Skier": ["Test Athlete"],
    })


def test_event_rating_injection_replaces_only_verified_ratings() -> None:
    accepted = athlete_frame()
    candidate = athlete_frame(elo=1400.0 + 2e-13).select([
        "Date", "Season", "Race", "Place", "ID", "RaceType", "Leg", "Pelo", "Elo",
    ])
    injected, report = inject_event_ratings(accepted, candidate, tolerance=1e-9)
    assert report["status"] == "passed"
    assert report["matched_rows"] == 1
    assert injected.item(0, "Elo") == 1400.0 + 2e-13
    assert injected.item(0, "Skier") == "Test Athlete"


def test_event_rating_injection_fails_closed_on_missing_or_drifted_rows() -> None:
    accepted = athlete_frame()
    missing, missing_report = inject_event_ratings(
        accepted,
        athlete_frame(athlete_id="11").select(accepted.columns[:-1]),
        tolerance=1e-9,
    )
    assert missing_report["status"] == "failed"
    assert missing_report["missing_rows"] == 1
    assert missing.equals(accepted)

    unchanged, drift_report = inject_event_ratings(
        accepted,
        athlete_frame(elo=1400.01).select(accepted.columns[:-1]),
        tolerance=1e-9,
    )
    assert drift_report["status"] == "failed"
    assert drift_report["rating_mismatches"] == 1
    assert unchanged.equals(accepted)


def team_frame(*, elo: float = 2800.0) -> pl.DataFrame:
    return pl.DataFrame({
        "Nation": ["Norway"],
        "Season": [2026],
        "Race": [1],
        "Place": [1],
        "Date": ["2025-12-02"],
        "City": ["Östersund"],
        "Country": ["Sweden"],
        "Event": ["World Cup"],
        "TeamSize": [2],
        "RaceType": ["Single Mixed Relay"],
        "Distance": ["N/A"],
        "MassStart": [0],
        "Team_Elo": [elo],
        "TeamMembers": [["Athlete One", "Athlete Two"]],
        "MemberSexes": [["M", "L"]],
        "MemberIDs": [["10", "20"]],
    })


def test_team_artifact_comparison_handles_lists_and_float_noise(
    tmp_path: Path,
) -> None:
    accepted_path = tmp_path / "accepted.csv"
    team_frame().with_columns([
        pl.col("TeamMembers").list.join(","),
        pl.col("MemberSexes").list.join(","),
        pl.col("MemberIDs").list.join(","),
    ]).write_csv(accepted_path)

    passed = compare_team_artifact(
        team_frame(elo=2800.0 + 2e-12), accepted_path, tolerance=1e-9
    )
    assert passed["status"] == "passed"
    assert passed["maximum_absolute_numeric_difference"] < 1e-9

    failed = compare_team_artifact(
        team_frame(elo=2800.01), accepted_path, tolerance=1e-9
    )
    assert failed["status"] == "failed"
    assert failed["value_mismatches"] == 1


def test_relay_parity_output_cannot_enter_accepted_tree(tmp_path: Path) -> None:
    accepted = tmp_path / "accepted"
    with pytest.raises(IBUContractError, match="outside accepted"):
        run_parity(
            event_ratings_dir=tmp_path / "ratings",
            accepted_dir=accepted,
            output_dir=accepted / "shadow",
            aggregator_path=tmp_path / "relay_chrono.py",
        )
