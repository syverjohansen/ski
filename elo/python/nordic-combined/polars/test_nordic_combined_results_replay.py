from __future__ import annotations

import csv
import hashlib
import math
from pathlib import Path

import pytest

from nordic_combined_results_replay import (
    _read,
    actual_scores,
    expected_scores,
    replay_history,
    run_replay,
    update_ratings,
)


ROOT = Path(__file__).resolve().parent
RELAY = ROOT / "relay/excel365"


def test_pairwise_scores_handle_ties_and_elo_update_is_zero_sum() -> None:
    assert actual_scores([1, 2, 2, 4]) == [3.0, 1.5, 1.5, 0.0]
    assert expected_scores([1300.0, 1300.0]) == [0.5, 0.5]
    ratings = [1420.0, 1350.0, 1280.0, 1210.0]
    updated = update_ratings(ratings, [2, 1, 3, 4], 0.5)
    assert sum(updated) == pytest.approx(sum(ratings), abs=1e-10)


@pytest.mark.parametrize("name", ["men", "ladies"])
def test_replay_without_insertion_reproduces_accepted_2026_elo(name: str) -> None:
    fields, rows = _read(RELAY / f"{name}_chrono.csv")
    candidate, _, changed = replay_history(rows, [], season=2026, fields=fields)
    accepted_events = {
        (row["Date"], row["Race"], row["ID"]): row
        for row in rows if row["Season"] == "2026" and row["Race"] != "0"
    }
    candidate_events = {
        (str(row["Date"]), str(row["Race"]), str(row["ID"])): row
        for row in candidate if str(row["Season"]) == "2026" and str(row["Race"]) != "0"
    }
    assert candidate_events.keys() == accepted_events.keys()
    assert changed == 0
    for key, accepted in accepted_events.items():
        assert float(candidate_events[key]["Pelo"]) == pytest.approx(float(accepted["Pelo"]), abs=1e-10)
        assert float(candidate_events[key]["Elo"]) == pytest.approx(float(accepted["Elo"]), abs=1e-10)


def test_live_mapped_candidate_inserts_mixed_teams_without_promoting(tmp_path: Path) -> None:
    marker = Path("/tmp/nordic-combined-replay-root.txt")
    if not marker.is_file():
        pytest.skip("live read-only evidence is not available")
    evidence = Path(marker.read_text(encoding="utf-8").strip())
    mapped = evidence / "identity/mapped_results.csv"
    if not mapped.is_file():
        pytest.skip("mapped read-only evidence is not available")
    sources = [
        mapped,
        RELAY / "men_chrono.csv",
        RELAY / "ladies_chrono.csv",
        RELAY / "team_sprint_chrono.csv",
    ]
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    report = run_replay(
        mapped_results=mapped,
        men_history=sources[1], ladies_history=sources[2],
        team_sprint_history=sources[3], season=2026,
        output_dir=tmp_path / "candidate",
    )
    assert report["inserted_member_rows"] == 28
    assert report["inserted_team_rows"] == 14
    assert report["accepted_inputs_modified"] is False
    assert report["promotion"] == {"status": "candidate", "performed": False}
    assert report["changed_existing_rating_rows"]["M"] > 0
    assert report["changed_existing_rating_rows"]["L"] > 0
    _, teams = _read(tmp_path / "candidate/candidate_team_sprint_chrono.csv")
    mixed = [row for row in teams if row["Date"] == "2026-03-07" and row["Sex"] == "Mixed"]
    assert len(mixed) == 14
    assert all(len(row["MemberIDs"].split(",")) == 2 for row in mixed)
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in sources} == before


def test_candidate_output_cannot_be_inside_accepted_data(tmp_path: Path) -> None:
    mapped = tmp_path / "mapped.csv"
    mapped.write_text("canonical_athlete_id\n1\n", encoding="utf-8")
    with pytest.raises(Exception, match="cannot be inside accepted data"):
        run_replay(
            mapped_results=mapped,
            men_history=RELAY / "men_chrono.csv",
            ladies_history=RELAY / "ladies_chrono.csv",
            team_sprint_history=RELAY / "team_sprint_chrono.csv",
            season=2026,
            output_dir=RELAY / "candidate",
        )
