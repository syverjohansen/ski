from __future__ import annotations

import csv
import hashlib
import math
from pathlib import Path

import pytest

from nordic_combined_results_replay import (
    _candidate_row,
    _read,
    actual_scores,
    expected_scores,
    mapped_event,
    replay_history,
    run_replay,
    update_ratings,
)


ROOT = Path(__file__).resolve().parent
RELAY = ROOT / "relay/excel365"
MAPPED_FIELDS = [
    "date", "city", "country", "sex", "season", "race", "event_kind",
    "distance", "race_type", "championship", "place", "team_name",
    "team_nation", "member_order", "fis_competitor_id", "fis_code",
    "name", "nation", "canonical_athlete_id", "canonical_name",
    "canonical_nation", "canonical_sex", "identity_match_method",
]


def write_individual(path: Path, *, championship: str = "0") -> None:
    rows = [
        ("1329", "Jens Lurås Oftebro", "Norway", 1),
        ("1348", "Julian Schmid", "Germany", 2),
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        for athlete_id, name, nation, place in rows:
            writer.writerow({
                "date": "2026-03-08", "city": "Oslo", "country": "Norway",
                "sex": "M", "season": "2026", "race": "1",
                "event_kind": "individual", "distance": "10",
                "race_type": "Individual", "championship": championship,
                "place": place, "fis_competitor_id": f"fis-{athlete_id}",
                "fis_code": f"code-{athlete_id}", "name": name,
                "nation": nation[:3].upper(), "canonical_athlete_id": athlete_id,
                "canonical_name": name, "canonical_nation": nation,
                "canonical_sex": "M", "identity_match_method": "test_fixture",
            })


def write_men_team(path: Path, *, event_kind: str) -> None:
    team_size = 4 if event_kind == "team" else 2
    athletes = {
        "Austria": [
            ("1469", "Johannes Lamparter"), ("72", "Franz-Josef Rehrl"),
            ("1467", "Thomas Rettenegger"), ("1775", "Stefan Rettenegger"),
        ],
        "Germany": [
            ("1348", "Julian Schmid"), ("14", "Johannes Rydzek"),
            ("1137", "Terence Weber"), ("1468", "David Mach"),
        ],
    }
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        for place, (nation, members) in enumerate(athletes.items(), 1):
            for order, (athlete_id, name) in enumerate(members[:team_size], 1):
                writer.writerow({
                    "date": "2026-03-08", "city": "Oslo", "country": "Norway",
                    "sex": "M", "season": "2026", "race": "1",
                    "event_kind": event_kind, "distance": "4x5" if team_size == 4 else "2x7.5",
                    "race_type": "Team" if event_kind == "team" else "Team Sprint",
                    "championship": "0", "place": place,
                    "team_name": f"{nation.upper()} I", "team_nation": nation,
                    "member_order": order, "fis_competitor_id": f"fis-{athlete_id}",
                    "fis_code": f"code-{athlete_id}", "name": name,
                    "nation": nation[:3].upper(), "canonical_athlete_id": athlete_id,
                    "canonical_name": name, "canonical_nation": nation,
                    "canonical_sex": "M", "identity_match_method": "test_fixture",
                })


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
        RELAY / "men_team_chrono.csv",
        RELAY / "team_sprint_chrono.csv",
    ]
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    report = run_replay(
        mapped_results=mapped,
        men_history=sources[1], ladies_history=sources[2],
        team_history=sources[3], team_sprint_history=sources[4], season=2026,
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
            team_history=RELAY / "men_team_chrono.csv",
            team_sprint_history=RELAY / "team_sprint_chrono.csv",
            season=2026,
            output_dir=RELAY / "candidate",
        )


def test_regular_individual_event_replays_only_its_sex(tmp_path: Path) -> None:
    mapped = tmp_path / "evidence/mapped.csv"
    mapped.parent.mkdir()
    write_individual(mapped)
    report = run_replay(
        mapped_results=mapped,
        men_history=RELAY / "men_chrono.csv",
        ladies_history=RELAY / "ladies_chrono.csv",
        team_history=RELAY / "men_team_chrono.csv",
        team_sprint_history=RELAY / "team_sprint_chrono.csv",
        season=2026, output_dir=tmp_path / "candidate",
    )
    assert report["event"] == {
        "date": "2026-03-08", "sex": "M", "event_kind": "individual", "race": "1",
        "race_type": "Individual", "championship": "0",
    }
    assert report["inserted_member_rows"] == 2
    assert report["inserted_team_rows"] == 0
    assert report["row_deltas"]["men_chrono.csv"] >= 2
    assert report["row_deltas"]["ladies_chrono.csv"] == 0
    assert report["row_deltas"]["men_team_chrono.csv"] == 0
    assert report["row_deltas"]["team_sprint_chrono.csv"] == 0
    _, men = _read(tmp_path / "candidate/candidate_men_chrono.csv")
    inserted = [row for row in men if row["Date"] == "2026-03-08"]
    assert {row["ID"] for row in inserted} == {"1329", "1348"}
    assert {row["RaceType"] for row in inserted} == {"Individual"}


def test_championship_individual_uses_olympic_event_label(tmp_path: Path) -> None:
    mapped = tmp_path / "evidence/mapped.csv"
    mapped.parent.mkdir()
    write_individual(mapped, championship="1")
    _, rows = _read(mapped)
    assert mapped_event(rows, 2026)["championship"] == "1"
    run_replay(
        mapped_results=mapped,
        men_history=RELAY / "men_chrono.csv",
        ladies_history=RELAY / "ladies_chrono.csv",
        team_history=RELAY / "men_team_chrono.csv",
        team_sprint_history=RELAY / "team_sprint_chrono.csv",
        season=2026, output_dir=tmp_path / "candidate",
    )
    _, men = _read(tmp_path / "candidate/candidate_men_chrono.csv")
    inserted = [row for row in men if row["Date"] == "2026-03-08"]
    assert {row["Event"] for row in inserted} == {"Olympic Games"}


def test_odd_season_championship_uses_world_championship_label() -> None:
    mapped = {
        "date": "2027-03-01", "city": "Falun", "country": "Sweden",
        "sex": "M", "canonical_sex": "M", "season": "2027", "race": "1",
        "event_kind": "individual", "distance": "10", "race_type": "Individual",
        "championship": "1", "place": "1", "canonical_name": "Test Athlete",
        "canonical_nation": "Norway", "canonical_athlete_id": "1", "member_order": "",
    }
    row = _candidate_row(mapped, {"Birthday": "2000-01-01", "Exp": "4"}, 1)
    assert row["Event"] == "World Championship"


def test_individual_replay_creates_race_zero_for_returning_athlete(tmp_path: Path) -> None:
    mapped = tmp_path / "evidence/mapped.csv"
    mapped.parent.mkdir()
    write_individual(mapped)
    fields, rows = _read(mapped)
    returning = rows[0]
    returning.update(
        canonical_athlete_id="7", canonical_name="Eric Frenzel",
        canonical_nation="Germany", name="Eric Frenzel", nation="GER", place="1",
    )
    with mapped.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow(returning)
    report = run_replay(
        mapped_results=mapped,
        men_history=RELAY / "men_chrono.csv",
        ladies_history=RELAY / "ladies_chrono.csv",
        team_history=RELAY / "men_team_chrono.csv",
        team_sprint_history=RELAY / "team_sprint_chrono.csv",
        season=2026, output_dir=tmp_path / "candidate",
    )
    assert report["row_deltas"]["men_chrono.csv"] == 2
    _, men = _read(tmp_path / "candidate/candidate_men_chrono.csv")
    race_zero = [row for row in men if row["Season"] == "2026" and row["ID"] == "7" and row["Race"] == "0"]
    assert len(race_zero) == 1
    assert race_zero[0]["Date"] == "2026-05-01"
    assert race_zero[0]["RaceType"] == "Offseason"


@pytest.mark.parametrize(
    ("event_kind", "aggregate_file", "team_size"),
    [
        ("team", "candidate_men_team_chrono.csv", "4"),
        ("team_sprint", "candidate_team_sprint_chrono.csv", "2"),
    ],
)
def test_same_sex_team_formats_replay_members_and_aggregate(
    tmp_path: Path, event_kind: str, aggregate_file: str, team_size: str
) -> None:
    mapped = tmp_path / "evidence/mapped.csv"
    mapped.parent.mkdir()
    write_men_team(mapped, event_kind=event_kind)
    report = run_replay(
        mapped_results=mapped,
        men_history=RELAY / "men_chrono.csv",
        ladies_history=RELAY / "ladies_chrono.csv",
        team_history=RELAY / "men_team_chrono.csv",
        team_sprint_history=RELAY / "team_sprint_chrono.csv",
        season=2026, output_dir=tmp_path / "candidate",
    )
    assert report["event"]["event_kind"] == event_kind
    assert report["inserted_team_rows"] == 2
    expected_aggregate = (
        "men_team_chrono.csv" if event_kind == "team"
        else "team_sprint_chrono.csv"
    )
    assert report["row_deltas"][expected_aggregate] == 2
    _, aggregate = _read(tmp_path / "candidate" / aggregate_file)
    inserted = [row for row in aggregate if row["Date"] == "2026-03-08"]
    assert len(inserted) == 2
    assert {row["Sex"] for row in inserted} == {"M"}
    assert {row["TeamSize"] for row in inserted} == {team_size}
    for row in inserted:
        assert float(row["Avg_Elo"]) == pytest.approx(
            float(row["Team_Elo"]) / int(team_size)
        )
        assert float(row["AvgAge"]) <= float(row["MaxAge"])
