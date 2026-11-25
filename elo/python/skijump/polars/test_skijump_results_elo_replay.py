from __future__ import annotations

import csv
import math
from pathlib import Path

import pytest
from skijump_results_elo_replay import (
    ReplayError,
    actual_scores,
    aggregate_mixed_teams,
    classify_acquired_events,
    expected_scores,
    replay_sex,
    update_ratings,
    validate_output_dir,
)

FIELDS = [
    "Elo",
    "Small_Elo",
    "Medium_Elo",
    "Normal_Elo",
    "Large_Elo",
    "Flying_Elo",
    "Pelo",
    "Small_Pelo",
    "Medium_Pelo",
    "Normal_Pelo",
    "Large_Pelo",
    "Flying_Pelo",
    "Date",
    "City",
    "Country",
    "Sex",
    "HillSize",
    "RaceType",
    "TeamEvent",
    "Event",
    "Place",
    "Skier",
    "Nation",
    "ID",
    "Season",
    "Race",
    "Birthday",
    "Age",
    "Exp",
    "Leg",
]


def history_row(**overrides: object) -> dict[str, str]:
    row = {
        "Date": "2025-05-01",
        "City": "Summer",
        "Country": "Break",
        "Sex": "M",
        "HillSize": "0",
        "RaceType": "Offseason",
        "TeamEvent": "0",
        "Event": "Offseason",
        "Place": "0",
        "Skier": "Alpha One",
        "Nation": "Norway",
        "ID": "1",
        "Season": "2025",
        "Race": "0",
        "Birthday": "2000-01-01 00:00:00",
        "Age": "25",
        "Exp": "1",
        "Leg": "0",
    }
    for stream in ("", "Small_", "Medium_", "Normal_", "Large_", "Flying_"):
        row[f"{stream}Pelo"] = "1600"
        row[f"{stream}Elo"] = "1500"
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def mapped_row(**overrides: object) -> dict[str, str]:
    row = {
        "date": "2025-11-21",
        "sex": "Mixed",
        "season": "2026",
        "race": "1",
        "event_kind": "team",
        "city": "Lillehammer",
        "country": "Norway",
        "hill_size": "140",
        "race_type": "Team Large",
        "championship": "0",
        "place": "1",
        "team_name": "NORWAY",
        "team_nation": "NOR",
        "member_order": "1",
        "fis_competitor_id": "101",
        "fis_code": "1001",
        "name": "ONE Alpha",
        "birth_year": "2000",
        "nation": "NOR",
        "source_url": "https://example.invalid/1",
        "source_sha256": "abc",
        "canonical_athlete_id": "1",
        "canonical_name": "Alpha One",
        "canonical_nation": "Norway",
        "canonical_sex": "M",
        "identity_match_method": "unique_normalized_name",
    }
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def accepted_event_rows() -> list[dict[str, str]]:
    accepted_ratings = update_ratings([1500.0, 1300.0], [1, 2], k_factor=1.0)
    rows = [
        history_row(
            Date="2025-11-22",
            City="Lillehammer",
            Country="Norway",
            HillSize="140",
            RaceType="Large",
            TeamEvent=0,
            Event="World Cup",
            Place=place,
            Skier=name,
            ID=athlete_id,
            Season=2026,
            Race=1,
            Pelo=starting,
            Elo=rating,
            Large_Pelo=starting,
            Large_Elo=rating,
        )
        for place, name, athlete_id, starting, rating in (
            (1, "Alpha One", "1", 1500, accepted_ratings[0]),
            (2, "Beta Two", "2", 1300, accepted_ratings[1]),
        )
    ]
    return rows


def test_pairwise_scores_handle_ties_and_updates_are_zero_sum() -> None:
    assert actual_scores([1, 2, 2, 4]) == [3.0, 1.5, 1.5, 0.0]
    expected = expected_scores([1500.0, 1400.0, 1300.0, 1200.0])
    assert math.isclose(sum(expected), 6.0)
    updated = update_ratings(
        [1500.0, 1400.0, 1300.0, 1200.0], [1, 2, 2, 4], k_factor=0.25
    )
    assert math.isclose(sum(updated), 5400.0)


def test_team_weight_is_exactly_one_quarter_of_individual_weight() -> None:
    ratings = [1500.0, 1300.0]
    individual = update_ratings(ratings, [1, 2], k_factor=1.0)
    team = update_ratings(ratings, [1, 2], k_factor=1.0 / 4.0)
    assert math.isclose(team[0] - ratings[0], (individual[0] - ratings[0]) / 4.0)
    assert math.isclose(team[1] - ratings[1], (individual[1] - ratings[1]) / 4.0)


def test_classification_uses_semantic_identity_and_major_event_policy() -> None:
    mapped = [
        mapped_row(
            date="2026-01-17",
            sex="M",
            event_kind="individual",
            city="Sapporo",
            race_type="Large",
        ),
        mapped_row(
            date="2026-01-13",
            sex="M",
            event_kind="individual",
            city="Zhangjiakou",
            race_type="Large",
        ),
        mapped_row(),
    ]
    chrono = [
        history_row(
            Date="2026-01-17", City="Sapporo", RaceType="Large", Race=18, Season=2026
        )
    ]
    raw = [
        history_row(
            Date="2026-01-13",
            City="Zhangjiakou",
            RaceType="Large",
            Event="Continental Cup",
            Race=42,
            Season=2026,
        )
    ]
    classifications, eligible = classify_acquired_events(
        mapped,
        men_chrono=chrono,
        ladies_chrono=[],
        mixed_team_chrono=[],
        men_raw=raw,
        ladies_raw=[],
    )
    statuses = {(item["date"], item["status"]) for item in classifications}
    assert ("2026-01-17", "already_accepted") in statuses
    assert ("2026-01-13", "policy_excluded") in statuses
    assert ("2025-11-21", "eligible") in statuses
    assert len(eligible) == 1


def test_replay_uses_prior_race_zero_elo_once_and_changes_downstream() -> None:
    history = [
        history_row(ID=1, Skier="Alpha One", Elo=1500, Large_Elo=1500),
        history_row(ID=2, Skier="Beta Two", Elo=1300, Large_Elo=1300),
        *accepted_event_rows(),
    ]
    inserted = [
        mapped_row(canonical_athlete_id=1, canonical_name="Alpha One", place=2),
        mapped_row(
            canonical_athlete_id=2,
            canonical_name="Beta Two",
            fis_competitor_id=102,
            fis_code=1002,
            name="TWO Beta",
            place=1,
            member_order=2,
        ),
    ]
    candidate, _, changed = replay_sex(history, inserted, season=2026, fields=FIELDS)
    first = next(
        row
        for row in candidate
        if row["Season"] == "2026" and row["Race"] == "1" and row["ID"] == "1"
    )
    assert (
        float(first["Pelo"]) == 1500.0
    )  # The accepted Race 0 Elo, not its pre-discount Pelo.
    assert changed == 2
    accepted = next(
        row
        for row in candidate
        if row["Season"] == "2026"
        and row["City"] == "Lillehammer"
        and row["TeamEvent"] == "0"
        and row["ID"] == "1"
    )
    assert float(accepted["Pelo"]) != 1500.0


def test_replay_without_insertions_reproduces_accepted_rows() -> None:
    history = [
        history_row(ID=1, Skier="Alpha One", Elo=1500, Large_Elo=1500),
        history_row(ID=2, Skier="Beta Two", Elo=1300, Large_Elo=1300),
        *accepted_event_rows(),
    ]
    _, _, changed = replay_sex(history, [], season=2026, fields=FIELDS)
    assert changed == 0


@pytest.mark.parametrize("sex", ["M", "L"])
def test_individual_insertion_is_not_marked_as_team_event(sex: str) -> None:
    history = [
        history_row(ID=1, Skier="Alpha One", Sex=sex, Elo=1500, Large_Elo=1500),
        history_row(ID=2, Skier="Beta Two", Sex=sex, Elo=1300, Large_Elo=1300),
        *[{**row, "Sex": sex} for row in accepted_event_rows()],
    ]
    inserted = [
        mapped_row(
            date="2025-11-21",
            sex=sex,
            event_kind="individual",
            race_type="Large",
            canonical_athlete_id=1,
            canonical_name="Alpha One",
            canonical_sex=sex,
            place=2,
            member_order="",
        ),
        mapped_row(
            date="2025-11-21",
            sex=sex,
            event_kind="individual",
            race_type="Large",
            canonical_athlete_id=2,
            canonical_name="Beta Two",
            canonical_sex=sex,
            fis_competitor_id=102,
            fis_code=1002,
            name="TWO Beta",
            place=1,
            member_order="",
        ),
    ]
    candidate, _, changed = replay_sex(
        history, inserted, season=2026, fields=FIELDS
    )
    inserted_rows = [
        row
        for row in candidate
        if row["Date"] == "2025-11-21" and row["City"] == "Lillehammer"
    ]
    assert len(inserted_rows) == 2
    assert {row["TeamEvent"] for row in inserted_rows} == {"0"}
    assert {row["Leg"] for row in inserted_rows} == {"0"}
    assert changed == 2


def test_duplicate_athlete_in_inserted_event_fails() -> None:
    history = [history_row(), *accepted_event_rows()]
    with pytest.raises(ReplayError, match="duplicate athlete"):
        replay_sex(history, [mapped_row(), mapped_row()], season=2026, fields=FIELDS)


def test_candidate_output_cannot_be_inside_accepted_directory(tmp_path: Path) -> None:
    accepted = tmp_path / "accepted" / "men.csv"
    accepted.parent.mkdir()
    accepted.write_text("example", encoding="utf-8")
    with pytest.raises(ReplayError, match="cannot be inside accepted"):
        validate_output_dir(accepted.parent / "candidate", [accepted])
    validate_output_dir(tmp_path / "isolated-candidate", [accepted])


def test_mixed_aggregation_requires_both_sexes_and_four_members() -> None:
    def member(sex: str, athlete_id: str, name: str) -> dict[str, object]:
        row: dict[str, object] = history_row(
            Date="2025-11-21",
            Season=2026,
            Race=1,
            RaceType="Team Large",
            TeamEvent=1,
            Sex=sex,
            ID=athlete_id,
            Skier=name,
            Nation="Norway",
            Place=1,
            Pelo=1400,
            Elo=1401,
            Large_Pelo=1400,
            Large_Elo=1401,
        )
        return row

    men = [member("M", "1", "A"), member("M", "2", "B")]
    ladies = [member("L", "3", "C"), member("L", "4", "D")]
    _, teams, imputed = aggregate_mixed_teams(
        men, ladies, season=2026, eligible_dates={"2025-11-21"}
    )
    assert len(teams) == 1
    assert teams[0]["TeamSize"] == 4
    assert teams[0]["MemberSex"] == "M,M,L,L"
    assert teams[0]["Total_Elo"] == 5604.0
    assert imputed == 0


def test_repository_history_is_an_exact_no_insertion_replay() -> None:
    base = Path(__file__).with_name("relay") / "excel365"
    for filename, prefix in (("men_chrono.csv", "M"), ("ladies_chrono.csv", "L")):
        path = base / filename
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fields, rows = list(reader.fieldnames or []), list(reader)
        stream_histories = {}
        for stream in ("Overall", "Small", "Medium", "Normal", "Large", "Flying"):
            stream_path = base / (
                f"{prefix}.csv" if stream == "Overall" else f"{prefix}_{stream}.csv"
            )
            with stream_path.open(newline="", encoding="utf-8-sig") as handle:
                stream_histories[stream] = list(csv.DictReader(handle))
        _, _, changed = replay_sex(
            rows, [], season=2026, fields=fields, stream_histories=stream_histories
        )
        assert changed == 0
