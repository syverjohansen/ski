from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest
from skijump_startlist_acquire import acquire_startlists
from skijump_startlist_normalize import (
    StartlistNormalizationError,
    normalize_startlists,
)

FIXTURES = Path(__file__).with_name("tests") / "fixtures"
SCHEDULE_FIELDS = [
    "Date",
    "Final_Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "HillSize",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
    "Race",
]
HISTORY_FIELDS = [
    "Elo",
    "Small_Elo",
    "Medium_Elo",
    "Normal_Elo",
    "Large_Elo",
    "Flying_Elo",
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
]
ALIAS_FIELDS = [
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "canonical_sex",
    "canonical_id",
    "canonical_name",
]


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def schedule_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "11/21/2025",
        "Final_Date": "11/22/2025",
        "Startlist": "https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7459",
        "Sex": "M",
        "City": "Lillehammer",
        "Country": "Norway",
        "HillSize": "140",
        "RaceType": "Large",
        "Period": "1",
        "Elevation": "323",
        "Championship": "0",
        "Race": "1",
    }
    row.update(overrides)
    return row


def history_row(
    athlete_id: int, name: str, sex: str, elo: int, **overrides: object
) -> dict[str, object]:
    row: dict[str, object] = {
        "Elo": elo,
        "Small_Elo": elo - 30,
        "Medium_Elo": elo - 20,
        "Normal_Elo": elo - 10,
        "Large_Elo": elo,
        "Flying_Elo": elo + 10,
        "Date": "2025-11-20",
        "City": "Falun",
        "Country": "Sweden",
        "Sex": sex,
        "HillSize": "140",
        "RaceType": "Large",
        "TeamEvent": "0",
        "Event": "World Cup",
        "Place": "1",
        "Skier": name,
        "Nation": "AUT",
        "ID": athlete_id,
        "Season": "2026",
        "Race": "1",
    }
    row.update(overrides)
    return row


def prepared_inputs(
    tmp_path: Path,
    *,
    schedule_rows: list[dict[str, object]],
    pages: dict[str, bytes],
    men_rows: list[dict[str, object]],
    ladies_rows: list[dict[str, object]],
    alias_rows: list[dict[str, object]] | None = None,
) -> dict[str, Path]:
    accepted = tmp_path / "accepted"
    schedule = write_csv(accepted / "races.csv", SCHEDULE_FIELDS, schedule_rows)
    men_history = write_csv(accepted / "men.csv", HISTORY_FIELDS, men_rows)
    ladies_history = write_csv(accepted / "ladies.csv", HISTORY_FIELDS, ladies_rows)
    aliases = write_csv(accepted / "aliases.csv", ALIAS_FIELDS, alias_rows or [])

    def fetcher(url: str) -> bytes:
        return pages[url.rsplit("=", 1)[-1]]

    evidence = tmp_path / "evidence"
    acquire_startlists(
        schedule=schedule,
        logical_date=date(2025, 11, 21),
        output_dir=evidence,
        protected_sources=(men_history, ladies_history, aliases),
        fetcher=fetcher,
    )
    return {
        "acquired_startlists": evidence / "startlists.csv",
        "acquisition_report": evidence / "report.json",
        "men_history": men_history,
        "ladies_history": ladies_history,
        "aliases_path": aliases,
    }


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_builds_exact_date_individual_candidate_with_as_of_ratings(
    tmp_path: Path,
) -> None:
    men = [
        history_row(1, "JUMPER One", "M", 1400),
        history_row(2, "JUMPER Two", "M", 1500),
        history_row(3, "JUMPER Three", "M", 1600),
        history_row(1, "JUMPER One", "M", 9999, Date="2025-11-21", Race="2"),
    ]
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=men,
        ladies_rows=[],
    )
    output = tmp_path / "normalized"
    report = normalize_startlists(
        **inputs, logical_date=date(2025, 11, 21), output_dir=output
    )
    assert report["status"] == "ready"
    assert report["prediction_eligible"] is True
    assert report["accepted_startlists_modified"] is False
    rows = read_rows(output / "candidates/startlist_races_men.csv")
    assert [row["ID"] for row in rows] == ["3", "2", "1"]
    assert rows[-1]["Elo"] == "1400.0"
    assert {row["Race_Date"] for row in rows} == {"11/21/2025"}
    assert {row["Race1_Prob"] for row in rows} == {"1.0"}


def test_multiple_individual_events_become_per_race_probabilities(
    tmp_path: Path,
) -> None:
    second = schedule_row(
        Race="2",
        Startlist="https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7461",
    )
    men = [
        history_row(1, "JUMPER One", "M", 1400),
        history_row(2, "JUMPER Two", "M", 1500),
        history_row(3, "JUMPER Three", "M", 1600),
    ]
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row(), second],
        pages={
            "7459": fixture("jp-individual-results.html"),
            "7461": fixture("jp-individual-results.html"),
        },
        men_rows=men,
        ladies_rows=[],
    )
    output = tmp_path / "normalized"
    normalize_startlists(**inputs, logical_date=date(2025, 11, 21), output_dir=output)
    rows = read_rows(output / "candidates/startlist_races_men.csv")
    assert {row["Race1_Prob"] for row in rows} == {"1.0"}
    assert {row["Race2_Prob"] for row in rows} == {"1.0"}


def test_builds_mixed_team_members_and_deterministic_elo_aggregates(
    tmp_path: Path,
) -> None:
    men = [
        history_row(11, "MEMBER One", "M", 1400),
        history_row(13, "MEMBER Three", "M", 1600),
    ]
    ladies = [
        history_row(12, "MEMBER Two", "L", 1500),
        history_row(14, "MEMBER Four", "L", 1700),
    ]
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row(Sex="Mixed", RaceType="Mixed Team")],
        pages={"7459": fixture("jp-team-results.html")},
        men_rows=men,
        ladies_rows=ladies,
    )
    output = tmp_path / "normalized"
    report = normalize_startlists(
        **inputs, logical_date=date(2025, 11, 21), output_dir=output
    )
    assert report["outputs"]["mixed_team"]["rows"] == 2
    rows = read_rows(output / "candidates/startlist_mixed_team_races_teams.csv")
    assert rows[0]["Member_1_ID"] == "11"
    assert rows[0]["Member_2_ID"] == "12"
    assert rows[0]["Member_1_Sex"] == "M"
    assert rows[0]["Member_2_Sex"] == "L"
    assert rows[0]["Avg_Elo"] == "1450.0"


def test_builds_same_sex_team_candidate(tmp_path: Path) -> None:
    men = [
        history_row(11, "MEMBER One", "M", 1400),
        history_row(12, "MEMBER Two", "M", 1500),
        history_row(13, "MEMBER Three", "M", 1600),
        history_row(14, "MEMBER Four", "M", 1700),
    ]
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row(Sex="M", RaceType="Team Large")],
        pages={"7459": fixture("jp-team-results.html")},
        men_rows=men,
        ladies_rows=[],
    )
    output = tmp_path / "normalized"
    report = normalize_startlists(
        **inputs, logical_date=date(2025, 11, 21), output_dir=output
    )
    assert report["outputs"]["men_team"]["rows"] == 2
    rows = read_rows(output / "candidates/startlist_team_races_men.csv")
    assert rows[0]["Member_1_ID"] == "11"
    assert rows[0]["Member_2_ID"] == "12"
    assert "Member_1_Sex" not in rows[0]
    assert rows[1]["Avg_Large_Elo"] == "1650.0"


def test_athlete_without_pre_race_rating_uses_baseline_features(tmp_path: Path) -> None:
    men = [
        history_row(1, "JUMPER One", "M", 1400),
        history_row(2, "JUMPER Two", "M", 1500),
        history_row(3, "JUMPER Three", "M", 1600, Date="2025-11-21"),
    ]
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=men,
        ladies_rows=[],
    )
    output = tmp_path / "normalized"
    report = normalize_startlists(
        **inputs, logical_date=date(2025, 11, 21), output_dir=output
    )
    assert report["status"] == "ready"
    assert report["baseline_rating_athletes"] == 1
    rows = read_rows(output / "candidates/startlist_races_men.csv")
    assert next(row for row in rows if row["ID"] == "3")["Elo"] == "1425.0"


def test_missing_identity_uses_stable_provisional_fis_id(tmp_path: Path) -> None:
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=[
            history_row(1, "JUMPER One", "M", 1400),
            history_row(2, "JUMPER Two", "M", 1500),
        ],
        ladies_rows=[],
    )
    output = tmp_path / "normalized"
    report = normalize_startlists(
        **inputs, logical_date=date(2025, 11, 21), output_dir=output
    )
    assert report["status"] == "ready"
    assert report["unresolved_identities"] == 0
    assert report["provisional_fis_identities"] == 1
    assert report["prediction_eligible"] is True
    rows = read_rows(output / "candidates/startlist_races_men.csv")
    provisional = next(row for row in rows if row["FISName"] == "JUMPER Three")
    assert provisional["ID"] == "-103"


def test_ambiguous_identity_still_blocks_prediction_candidates(tmp_path: Path) -> None:
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=[
            history_row(1, "JUMPER One", "M", 1400),
            history_row(2, "JUMPER Two", "M", 1500),
            history_row(3, "JUMPER Three", "M", 1600),
            history_row(4, "Three Jumper", "M", 1300),
        ],
        ladies_rows=[],
    )
    output = tmp_path / "normalized"
    report = normalize_startlists(
        **inputs, logical_date=date(2025, 11, 21), output_dir=output
    )
    assert report["status"] == "blocked"
    assert report["unresolved_identities"] == 1
    assert report["prediction_eligible"] is False
    assert not (output / "candidates").exists()


def test_mixed_team_provisional_sex_follows_resolved_team_order(
    tmp_path: Path,
) -> None:
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row(Sex="Mixed", RaceType="Mixed Team")],
        pages={"7459": fixture("jp-team-results.html")},
        men_rows=[history_row(11, "MEMBER One", "M", 1400)],
        ladies_rows=[
            history_row(12, "MEMBER Two", "L", 1500),
            history_row(14, "MEMBER Four", "L", 1700),
        ],
    )
    output = tmp_path / "normalized"
    report = normalize_startlists(
        **inputs, logical_date=date(2025, 11, 21), output_dir=output
    )
    assert report["status"] == "ready"
    assert report["provisional_fis_identities"] == 1
    rows = read_rows(output / "candidates/startlist_mixed_team_races_teams.csv")
    second_team = rows[1]
    assert second_team["Member_1_ID"] == "-203"
    assert second_team["Member_1_Sex"] == "M"


def test_unrelated_stale_alias_does_not_block_startlist(tmp_path: Path) -> None:
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=[
            history_row(1, "JUMPER One", "M", 1400),
            history_row(2, "JUMPER Two", "M", 1500),
            history_row(3, "JUMPER Three", "M", 1600),
        ],
        ladies_rows=[],
        alias_rows=[
            {
                "fis_competitor_id": "999999",
                "fis_code": "9999",
                "fis_name": "UNRELATED Athlete",
                "canonical_sex": "M",
                "canonical_id": "404",
                "canonical_name": "Missing Athlete",
            }
        ],
    )
    report = normalize_startlists(
        **inputs,
        logical_date=date(2025, 11, 21),
        output_dir=tmp_path / "normalized",
    )
    assert report["status"] == "ready"


def test_invalid_relevant_alias_fails_closed(tmp_path: Path) -> None:
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=[
            history_row(1, "JUMPER One", "M", 1400),
            history_row(2, "JUMPER Two", "M", 1500),
            history_row(3, "JUMPER Three", "M", 1600),
        ],
        ladies_rows=[],
        alias_rows=[
            {
                "fis_competitor_id": "101",
                "fis_code": "7001",
                "fis_name": "JUMPER One",
                "canonical_sex": "M",
                "canonical_id": "404",
                "canonical_name": "Missing Athlete",
            }
        ],
    )
    with pytest.raises(StartlistNormalizationError, match="unknown canonical"):
        normalize_startlists(
            **inputs,
            logical_date=date(2025, 11, 21),
            output_dir=tmp_path / "normalized",
        )


def test_acquisition_checksum_drift_is_rejected(tmp_path: Path) -> None:
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=[
            history_row(1, "JUMPER One", "M", 1400),
            history_row(2, "JUMPER Two", "M", 1500),
            history_row(3, "JUMPER Three", "M", 1600),
        ],
        ladies_rows=[],
    )
    inputs["acquired_startlists"].write_bytes(
        inputs["acquired_startlists"].read_bytes() + b"\n"
    )
    with pytest.raises(StartlistNormalizationError, match="checksum"):
        normalize_startlists(
            **inputs,
            logical_date=date(2025, 11, 21),
            output_dir=tmp_path / "normalized",
        )


def test_candidate_output_cannot_be_inside_accepted_inputs(tmp_path: Path) -> None:
    inputs = prepared_inputs(
        tmp_path,
        schedule_rows=[schedule_row()],
        pages={"7459": fixture("jp-individual-results.html")},
        men_rows=[
            history_row(1, "JUMPER One", "M", 1400),
            history_row(2, "JUMPER Two", "M", 1500),
            history_row(3, "JUMPER Three", "M", 1600),
        ],
        ladies_rows=[],
    )
    with pytest.raises(StartlistNormalizationError, match="cannot be inside"):
        normalize_startlists(
            **inputs,
            logical_date=date(2025, 11, 21),
            output_dir=inputs["men_history"].parent / "candidate",
        )
