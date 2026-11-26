from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest

from nordic_combined_results_gap import (
    audit_races,
    build_report,
    event_kind,
    load_schedule,
    validate_results_url,
)

ROOT = Path(__file__).resolve().parent
SCHEDULE_FIELDS = [
    "Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
    "Race",
]
HISTORY_FIELDS = ["Date", "Sex", "Season", "Race"]


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def schedule_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "11/22/2025",
        "Startlist": (
            "https://www.fis-ski.com/DB/general/results.html?sectorcode=NK&raceid=3474"
        ),
        "Sex": "M",
        "City": "Lillehammer",
        "Country": "Norway",
        "Distance": "10",
        "RaceType": "Individual",
        "Period": "1",
        "Elevation": "323",
        "Championship": "0",
        "Race": "1",
    }
    row.update(overrides)
    return row


def test_event_formats_remain_semantically_distinct() -> None:
    assert event_kind("Individual") == "individual"
    assert event_kind("IndividualCompact") == "individual"
    assert event_kind("Mass Start") == "individual"
    assert event_kind("Team") == "team"
    assert event_kind("Team Sprint") == "team_sprint"
    with pytest.raises(ValueError, match="unsupported"):
        event_kind("Relay")


def test_only_nordic_combined_fis_urls_are_accepted() -> None:
    url = "https://www.fis-ski.com/DB/general/results.html?sectorcode=NK&raceid=3529"
    assert validate_results_url(url) == "3529"
    with pytest.raises(ValueError, match="Nordic-combined"):
        validate_results_url(url.replace("sectorcode=NK", "sectorcode=JP"))


def test_future_races_and_mixed_individuals_fail_closed(tmp_path: Path) -> None:
    schedule = write_csv(
        tmp_path / "races.csv",
        SCHEDULE_FIELDS,
        [schedule_row(Date="11/23/2025")],
    )
    assert load_schedule(schedule, date(2025, 11, 22)) == []
    write_csv(
        schedule,
        SCHEDULE_FIELDS,
        [schedule_row(Sex="Mixed", RaceType="Individual")],
    )
    with pytest.raises(ValueError, match="Mixed schedule rows"):
        load_schedule(schedule, date(2025, 11, 22))


def test_wrong_sex_history_cannot_close_mixed_team_sprint(tmp_path: Path) -> None:
    schedule = write_csv(
        tmp_path / "races.csv",
        SCHEDULE_FIELDS,
        [schedule_row(Sex="Mixed", RaceType="Team Sprint")],
    )
    history = write_csv(
        tmp_path / "team-sprint.csv",
        HISTORY_FIELDS,
        [{"Date": "2025-11-22", "Sex": "M", "Season": 2026, "Race": 1}],
    )
    missing, completed = audit_races(
        schedule=schedule,
        individual_histories=[],
        team_histories=[],
        team_sprint_histories=[history],
        as_of=date(2025, 11, 22),
    )
    assert completed == 1
    assert [(item.sex, item.event_kind) for item in missing] == [
        ("Mixed", "team_sprint")
    ]


def test_history_race_number_is_not_completion_identity(tmp_path: Path) -> None:
    schedule = write_csv(tmp_path / "races.csv", SCHEDULE_FIELDS, [schedule_row()])
    history = write_csv(
        tmp_path / "individual.csv",
        HISTORY_FIELDS,
        [{"Date": "2025-11-22", "Sex": "M", "Season": 2026, "Race": 99}],
    )
    missing, completed = audit_races(
        schedule=schedule,
        individual_histories=[history],
        team_histories=[],
        team_sprint_histories=[],
        as_of=date(2025, 11, 22),
    )
    assert completed == 1
    assert missing == []


def test_2026_archive_exposes_known_historical_result_gaps() -> None:
    relay = ROOT / "relay/excel365"
    missing, completed = audit_races(
        schedule=ROOT / "excel365/races2026.csv",
        individual_histories=[
            ROOT / "excel365/men_chrono.csv",
            ROOT / "excel365/ladies_chrono.csv",
        ],
        team_histories=[
            relay / "men_team_chrono.csv",
            relay / "mixed_team_chrono.csv",
        ],
        team_sprint_histories=[
            relay / "team_sprint_chrono.csv",
            relay / "men_team_sprint_chrono.csv",
        ],
        as_of=date(2026, 3, 15),
    )
    assert completed == 39
    assert [race.fis_race_id for race in missing] == [
        "3498",
        "3497",
        "3500",
        "3499",
        "3529",
    ]
    report = build_report(missing, as_of=date(2026, 3, 15), completed_count=completed)
    assert report["missing_race_count"] == 5
    assert report["missing_team_sprint_race_count"] == 2
    assert report["missing_individual_race_count"] == 2

