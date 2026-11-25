from __future__ import annotations

import csv
import hashlib
from datetime import date
from pathlib import Path

import pytest

from nordic_combined_results_acquire import (
    AcquisitionError,
    acquire_results,
    page_status,
    parse_individual_results,
    parse_team_results,
    validate_page_identity,
)
from nordic_combined_results_gap import MissingRace

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT.parents[1] / "skijump/polars/tests/fixtures"
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


def inputs(tmp_path: Path, rows: list[dict[str, object]]) -> dict[str, object]:
    accepted = tmp_path / "accepted"
    accepted.mkdir()
    return {
        "schedule": write_csv(accepted / "races.csv", SCHEDULE_FIELDS, rows),
        "individual_histories": [
            write_csv(accepted / "individual.csv", HISTORY_FIELDS, [])
        ],
        "team_histories": [write_csv(accepted / "team.csv", HISTORY_FIELDS, [])],
        "team_sprint_histories": [
            write_csv(accepted / "team-sprint.csv", HISTORY_FIELDS, [])
        ],
    }


def gap(**overrides: object) -> MissingRace:
    values: dict[str, object] = {
        "date": "2025-11-22",
        "sex": "M",
        "season": 2026,
        "race": 1,
        "event_kind": "individual",
        "city": "Lillehammer",
        "country": "Norway",
        "distance": "10",
        "race_type": "Individual",
        "championship": False,
        "results_url": (
            "https://www.fis-ski.com/DB/general/results.html?sectorcode=NK&raceid=3474"
        ),
        "fis_race_id": "3474",
    }
    values.update(overrides)
    return MissingRace(**values)


def test_parses_individuals_and_ranked_team_members() -> None:
    individuals = parse_individual_results(fixture("jp-individual-results.html"))
    teams = parse_team_results(fixture("jp-team-results.html"))
    assert [row["place"] for row in individuals] == [1, 2, 3]
    assert individuals[0]["fis_competitor_id"] == "101"
    assert len(teams) == 4
    assert teams[0]["team_name"] == "TEAM ONE"
    assert teams[-1]["member_order"] == 2


def test_team_sprint_uses_team_parser_and_empty_status_is_explicit() -> None:
    status, rows = page_status(fixture("jp-team-results.html"), "team_sprint")
    assert status == "classified"
    assert len(rows) == 4
    assert page_status(fixture("jp-cancelled-results.html"), "individual") == (
        "cancelled",
        [],
    )
    assert page_status(fixture("jp-no-results.html"), "individual") == (
        "no_results",
        [],
    )


def test_page_identity_must_match_city_and_season() -> None:
    with pytest.raises(AcquisitionError, match="identity mismatch"):
        validate_page_identity(
            fixture("jp-individual-results.html"), gap(city="Planica")
        )


def test_acquisition_retains_hashes_without_modifying_accepted_inputs(
    tmp_path: Path,
) -> None:
    rows = [
        schedule_row(),
        schedule_row(
            Sex="Mixed",
            Race=2,
            RaceType="Team Sprint",
            Startlist=(
                "https://www.fis-ski.com/DB/general/results.html?"
                "sectorcode=NK&raceid=3529"
            ),
        ),
    ]
    source_inputs = inputs(tmp_path, rows)
    source_paths = [
        source_inputs["schedule"],
        *source_inputs["individual_histories"],
        *source_inputs["team_histories"],
        *source_inputs["team_sprint_histories"],
    ]
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}
    pages = {
        "3474": fixture("jp-individual-results.html"),
        "3529": fixture("jp-team-results.html"),
    }
    output = tmp_path / "evidence"
    report = acquire_results(
        **source_inputs,
        as_of=date(2025, 11, 22),
        output_dir=output,
        fetcher=lambda url: pages[url.rsplit("=", 1)[-1]],
    )
    assert report["classified_race_count"] == 2
    assert report["unresolved_race_count"] == 0
    assert report["acquired_result_rows"] == 7
    assert report["accepted_histories_modified"] is False
    assert report["replay_eligible"] is True
    for item in report["races"]:
        evidence = (output / item["source_file"]).read_bytes()
        assert hashlib.sha256(evidence).hexdigest() == item["source_sha256"]
    after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}
    assert after == before


def test_unexplained_empty_page_blocks_replay_but_keeps_source(tmp_path: Path) -> None:
    source_inputs = inputs(tmp_path, [schedule_row()])
    output = tmp_path / "evidence"
    report = acquire_results(
        **source_inputs,
        as_of=date(2025, 11, 22),
        output_dir=output,
        fetcher=lambda _: fixture("jp-no-results.html"),
    )
    assert report["unresolved_race_count"] == 1
    assert report["replay_eligible"] is False
    assert (output / report["races"][0]["source_file"]).is_file()


def test_output_cannot_be_inside_accepted_directory(tmp_path: Path) -> None:
    source_inputs = inputs(tmp_path, [schedule_row()])
    with pytest.raises(AcquisitionError, match="cannot be inside"):
        acquire_results(
            **source_inputs,
            as_of=date(2025, 11, 22),
            output_dir=source_inputs["schedule"].parent / "candidate",
            fetcher=lambda _: fixture("jp-individual-results.html"),
        )

