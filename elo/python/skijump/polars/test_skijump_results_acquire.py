from __future__ import annotations

import csv
import hashlib
from datetime import date
from pathlib import Path

import pytest

from skijump_results_acquire import (
    AcquisitionError,
    acquire_results,
    page_status,
    parse_individual_results,
    parse_team_results,
    validate_page_identity,
)
from skijump_results_gap import MissingRace


FIXTURES = Path(__file__).with_name("tests") / "fixtures"
SCHEDULE_FIELDS = [
    "Date", "Final_Date", "Startlist", "Sex", "City", "Country",
    "HillSize", "RaceType", "Period", "Elevation", "Championship", "Race",
]
INDIVIDUAL_FIELDS = ["Date", "Sex", "Season", "Race", "TeamEvent"]
TEAM_FIELDS = ["Date", "Sex", "Season", "Race"]


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def schedule_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "11/21/2025",
        "Final_Date": "11/22/2025",
        "Startlist": (
            "https://www.fis-ski.com/DB/general/results.html?"
            "sectorcode=JP&raceid=7459"
        ),
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


def inputs(tmp_path: Path, rows: list[dict[str, object]]) -> dict[str, Path]:
    accepted = tmp_path / "accepted"
    accepted.mkdir()
    return {
        "schedule": write_csv(accepted / "races.csv", SCHEDULE_FIELDS, rows),
        "men_chrono": write_csv(accepted / "men.csv", INDIVIDUAL_FIELDS, []),
        "ladies_chrono": write_csv(
            accepted / "ladies.csv", INDIVIDUAL_FIELDS, []
        ),
        "men_team_chrono": write_csv(accepted / "men-team.csv", TEAM_FIELDS, []),
        "ladies_team_chrono": write_csv(
            accepted / "ladies-team.csv", TEAM_FIELDS, []
        ),
        "mixed_team_chrono": write_csv(
            accepted / "mixed-team.csv", TEAM_FIELDS, []
        ),
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
        "hill_size": "140",
        "race_type": "Large",
        "championship": False,
        "results_url": (
            "https://www.fis-ski.com/DB/general/results.html?"
            "sectorcode=JP&raceid=7459"
        ),
        "fis_race_id": "7459",
    }
    values.update(overrides)
    return MissingRace(**values)


def test_parses_individual_classification() -> None:
    results = parse_individual_results(fixture("jp-individual-results.html"))
    assert [row["place"] for row in results] == [1, 2, 3]
    assert results[0] == {
        "place": 1,
        "team_name": "",
        "team_nation": "",
        "member_order": "",
        "fis_competitor_id": "101",
        "fis_code": "7001",
        "name": "JUMPER One",
        "birth_year": "1998",
        "nation": "AUT",
    }


def test_parses_ranked_teams_and_members() -> None:
    results = parse_team_results(fixture("jp-team-results.html"))
    assert len(results) == 4
    assert results[0]["team_name"] == "TEAM ONE"
    assert results[0]["team_nation"] == "AUT"
    assert results[0]["member_order"] == 1
    assert results[-1]["place"] == 2
    assert results[-1]["fis_competitor_id"] == "204"


def test_distinguishes_cancelled_and_unexplained_empty_pages() -> None:
    assert page_status(fixture("jp-cancelled-results.html"), "individual") == (
        "cancelled", []
    )
    assert page_status(fixture("jp-no-results.html"), "individual") == (
        "no_results", []
    )


def test_page_identity_must_match_city_and_season() -> None:
    with pytest.raises(AcquisitionError, match="identity mismatch"):
        validate_page_identity(
            fixture("jp-individual-results.html"), gap(city="Planica")
        )


def test_acquisition_retains_sources_checksums_and_team_members(tmp_path: Path) -> None:
    rows = [
        schedule_row(),
        schedule_row(
            Sex="Mixed",
            Race=2,
            RaceType="Team Large",
            Startlist=(
                "https://www.fis-ski.com/DB/general/results.html?"
                "sectorcode=JP&raceid=7460"
            ),
        ),
    ]
    source_inputs = inputs(tmp_path, rows)
    before = {
        path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source_inputs.values()
    }
    pages = {
        "7459": fixture("jp-individual-results.html"),
        "7460": fixture("jp-team-results.html"),
    }

    def fetcher(url: str) -> bytes:
        race_id = url.rsplit("=", 1)[-1]
        return pages[race_id]

    output = tmp_path / "evidence"
    report = acquire_results(
        **source_inputs,
        as_of=date(2025, 11, 22),
        output_dir=output,
        fetcher=fetcher,
    )
    assert report["classified_race_count"] == 2
    assert report["unresolved_race_count"] == 0
    assert report["acquired_result_rows"] == 7
    assert report["accepted_histories_modified"] is False
    assert report["replay_eligible"] is True
    assert (output / "results.csv").is_file()
    for item in report["races"]:
        evidence = (output / item["source_file"]).read_bytes()
        assert hashlib.sha256(evidence).hexdigest() == item["source_sha256"]
    after = {
        path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source_inputs.values()
    }
    assert after == before


def test_unexplained_empty_page_blocks_replay_but_keeps_evidence(
    tmp_path: Path,
) -> None:
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
    assert report["races"][0]["status"] == "no_results"
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
