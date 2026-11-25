from __future__ import annotations

import csv
import hashlib
from datetime import date
from pathlib import Path

import pytest

from nordic_combined_startlist_acquire import (
    StartlistAcquisitionError,
    acquire_startlists,
    parse_individual_entrants,
    parse_team_entrants,
)


FIXTURES = Path(__file__).resolve().parents[2] / "skijump/polars/tests/fixtures"
FIELDS = [
    "Date", "Final_Date", "Startlist", "Sex", "City", "Country", "Distance",
    "RaceType", "Period", "Elevation", "Championship", "Race",
]


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def row(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "Date": "11/21/2025", "Final_Date": "11/21/2025",
        "Startlist": "https://www.fis-ski.com/DB/general/results.html?sectorcode=NK&raceid=7459",
        "Sex": "M", "City": "Lillehammer", "Country": "Norway",
        "Distance": "10", "RaceType": "Individual", "Period": "1",
        "Elevation": "323", "Championship": "0", "Race": "1",
    }
    value.update(overrides)
    return value


def schedule(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    accepted = tmp_path / "accepted"
    accepted.mkdir()
    path = accepted / "races.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def test_parses_individual_and_team_source_identities() -> None:
    individuals = parse_individual_entrants(fixture("jp-individual-results.html"))
    teams = parse_team_entrants(fixture("jp-team-results.html"))
    assert [item["fis_competitor_id"] for item in individuals] == ["101", "102", "103"]
    assert [(item["team_nation"], item["member_order"]) for item in teams] == [
        ("AUT", 1), ("AUT", 2), ("GER", 1), ("GER", 2)
    ]


def test_exact_date_acquisition_is_isolated_and_checksummed(tmp_path: Path) -> None:
    source = schedule(tmp_path, [row(), row(Date="11/22/2025", Race="2")])
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    calls: list[str] = []
    report = acquire_startlists(
        schedule=source, logical_date=date(2025, 11, 21),
        output_dir=tmp_path / "evidence",
        fetcher=lambda url: calls.append(url) or fixture("jp-individual-results.html"),
    )
    assert len(calls) == 1
    assert report["status"] == "ready"
    assert report["prediction_eligible"] is True
    assert report["entrant_row_count"] == 3
    assert report["normalization_status"] == "source_identities_only"
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before


@pytest.mark.parametrize(
    ("race_type", "sex", "expected"),
    [("Team", "M", "team"), ("Team Sprint", "M", "team_sprint"),
     ("Team", "Mixed", "team")],
)
def test_team_formats_use_member_contract(
    tmp_path: Path, race_type: str, sex: str, expected: str
) -> None:
    source = schedule(tmp_path, [row(RaceType=race_type, Sex=sex)])
    report = acquire_startlists(
        schedule=source, logical_date=date(2025, 11, 21),
        output_dir=tmp_path / "evidence",
        fetcher=lambda _: fixture("jp-team-results.html"),
    )
    assert report["status"] == "ready"
    assert report["events"][0]["event_kind"] == expected
    assert report["entrant_row_count"] == 4


def test_missing_official_list_waits_without_fabricating(tmp_path: Path) -> None:
    source = schedule(tmp_path, [row()])
    report = acquire_startlists(
        schedule=source, logical_date=date(2025, 11, 21),
        output_dir=tmp_path / "evidence",
        fetcher=lambda _: fixture("jp-no-results.html"),
    )
    assert report["status"] == "waiting"
    assert report["prediction_eligible"] is False
    assert report["entrant_row_count"] == 0


def test_page_identity_mismatch_and_accepted_output_are_blocked(tmp_path: Path) -> None:
    source = schedule(tmp_path, [row(City="Oslo")])
    with pytest.raises(StartlistAcquisitionError, match="identity mismatch"):
        acquire_startlists(
            schedule=source, logical_date=date(2025, 11, 21),
            output_dir=tmp_path / "evidence",
            fetcher=lambda _: fixture("jp-individual-results.html"),
        )
    with pytest.raises(StartlistAcquisitionError, match="cannot be inside"):
        acquire_startlists(
            schedule=source, logical_date=date(2025, 11, 21),
            output_dir=source.parent / "evidence",
            fetcher=lambda _: fixture("jp-individual-results.html"),
        )
