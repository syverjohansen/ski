from __future__ import annotations

import csv
import hashlib
from datetime import date
from pathlib import Path

import pytest
from skijump_startlist_acquire import (
    StartlistAcquisitionError,
    acquire_startlists,
    parse_individual_entrants,
    parse_team_entrants,
)

FIXTURES = Path(__file__).with_name("tests") / "fixtures"
SCHEDULE_FIELDS = [
    "Date", "Final_Date", "Startlist", "Sex", "City", "Country",
    "HillSize", "RaceType", "Period", "Elevation", "Championship", "Race",
]


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def schedule_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "11/21/2025", "Final_Date": "11/22/2025",
        "Startlist": "https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7459",
        "Sex": "M", "City": "Lillehammer", "Country": "Norway",
        "HillSize": "140", "RaceType": "Large", "Period": "1",
        "Elevation": "323", "Championship": "0", "Race": "1",
    }
    row.update(overrides)
    return row


def write_schedule(path: Path, rows: list[dict[str, object]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCHEDULE_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def accepted_schedule(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    accepted = tmp_path / "accepted"
    accepted.mkdir()
    return write_schedule(accepted / "races.csv", rows)


def test_parses_individual_source_identities() -> None:
    entrants = parse_individual_entrants(fixture("jp-individual-results.html"))
    assert [row["source_order"] for row in entrants] == [1, 2, 3]
    assert entrants[0]["fis_competitor_id"] == "101"
    assert entrants[0]["fis_code"] == "7001"


def test_parses_same_sex_or_mixed_team_members() -> None:
    entrants = parse_team_entrants(fixture("jp-team-results.html"))
    assert len(entrants) == 4
    assert entrants[0]["team_nation"] == "AUT"
    assert entrants[0]["member_order"] == 1
    assert entrants[-1]["fis_competitor_id"] == "204"


def test_exact_date_acquisition_is_isolated_and_checksummed(tmp_path: Path) -> None:
    accepted = tmp_path / "accepted"
    accepted.mkdir()
    schedule = write_schedule(
        accepted / "races.csv",
        [
            schedule_row(),
            schedule_row(
                Date="11/22/2025", Race=2, Sex="Mixed", RaceType="Mixed Team",
                Startlist="https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7460",
            ),
        ],
    )
    before = hashlib.sha256(schedule.read_bytes()).hexdigest()
    calls: list[str] = []

    def fetcher(url: str) -> bytes:
        calls.append(url)
        return fixture("jp-individual-results.html")

    output = tmp_path / "evidence"
    report = acquire_startlists(
        schedule=schedule,
        logical_date=date(2025, 11, 21),
        output_dir=output,
        fetcher=fetcher,
    )
    assert len(calls) == 1
    assert report["status"] == "ready"
    assert report["scheduled_event_count"] == 1
    assert report["entrant_row_count"] == 3
    assert report["accepted_startlists_modified"] is False
    assert report["prediction_eligible"] is True
    event = report["events"][0]
    assert hashlib.sha256((output / event["source_file"]).read_bytes()).hexdigest() == event["source_sha256"]
    assert hashlib.sha256(schedule.read_bytes()).hexdigest() == before


def test_team_event_uses_team_contract(tmp_path: Path) -> None:
    schedule = accepted_schedule(
        tmp_path,
        [schedule_row(Sex="Mixed", RaceType="Mixed Team")],
    )
    report = acquire_startlists(
        schedule=schedule,
        logical_date=date(2025, 11, 21),
        output_dir=tmp_path / "evidence",
        fetcher=lambda _: fixture("jp-team-results.html"),
    )
    assert report["status"] == "ready"
    assert report["events"][0]["event_kind"] == "mixed_team"
    assert report["entrant_row_count"] == 4


def test_missing_official_list_is_waiting_not_fabricated(tmp_path: Path) -> None:
    schedule = accepted_schedule(tmp_path, [schedule_row()])
    report = acquire_startlists(
        schedule=schedule,
        logical_date=date(2025, 11, 21),
        output_dir=tmp_path / "evidence",
        fetcher=lambda _: fixture("jp-no-results.html"),
    )
    assert report["status"] == "waiting"
    assert report["unresolved_event_count"] == 1
    assert report["entrant_row_count"] == 0
    assert report["prediction_eligible"] is False


def test_blank_startlist_url_is_waiting_without_network(tmp_path: Path) -> None:
    schedule = accepted_schedule(tmp_path, [schedule_row(Startlist="")])
    report = acquire_startlists(
        schedule=schedule,
        logical_date=date(2025, 11, 21),
        output_dir=tmp_path / "evidence",
        fetcher=lambda _: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )
    assert report["status"] == "waiting"
    assert report["events"][0]["status"] == "not_available"


def test_date_without_schedule_rows_is_explicit_no_work(tmp_path: Path) -> None:
    schedule = accepted_schedule(tmp_path, [schedule_row()])
    report = acquire_startlists(
        schedule=schedule,
        logical_date=date(2025, 11, 20),
        output_dir=tmp_path / "evidence",
        fetcher=lambda _: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )
    assert report["status"] == "no_work"
    assert report["scheduled_event_count"] == 0
    assert report["prediction_eligible"] is False


def test_page_identity_mismatch_fails_closed(tmp_path: Path) -> None:
    schedule = accepted_schedule(
        tmp_path, [schedule_row(City="Planica")]
    )
    with pytest.raises(StartlistAcquisitionError, match="identity mismatch"):
        acquire_startlists(
            schedule=schedule,
            logical_date=date(2025, 11, 21),
            output_dir=tmp_path / "evidence",
            fetcher=lambda _: fixture("jp-individual-results.html"),
        )


def test_output_cannot_be_inside_accepted_directory(tmp_path: Path) -> None:
    accepted = tmp_path / "accepted"
    accepted.mkdir()
    schedule = write_schedule(accepted / "races.csv", [schedule_row()])
    with pytest.raises(StartlistAcquisitionError, match="cannot be inside"):
        acquire_startlists(
            schedule=schedule,
            logical_date=date(2025, 11, 21),
            output_dir=accepted / "candidate",
            fetcher=lambda _: fixture("jp-individual-results.html"),
        )
