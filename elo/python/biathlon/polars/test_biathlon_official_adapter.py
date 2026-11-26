from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from biathlon_official_adapter import (
    CORE_RESULT_FIELDS,
    RESULT_FIELDS,
    IdentityResolver,
    adapt_evidence,
    normalized_name,
)
from ibu_source_contract import IBUContractError, capture_contract
from test_ibu_source_contract import EVENT_ID, FIXTURES, SPRINT_ID, FixtureClient


ACCEPTED = Path(__file__).parent / "excel365"


def _write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def test_normalized_name_is_order_and_diacritic_independent() -> None:
    assert normalized_name("MINKKINEN Suvi") == normalized_name("Suvi Minkkinen")
    assert normalized_name("Jørgen Brænd", loose=True) == "brand jorgen"


def test_resolver_collapses_historical_metadata_to_local_identity() -> None:
    rows = [
        {"Date": "2025-01-01", "Exp": "1", "Sex": "M", "ID": "7", "Skier": "Aksel Oe", "Nation": "Norway", "Birthday": "1965-01-01T00:00:00"},
        {"Date": "2026-01-01", "Exp": "2", "Sex": "M", "ID": "7", "Skier": "Aksel Ø", "Nation": "Norway", "Birthday": "2000-01-01T00:00:00"},
    ]
    identity, method = IdentityResolver(rows).resolve(
        ibu_id="BTNOR1", official_name="OE Aksel", sex="M"
    )
    assert method == "exact_name"
    assert identity is not None
    assert identity.local_id == "7"
    assert identity.birthday == "2000-01-01T00:00:00"


def test_resolver_fails_closed_on_ambiguous_name_and_requires_reviewed_alias() -> None:
    rows = [
        {"Date": "2026-01-01", "Exp": "1", "Sex": "L", "ID": local_id, "Skier": "Anna Test", "Nation": nation, "Birthday": birthday}
        for local_id, nation, birthday in (
            ("1", "Sweden", "2000-01-01T00:00:00"),
            ("2", "Finland", "2001-01-01T00:00:00"),
        )
    ]
    identity, method = IdentityResolver(rows).resolve(
        ibu_id="BTSWE1", official_name="TEST Anna", sex="L"
    )
    assert identity is None
    assert method == "ambiguous_exact_name"
    identity, method = IdentityResolver(rows, {"BTSWE1": "1"}).resolve(
        ibu_id="BTSWE1", official_name="TEST Anna", sex="L"
    )
    assert identity is not None and identity.local_id == "1"
    assert method == "reviewed_alias"


def test_sprint_fixture_adapts_to_exact_accepted_core_rows(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    capture_contract(
        client=FixtureClient(),  # type: ignore[arg-type]
        season_id="2526",
        event_id=EVENT_ID,
        race_ids=[SPRINT_ID],
        output_dir=evidence,
    )
    accepted = tmp_path / "accepted"
    source_ladies = _read_csv(ACCEPTED / "ladies_scrape_update.csv")
    expected = [
        row for row in source_ladies
        if row["Date"] == "2025-12-05" and row["RaceType"] == "Sprint" and int(row["Place"]) <= 5
    ]
    assert len(expected) == 5
    _write_csv(accepted / "ladies_scrape_update.csv", expected, RESULT_FIELDS)
    _write_csv(accepted / "men_scrape_update.csv", [], RESULT_FIELDS)
    races = [
        row for row in _read_csv(ACCEPTED / "races2026.csv")
        if row["Date"] == "12/05/2025" and row["RaceType"] == "Sprint" and row["Sex"] == "L"
    ]
    _write_csv(accepted / "races2026.csv", races, list(races[0]))

    output = tmp_path / "candidate"
    report = adapt_evidence(
        evidence_dir=evidence,
        accepted_dir=accepted,
        output_dir=output,
    )
    assert report["status"] == "passed"
    assert report["identity"] == {
        "resolved": 5,
        "unresolved": 0,
        "methods": {"exact_name": 4, "loose_name": 1},
        "unresolved_examples": [],
    }
    assert report["result_parity"]["L"]["core_parity"] == "passed"
    actual = _read_csv(output / "ladies_results_candidate.csv")
    assert {
        tuple(row[field] for field in CORE_RESULT_FIELDS) for row in actual
    } == {
        tuple(row[field] for field in CORE_RESULT_FIELDS) for row in expected
    }
    assert _read_csv(output / "schedule_candidate.csv")[0]["RaceType"] == "Sprint"
    assert report["schedule_parity"]["core_parity"] == "passed"
    assert report["accepted_data_mutated"] is False
    assert report["publication"] == {"status": "disabled"}


def test_adapter_rejects_output_inside_accepted_tree(tmp_path: Path) -> None:
    with pytest.raises(IBUContractError, match="outside accepted"):
        adapt_evidence(
            evidence_dir=tmp_path / "evidence",
            accepted_dir=tmp_path / "accepted",
            output_dir=tmp_path / "accepted" / "candidate",
        )
