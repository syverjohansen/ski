from __future__ import annotations

import csv
import json
import shutil
import sys
from datetime import date
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from biathlon_startlist_acquire import acquire_startlists
from biathlon_startlist_normalize import _member_sex, normalize_startlists


FIXTURES = HERE / "tests/fixtures/ibu"
EXCEL = HERE / "excel365"


class FakeClient:
    def __init__(self, *, ready: bool = True):
        self.ready = ready
        self.results_called = False

    def events(self, season_id: str, level: int = 1):
        return json.loads((FIXTURES / "events-2526-level1.json").read_text())

    def competitions(self, event_id: str):
        rows = json.loads((FIXTURES / "competitions-cp01.json").read_text())
        selected = [row for row in rows if row["RaceId"] == "BT2526SWRLCP01SWSP"]
        row = selected[0]
        row["StartTime"] = "2025-12-05T15:00:00Z"
        if self.ready:
            row.update(StatusText="Start List", ResultStatus="START_LIST", ScheduleStatus="SCHEDULED")
        else:
            row.update(StatusText="Prov. Start List", ResultStatus="PROVISIONAL_STARTLIST", ScheduleStatus="SCHEDULED")
        return selected

    def results(self, race_id: str):
        self.results_called = True
        return json.loads((FIXTURES / "results-women-sprint.json").read_text())

    def reports(self, race_id: str):
        return json.loads((FIXTURES / "reports-women-sprint.json").read_text())


def _schedule(path: Path) -> None:
    path.write_text(
        "Date,Startlist,Sex,City,Country,Distance,RaceType,Period,Elevation,Championship,Race\n"
        "12/05/2025,https://www.biathlonworld.com/startlist/BT2526SWRLCP01SWSP,L,Ostersund,Sweden,7.5,Sprint,1,0,0,1\n",
        encoding="utf-8",
    )


def test_acquisition_requires_complete_official_startlist(tmp_path: Path) -> None:
    schedule = tmp_path / "races.csv"
    _schedule(schedule)
    client = FakeClient(ready=True)
    report = acquire_startlists(
        client=client, schedule=schedule, logical_date=date(2025, 12, 5),
        output_dir=tmp_path / "evidence",
    )
    assert report["status"] == "ready"
    assert client.results_called
    assert report["races"][0]["summary"]["athlete_rows"] == 5
    assert report["publication"] == {"status": "disabled"}


def test_provisional_startlist_waits_without_reading_rows(tmp_path: Path) -> None:
    schedule = tmp_path / "races.csv"
    _schedule(schedule)
    client = FakeClient(ready=False)
    report = acquire_startlists(
        client=client, schedule=schedule, logical_date=date(2025, 12, 5),
        output_dir=tmp_path / "evidence",
    )
    assert report["status"] == "waiting"
    assert not client.results_called
    assert report["races"][0]["reason"] == "official_start_list_not_ready"


def test_normalizes_individual_official_roster_without_mutating_accepted(tmp_path: Path) -> None:
    schedule = tmp_path / "races.csv"
    evidence = tmp_path / "evidence"
    output = tmp_path / "normalized"
    _schedule(schedule)
    acquire_startlists(
        client=FakeClient(), schedule=schedule, logical_date=date(2025, 12, 5),
        output_dir=evidence,
    )
    before = (EXCEL / "ladies_chrono.csv").stat().st_mtime_ns
    report = normalize_startlists(
        acquisition_dir=evidence,
        men_history=EXCEL / "men_chrono.csv",
        ladies_history=EXCEL / "ladies_chrono.csv",
        aliases=HERE / "ibu_identity_aliases.csv",
        output_dir=output,
    )
    candidate = output / "candidates/startlist_races_ladies.csv"
    with candidate.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert report["status"] == "ready"
    assert report["prediction_eligible"] is True
    assert len(rows) == 5
    assert len({row["ID"] for row in rows}) == 5
    assert {row["Race1_Prob"] for row in rows} == {"1.0"}
    assert (EXCEL / "ladies_chrono.csv").stat().st_mtime_ns == before


def test_normalizes_complete_relay_rosters(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    raw = evidence / "raw"
    raw.mkdir(parents=True)
    race_id = "BT2526SWRLCP01SWRL"
    shutil.copy(FIXTURES / "results-women-relay.json", raw / f"{race_id}-results.json")
    acquisition = {
        "schema_version": "nordicnumbers.biathlon-startlist-acquisition/v1",
        "status": "ready", "logical_date": "2025-11-29",
        "races": [{
            "race_id": race_id, "event_kind": "relay", "sex": "L", "status": "ready",
            "gender_order": "",
            "schedule": {"City": "Ostersund", "Country": "Sweden", "RaceType": "Relay"},
        }],
    }
    (evidence / "report.json").write_text(json.dumps(acquisition), encoding="utf-8")
    report = normalize_startlists(
        acquisition_dir=evidence,
        men_history=EXCEL / "men_chrono.csv",
        ladies_history=EXCEL / "ladies_chrono.csv",
        aliases=HERE / "ibu_identity_aliases.csv",
        output_dir=tmp_path / "normalized",
    )
    target = tmp_path / "normalized/candidates/startlist_relay_races_teams_ladies.csv"
    with target.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert report["outputs"]["ladies_relay"]["rows"] == 3
    assert len(rows) == 3
    assert all(row["Member_4_ID"] for row in rows)


def test_normalization_refuses_waiting_acquisition(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / "report.json").write_text(json.dumps({
        "schema_version": "nordicnumbers.biathlon-startlist-acquisition/v1",
        "status": "waiting",
    }))
    with pytest.raises(Exception, match="complete ready acquisition"):
        normalize_startlists(
            acquisition_dir=evidence,
            men_history=EXCEL / "men_chrono.csv",
            ladies_history=EXCEL / "ladies_chrono.csv",
            aliases=HERE / "ibu_identity_aliases.csv",
            output_dir=tmp_path / "normalized",
        )


def test_mixed_gender_order_comes_from_official_competition() -> None:
    mixed = {
        "race_id": "MIXED", "event_kind": "mixed_relay",
        "sex": "Mixed", "gender_order": "W+M",
    }
    single = {**mixed, "event_kind": "single_mixed_relay"}
    assert [_member_sex(mixed, leg) for leg in range(1, 5)] == ["L", "L", "M", "M"]
    assert [_member_sex(single, leg) for leg in range(1, 5)] == ["L", "M", "L", "M"]
