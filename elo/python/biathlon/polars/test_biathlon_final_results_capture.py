from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

import biathlon_final_results_capture as capture
import biathlon_results_gap as gap
from ibu_source_contract import IBUContractError


SEASON = "2627"
EVENT = "BT2627SWRLCP01"
FINAL_RACE = f"{EVENT}SMSP"
ACCEPTED_RACE = f"{EVENT}MXRL"
PENDING_RACE = f"{EVENT}SWSP"


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class FixtureClient:
    api_key = "must-not-be-persisted"

    def __init__(self) -> None:
        self.result_calls: list[str] = []
        self.report_calls: list[str] = []

    def events(self, season_id: str, level: int = 1):
        assert (season_id, level) == (SEASON, 1)
        return [
            {
                "EventId": EVENT,
                "SeasonId": SEASON,
                "Level": 1,
                "StartDate": "2026-11-23T00:00:00Z",
                "EndDate": "2026-11-29T23:59:59Z",
                "Description": "World Cup",
                "ShortDescription": "Kontiolahti",
                "Nat": "FIN",
                "EventClassificationId": "BTSWRLCP",
            }
        ]

    def competitions(self, event_id: str):
        assert event_id == EVENT
        final = {
            "StatusId": 11,
            "StatusText": "Final",
            "ResultStatus": "OFFICIAL",
            "ScheduleStatus": "FINISHED",
        }
        return [
            {
                **final,
                "RaceId": FINAL_RACE,
                "StartTime": "2026-11-26T15:15:00Z",
                "ShortDescription": "Men 10 km Sprint",
                "DisciplineId": "SP",
                "catId": "SM",
            },
            {
                **final,
                "RaceId": ACCEPTED_RACE,
                "StartTime": "2026-11-27T15:15:00Z",
                "ShortDescription": "Mixed Relay",
                "DisciplineId": "RL",
                "catId": "MX",
            },
            {
                "StatusId": 1,
                "StatusText": "Scheduled",
                "ResultStatus": "",
                "ScheduleStatus": "SCHEDULED",
                "RaceId": PENDING_RACE,
                "StartTime": "2026-11-28T15:15:00Z",
                "ShortDescription": "Women 7.5 km Sprint",
                "DisciplineId": "SP",
                "catId": "SW",
            },
        ]

    def results(self, race_id: str):
        self.result_calls.append(race_id)
        assert race_id == FINAL_RACE
        return {
            "RaceId": race_id,
            "Competition": {
                "RaceId": race_id,
                "StatusId": 11,
                "StatusText": "Final",
            },
            "Results": [
                {
                    "IBUId": "BTFIN10101199001",
                    "Name": "Fixture Athlete",
                    "Nat": "FIN",
                    "Rank": "1",
                    "IsTeam": False,
                }
            ],
        }

    def reports(self, race_id: str):
        self.report_calls.append(race_id)
        assert race_id == FINAL_RACE
        return {
            "RaceId": race_id,
            "Reports": [
                {
                    "Description": "Start List",
                    "URL": (
                        "https://ibu.blob.core.windows.net/docs/2627/"
                        f"{race_id}_SL.pdf"
                    ),
                }
            ],
        }

    def bio(self, ibu_id: str):
        return {
            "IBUId": ibu_id,
            "FullName": "Fixture Athlete",
            "FamilyName": "Athlete",
            "GivenName": "Fixture",
            "NAT": "FIN",
            "Birthdate": "1990-01-01T00:00:00",
            "GenderId": "M",
        }


def accepted_fixture(tmp_path: Path, *, include_final: bool = False):
    accepted = tmp_path / "accepted"
    individual = accepted / "individual"
    relay = accepted / "relay"
    schedule = individual / "races.csv"
    write_csv(
        schedule,
        [
            {"Date": "11/26/2026", "RaceType": "Sprint", "Sex": "M"},
            {"Date": "11/27/2026", "RaceType": "Mixed Relay", "Sex": "Mixed"},
            {"Date": "11/28/2026", "RaceType": "Sprint", "Sex": "L"},
        ],
    )
    men = [
        {"Date": "2026-11-27", "RaceType": "Mixed Relay", "Sex": "M", "ID": "1"}
    ]
    if include_final:
        men.insert(
            0,
            {"Date": "2026-11-26", "RaceType": "Sprint", "Sex": "M", "ID": "2"},
        )
    ladies = [
        {"Date": "2026-11-27", "RaceType": "Mixed Relay", "Sex": "L", "ID": "3"}
    ]
    for root in (individual, relay):
        write_csv(root / "men_scrape_update.csv", men)
        write_csv(root / "ladies_scrape_update.csv", ladies)
    return schedule, individual, relay


def audit_fixture(tmp_path: Path, client: FixtureClient, *, include_final: bool = False):
    schedule, individual, relay = accepted_fixture(tmp_path, include_final=include_final)
    evidence = tmp_path / "gap"
    gap.audit_gaps(
        client=client,  # type: ignore[arg-type]
        season_id=SEASON,
        schedule_path=schedule,
        individual_dir=individual,
        relay_dir=relay,
        output_dir=evidence,
    )
    return schedule, individual, relay, evidence


def run_capture(tmp_path: Path, *, include_final: bool = False):
    client = FixtureClient()
    schedule, individual, relay, evidence = audit_fixture(
        tmp_path, client, include_final=include_final
    )
    output = tmp_path / "capture"
    report = capture.capture_final_results(
        client=client,  # type: ignore[arg-type]
        evidence_dir=evidence,
        schedule_path=schedule,
        individual_dir=individual,
        relay_dir=relay,
        output_dir=output,
    )
    return report, output, client, schedule, individual, relay, evidence


def test_captures_only_audited_final_missing_results(tmp_path: Path) -> None:
    report, output, client, *_ = run_capture(tmp_path)

    assert report["status"] == "passed"
    assert report["summary"] == {
        "audited_final_missing": 1,
        "captured_results": 1,
        "waiting_for_first_final": False,
    }
    assert client.result_calls == [FINAL_RACE]
    assert client.report_calls == [FINAL_RACE]
    assert report["races"][0]["race_id"] == FINAL_RACE
    assert (output / "raw" / f"{FINAL_RACE}-results.json").is_file()
    assert not (output / "raw" / f"{PENDING_RACE}-results.json").exists()


def test_noop_capture_waits_without_fetching_pending_results(tmp_path: Path) -> None:
    report, output, client, *_ = run_capture(tmp_path, include_final=True)

    assert report["summary"] == {
        "audited_final_missing": 0,
        "captured_results": 0,
        "waiting_for_first_final": True,
    }
    assert client.result_calls == []
    assert client.report_calls == []
    assert (output / "capture_report.json").is_file()


def test_preserves_accepted_data_disables_apply_and_omits_secret(tmp_path: Path) -> None:
    report, output, _, *_ = run_capture(tmp_path)

    assert report["accepted_data_mutated"] is False
    assert report["accepted_fingerprints"]["before"] == report["accepted_fingerprints"]["after"]
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}
    assert "must-not-be-persisted" not in "".join(
        path.read_text(encoding="utf-8") for path in output.rglob("*.json")
    )


def test_rejects_accepted_drift_after_gap_audit(tmp_path: Path) -> None:
    client = FixtureClient()
    schedule, individual, relay, evidence = audit_fixture(tmp_path, client)
    with (individual / "men_scrape_update.csv").open("a", encoding="utf-8") as handle:
        handle.write("2026-11-29,Sprint,M,99\n")

    with pytest.raises(IBUContractError, match="drifted"):
        capture.capture_final_results(
            client=client,  # type: ignore[arg-type]
            evidence_dir=evidence,
            schedule_path=schedule,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "capture",
        )


def test_rejects_tampered_final_candidate_classification(tmp_path: Path) -> None:
    client = FixtureClient()
    schedule, individual, relay, evidence = audit_fixture(tmp_path, client)
    path = evidence / "gap_report.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    report["additive_candidates"]["final_result_race_ids"] = [PENDING_RACE]
    path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(IBUContractError, match="not an audited finalized"):
        capture.capture_final_results(
            client=client,  # type: ignore[arg-type]
            evidence_dir=evidence,
            schedule_path=schedule,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "capture",
        )
