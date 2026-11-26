from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pytest

import biathlon_results_gap as gap
import biathlon_schedule_candidate as candidate
from ibu_source_contract import IBUContractError

SEASON = "2627"
EVENT = "BT2627SWRLCP01"


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class FixtureClient:
    api_key = "must-not-be-persisted"

    def events(self, season_id: str, level: int = 1):
        assert (season_id, level) == (SEASON, 1)
        return [
            {
                "EventId": EVENT,
                "SeasonId": SEASON,
                "Level": 1,
                "StartDate": "2026-11-23T00:00:00Z",
                "EndDate": "2026-11-29T23:59:59Z",
                "Description": "BMW IBU World Cup Biathlon",
                "ShortDescription": "Kontiolahti",
                "Organizer": "Kontiolahti",
                "Nat": "FIN",
                "NatLong": "Finland",
                "Altitude": "118",
                "EventClassificationId": "BTSWRLCP",
            }
        ]

    def competitions(self, event_id: str):
        assert event_id == EVENT
        common = {
            "StatusId": 1,
            "StatusText": "Scheduled",
            "ResultStatus": "NONE",
            "ScheduleStatus": "SCHEDULED",
        }
        return [
            {
                **common,
                "RaceId": f"{EVENT}SMSP",
                "StartTime": "2026-11-26T15:15:00Z",
                "ShortDescription": "Men 10 km Sprint",
                "km": "10",
                "DisciplineId": "SP",
                "catId": "SM",
            },
            {
                **common,
                "RaceId": f"{EVENT}MXRL",
                "StartTime": "2026-11-27T15:15:00Z",
                "ShortDescription": "Mixed Relay",
                "km": "4x6",
                "DisciplineId": "RL",
                "catId": "MX",
            },
            {
                **common,
                "RaceId": f"{EVENT}SWSP",
                "StartTime": "2026-11-28T15:15:00Z",
                "ShortDescription": "Women 7.5 km Sprint",
                "km": "7.5",
                "DisciplineId": "SP",
                "catId": "SW",
            },
        ]


def audit_fixture(tmp_path: Path) -> tuple[Path, Path]:
    accepted = tmp_path / "accepted"
    schedule = accepted / "races.csv"
    write_csv(
        schedule,
        [
            {
                "Date": "11/28/2026",
                "RaceType": "Sprint",
                "Sex": "L",
            }
        ],
    )
    history = [
        {
            "Date": "2026-11-28",
            "RaceType": "Sprint",
            "Sex": "M",
            "ID": "1",
        }
    ]
    for root in (accepted / "individual", accepted / "relay"):
        write_csv(root / "men_scrape_update.csv", history)
        write_csv(
            root / "ladies_scrape_update.csv",
            [{**history[0], "Sex": "L", "ID": "2"}],
        )
    evidence = tmp_path / "evidence"
    gap.audit_gaps(
        client=FixtureClient(),  # type: ignore[arg-type]
        season_id=SEASON,
        schedule_path=schedule,
        individual_dir=accepted / "individual",
        relay_dir=accepted / "relay",
        output_dir=evidence,
    )
    return schedule, evidence


def build(tmp_path: Path) -> tuple[dict[str, object], Path, Path]:
    schedule, evidence = audit_fixture(tmp_path)
    output = tmp_path / "candidate"
    report = candidate.build_schedule_candidate(
        evidence_dir=evidence,
        schedule_path=schedule,
        output_dir=output,
    )
    return report, output, schedule


def test_builds_missing_races_and_weekends_candidates(tmp_path: Path) -> None:
    report, output, _ = build(tmp_path)

    assert report["status"] == "passed"
    assert report["summary"] == {
        "official_events": 1,
        "official_competitions": 3,
        "accepted_competitions": 1,
        "candidate_competitions": 2,
        "candidate_by_sex": {"M": 1, "Mixed": 1},
        "candidate_by_period": {"1": 2},
    }
    races = read_csv(output / "races2027_candidate.csv")
    assert [row["RaceType"] for row in races] == ["Sprint", "Mixed Relay"]
    assert races[0] == {
        "Date": "11/26/2026",
        "Startlist": f"https://www.biathlonworld.com/startlist/{EVENT}SMSP",
        "Sex": "M",
        "City": "Kontiolahti",
        "Country": "Finland",
        "Distance": "10",
        "RaceType": "Sprint",
        "Period": "1",
        "Elevation": "118",
        "Championship": "0",
        "Race": "1",
    }
    weekends = read_csv(output / "weekends_candidate.csv")
    assert weekends[1]["Date"] == "11/26/2026"
    assert weekends[1]["Race_Date"] == "11/27/2026"
    assert weekends[1]["Distance"] == "N/A"
    assert report["candidate_files"] == {
        "races2027_candidate.csv": {
            "rows": 2,
            "sha256": candidate._sha256(output / "races2027_candidate.csv"),
        },
        "weekends_candidate.csv": {
            "rows": 2,
            "sha256": candidate._sha256(output / "weekends_candidate.csv"),
        },
    }


def test_preserves_accepted_schedule_and_disables_application(tmp_path: Path) -> None:
    report, output, schedule = build(tmp_path)
    before = report["accepted_schedule"]["sha256_before"]
    assert report["accepted_schedule"] == {
        "file": str(schedule.resolve()),
        "sha256_before": before,
        "sha256_after": before,
        "mutated": False,
    }
    assert report["ready_for_review"] is True
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}
    assert "must-not-be-persisted" not in "".join(
        path.read_text(encoding="utf-8") for path in output.rglob("*") if path.is_file()
    )


def test_rejects_tampered_official_evidence(tmp_path: Path) -> None:
    schedule, evidence = audit_fixture(tmp_path)
    path = evidence / "raw" / "events.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[0]["Altitude"] = "999"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IBUContractError, match="event evidence hash mismatch"):
        candidate.build_schedule_candidate(
            evidence_dir=evidence,
            schedule_path=schedule,
            output_dir=tmp_path / "candidate",
        )


def test_rejects_accepted_schedule_drift(tmp_path: Path) -> None:
    schedule, evidence = audit_fixture(tmp_path)
    with schedule.open("a", encoding="utf-8") as handle:
        handle.write("11/29/2026,Sprint,M\n")
    with pytest.raises(IBUContractError, match="schedule drifted"):
        candidate.build_schedule_candidate(
            evidence_dir=evidence,
            schedule_path=schedule,
            output_dir=tmp_path / "candidate",
        )


def test_rejects_incomplete_event_metadata(tmp_path: Path) -> None:
    schedule, evidence = audit_fixture(tmp_path)
    events_path = evidence / "raw" / "events.json"
    events = json.loads(events_path.read_text(encoding="utf-8"))
    events[0]["Altitude"] = ""
    events_path.write_text(json.dumps(events), encoding="utf-8")
    report_path = evidence / "gap_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["source"]["event_payload_sha256"] = candidate._json_sha256(events)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IBUContractError, match="incomplete official event metadata"):
        candidate.build_schedule_candidate(
            evidence_dir=evidence,
            schedule_path=schedule,
            output_dir=tmp_path / "candidate",
        )


def test_balanced_periods_preserve_championship_boundary() -> None:
    rows: list[dict[str, object]] = []
    for index, day in enumerate(
        [
            date(2026, 11, 1),
            date(2026, 11, 2),
            date(2026, 12, 1),
            date(2026, 12, 2),
            date(2027, 1, 1),
            date(2027, 1, 2),
        ]
    ):
        for sex in ("M", "L"):
            rows.append(
                {
                    "day": day,
                    "sex": sex,
                    "race_id": f"{sex}{index}",
                    "championship": 0,
                }
            )
    rows.extend(
        [
            {
                "day": date(2027, 2, 1),
                "sex": sex,
                "race_id": f"{sex}CH",
                "championship": 1,
            }
            for sex in ("M", "L", "Mixed")
        ]
    )
    rows.extend(
        [
            {
                "day": date(2027, 3, 1),
                "sex": sex,
                "race_id": f"{sex}POST",
                "championship": 0,
            }
            for sex in ("M", "L", "Mixed")
        ]
    )

    candidate._balanced_periods(rows)  # type: ignore[arg-type]

    for sex in ("M", "L"):
        assert [row["period"] for row in rows if row["sex"] == sex] == [
            1,
            1,
            2,
            2,
            3,
            3,
            4,
            5,
        ]
    assert [row["period"] for row in rows if row["sex"] == "Mixed"] == [4, 5]


def test_output_must_be_new_and_outside_inputs(tmp_path: Path) -> None:
    schedule, evidence = audit_fixture(tmp_path)
    for output in (
        evidence / "candidate",
        schedule.parent / "candidate",
        tmp_path / "exists",
    ):
        if output.name == "exists":
            output.mkdir()
        with pytest.raises(IBUContractError, match="output"):
            candidate.build_schedule_candidate(
                evidence_dir=evidence,
                schedule_path=schedule,
                output_dir=output,
            )


def test_cli_maps_schedule_to_builder_parameter(tmp_path: Path) -> None:
    arguments = candidate.parser().parse_args(
        [
            "--evidence-dir",
            str(tmp_path / "evidence"),
            "--schedule",
            str(tmp_path / "races.csv"),
            "--output-dir",
            str(tmp_path / "candidate"),
        ]
    )
    assert vars(arguments) == {
        "evidence_dir": tmp_path / "evidence",
        "schedule_path": tmp_path / "races.csv",
        "output_dir": tmp_path / "candidate",
    }
