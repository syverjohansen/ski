from __future__ import annotations

import csv
from pathlib import Path

import pytest

import biathlon_results_gap as gap
from ibu_source_contract import IBUContractError

SEASON = "2627"
EVENT = "BT2627SWRLCP01"


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


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
                "Description": "World Cup",
                "ShortDescription": "Kontiolahti",
                "Nat": "FIN",
                "EventClassificationId": "BTSWRLCP",
            }
        ]

    def competitions(self, event_id: str):
        assert event_id == EVENT
        common = {
            "StatusId": 11,
            "StatusText": "Final",
            "ResultStatus": "OFFICIAL",
            "ScheduleStatus": "FINISHED",
        }
        return [
            {
                **common,
                "RaceId": f"{EVENT}SMSP",
                "StartTime": "2026-11-26T15:15:00Z",
                "ShortDescription": "Men's Sprint",
                "DisciplineId": "SP",
                "catId": "SM",
            },
            {
                **common,
                "RaceId": f"{EVENT}MXRL",
                "StartTime": "2026-11-27T15:15:00Z",
                "ShortDescription": "Mixed Relay",
                "DisciplineId": "RL",
                "catId": "MX",
            },
            {
                **common,
                "RaceId": f"{EVENT}SWSP",
                "StartTime": "2026-11-28T15:15:00Z",
                "ShortDescription": "Women's Sprint",
                "DisciplineId": "SP",
                "catId": "SW",
                "StatusId": 1,
                "StatusText": "Scheduled",
                "ResultStatus": "",
                "ScheduleStatus": "SCHEDULED",
            },
        ]


def fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    individual = tmp_path / "accepted" / "individual"
    relay = tmp_path / "accepted" / "relay"
    schedule = individual / "races.csv"
    schedule_fields = ["Date", "RaceType", "Sex"]
    write_csv(
        schedule,
        schedule_fields,
        [
            {"Date": "11/27/2026", "RaceType": "Mixed Relay", "Sex": "Mixed"},
            {"Date": "11/28/2026", "RaceType": "Sprint", "Sex": "L"},
        ],
    )
    history_fields = ["Date", "RaceType", "Sex", "ID"]
    for root in (individual, relay):
        write_csv(
            root / "men_scrape_update.csv",
            history_fields,
            [{"Date": "2026-11-27", "RaceType": "Mixed Relay", "Sex": "M", "ID": "M1"}],
        )
        write_csv(
            root / "ladies_scrape_update.csv",
            history_fields,
            [{"Date": "2026-11-28", "RaceType": "Sprint", "Sex": "L", "ID": "L1"}],
        )
    # Only the relay-inclusive policy has both halves of the final mixed event.
    write_csv(
        relay / "ladies_scrape_update.csv",
        history_fields,
        [
            {"Date": "2026-11-27", "RaceType": "Mixed Relay", "Sex": "L", "ID": "L2"},
            {"Date": "2026-11-28", "RaceType": "Sprint", "Sex": "L", "ID": "L1"},
        ],
    )
    return schedule, individual, relay


def run(tmp_path: Path) -> tuple[dict[str, object], Path]:
    schedule, individual, relay = fixture(tmp_path)
    output = tmp_path / "audit"
    report = gap.audit_gaps(
        client=FixtureClient(),  # type: ignore[arg-type]
        season_id=SEASON,
        schedule_path=schedule,
        individual_dir=individual,
        relay_dir=relay,
        output_dir=output,
    )
    return report, output


def test_classifies_schedule_final_and_pending_gaps(tmp_path: Path) -> None:
    report, output = run(tmp_path)

    assert report["status"] == "passed"
    assert report["summary"] == {
        "official_events": 1,
        "official_competitions": 3,
        "schedule_missing": 1,
        "final_results_missing": 2,
        "results_not_final": 1,
        "fully_accepted": 0,
    }
    assert report["additive_candidates"]["schedule_race_ids"] == [f"{EVENT}SMSP"]
    assert report["additive_candidates"]["final_result_race_ids"] == [
        f"{EVENT}SMSP",
        f"{EVENT}MXRL",
    ]
    women = next(
        item for item in report["coverage"] if item["race_id"] == f"{EVENT}SWSP"
    )
    assert women["result_state"] == "not_final"
    assert (output / "gap_report.json").is_file()
    assert (output / "raw" / "events.json").is_file()


def test_mixed_result_requires_both_sexes_in_both_policies(tmp_path: Path) -> None:
    report, _ = run(tmp_path)
    mixed = next(
        item for item in report["coverage"] if item["race_id"] == f"{EVENT}MXRL"
    )
    assert mixed["individual_results_present"] is False
    assert mixed["relay_results_present"] is True
    assert mixed["result_state"] == "missing"


def test_audit_preserves_accepted_data_and_secret(tmp_path: Path) -> None:
    report, output = run(tmp_path)
    assert report["accepted_data_mutated"] is False
    assert (
        report["accepted_fingerprints"]["before"]
        == report["accepted_fingerprints"]["after"]
    )
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}
    assert "must-not-be-persisted" not in "".join(
        path.read_text(encoding="utf-8") for path in output.rglob("*.json")
    )


def test_rejects_duplicate_accepted_schedule_identity(tmp_path: Path) -> None:
    schedule, individual, relay = fixture(tmp_path)
    write_csv(
        schedule,
        ["Date", "RaceType", "Sex"],
        [
            {"Date": "11/27/2026", "RaceType": "Mixed Relay", "Sex": "Mixed"},
            {"Date": "11/27/2026", "RaceType": "Mixed Relay", "Sex": "Mixed"},
        ],
    )
    with pytest.raises(IBUContractError, match="duplicate event identities"):
        gap.audit_gaps(
            client=FixtureClient(),  # type: ignore[arg-type]
            season_id=SEASON,
            schedule_path=schedule,
            individual_dir=individual,
            relay_dir=relay,
            output_dir=tmp_path / "audit",
        )


def test_output_must_be_new_and_outside_accepted_data(tmp_path: Path) -> None:
    schedule, individual, relay = fixture(tmp_path)
    for output in (individual / "audit", tmp_path / "existing"):
        if output.name == "existing":
            output.mkdir()
        with pytest.raises(IBUContractError, match="output"):
            gap.audit_gaps(
                client=FixtureClient(),  # type: ignore[arg-type]
                season_id=SEASON,
                schedule_path=schedule,
                individual_dir=individual,
                relay_dir=relay,
                output_dir=output,
            )


def test_cutoff_defers_even_final_races_after_requested_date(tmp_path: Path) -> None:
    from datetime import date
    schedule, individual, relay = fixture(tmp_path)
    report = gap.audit_gaps(client=FixtureClient(), season_id=SEASON,
        schedule_path=schedule, individual_dir=individual, relay_dir=relay,
        output_dir=tmp_path / "cutoff", as_of=date(2026, 11, 26))
    assert f"{EVENT}MXRL" not in report["additive_candidates"]["final_result_race_ids"]
    mixed = next(row for row in report["coverage"] if row["race_id"] == f"{EVENT}MXRL")
    assert mixed["result_state"] == "after_cutoff"
