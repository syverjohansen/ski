from __future__ import annotations

import json
from pathlib import Path

import pytest

from ibu_source_contract import (
    IBUClient,
    IBUContractError,
    capture_contract,
    discover_public_api_key,
    validate_competitions,
    validate_events,
    validate_reports,
    validate_results,
)


FIXTURES = Path(__file__).parent / "tests/fixtures/ibu"
EVENT_ID = "BT2526SWRLCP01"
SPRINT_ID = f"{EVENT_ID}SWSP"
RELAY_ID = f"{EVENT_ID}SWRL"
SINGLE_MIXED_ID = f"{EVENT_ID}MXSR"


def fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def competitions():
    return validate_competitions(fixture("competitions-cp01.json"), event_id=EVENT_ID)


def competition(race_id: str):
    return next(item for item in competitions() if item["race_id"] == race_id)


def test_discovers_public_key_from_official_app_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IBU_API_KEY", raising=False)
    calls: list[str] = []

    def fetcher(url: str, _: dict[str, str]) -> bytes:
        calls.append(url)
        if url.endswith("/"):
            return b'<script src="vendors.js"></script><script src=app~source.chunk.js></script>'
        return b'class App { constructor(){ window.ApiKey="fixture-public-key" } }'

    assert discover_public_api_key(fetcher) == "fixture-public-key"
    assert calls == [
        "https://biathlonresults.com/",
        "https://biathlonresults.com/app~source.chunk.js",
    ]


def test_validates_world_cup_event_and_all_supported_competition_shapes() -> None:
    events = validate_events(fixture("events-2526-level1.json"), season_id="2526", level=1)
    races = competitions()
    short_individual_week = validate_competitions(
        fixture("competitions-cp06.json"), event_id="BT2526SWRLCP06"
    )
    assert len(events) == 10
    assert {event["classification"] for event in events} == {"BTSWRLCP", "BTSWRLOG"}
    assert {race["event_kind"] for race in [*races, *short_individual_week]} == {
        "individual", "sprint", "pursuit", "relay", "mixed_relay",
        "single_mixed_relay", "short_individual", "mass_start",
    }
    assert next(
        race for race in short_individual_week if race["event_kind"] == "short_individual"
    )["sex"] == "M"
    assert all(race["status_text"] == "Final" for race in races)
    assert all(race["result_status"] == "OFFICIAL" for race in races)
    assert all(race["schedule_status"] == "FINISHED" for race in races)


def test_individual_results_preserve_stable_ibu_athlete_ids() -> None:
    summary = validate_results(
        fixture("results-women-sprint.json"), competition(SPRINT_ID)
    )
    assert summary == {
        "race_id": SPRINT_ID,
        "event_kind": "sprint",
        "sex": "L",
        "status_id": 11,
        "status": "Final",
        "result_rows": 5,
        "athlete_rows": 5,
        "team_rows": 0,
        "unique_athletes": 5,
    }


@pytest.mark.parametrize(
    ("results_fixture", "race_id", "kind", "unique_athletes"),
    [
        ("results-women-relay.json", RELAY_ID, "relay", 12),
        ("results-single-mixed-relay.json", SINGLE_MIXED_ID, "single_mixed_relay", 6),
    ],
)
def test_relay_results_preserve_teams_members_and_leg_identity(
    results_fixture: str, race_id: str, kind: str, unique_athletes: int
) -> None:
    summary = validate_results(fixture(results_fixture), competition(race_id))
    assert summary["event_kind"] == kind
    assert summary["team_rows"] == 3
    assert summary["athlete_rows"] == 12
    assert summary["unique_athletes"] == unique_athletes


@pytest.mark.parametrize(
    ("reports_fixture", "race_id"),
    [
        ("reports-women-sprint.json", SPRINT_ID),
        ("reports-women-relay.json", RELAY_ID),
        ("reports-single-mixed-relay.json", SINGLE_MIXED_ID),
    ],
)
def test_requires_one_official_ibu_start_list_pdf(
    reports_fixture: str, race_id: str
) -> None:
    start_list = validate_reports(fixture(reports_fixture), race_id=race_id)
    assert start_list["url"].startswith("https://ibu.blob.core.windows.net/docs/2526/")
    assert start_list["url"].endswith(".pdf")


def test_rejects_incomplete_relay_and_untrusted_report_url() -> None:
    results = fixture("results-women-relay.json")
    results["Results"] = results["Results"][:-1]
    with pytest.raises(IBUContractError, match="incomplete IBU relay teams"):
        validate_results(results, competition(RELAY_ID))

    reports = fixture("reports-women-sprint.json")
    start_list = next(item for item in reports["Reports"] if item["Description"] == "Start List")
    start_list["URL"] = "https://example.com/startlist.pdf"
    with pytest.raises(IBUContractError, match="untrusted official source URL"):
        validate_reports(reports, race_id=SPRINT_ID)


class FixtureClient:
    api_key = "must-never-be-persisted"

    def events(self, season_id: str, level: int = 1):
        assert (season_id, level) == ("2526", 1)
        return fixture("events-2526-level1.json")

    def competitions(self, event_id: str):
        assert event_id == EVENT_ID
        return fixture("competitions-cp01.json")

    def results(self, race_id: str):
        names = {
            SPRINT_ID: "results-women-sprint.json",
            RELAY_ID: "results-women-relay.json",
            SINGLE_MIXED_ID: "results-single-mixed-relay.json",
        }
        return fixture(names[race_id])

    def reports(self, race_id: str):
        names = {
            SPRINT_ID: "reports-women-sprint.json",
            RELAY_ID: "reports-women-relay.json",
            SINGLE_MIXED_ID: "reports-single-mixed-relay.json",
        }
        return fixture(names[race_id])


def test_capture_is_isolated_checksummed_and_publication_disabled(tmp_path: Path) -> None:
    output = tmp_path / "evidence"
    report = capture_contract(
        client=FixtureClient(),  # type: ignore[arg-type]
        season_id="2526",
        event_id=EVENT_ID,
        race_ids=[SPRINT_ID, RELAY_ID, SINGLE_MIXED_ID],
        output_dir=output,
    )
    assert report["status"] == "passed"
    assert report["accepted_data_mutated"] is False
    assert report["publication"] == {"status": "disabled"}
    assert report["discipline_counts"] == {
        "individual": 2,
        "mixed_relay": 1,
        "pursuit": 2,
        "relay": 2,
        "single_mixed_relay": 1,
        "sprint": 2,
    }
    assert len(report["races"]) == 3
    assert all(len(item["results_sha256"]) == 64 for item in report["races"])
    assert (output / "report.json").is_file()
    assert "must-never-be-persisted" not in "".join(
        path.read_text(encoding="utf-8") for path in output.rglob("*.json")
    )


def test_client_restricts_endpoints_and_evidence_rejects_accepted_tree(tmp_path: Path) -> None:
    client = IBUClient(api_key="fixture", fetcher=lambda *_: b"{}")
    with pytest.raises(IBUContractError, match="unsupported IBU endpoint"):
        client._get("Anything")
    with pytest.raises(IBUContractError, match="accepted excel365"):
        capture_contract(
            client=FixtureClient(),  # type: ignore[arg-type]
            season_id="2526",
            event_id=EVENT_ID,
            race_ids=[SPRINT_ID],
            output_dir=tmp_path / "excel365" / "evidence",
        )


def test_client_reads_official_bio_for_identity_corroboration() -> None:
    ibu_id = "BTFIN20812199401"
    payload = {"IBUId": ibu_id, "GenderId": "W", "Birthdate": "1994-12-08"}
    calls = []

    def fetcher(url: str, headers: dict[str, str]) -> bytes:
        calls.append((url, headers))
        return json.dumps(payload).encode("utf-8")

    assert IBUClient(api_key="public-key", fetcher=fetcher).bio(ibu_id) == payload
    assert calls[0][0].endswith(f"CISBios?IBUId={ibu_id}")
    assert calls[0][1] == {"apikey": "public-key"}


def test_client_rejects_mismatched_official_bio() -> None:
    client = IBUClient(
        api_key="fixture",
        fetcher=lambda *_: b'{"IBUId":"BTOTHER"}',
    )
    with pytest.raises(IBUContractError, match="identity mismatch"):
        client.bio("BTFIN20812199401")
