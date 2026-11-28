from __future__ import annotations

import csv
from pathlib import Path

import biathlon_rating_chrono_candidate as candidate
import polars as pl
import pytest
from biathlon_chrono_parity import SEXES, build_chrono
from biathlon_elevation_parity import add_elevation
from biathlon_elo_parity import (
    ARTIFACT_NAMES,
    HISTORY_FIELDS,
    STREAMS,
    history_frame,
    stream_input,
)
from ibu_source_contract import IBUContractError


def row(sex: str, race_type: str = "Sprint") -> dict[str, str]:
    race_number = {
        "Individual": 1,
        "Sprint": 2,
        "Pursuit": 3,
        "Mass Start": 4,
    }[race_type]
    return {
        "Date": f"2025-12-{race_number + 1:02d}",
        "City": "Östersund",
        "Country": "Sweden",
        "Event": "World Cup",
        "Distance": "7.5",
        "RaceType": race_type,
        "MassStart": "0",
        "Season": "2026",
        "Race": str(race_number),
        "Place": "1",
        "Skier": f"Test {sex}",
        "Nation": "Norway",
        "ID": f"{sex}1",
        "Sex": sex,
        "Birthday": "2000-01-01T00:00:00",
        "Leg": "0",
        "Age": "25.9",
        "Exp": "1",
    }


def write_rows(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class FakeEngine:
    @staticmethod
    def elo(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            pl.lit(1300.0).alias("Pelo"),
            pl.lit(1300.0).alias("Elo"),
        )


def fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    candidates = tmp_path / "adapter"
    individual = tmp_path / "accepted" / "individual"
    relay = tmp_path / "accepted" / "relay"
    for accepted_dir, include_relays in ((individual, False), (relay, True)):
        accepted_dir.mkdir(parents=True)
        write_rows(
            accepted_dir / "elevation.csv",
            [{"City": "Östersund", "Elevation": 366}],
            ["City", "Elevation"],
        )
        for sex, prefix in (("M", "men"), ("L", "ladies")):
            history = [
                row(sex, race_type)
                for race_type in ("Individual", "Sprint", "Pursuit", "Mass Start")
            ]
            write_rows(
                accepted_dir / f"{prefix}_scrape_update.csv",
                history,
                HISTORY_FIELDS,
            )
            frame = history_frame(history)
            for stream, race_type in STREAMS.items():
                generated = FakeEngine.elo(
                    stream_input(
                        frame,
                        include_relays=include_relays,
                        race_type=race_type,
                    )
                )
                generated.write_csv(accepted_dir / ARTIFACT_NAMES[stream][sex])
        for sex, label in SEXES.items():
            chrono = build_chrono(accepted_dir, sex)
            chrono.write_csv(accepted_dir / f"{label}_chrono.csv")
            add_elevation(chrono, accepted_dir / "elevation.csv").write_csv(
                accepted_dir / f"{label}_chrono_elevation.csv"
            )
    for sex, prefix in (("M", "men"), ("L", "ladies")):
        write_rows(
            candidates / f"{prefix}_results_candidate.csv",
            [
                row(sex, race_type)
                for race_type in ("Individual", "Sprint", "Pursuit", "Mass Start")
            ],
            HISTORY_FIELDS,
        )
    return candidates, individual, relay


def run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, object], Path]:
    candidates, individual, relay = fixture(tmp_path)
    monkeypatch.setattr(candidate, "load_engine", lambda _: FakeEngine())
    output = tmp_path / "output"
    report = candidate.build_candidates(
        candidate_dir=candidates,
        individual_dir=individual,
        relay_dir=relay,
        engine_path=tmp_path / "elo.py",
        output_dir=output,
    )
    return report, output


def test_builds_all_rating_and_chrono_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, output = run(tmp_path, monkeypatch)

    assert report["status"] == "passed"
    assert report["accepted_data_mutated"] is False
    assert report["source_candidates_mutated"] is False
    assert (
        report["source_candidate_fingerprints"]["before"]
        == report["source_candidate_fingerprints"]["after"]
    )
    assert report["apply_ready"] is False
    assert report["application"] == {"status": "disabled", "performed": False}
    assert report["publication"] == {"status": "disabled"}
    assert report["remaining_blockers"] == [
        "integration with the checksummed promotion-review atomic set"
    ]
    assert (
        sum(
            len(report["policies"][policy][sex]["streams"])
            for policy in ("individual", "relay")
            for sex in ("M", "L")
        )
        == 20
    )
    assert all(
        report["policies"][policy][sex]["streams"][stream]["status"] == "passed"
        for policy in ("individual", "relay")
        for sex in ("M", "L")
        for stream in STREAMS
    )
    assert all(
        report["policies"][policy]["chronos"][sex]["status"] == "passed"
        for policy in ("individual", "relay")
        for sex in ("M", "L")
    )
    assert all(
        report["policies"][policy]["elevation_chronos"][sex]["status"] == "passed"
        for policy in ("individual", "relay")
        for sex in ("M", "L")
    )
    assert (output / "rating_chrono_candidate_report.json").is_file()


def test_input_parity_failure_blocks_chronos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidates, individual, relay = fixture(tmp_path)
    bad = row("M")
    bad["ID"] = "unexpected"
    write_rows(candidates / "men_results_candidate.csv", [bad], HISTORY_FIELDS)
    monkeypatch.setattr(candidate, "load_engine", lambda _: FakeEngine())

    report = candidate.build_candidates(
        candidate_dir=candidates,
        individual_dir=individual,
        relay_dir=relay,
        engine_path=tmp_path / "elo.py",
        output_dir=tmp_path / "output",
    )

    assert report["status"] == "failed"
    assert report["policies"]["individual"]["M"]["input_parity"]["status"] == "failed"
    assert report["policies"]["individual"]["chronos"] == {}


def test_candidates_are_byte_deterministic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, _ = run(tmp_path / "first", monkeypatch)
    second, _ = run(tmp_path / "second", monkeypatch)

    def hashes(payload: dict[str, object]) -> list[str]:
        return sorted(
            comparison["candidate"]["sha256"]
            for policy in payload["policies"].values()
            for section in [
                *[sex["streams"] for key, sex in policy.items() if key in {"M", "L"}],
                policy["chronos"],
                policy["elevation_chronos"],
            ]
            for comparison in section.values()
        )

    assert hashes(report) == hashes(second)


def test_builds_policy_specific_additive_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidates, individual, relay = fixture(tmp_path)
    additive = row("L", "Sprint")
    additive.update({"Date": "2026-11-26", "Season": "2027", "Race": "1", "Exp": "2"})
    for policy in ("individual", "relay"):
        for sex, prefix in (("M", "men"), ("L", "ladies")):
            rows = [additive] if sex == "L" else []
            write_rows(
                candidates / policy / f"{prefix}_results_candidate.csv",
                rows,
                HISTORY_FIELDS,
            )
    # Remove the legacy flat candidates so the policy-specific adapter layout
    # is the only source available.
    (candidates / "men_results_candidate.csv").unlink()
    (candidates / "ladies_results_candidate.csv").unlink()
    # An unrelated accepted chrono may retain prior archive semantics that
    # differ from a fresh rebuild. A women's update must preserve men's bytes.
    unchanged = {}
    for policy, accepted in (("individual", individual), ("relay", relay)):
        for name in ("men_chrono.csv", "men_chrono_elevation.csv"):
            path = accepted / name
            frame = pl.read_csv(path).with_columns(pl.lit(1456.0).alias("Elo"))
            frame.write_csv(path)
            unchanged[(policy, name)] = path.read_bytes()
    monkeypatch.setattr(candidate, "load_engine", lambda _: FakeEngine())

    report = candidate.build_candidates(
        candidate_dir=candidates,
        individual_dir=individual,
        relay_dir=relay,
        engine_path=tmp_path / "elo.py",
        output_dir=tmp_path / "output",
    )

    for (policy, name), expected in unchanged.items():
        family = "full_elevation_chronos" if "elevation" in name else "full_chronos"
        assert (tmp_path / "output" / family / policy / name).read_bytes() == expected
    assert report["status"] == "passed"
    assert report["ready_for_promotion_bundle"] is True
    for policy in ("individual", "relay"):
        assert report["policies"][policy]["M"]["input_parity"]["mode"] == "no_change"
        assert report["policies"][policy]["L"]["input_parity"]["mode"] == "additive"
        assert (
            report["policies"][policy]["L"]["streams"]["overall"]
            ["authorized_additive_rows"]
            == 1
        )
        assert (
            report["policies"][policy]["chronos"]["L"]
            ["authorized_additive_rows"]
            == 1
        )


def test_output_cannot_enter_source_or_accepted_tree(tmp_path: Path) -> None:
    candidates, individual, relay = fixture(tmp_path)
    with pytest.raises(IBUContractError, match="outside source and accepted"):
        candidate.build_candidates(
            candidate_dir=candidates,
            individual_dir=individual,
            relay_dir=relay,
            engine_path=tmp_path / "elo.py",
            output_dir=individual / "candidate",
        )


def test_output_directory_must_be_new(tmp_path: Path) -> None:
    candidates, individual, relay = fixture(tmp_path)
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(IBUContractError, match="output exists"):
        candidate.build_candidates(
            candidate_dir=candidates,
            individual_dir=individual,
            relay_dir=relay,
            engine_path=tmp_path / "elo.py",
            output_dir=output,
        )


def test_additive_team_aggregation_preserves_historical_values_and_member_order(tmp_path):
    accepted = pl.DataFrame({'Date':['2025-11-28'], 'RaceType':['Mixed Relay'],
                             'Team_Elo':[5000.0], 'MemberIDs':['4,1,3,2']})
    path = tmp_path/'teams.csv'
    accepted.write_csv(path)
    generated = pl.DataFrame({'Date':['2025-11-28','2026-11-28'],
        'RaceType':['Mixed Relay','Mixed Relay'], 'Team_Elo':[4997.0,5123.0],
        'MemberIDs':['1,2,3,4','5,6,7,8']})
    merged, additions = candidate.append_team_events(generated,path,{('2026-11-28','Mixed Relay')})
    assert merged.row(0,named=True) == accepted.row(0,named=True)
    assert additions['Team_Elo'].to_list() == [5123.0]
    with pytest.raises(IBUContractError,match='already has accepted'):
        candidate.append_team_events(generated,path,{('2025-11-28','Mixed Relay')})
    with pytest.raises(IBUContractError,match='every new event'):
        candidate.append_team_events(generated,path,{('2027-11-28','Mixed Relay')})
