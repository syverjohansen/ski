from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest
from skijump_results_elo_replay import STREAMS, _stream_columns, offseason_rating
from skijump_season_close import (
    SeasonCloseError,
    build_closed_history,
    build_season_close,
)

FIELDS = [
    "Date",
    "City",
    "Country",
    "Sex",
    "HillSize",
    "RaceType",
    "TeamEvent",
    "Event",
    "Place",
    "Skier",
    "Nation",
    "ID",
    "Season",
    "Race",
    "Birthday",
    "Age",
    "Exp",
    "Leg",
    *[column for stream in STREAMS for column in _stream_columns(stream)],
]


def _row(**overrides: object) -> dict[str, str]:
    row = {
        "Date": "2026-03-28",
        "City": "Planica",
        "Country": "Slovenia",
        "Sex": "L",
        "HillSize": "240",
        "RaceType": "Flying",
        "TeamEvent": "0",
        "Event": "World Cup",
        "Place": "1",
        "Skier": "Example Jumper",
        "Nation": "Japan",
        "ID": "1",
        "Season": "2026",
        "Race": "38",
        "Birthday": "1996-10-08 00:00:00",
        "Age": "29",
        "Exp": "10",
        "Leg": "0",
    }
    for stream in STREAMS:
        pelo, elo = _stream_columns(stream)
        row[pelo] = "1590"
        row[elo] = "1600"
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def _offseason(**overrides: object) -> dict[str, str]:
    row = _row(
        Date="2026-05-01",
        City="Summer",
        Country="Break",
        HillSize=0,
        RaceType="Offseason",
        Event="Offseason",
        Place=0,
        Race=0,
    )
    for stream in STREAMS:
        pelo, elo = _stream_columns(stream)
        row[pelo] = "1600"
        row[elo] = "1600"
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def _write(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_close_repairs_undiscounted_race_zero_and_is_idempotent(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    candidate = tmp_path / "candidate.csv"
    _write(source, [_row(), _offseason()])

    result = build_closed_history(source=source, output=candidate, sex="L", season=2026)

    assert result["status"] == "candidate"
    assert result["corrected_ratings"] == len(STREAMS)
    closed = _read(candidate)[-1]
    for stream in STREAMS:
        pelo, elo = _stream_columns(stream)
        assert float(closed[pelo]) == 1600
        assert float(closed[elo]) == pytest.approx(offseason_rating(1600))

    first_bytes = candidate.read_bytes()
    rerun = build_closed_history(
        source=candidate,
        output=tmp_path / "rerun.csv",
        sex="L",
        season=2026,
    )
    assert rerun["status"] == "no_changes"
    assert rerun["corrected_ratings"] == 0
    assert (tmp_path / "rerun.csv").read_bytes() == first_bytes


def test_close_requires_replay_to_have_created_complete_race_zero(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    _write(source, [_row()])
    with pytest.raises(SeasonCloseError, match="run results replay first"):
        build_closed_history(
            source=source,
            output=tmp_path / "candidate.csv",
            sex="L",
            season=2026,
        )


def test_close_rejects_partial_race_zero(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    _write(
        source,
        [
            _row(),
            _row(ID=2, Skier="Second Jumper", Place=2),
            _offseason(),
        ],
    )
    with pytest.raises(SeasonCloseError, match="partial offseason rows"):
        build_closed_history(
            source=source,
            output=tmp_path / "candidate.csv",
            sex="L",
            season=2026,
        )


def test_repository_2026_close_repairs_rollover_only(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parent
    accepted = root / "relay" / "excel365"
    report = build_season_close(
        schedule=root / "excel365" / "races2026.csv",
        men_chrono=accepted / "men_chrono.csv",
        ladies_chrono=accepted / "ladies_chrono.csv",
        men_team_chrono=accepted / "men_team_chrono.csv",
        ladies_team_chrono=accepted / "ladies_team_chrono.csv",
        mixed_team_chrono=accepted / "mixed_team_chrono.csv",
        policy_exclusions=root / "skijump_results_policy_exclusions.csv",
        season=2026,
        as_of=date(2026, 5, 1),
        output_root=tmp_path / "close",
    )

    assert report["status"] == "candidate"
    assert report["completed_schedule_races"] == 75
    assert report["policy_excluded_races"] == 2
    assert report["accepted_team_rows"] == {"M": 61, "L": 0, "Mixed": 36}
    assert report["source_histories_modified"] is False
    assert all(output["corrected_ratings"] > 0 for output in report["outputs"])


def test_close_cannot_run_before_may_first(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parent
    accepted = root / "relay" / "excel365"
    with pytest.raises(SeasonCloseError, match="cannot close before"):
        build_season_close(
            schedule=root / "excel365" / "races.csv",
            men_chrono=accepted / "men_chrono.csv",
            ladies_chrono=accepted / "ladies_chrono.csv",
            men_team_chrono=accepted / "men_team_chrono.csv",
            ladies_team_chrono=accepted / "ladies_team_chrono.csv",
            mixed_team_chrono=accepted / "mixed_team_chrono.csv",
            policy_exclusions=root / "skijump_results_policy_exclusions.csv",
            season=2026,
            as_of=date(2026, 4, 30),
            output_root=tmp_path / "close",
        )
