from __future__ import annotations

import csv
import json
import tempfile
from datetime import date
from pathlib import Path

import pytest

from biathlon_season_close import SeasonCloseError, verify_season_close


FIELDS = [
    "Date", "City", "Country", "Sex", "Distance", "RaceType", "MassStart",
    "Event", "Place", "Skier", "Nation", "ID", "Season", "Race",
    "Birthday", "Age", "Exp", "Leg", "Pelo", "Elo", "Individual_Pelo",
    "Individual_Elo", "Sprint_Pelo", "Sprint_Elo", "Pursuit_Pelo",
    "Pursuit_Elo", "MassStart_Pelo", "MassStart_Elo",
]


def _write(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _race(sex: str, athlete_id: int, race_type: str, race: int) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "2026-11-26", "City": "Kontiolahti", "Country": "Finland",
        "Sex": sex, "Distance": "20", "RaceType": race_type, "MassStart": 0,
        "Event": "World Cup", "Place": 1, "Skier": f"Athlete {athlete_id}",
        "Nation": "Norway", "ID": athlete_id, "Season": 2027, "Race": race,
        "Birthday": "2000-01-01", "Age": 26, "Exp": 1, "Leg": 0,
    }
    for pelo, elo in (
        ("Pelo", "Elo"), ("Individual_Pelo", "Individual_Elo"),
        ("Sprint_Pelo", "Sprint_Elo"), ("Pursuit_Pelo", "Pursuit_Elo"),
        ("MassStart_Pelo", "MassStart_Elo"),
    ):
        row[pelo] = 1400
        row[elo] = 1410
    return row


def _close(race: dict[str, object]) -> dict[str, object]:
    row = dict(race)
    row.update(
        Date="2027-05-01", City="Summer", Country="Break",
        RaceType="Offseason", Event="Offseason", Race=0, Place=0,
    )
    for pelo, elo in (
        ("Pelo", "Elo"), ("Individual_Pelo", "Individual_Elo"),
        ("Sprint_Pelo", "Sprint_Elo"), ("Pursuit_Pelo", "Pursuit_Elo"),
        ("MassStart_Pelo", "MassStart_Elo"),
    ):
        row[pelo] = 1410
        row[elo] = 1410
    return row


def _fixture(root: Path) -> dict[str, object]:
    schedule = root / "races.csv"
    _write(
        schedule,
        ["Date", "City", "Sex", "RaceType"],
        [
            {"Date": "11/26/2026", "City": "Kontiolahti", "Sex": "M", "RaceType": "Individual"},
            {"Date": "11/26/2026", "City": "Kontiolahti", "Sex": "L", "RaceType": "Individual"},
        ],
    )
    men_race = _race("M", 1, "Individual", 1)
    ladies_race = _race("L", 2, "Individual", 1)
    men = root / "men_chrono.csv"
    ladies = root / "ladies_chrono.csv"
    _write(men, FIELDS, [men_race, _close(men_race)])
    _write(ladies, FIELDS, [ladies_race, _close(ladies_race)])
    team = root / "men_relay_chrono.csv"
    _write(team, ["Season", "Race"], [{"Season": 2027, "Race": 1}])
    return {
        "schedule": schedule, "men_chrono": men, "ladies_chrono": ladies,
        "team_histories": (team,), "season": 2027,
        "as_of": date(2027, 5, 1), "output_root": root / "output",
    }


def test_verifies_complete_undiscounted_close_without_mutating_sources() -> None:
    with tempfile.TemporaryDirectory() as directory:
        args = _fixture(Path(directory))
        before = {path: path.read_bytes() for path in (
            args["schedule"], args["men_chrono"], args["ladies_chrono"],
            *args["team_histories"],
        )}
        report = verify_season_close(**args)
        assert report["status"] == "verified"
        assert report["ratings_mutated"] is False
        assert report["chronos"]["M"]["athletes"] == 1
        assert json.loads((args["output_root"] / "report.json").read_text()) == report
        assert {path: path.read_bytes() for path in before} == before


def test_rejects_early_or_discounted_close() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        args = _fixture(root)
        args["as_of"] = date(2027, 4, 30)
        with pytest.raises(SeasonCloseError, match="cannot close before"):
            verify_season_close(**args)
        args["as_of"] = date(2027, 5, 1)
        rows = list(csv.DictReader(args["men_chrono"].open()))
        rows[-1]["Elo"] = "1393.5"
        _write(args["men_chrono"], FIELDS, rows)
        with pytest.raises(SeasonCloseError, match="discounted public Race=0"):
            verify_season_close(**args)


def test_rejects_missing_season_history_and_team_race_zero() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        args = _fixture(root)
        fields, rows = FIELDS, list(csv.DictReader(args["ladies_chrono"].open()))
        _write(args["ladies_chrono"], fields, [rows[-1]])
        with pytest.raises(SeasonCloseError, match="no race rows"):
            verify_season_close(**args)
        args = _fixture(root / "second")
        _write(args["team_histories"][0], ["Season", "Race"], [{"Season": 2027, "Race": 0}])
        with pytest.raises(SeasonCloseError, match="synthetic Race=0"):
            verify_season_close(**args)
