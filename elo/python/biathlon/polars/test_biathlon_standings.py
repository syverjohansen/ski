from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from standings_scrape import build_standings, write_current_standings


def _row(
    athlete_id: int, place: int, *, race_type: str = "Sprint",
    event: str = "World Cup", season: int = 2027, race: int = 1,
) -> dict[str, str]:
    return {
        "Season": str(season), "Race": str(race), "Event": event,
        "RaceType": race_type, "Place": str(place),
        "Skier": f"Athlete {athlete_id}", "ID": str(athlete_id),
        "Nation": "Norway",
    }


def test_builds_points_from_only_current_world_cup_individual_results() -> None:
    rows = [
        _row(1, 1),
        _row(2, 2),
        _row(1, 30, race_type="Mass Start", race=2),
        _row(2, 1, race_type="Relay", race=3),
        _row(2, 1, event="World Championship", race=4),
        _row(2, 1, season=2026, race=5),
        _row(1, 0, event="Offseason", race=0),
    ]
    assert build_standings(rows, 2027) == [
        {"Skier": "Athlete 1", "ID": 1, "Nation": "Norway", "Ranking": 1, "Points": 92, "Wins": 1},
        {"Skier": "Athlete 2", "ID": 2, "Nation": "Norway", "Ranking": 2, "Points": 75, "Wins": 0},
    ]


def test_writes_both_gender_files_atomically() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        fields = ["Season", "Race", "Event", "RaceType", "Place", "Skier", "ID", "Nation"]
        for gender, athlete_id in (("men", 1), ("ladies", 2)):
            with (root / f"{gender}_chrono.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
                writer.writeheader()
                writer.writerow(_row(athlete_id, 1))
        assert write_current_standings(root) == 2027
        for gender in ("men", "ladies"):
            with (root / f"{gender}_standings.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            assert rows[0]["Points"] == "90"
