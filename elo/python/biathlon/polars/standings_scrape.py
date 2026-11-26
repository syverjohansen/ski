#!/usr/bin/env python3
"""Build current Biathlon World Cup standings from accepted result history.

This replaces the authenticated FirstSkiSport page dependency. Only accepted
World Cup individual results count; championship and relay rows are excluded.
"""

from __future__ import annotations

import argparse
import csv
import tempfile
from collections import defaultdict
from pathlib import Path


class StandingsError(RuntimeError):
    """Raised when accepted results cannot produce current standings."""


REGULAR_POINTS = (
    90, 75, 65, 55, 50, 45, 41, 37, 34, 31, 30, 29, 28, 27, 26, 25,
    24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8,
    7, 6, 5, 4, 3, 2, 1,
)
MASS_START_POINTS = (
    90, 75, 65, 55, 50, 45, 41, 37, 34, 31, 30, 29, 28, 27, 26, 25,
    24, 23, 22, 21, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2,
)
OUTPUT_FIELDS = ("Skier", "ID", "Nation", "Ranking", "Points", "Wins")


def _read_history(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            missing = sorted(
                {"Season", "Race", "Event", "RaceType", "Place", "Skier", "ID", "Nation"}
                - set(reader.fieldnames or [])
            )
    except OSError as exc:
        raise StandingsError(f"cannot read {path}: {exc}") from exc
    if missing:
        raise StandingsError(f"{path} is missing columns: {missing}")
    if not rows:
        raise StandingsError(f"accepted history is empty: {path}")
    return rows


def build_standings(rows: list[dict[str, str]], season: int) -> list[dict[str, object]]:
    points: dict[str, int] = defaultdict(int)
    wins: dict[str, int] = defaultdict(int)
    identity: dict[str, dict[str, str]] = {}
    observed = 0
    for row in rows:
        if (
            int(row["Season"]) != season
            or row["Race"] == "0"
            or row["Event"] != "World Cup"
            or "relay" in row["RaceType"].casefold()
        ):
            continue
        try:
            place = int(float(row["Place"]))
        except ValueError as exc:
            raise StandingsError(f"invalid Place: {row['Place']!r}") from exc
        athlete_id = str(int(row["ID"]))
        identity[athlete_id] = row
        table = MASS_START_POINTS if row["RaceType"] == "Mass Start" else REGULAR_POINTS
        if 1 <= place <= len(table):
            points[athlete_id] += table[place - 1]
        else:
            points.setdefault(athlete_id, 0)
        wins[athlete_id] += place == 1
        observed += 1
    if not observed:
        raise StandingsError(f"season {season} has no accepted World Cup results")
    ordered = sorted(
        points,
        key=lambda athlete_id: (
            -points[athlete_id], identity[athlete_id]["Skier"], int(athlete_id)
        ),
    )
    return [
        {
            "Skier": identity[athlete_id]["Skier"],
            "ID": int(athlete_id),
            "Nation": identity[athlete_id]["Nation"],
            "Ranking": ranking,
            "Points": points[athlete_id],
            "Wins": wins[athlete_id],
        }
        for ranking, athlete_id in enumerate(ordered, start=1)
    ]


def _atomic_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", newline="", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        temporary = Path(handle.name)
    temporary.replace(path)


def write_current_standings(data_root: Path, season: int | None = None) -> int:
    data_root = data_root.expanduser().resolve()
    histories = {
        "men": _read_history(data_root / "men_chrono.csv"),
        "ladies": _read_history(data_root / "ladies_chrono.csv"),
    }
    if season is None:
        raced_seasons = {
            int(row["Season"])
            for rows in histories.values()
            for row in rows
            if row["Race"] != "0"
        }
        if not raced_seasons:
            raise StandingsError("accepted histories contain no race seasons")
        season = max(raced_seasons)
    for gender, rows in histories.items():
        _atomic_csv(
            data_root / f"{gender}_standings.csv",
            build_standings(rows, season),
        )
    return season


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--season", type=int)
    args = parser.parse_args()
    season = write_current_standings(args.data_root, args.season)
    print(f"Built accepted-result Biathlon standings for season {season}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
