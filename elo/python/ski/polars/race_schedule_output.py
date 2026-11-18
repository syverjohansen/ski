"""Serialize acquired cross-country races into the canonical CSV contract."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

OUTPUT_COLUMNS = (
    "Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "Technique",
    "MS",
    "Period",
    "Pursuit",
    "Stage",
    "Final_Climb",
    "Elevation",
    "Race",
    "Championship",
)


def period_for_date(date_text, *, championship):
    race_date = datetime.strptime(date_text, "%m/%d/%Y")
    if championship == "1" or race_date.month == 2:
        return "4"
    if race_date.month == 12 and race_date.day >= 25 or race_date.month == 1 and race_date.day <= 7:
        return "2"
    if race_date.month == 1:
        return "3"
    if 3 <= race_date.month <= 6:
        return "5"
    return "1"


def normalize_races(races):
    """Assign deterministic period, stage, final-climb, and race numbers."""
    ordered = sorted(
        (dict(race) for race in races),
        key=lambda race: datetime.strptime(race["date"], "%m/%d/%Y"),
    )
    counters = {"M": 0, "L": 0}
    for race in ordered:
        sex = race["sex"]
        if sex not in counters:
            raise ValueError(f"unsupported cross-country race sex: {sex!r}")
        race["period"] = (
            "2"
            if race.get("tour_stage") == "1"
            else period_for_date(race["date"], championship=race["championship"])
        )
        race["stage"] = "1" if race["period"] == "2" else "0"
        city = race["city"].casefold()
        race["final_climb"] = (
            "1"
            if "val di fiemme" in city
            and race["stage"] == "1"
            and race["ms"] == "1"
            and race["technique"] == "F"
            else "0"
        )
        counters[sex] += 1
        race["race"] = str(counters[sex])
    return ordered


def save_cross_country_races(races, output_file):
    """Write canonical races.csv with the historical CRLF byte format."""
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    ordered = normalize_races(races)
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\r\n")
        writer.writerow(OUTPUT_COLUMNS)
        for race in ordered:
            writer.writerow(
                [
                    race["date"],
                    race["startlist_url"],
                    race["sex"],
                    race["city"],
                    race["country"],
                    race["distance"],
                    race["technique"],
                    race["ms"],
                    race["period"],
                    race["pursuit"],
                    race["stage"],
                    race["final_climb"],
                    race["elevation"],
                    race["race"],
                    race["championship"],
                ]
            )
    return ordered
