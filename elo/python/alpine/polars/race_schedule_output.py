"""Serialize acquired Alpine races into the legacy canonical CSV contract."""

import csv
from datetime import datetime


def assign_periods_by_race_count(races):
    """Assign prediction periods around the championship portion of a season."""
    olympic_end_date = None
    for race in races:
        if race["championship"] == "1":
            race_date = datetime.strptime(race["date"], "%m/%d/%Y")
            if olympic_end_date is None or race_date > olympic_end_date:
                olympic_end_date = race_date

    olympic_races = []
    non_olympic_races = []
    post_olympic_races = []
    for race in races:
        if race["championship"] == "1":
            olympic_races.append(race)
            race["period"] = "4"
        else:
            race_date = datetime.strptime(race["date"], "%m/%d/%Y")
            if olympic_end_date and race_date > olympic_end_date:
                post_olympic_races.append(race)
                race["period"] = "5"
            else:
                non_olympic_races.append(race)

    non_olympic_races.sort(
        key=lambda item: datetime.strptime(item["date"], "%m/%d/%Y")
    )
    total_races = len(non_olympic_races)
    if total_races:
        races_per_period, remainder = divmod(total_races, 3)
        offset = 0
        for period, extra in (("1", 1), ("2", 2), ("3", 0)):
            if period == "3":
                end = total_races
            else:
                end = offset + races_per_period + (1 if remainder >= extra else 0)
            for index in range(offset, end):
                non_olympic_races[index]["period"] = period
            offset = end
    return non_olympic_races + olympic_races + post_olympic_races


def assign_race_numbers(races):
    """Assign race numbers within each supported sex, starting from one."""
    race_counters = {"M": 0, "L": 0}
    for race in races:
        sex = race["sex"]
        if sex not in race_counters:
            raise ValueError(f"unsupported Alpine race sex: {sex!r}")
        race_counters[sex] += 1
        race["race"] = race_counters[sex]
    return races


def save_alpine_races(races, output_file):
    """Write acquired races to the canonical legacy CSV schema."""
    races_with_periods = assign_periods_by_race_count(races)
    races_with_numbers = assign_race_numbers(races_with_periods)
    sorted_races = sorted(
        races_with_numbers,
        key=lambda item: datetime.strptime(item["date"], "%m/%d/%Y"),
    )
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "Date",
                "Startlist",
                "Sex",
                "City",
                "Country",
                "Distance",
                "Period",
                "Championship",
                "Race",
            ]
        )
        for race in sorted_races:
            writer.writerow(
                [
                    race["date"],
                    race["startlist_url"],
                    race["sex"],
                    race["city"],
                    race["country"],
                    race["distance"],
                    race["period"],
                    race["championship"],
                    race["race"],
                ]
            )
    print(f"Saved {len(sorted_races)} alpine races to {output_file}")
