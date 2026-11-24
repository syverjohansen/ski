"""Serialize acquired ski-jumping races into the legacy CSV contract."""

import csv
from datetime import datetime


def assign_periods_by_race_count(races):
    """Assign sex-specific periods around the championship portion."""
    championship_end = None
    for race in races:
        if race["championship"] == "1":
            race_date = datetime.strptime(race["date"], "%m/%d/%Y")
            championship_end = max(championship_end or race_date, race_date)

    by_sex = {"M": [], "L": [], "Mixed": []}
    for race in races:
        try:
            by_sex[race["sex"]].append(race)
        except KeyError as exc:
            raise ValueError(
                f"unsupported ski-jumping race sex: {race['sex']!r}"
            ) from exc

    for sex in ("M", "L"):
        pre_championship = []
        for race in by_sex[sex]:
            race_date = datetime.strptime(race["date"], "%m/%d/%Y")
            if race["championship"] == "1":
                race["period"] = "4"
            elif championship_end and race_date > championship_end:
                race["period"] = "5"
            else:
                pre_championship.append(race)

        pre_championship.sort(
            key=lambda item: datetime.strptime(item["date"], "%m/%d/%Y")
        )
        count, remainder = divmod(len(pre_championship), 3)
        offset = 0
        for period, extra in (("1", 1), ("2", 2), ("3", 0)):
            end = (
                len(pre_championship)
                if period == "3"
                else offset + count + (1 if remainder >= extra else 0)
            )
            for index in range(offset, end):
                pre_championship[index]["period"] = period
            offset = end

    mens = sorted(
        by_sex["M"],
        key=lambda item: datetime.strptime(item["date"], "%m/%d/%Y"),
    )
    for race in by_sex["Mixed"]:
        race_date = datetime.strptime(race["date"], "%m/%d/%Y")
        if race["championship"] == "1":
            race["period"] = "4"
        elif championship_end and race_date > championship_end:
            race["period"] = "5"
        else:
            same_date = next(
                (item for item in mens if item["date"] == race["date"]),
                None,
            )
            reference = same_date or min(
                mens,
                key=lambda item: abs(
                    (
                        datetime.strptime(item["date"], "%m/%d/%Y")
                        - race_date
                    ).days
                ),
                default=None,
            )
            race["period"] = reference["period"] if reference else "1"
    return by_sex["M"] + by_sex["L"] + by_sex["Mixed"]


def assign_race_numbers(races):
    """Assign race numbers independently for men, ladies, and mixed events."""
    counters = {"M": 0, "L": 0, "Mixed": 0}
    for race in races:
        sex = race["sex"]
        if sex not in counters:
            raise ValueError(f"unsupported ski-jumping race sex: {sex!r}")
        counters[sex] += 1
        race["race"] = counters[sex]
    return races


def save_skijump_races(races, output_file):
    """Write acquired races in the exact accepted column and byte format."""
    assigned = assign_periods_by_race_count(races)
    sorted_races = sorted(
        assigned,
        key=lambda item: datetime.strptime(item["date"], "%m/%d/%Y"),
    )
    assign_race_numbers(sorted_races)
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "Date",
                "Final_Date",
                "Startlist",
                "Sex",
                "City",
                "Country",
                "HillSize",
                "RaceType",
                "Period",
                "Elevation",
                "Championship",
                "Race",
            ]
        )
        for race in sorted_races:
            writer.writerow(
                [
                    race["date"],
                    race["final_date"],
                    race["startlist_url"],
                    race["sex"],
                    race["city"],
                    race["country"],
                    race["hill_size"],
                    race["race_type"],
                    race["period"],
                    race["elevation"],
                    race["championship"],
                    race["race"],
                ]
            )
