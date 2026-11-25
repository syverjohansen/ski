"""Serialize acquired Nordic-combined races into the accepted CSV contract."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path


def _date(row: dict[str, str]) -> datetime:
    return datetime.strptime(row["date"], "%m/%d/%Y")


def _divide_periods(rows: list[dict[str, str]], periods: tuple[str, ...]) -> None:
    rows.sort(key=_date)
    count, remainder = divmod(len(rows), len(periods))
    offset = 0
    for index, period in enumerate(periods):
        size = count + (1 if index < remainder else 0)
        for row in rows[offset : offset + size]:
            row["period"] = period
        offset += size


def assign_periods(races: list[dict[str, str]]) -> list[dict[str, str]]:
    by_sex = {sex: [] for sex in ("M", "L", "Mixed")}
    for race in races:
        try:
            by_sex[race["sex"]].append(race)
        except KeyError as exc:
            raise ValueError(f"unsupported Nordic-combined sex: {race['sex']!r}") from exc

    for sex in ("M", "L"):
        rows = by_sex[sex]
        championships = [row for row in rows if row["championship"] == "1"]
        if not championships:
            _divide_periods(rows, ("1", "2", "3", "4", "5"))
            continue
        end = max(_date(row) for row in championships)
        before = []
        for row in rows:
            if row["championship"] == "1":
                row["period"] = "4"
            elif _date(row) > end:
                row["period"] = "5"
            else:
                before.append(row)
        _divide_periods(before, ("1", "2", "3"))

    mens = sorted(by_sex["M"], key=_date)
    championship_dates = [_date(row) for rows in by_sex.values() for row in rows if row["championship"] == "1"]
    championship_end = max(championship_dates, default=None)
    for row in by_sex["Mixed"]:
        if row["championship"] == "1":
            row["period"] = "4"
        elif championship_end and _date(row) > championship_end:
            row["period"] = "5"
        else:
            reference = min(mens, key=lambda item: abs((_date(item) - _date(row)).days), default=None)
            row["period"] = reference["period"] if reference else "1"
    return races


def save_nordic_combined_races(races: list[dict[str, str]], output: Path) -> None:
    assigned = sorted(assign_periods([dict(row) for row in races]), key=_date)
    counters = {"M": 0, "L": 0, "Mixed": 0}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "Date", "Startlist", "Sex", "City", "Country", "Distance",
                "RaceType", "Period", "Elevation", "Championship", "Race",
            ]
        )
        for row in assigned:
            counters[row["sex"]] += 1
            writer.writerow(
                [
                    row["date"], row["startlist_url"], row["sex"], row["city"],
                    row["country"], row["distance"], row["race_type"], row["period"],
                    row["elevation"], row["championship"], counters[row["sex"]],
                ]
            )
