#!/usr/bin/env python3
"""Build leak-free Alpine prediction inputs from an archived race date.

The replay uses the competitors visible on an archived FIS results page as a
proxy for the pre-race startlist. Result-only values are written to a separate
evaluation artifact and never enter the prediction startlist. When no usable
FIS page is available, the replay builds a clearly labelled provisional field
from the latest pre-race season in the rating history.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from startlist_common import (
    determine_discipline_from_race_type,
    extract_individual_results,
    get_race_specific_elo,
)

ELO_COLUMNS = (
    "Elo",
    "Downhill_Elo",
    "Super G_Elo",
    "Giant Slalom_Elo",
    "Slalom_Elo",
    "Combined_Elo",
    "Tech_Elo",
    "Speed_Elo",
)
RACES_COLUMNS = (
    "Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "Period",
    "Championship",
    "Race",
)
RESULT_ONLY_FIELDS = ("Rank", "Run1", "Run2", "Time", "Points")


class ReplayError(RuntimeError):
    """Raised when a replay input cannot satisfy the prediction contract."""


def normalize_id(value: object) -> str:
    """Return a stable string form for numeric or textual athlete IDs."""

    if pd.isna(value):
        return ""
    text = str(value).strip()
    return text[:-2] if text.endswith(".0") and text[:-2].isdigit() else text


def parse_logical_date(value: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, format="%m/%d/%Y", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(value, format="%Y-%m-%d", errors="coerce")
    if pd.isna(parsed):
        raise ReplayError(f"invalid logical date: {value}")
    return parsed.normalize()


def race_id_from_url(url: object) -> str:
    match = re.search(r"(?:raceid|raceId)=(\d+)", str(url or ""))
    return match.group(1) if match else ""


def filter_history_before(
    history: pd.DataFrame,
    logical_date: str,
) -> pd.DataFrame:
    """Exclude the target race and all later observations from model input."""

    if "Date" not in history.columns:
        raise ReplayError("rating history is missing Date")
    cutoff = parse_logical_date(logical_date)
    dates = pd.to_datetime(history["Date"], errors="coerce")
    filtered = history.loc[dates.notna() & (dates < cutoff)].copy()
    if filtered.empty:
        raise ReplayError(f"no rating history exists before {logical_date}")
    filtered["Date"] = dates.loc[filtered.index].dt.strftime("%Y-%m-%d")
    return filtered


def parse_archived_results(html: str) -> tuple[list[dict], list[dict]]:
    """Return prediction-safe participants and isolated result evidence."""

    athletes = extract_individual_results(BeautifulSoup(html, "html.parser"))
    participants: list[dict] = []
    results: list[dict] = []
    seen: set[str] = set()
    for athlete in athletes:
        athlete_id = normalize_id(athlete.get("ID") or athlete.get("FisCode"))
        name = str(athlete.get("Name", "")).strip()
        if not athlete_id or not name or athlete_id in seen:
            continue
        seen.add(athlete_id)
        participants.append(
            {
                "ID": athlete_id,
                "FisCode": str(athlete.get("FisCode", "")).strip(),
                "Name": name,
                "Nation": str(athlete.get("Nation", "")).strip(),
                "Bib": str(athlete.get("Bib", "")).strip(),
                "Year": str(athlete.get("Year", "")).strip(),
            }
        )
        results.append(
            {
                "ID": athlete_id,
                "Name": name,
                **{field: athlete.get(field, "") for field in RESULT_ONLY_FIELDS},
            }
        )
    return participants, results


def latest_pre_race_scores(history: pd.DataFrame) -> pd.DataFrame:
    required = {"ID", "Skier", "Nation"}
    missing = sorted(required - set(history.columns))
    if missing:
        raise ReplayError(f"rating history is missing columns: {missing}")
    prepared = history.copy()
    prepared["_athlete_id"] = prepared["ID"].map(normalize_id)
    prepared["_date"] = pd.to_datetime(prepared["Date"], errors="coerce")
    prepared = prepared[prepared["_athlete_id"] != ""].sort_values("_date")
    return prepared.groupby("_athlete_id", as_index=False).tail(1)


def generated_participants(history: pd.DataFrame) -> list[dict]:
    """Build a provisional field from the latest available pre-race season."""

    active = history
    if "Season" in active.columns:
        seasons = pd.to_numeric(active["Season"], errors="coerce")
        if seasons.notna().any():
            active = active.loc[seasons == seasons.max()]
    latest = latest_pre_race_scores(active)
    return [
        {
            "ID": normalize_id(row["ID"]),
            "FisCode": "",
            "Name": str(row["Skier"]).strip(),
            "Nation": str(row["Nation"]).strip(),
            "Bib": "999",
            "Year": "",
        }
        for _, row in latest.iterrows()
    ]


def build_startlist(
    race: pd.Series,
    participants: Iterable[dict],
    history: pd.DataFrame,
    probability_column: str,
    source: str,
) -> pd.DataFrame:
    latest = latest_pre_race_scores(history)
    by_id = {normalize_id(row["ID"]): row for _, row in latest.iterrows()}
    discipline = determine_discipline_from_race_type(str(race["Distance"]))
    sex_name = "Men's" if race["Sex"] == "M" else "Ladies'"
    rows: list[dict] = []
    for order, participant in enumerate(participants, start=1):
        athlete_id = normalize_id(participant.get("ID"))
        if not athlete_id:
            continue
        rating = by_id.get(athlete_id)
        rating_values = rating.to_dict() if rating is not None else {}
        skier = str(rating_values.get("Skier") or participant["Name"]).strip()
        nation = str(
            participant.get("Nation") or rating_values.get("Nation") or ""
        ).strip()
        row = {
            "FISName": str(participant["Name"]).strip(),
            "Skier": skier,
            "ID": athlete_id,
            "Nation": nation,
            "StartOrder": order,
            "Bib": participant.get("Bib") or order,
            "Is_Host_Nation": nation == str(race["Country"]),
            "In_Startlist": True,
            "Year": participant.get("Year", ""),
            "Discipline": discipline,
            "City": race["City"],
            "Country": race["Country"],
            "Race_Type": f"{sex_name} {race['Distance']}",
            "Race_Date": race["Date"],
            "Startlist_Source": source,
            probability_column: 1.0,
        }
        for column in ELO_COLUMNS:
            if column in rating_values:
                row[column] = rating_values[column]
        row["Race_Elo"] = get_race_specific_elo(rating_values, discipline)
        rows.append(row)
    if not rows:
        raise ReplayError(f"{source} produced no usable athletes")
    return pd.DataFrame(rows)


def merge_startlists(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    probability_column: str,
) -> pd.DataFrame:
    if existing is None:
        return incoming
    previous_probability_columns = [
        column for column in existing.columns if re.fullmatch(r"Race\d+_Prob", column)
    ]
    merged = existing.copy().set_index("ID", drop=False)
    merged[probability_column] = 0.0
    for _, row in incoming.iterrows():
        athlete_id = normalize_id(row["ID"])
        if athlete_id in merged.index:
            merged.loc[athlete_id, probability_column] = row[probability_column]
            continue
        new_row = row.copy()
        for column in previous_probability_columns:
            new_row[column] = 0.0
        merged.loc[athlete_id] = new_row
    merged[probability_column] = merged[probability_column].fillna(0.0)
    return merged.reset_index(drop=True)


def atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False)
    os.replace(temporary, path)


def fetch_html(race_id: str) -> str:
    url = (
        "https://www.fis-ski.com/DB/general/results.html"
        f"?sectorcode=AL&raceid={race_id}"
    )
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0 NordicNumbersReplay/1.0"},
        timeout=30,
    )
    response.raise_for_status()
    return response.text


def build_replay(
    *,
    logical_date: str,
    races_path: Path,
    men_history_path: Path,
    ladies_history_path: Path,
    output_dir: Path,
    fixture_dir: Path | None = None,
    allow_network: bool = False,
    fetch_fn: Callable[[str], str] = fetch_html,
) -> dict:
    races = pd.read_csv(races_path, dtype=str).fillna("")
    missing_columns = sorted(set(RACES_COLUMNS) - set(races.columns))
    if missing_columns:
        raise ReplayError(f"races file is missing columns: {missing_columns}")
    target = parse_logical_date(logical_date)
    race_dates = pd.to_datetime(races["Date"], errors="coerce").dt.normalize()
    selected = races.loc[race_dates == target].copy()
    selected = selected[selected["Sex"].isin(["M", "L"])]
    if selected.empty:
        raise ReplayError(f"no Alpine races found on {logical_date}")

    histories = {
        "M": filter_history_before(
            pd.read_csv(men_history_path, low_memory=False), logical_date
        ),
        "L": filter_history_before(
            pd.read_csv(ladies_history_path, low_memory=False), logical_date
        ),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_csv(selected[list(RACES_COLUMNS)], output_dir / "races.csv")
    atomic_csv(histories["M"], output_dir / "men_chrono.csv")
    atomic_csv(histories["L"], output_dir / "ladies_chrono.csv")

    report: dict = {
        "schema_version": "nordicnumbers.alpine-startlist-replay/v1",
        "logical_date": target.strftime("%Y-%m-%d"),
        "status": "passed",
        "publication": {"status": "disabled"},
        "races": [],
    }
    results_evidence: list[dict] = []

    for sex, gender in (("M", "men"), ("L", "ladies")):
        gender_races = selected[selected["Sex"] == sex]
        if gender_races.empty:
            continue
        consolidated: pd.DataFrame | None = None
        for index, (_, race) in enumerate(gender_races.iterrows(), start=1):
            race_id = race_id_from_url(race["Startlist"])
            html = ""
            fixture = (
                fixture_dir / f"{race_id}.html" if fixture_dir and race_id else None
            )
            if fixture and fixture.is_file():
                html = fixture.read_text(encoding="utf-8")
            elif allow_network and race_id:
                html = fetch_fn(race_id)

            participants, actual = parse_archived_results(html) if html else ([], [])
            source = "historical_result_proxy"
            if not participants:
                participants = generated_participants(histories[sex])
                source = "generated_pre_race_history"
            probability_column = f"Race{index}_Prob"
            startlist = build_startlist(
                race,
                participants,
                histories[sex],
                probability_column,
                source,
            )
            consolidated = merge_startlists(
                consolidated,
                startlist,
                probability_column,
            )
            results_evidence.extend(
                {"race_id": race_id, "sex": sex, **row} for row in actual
            )
            report["races"].append(
                {
                    "race_id": race_id,
                    "sex": sex,
                    "source": source,
                    "athletes": len(startlist),
                    "result_fields_in_prediction_input": False,
                }
            )
        assert consolidated is not None
        atomic_csv(
            consolidated,
            output_dir / f"startlist_races_{gender}.csv",
        )

    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "replay.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (reports_dir / "actual-results.json").write_text(
        json.dumps(results_evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logical-date", required=True)
    parser.add_argument("--races", type=Path, required=True)
    parser.add_argument("--men-history", type=Path, required=True)
    parser.add_argument("--ladies-history", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fixture-dir", type=Path)
    parser.add_argument("--allow-network", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_replay(
        logical_date=args.logical_date,
        races_path=args.races,
        men_history_path=args.men_history,
        ladies_history_path=args.ladies_history,
        output_dir=args.output_dir,
        fixture_dir=args.fixture_dir,
        allow_network=args.allow_network,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
