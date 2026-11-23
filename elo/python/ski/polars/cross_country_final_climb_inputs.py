#!/usr/bin/env python3
"""Build an isolated, identity-safe Tour de Ski Final Climb input bundle."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Callable

from cross_country_final_climb_contract import build_final_climb_plan
from cross_country_results_identity import load_aliases, load_history
from cross_country_tour_contract import read_schedule
from cross_country_tour_inputs import (
    MODEL_FIELDS,
    PROVENANCE_FIELDS,
    _atomic_text,
    _clear_previous_bundle,
    _csv_text,
    _resolve_athletes,
    _sha256,
    _validate_output_dir,
)
from cross_country_weekend_inputs import (
    OfficialAthlete,
    fetch_html,
    parse_official_startlist,
)
from schedule_contract import WEEKENDS_COLUMNS

REPORT_SCHEMA = "nordicnumbers.cross-country-final-climb-inputs/v1"
GENERATED_FILES = (
    "schedule.csv",
    "startlist_final_climb_men.csv",
    "startlist_final_climb_ladies.csv",
    "startlist_provenance.csv",
    "report.json",
)


def _clear_bundle(output_dir: Path) -> None:
    _clear_previous_bundle(output_dir)
    for name in GENERATED_FILES:
        (output_dir / name).unlink(missing_ok=True)


def _final_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row["Period"] == "2" and row["Stage"] == "1" and row["Final_Climb"] == "1"
    ]


def build_final_climb_inputs(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    aliases_path: Path,
    logical_date: date,
    output_dir: Path,
    fetcher: Callable[[str], bytes] = fetch_html,
) -> dict[str, object]:
    sources = (schedule, men_chrono, ladies_chrono, aliases_path)
    output_dir = _validate_output_dir(output_dir, sources)
    _clear_bundle(output_dir)
    schedule_rows = read_schedule(schedule)
    initial = build_final_climb_plan(
        schedule_rows=schedule_rows,
        logical_date=logical_date,
    )
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "price_policy": "zero_not_used_by_final_climb_model",
        "input_sha256": {path.name: _sha256(path.read_bytes()) for path in sources},
        "plan": initial.to_dict(),
    }
    if initial.status == "no_work":
        report.update({"status": "no_work", "startlist_rows": 0})
        _atomic_text(
            output_dir / "report.json",
            json.dumps(report, indent=2, sort_keys=True) + "\n",
        )
        return report

    rows_by_sex = {row["Sex"]: row for row in _final_rows(schedule_rows)}
    fetched: list[tuple[dict[str, str], bytes, list[OfficialAthlete]]] = []
    available: list[str] = []
    events: list[dict[str, object]] = []
    event_ids = {event["sex"]: event["fis_race_id"] for event in initial.events}
    for sex in ("M", "L"):
        row = rows_by_sex[sex]
        html = fetcher(row["Startlist"])
        official = parse_official_startlist(html)
        race_id = event_ids[sex]
        fetched.append((row, html, official))
        if official:
            available.append(race_id)
        events.append(
            {
                "sex": sex,
                "fis_race_id": race_id,
                "athlete_rows": len(official),
                "source_sha256": _sha256(html),
                "availability": "official" if official else "not_yet_published",
            }
        )

    plan = build_final_climb_plan(
        schedule_rows=schedule_rows,
        logical_date=logical_date,
        available_startlist_race_ids=available,
    )
    report.update({"plan": plan.to_dict(), "events": events})
    if plan.status != "ready":
        report.update({"status": "waiting_for_startlists", "startlist_rows": 0})
        _atomic_text(
            output_dir / "report.json",
            json.dumps(report, indent=2, sort_keys=True) + "\n",
        )
        return report

    athletes, name_index, names_by_id = load_history(men_chrono, ladies_chrono)
    aliases = load_aliases(aliases_path, athletes, names_by_id)
    model_by_sex: dict[str, list[dict[str, str]]] = {"M": [], "L": []}
    provenance_rows: list[dict[str, str]] = []
    for row, html, official in fetched:
        sex = row["Sex"]
        model_rows, provenance = _resolve_athletes(
            sex=sex,
            fis_race_id=event_ids[sex],
            source_url=row["Startlist"],
            source_sha=_sha256(html),
            official=official,
            athletes=athletes,
            name_index=name_index,
            aliases=aliases,
        )
        model_by_sex[sex].extend(model_rows)
        provenance_rows.extend(provenance)

    output_text = {
        "schedule.csv": _csv_text(_final_rows(schedule_rows), WEEKENDS_COLUMNS),
        "startlist_final_climb_men.csv": _csv_text(model_by_sex["M"], MODEL_FIELDS),
        "startlist_final_climb_ladies.csv": _csv_text(model_by_sex["L"], MODEL_FIELDS),
        "startlist_provenance.csv": _csv_text(provenance_rows, PROVENANCE_FIELDS),
    }
    for name, text in output_text.items():
        _atomic_text(output_dir / name, text)
    report.update(
        {
            "status": "candidate",
            "startlist_rows": sum(len(rows) for rows in model_by_sex.values()),
            "outputs": {
                name: {
                    "rows": text.count("\n") - 1,
                    "sha256": _sha256(text.encode()),
                }
                for name, text in output_text.items()
            },
        }
    )
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", type=Path, required=True)
    parser.add_argument("--men-chrono", type=Path, required=True)
    parser.add_argument("--ladies-chrono", type=Path, required=True)
    parser.add_argument("--aliases", type=Path, required=True)
    parser.add_argument("--logical-date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    build_final_climb_inputs(
        schedule=args.schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        aliases_path=args.aliases,
        logical_date=args.logical_date,
        output_dir=args.output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
