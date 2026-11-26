#!/usr/bin/env python3
"""Build an isolated, identity-safe Tour de Ski opening-startlist bundle.

The overall Tour model runs only on the opening date and uses the two official
opening startlists as its participant set.  Accepted schedules, histories, and
reviewed aliases are read-only.  If either FIS list is unpublished, this writes
an explicit waiting report and no model input.  Unknown or ambiguous athletes
fail closed; fuzzy name matching and candidate-pool substitution are forbidden.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
from collections.abc import Callable, Iterable
from datetime import date, datetime
from pathlib import Path

from cross_country_results_identity import load_aliases, load_history, normalized_name
from cross_country_tour_contract import build_tour_plan, read_schedule
from cross_country_weekend_inputs import (
    OfficialAthlete,
    fetch_html,
    parse_official_startlist,
)
from schedule_contract import WEEKENDS_COLUMNS

REPORT_SCHEMA = "nordicnumbers.cross-country-tour-inputs/v1"
MODEL_FIELDS = ("ID", "Skier", "Nation", "Price")
PROVENANCE_FIELDS = (
    "sex",
    "fis_race_id",
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "fis_nation",
    "canonical_athlete_id",
    "canonical_name",
    "canonical_nation",
    "identity_match_method",
    "source_url",
    "source_sha256",
)
GENERATED_FILES = (
    "schedule.csv",
    "startlist_tds_men.csv",
    "startlist_tds_ladies.csv",
    "opening_startlist_provenance.csv",
    "report.json",
)


class TourInputError(RuntimeError):
    """A trustworthy Tour input bundle cannot be produced."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _csv_text(rows: list[dict[str, str]], fields: Iterable[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise TourInputError(
                f"bundle output {resolved} cannot be inside accepted source directory "
                f"{source_parent}"
            )
    return resolved


def _clear_previous_bundle(output_dir: Path) -> None:
    """Prevent an earlier candidate from surviving a failed or waiting rerun."""
    for name in GENERATED_FILES:
        (output_dir / name).unlink(missing_ok=True)


def _opening_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    tour = [row for row in rows if row["Period"] == "2" and row["Stage"] == "1"]
    if not tour:
        return []
    opening_date = min(
        datetime.strptime(row["Race_Date"], "%m/%d/%Y").date() for row in tour
    )
    return [
        row
        for row in tour
        if datetime.strptime(row["Race_Date"], "%m/%d/%Y").date() == opening_date
    ]


def _resolve_athletes(
    *,
    sex: str,
    fis_race_id: str,
    source_url: str,
    source_sha: str,
    official: list[OfficialAthlete],
    athletes: dict,
    name_index: dict,
    aliases: dict,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    model_rows: list[dict[str, str]] = []
    provenance: list[dict[str, str]] = []
    canonical_seen: set[str] = set()
    for athlete in official:
        alias = aliases.get((sex, athlete.fis_code))
        if alias:
            canonical_id = alias.canonical_id
            method = "reviewed_alias"
        else:
            candidates = name_index.get((sex, normalized_name(athlete.name)), set())
            if len(candidates) != 1:
                state = "missing" if not candidates else "ambiguous"
                raise TourInputError(
                    f"{state} identity for {sex}/{athlete.competitor_id} "
                    f"{athlete.name!r}; candidates={sorted(candidates)}"
                )
            canonical_id = next(iter(candidates))
            reviewed_codes = {
                reviewed.fis_code
                for reviewed in aliases.values()
                if reviewed.sex == sex and reviewed.canonical_id == canonical_id
            }
            if reviewed_codes:
                raise TourInputError(
                    "FIS code changed for reviewed athlete "
                    f"{sex}/{canonical_id}; observed={athlete.fis_code}, "
                    f"reviewed={sorted(reviewed_codes)}"
                )
            method = "unique_normalized_name"
        if canonical_id in canonical_seen:
            raise TourInputError(
                f"duplicate canonical athlete in {sex} opening list: {canonical_id}"
            )
        canonical_seen.add(canonical_id)
        canonical = athletes[(sex, canonical_id)]
        model_rows.append(
            {
                "ID": canonical.athlete_id,
                "Skier": canonical.name,
                "Nation": canonical.nation,
                "Price": "0",
            }
        )
        provenance.append(
            {
                "sex": sex,
                "fis_race_id": fis_race_id,
                "fis_competitor_id": athlete.competitor_id,
                "fis_code": athlete.fis_code,
                "fis_name": athlete.name,
                "fis_nation": athlete.nation,
                "canonical_athlete_id": canonical.athlete_id,
                "canonical_name": canonical.name,
                "canonical_nation": canonical.nation,
                "identity_match_method": method,
                "source_url": source_url,
                "source_sha256": source_sha,
            }
        )
    return model_rows, provenance


def build_tour_inputs(
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
    _clear_previous_bundle(output_dir)
    schedule_rows = read_schedule(schedule)
    initial = build_tour_plan(schedule_rows=schedule_rows, logical_date=logical_date)
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "price_policy": "zero_not_used_by_overall_model",
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

    opening = _opening_rows(schedule_rows)
    opening_ids = {
        event["sex"]: event["fis_race_id"]
        for event in initial.stage_events
        if event["race_date"] == initial.tour_start_date
    }
    fetched: list[tuple[dict[str, str], bytes, list[OfficialAthlete]]] = []
    available: list[str] = []
    events: list[dict[str, object]] = []
    for row in opening:
        html = fetcher(row["Startlist"])
        official = parse_official_startlist(html)
        fis_race_id = opening_ids[row["Sex"]]
        fetched.append((row, html, official))
        if official:
            available.append(fis_race_id)
        events.append(
            {
                "sex": row["Sex"],
                "fis_race_id": fis_race_id,
                "athlete_rows": len(official),
                "source_sha256": _sha256(html),
                "availability": "official" if official else "not_yet_published",
            }
        )

    plan = build_tour_plan(
        schedule_rows=schedule_rows,
        logical_date=logical_date,
        available_startlist_race_ids=available,
    )
    report["plan"] = plan.to_dict()
    report["events"] = events
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
        fis_race_id = next(
            event["fis_race_id"]
            for event in events
            if event["sex"] == row["Sex"]
        )
        model_rows, provenance = _resolve_athletes(
            sex=row["Sex"],
            fis_race_id=str(fis_race_id),
            source_url=row["Startlist"],
            source_sha=_sha256(html),
            official=official,
            athletes=athletes,
            name_index=name_index,
            aliases=aliases,
        )
        model_by_sex[row["Sex"]].extend(model_rows)
        provenance_rows.extend(provenance)

    tour_schedule = [
        row for row in schedule_rows if row["Period"] == "2" and row["Stage"] == "1"
    ]
    output_text = {
        "schedule.csv": _csv_text(tour_schedule, WEEKENDS_COLUMNS),
        "startlist_tds_men.csv": _csv_text(model_by_sex["M"], MODEL_FIELDS),
        "startlist_tds_ladies.csv": _csv_text(model_by_sex["L"], MODEL_FIELDS),
        "opening_startlist_provenance.csv": _csv_text(
            provenance_rows, PROVENANCE_FIELDS
        ),
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--logical-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_tour_inputs(
        schedule=args.schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        aliases_path=args.aliases,
        logical_date=args.logical_date,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
