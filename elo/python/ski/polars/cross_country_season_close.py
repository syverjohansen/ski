#!/usr/bin/env python3
"""Build a validated cross-country offseason candidate without mutating inputs.

The May 1 Race 0 row preserves each athlete's undiscounted season-ending Elo.
The results replay applies the 85/15 regression when that athlete next races,
including one regression for every missed season boundary.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import tempfile
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from cross_country_results_elo_replay import (
    BASE_ELO,
    PREVIOUS_COLUMNS,
    ROLLOVER_DISCOUNT,
)
from cross_country_results_gap import ScheduleRace, audit_results
from schedule_contract import RACES_COLUMNS, infer_fis_season, read_csv


class SeasonCloseError(RuntimeError):
    """Raised when a season cannot be closed safely."""


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _csv_line(values: list[str]) -> str:
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerow(values)
    return output.getvalue()


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(data)
        temporary = Path(handle.name)
    temporary.replace(path)


def _atomic_json(path: Path, document: dict[str, object]) -> None:
    _atomic_bytes(
        path, (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    )


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _validate_output_root(output_root: Path, sources: Iterable[Path]) -> Path:
    resolved = output_root.expanduser().resolve()
    for source in sources:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise SeasonCloseError(
                f"season-close output {resolved} cannot be inside source directory "
                f"{source_parent}"
            )
    return resolved


def _age_on(birthday: str, close_date: date, fallback: str) -> str:
    if not birthday.strip():
        return fallback
    try:
        born = datetime.fromisoformat(birthday).date()
    except ValueError as exc:
        raise SeasonCloseError(f"invalid birthday {birthday!r}") from exc
    return str((close_date - born).days / 365.25)


def _validate_existing_offseason(
    *,
    rows: list[dict[str, str]],
    athletes: dict[str, dict[str, str]],
    sex: str,
    season: int,
    close_date: date,
) -> None:
    by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        athlete_id = row["ID"].strip()
        if athlete_id in by_id:
            raise SeasonCloseError(
                f"duplicate offseason row for {sex}/{season}/{athlete_id}"
            )
        by_id[athlete_id] = row
        expected = {
            "Date": close_date.isoformat(),
            "Sex": sex,
            "Event": "Offseason",
            "Season": str(season),
            "Race": "0",
        }
        for field, value in expected.items():
            if row[field] != value:
                raise SeasonCloseError(
                    f"invalid offseason {field} for {sex}/{season}/{athlete_id}: "
                    f"{row[field]!r} != {value!r}"
                )
        for elo_column, pelo_column in PREVIOUS_COLUMNS.items():
            previous_raw = row[pelo_column].strip()
            current_raw = row[elo_column].strip()
            if not previous_raw:
                if current_raw:
                    raise SeasonCloseError(
                        f"{sex}/{season}/{athlete_id} has {elo_column} without "
                        f"{pelo_column}"
                    )
                continue
            previous = float(previous_raw)
            current = float(current_raw) if current_raw else previous
            discounted = previous * ROLLOVER_DISCOUNT + BASE_ELO * (
                1.0 - ROLLOVER_DISCOUNT
            )
            if not (
                math.isclose(current, previous, rel_tol=0.0, abs_tol=1e-9)
                or math.isclose(current, discounted, rel_tol=0.0, abs_tol=1e-9)
            ):
                raise SeasonCloseError(
                    f"{sex}/{season}/{athlete_id} {elo_column} is neither the "
                    "season-ending nor delayed-rollover value"
                )
    expected_ids = set(athletes)
    observed_ids = set(by_id)
    if observed_ids != expected_ids:
        raise SeasonCloseError(
            f"partial offseason rows for {sex}/{season}: "
            f"missing={sorted(expected_ids - observed_ids)}, "
            f"unexpected={sorted(observed_ids - expected_ids)}"
        )


def build_closed_history(
    *, source: Path, output: Path, sex: str, season: int
) -> dict[str, object]:
    source_bytes = source.read_bytes()
    fields, rows = _read_rows(source)
    required = {
        "Date",
        "City",
        "Country",
        "Sex",
        "Distance",
        "Event",
        "MS",
        "Technique",
        "Place",
        "Skier",
        "Nation",
        "ID",
        "Season",
        "Race",
        "Birthday",
        "Age",
        "Exp",
        *PREVIOUS_COLUMNS,
        *PREVIOUS_COLUMNS.values(),
    }
    missing = sorted(required - set(fields))
    if missing:
        raise SeasonCloseError(f"{source} is missing columns: {missing}")

    athletes: dict[str, dict[str, str]] = {}
    for row in rows:
        if row["Event"] == "Offseason" or int(row["Season"]) != season:
            continue
        athlete_id = row["ID"].strip()
        if not athlete_id:
            raise SeasonCloseError(f"{source} contains a blank athlete ID")
        order = (row["Date"], int(row["Race"]))
        current = athletes.get(athlete_id)
        if current is None or order > (current["Date"], int(current["Race"])):
            athletes[athlete_id] = row
    if not athletes:
        raise SeasonCloseError(f"{source} has no race rows for season {season}")

    close_date = date(season, 5, 1)
    existing = [
        row
        for row in rows
        if row["Event"] == "Offseason" and int(row["Season"]) == season
    ]
    if existing:
        _validate_existing_offseason(
            rows=existing,
            athletes=athletes,
            sex=sex,
            season=season,
            close_date=close_date,
        )
        _atomic_bytes(output, source_bytes)
        return {
            "sex": sex,
            "status": "no_changes",
            "athletes": len(athletes),
            "appended_rows": 0,
            "source_sha256": _sha256(source_bytes),
            "output_sha256": _sha256(source_bytes),
            "output_file": output.name,
        }

    additions = []
    for athlete_id in sorted(athletes, key=int):
        previous = athletes[athlete_id]
        row = dict(previous)
        row.update(
            {
                "Date": close_date.isoformat(),
                "City": "Summer",
                "Country": "Break",
                "Sex": sex,
                "Distance": "0",
                "Event": "Offseason",
                "MS": "0",
                "Technique": "",
                "Place": "0",
                "Season": str(season),
                "Race": "0",
                "Age": _age_on(previous["Birthday"], close_date, previous["Age"]),
            }
        )
        for elo_column, pelo_column in PREVIOUS_COLUMNS.items():
            row[pelo_column] = previous[elo_column].strip()
            row[elo_column] = previous[elo_column].strip()
        additions.append(_csv_line([row[field] for field in fields]))

    data = source_bytes
    if data and not data.endswith(b"\n"):
        data += b"\n"
    data += "".join(additions).encode()
    _atomic_bytes(output, data)
    return {
        "sex": sex,
        "status": "candidate",
        "athletes": len(athletes),
        "appended_rows": len(additions),
        "source_sha256": _sha256(source_bytes),
        "output_sha256": _sha256(data),
        "output_file": output.name,
    }


def _gap_label(item: ScheduleRace) -> str:
    return f"{item.date}/{item.sex}/race-{item.race}"


def build_season_close(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    season: int,
    as_of: date,
    output_root: Path,
) -> dict[str, object]:
    close_date = date(season, 5, 1)
    if as_of < close_date:
        raise SeasonCloseError(
            f"season {season} cannot close before {close_date.isoformat()}"
        )
    schedule_season = infer_fis_season(read_csv(schedule, RACES_COLUMNS))
    if schedule_season != season:
        raise SeasonCloseError(
            f"schedule season {schedule_season} does not match requested season {season}"
        )
    output_root = _validate_output_root(
        output_root, (schedule, men_chrono, ladies_chrono)
    )
    audit = audit_results(schedule, men_chrono, ladies_chrono, close_date)
    if audit.missing:
        raise SeasonCloseError(
            "season has unprocessed scheduled individual results: "
            + ", ".join(_gap_label(item) for item in audit.missing)
        )

    outputs = [
        build_closed_history(
            source=men_chrono,
            output=output_root / "candidate/men_chrono.csv",
            sex="M",
            season=season,
        ),
        build_closed_history(
            source=ladies_chrono,
            output=output_root / "candidate/ladies_chrono.csv",
            sex="L",
            season=season,
        ),
    ]
    status = (
        "candidate"
        if any(output["status"] == "candidate" for output in outputs)
        else "no_changes"
    )
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.cross-country-season-close/v1",
        "status": status,
        "season": season,
        "as_of": as_of.isoformat(),
        "close_date": close_date.isoformat(),
        "completed_individual_races": audit.completed_individual,
        "completed_team_events_excluded": audit.completed_team_events,
        "rollover": {
            "timing": "delayed_until_first_next-season_result",
            "base_elo": BASE_ELO,
            "discount": ROLLOVER_DISCOUNT,
        },
        "outputs": outputs,
        "source_histories_modified": False,
    }
    _atomic_json(output_root / "report.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-root", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = build_season_close(
            schedule=args.schedule,
            men_chrono=args.men_chrono,
            ladies_chrono=args.ladies_chrono,
            season=args.season,
            as_of=args.as_of,
            output_root=args.output_root,
        )
    except Exception as exc:
        try:
            _atomic_json(
                args.output_root / "report.json",
                {
                    "schema_version": "nordicnumbers.cross-country-season-close/v1",
                    "status": "failed",
                    "season": args.season,
                    "as_of": args.as_of.isoformat(),
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
        except OSError:
            pass
        raise
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
