#!/usr/bin/env python3
"""Build a validated Alpine offseason candidate without mutating accepted data.

The May 1 row records each athlete's final rating twice: ``Pelo`` is the
season-ending rating and ``Elo`` remains equal to it.  The existing results
replay applies the 85/15 rollover discount only when the athlete first appears
in the next season.  This preserves the undiscounted Race 0 archive while
starting the next season from the regressed rating.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from alpine_results_elo_replay import BASE_ELO, PREVIOUS_COLUMNS, ROLLOVER_DISCOUNT
from alpine_results_gap import MissingRace, find_missing_races
from schedule_contract import RACES_COLUMNS, infer_fis_season, read_csv


class SeasonCloseError(RuntimeError):
    """Raised when a season cannot be closed without changing accepted data."""


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
    rendered = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    _atomic_bytes(path, rendered)


def _digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise SeasonCloseError(
                f"season-close output {resolved} cannot be inside source directory {parent}"
            )
    return resolved


def _age_on(birthday: str, close_date: date, fallback: str) -> str:
    if not birthday.strip():
        return fallback
    try:
        born = datetime.fromisoformat(birthday.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise SeasonCloseError(f"invalid birthday {birthday!r}") from exc
    return str((close_date - born).days / 365.25)


def _row_order(row: dict[str, str]) -> tuple[str, int]:
    return row["Date"], int(row["Race"])


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
                        f"{sex}/{season}/{athlete_id} has {elo_column} without {pelo_column}"
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
    *,
    source: Path,
    output: Path,
    sex: str,
    season: int,
) -> dict[str, object]:
    source_bytes = source.read_bytes()
    source_lines = source_bytes.decode("utf-8-sig").splitlines(keepends=True)
    fields, rows = _read_rows(source)
    required = {
        "Date", "City", "Country", "Sex", "Distance", "Event", "MS",
        "Technique", "Place", "Skier", "Nation", "ID", "Season", "Race",
        "Birthday", "Age", "Exp", *PREVIOUS_COLUMNS, *PREVIOUS_COLUMNS.values(),
    }
    missing = sorted(required - set(fields))
    if missing:
        raise SeasonCloseError(f"{source} is missing columns: {missing}")

    race_rows = [
        row
        for row in rows
        if row["Event"] != "Offseason" and int(row["Season"]) == season
    ]
    if not race_rows:
        raise SeasonCloseError(f"{source} has no race rows for season {season}")
    athletes: dict[str, dict[str, str]] = {}
    for row in race_rows:
        athlete_id = row["ID"].strip()
        if not athlete_id:
            raise SeasonCloseError(f"{source} contains a blank athlete ID")
        if athlete_id not in athletes or _row_order(row) > _row_order(
            athletes[athlete_id]
        ):
            athletes[athlete_id] = row

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
            "source_sha256": _digest_bytes(source_bytes),
            "output_sha256": _digest_bytes(source_bytes),
            "output_file": output.name,
        }

    additions: list[str] = []
    for athlete_id in sorted(athletes, key=lambda value: int(value)):
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
            final_rating = previous[elo_column].strip()
            row[pelo_column] = final_rating
            row[elo_column] = final_rating
        additions.append(_csv_line([row[field] for field in fields]))

    data = source_bytes
    if data and not data.endswith(b"\n"):
        data += b"\n"
    data += "".join(additions).encode("utf-8")
    _atomic_bytes(output, data)
    return {
        "sex": sex,
        "status": "candidate",
        "athletes": len(athletes),
        "appended_rows": len(additions),
        "source_sha256": _digest_bytes(source_bytes),
        "output_sha256": _digest_bytes(data),
        "output_file": output.name,
    }


def _gap_label(item: MissingRace) -> str:
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
    output_root = _validate_output_dir(
        output_root, (schedule, men_chrono, ladies_chrono)
    )
    missing, completed_count = find_missing_races(
        schedule, men_chrono, ladies_chrono, close_date
    )
    if missing:
        raise SeasonCloseError(
            "season has unprocessed scheduled results: "
            + ", ".join(_gap_label(item) for item in missing)
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
        "schema_version": "nordicnumbers.alpine-season-close/v1",
        "status": status,
        "season": season,
        "as_of": as_of.isoformat(),
        "close_date": close_date.isoformat(),
        "completed_schedule_races": completed_count,
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
                    "schema_version": "nordicnumbers.alpine-season-close/v1",
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
