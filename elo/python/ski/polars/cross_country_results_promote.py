#!/usr/bin/env python3
"""Build promotion-ready cross-country chrono histories from replay rows.

The command never modifies accepted files. It preserves every accepted byte,
including undiscounted Race=0 archive rows, and appends only validated newer
race rows to complete candidate histories in a separate output directory.
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
from datetime import date
from pathlib import Path

from cross_country_results_elo_replay import (
    PREVIOUS_COLUMNS,
    load_starting_states,
)


class PromotionError(RuntimeError):
    """Raised when candidate rows cannot be promoted safely."""


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


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise PromotionError(
                f"promotion output {resolved} cannot be inside source directory {parent}"
            )
    return resolved


def _digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_increment(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _season_for_date(value: str) -> int:
    raced = date.fromisoformat(value)
    return raced.year + 1 if raced.month >= 7 else raced.year


def _validate_first_previous_ratings(
    *, source: Path, rows: list[dict[str, str]], expected_season: int
) -> None:
    if not rows:
        return
    _, states, _ = load_starting_states(source, expected_season)
    first_by_athlete: dict[str, dict[str, str]] = {}
    for row in sorted(rows, key=lambda item: (item["Date"], int(item["Race"]))):
        first_by_athlete.setdefault(row["ID"], row)
    for athlete_id, row in first_by_athlete.items():
        state = states.get(athlete_id)
        if state is None:
            raise PromotionError(
                f"canonical athlete {athlete_id} has no accepted starting state"
            )
        for stream, previous_column in PREVIOUS_COLUMNS.items():
            raw = row[previous_column].strip()
            if not raw:
                raise PromotionError(
                    f"first candidate row for {athlete_id} is missing {previous_column}"
                )
            if not math.isclose(
                float(raw), state.ratings[stream], rel_tol=0.0, abs_tol=1e-8
            ):
                raise PromotionError(
                    f"first candidate row for {athlete_id} has invalid "
                    f"{previous_column}: {raw} != {state.ratings[stream]}"
                )


def build_candidate_history(
    *,
    source: Path,
    increments: Path,
    output: Path,
    sex: str,
    expected_season: int,
) -> dict[str, object]:
    source_bytes = source.read_bytes()
    if not source_bytes:
        raise PromotionError(f"accepted history is empty: {source}")
    if not source_bytes.endswith(b"\n"):
        raise PromotionError(f"accepted history must end with a newline: {source}")
    source_text = source_bytes.decode("utf-8-sig")
    source_reader = csv.DictReader(io.StringIO(source_text, newline=""))
    header = list(source_reader.fieldnames or [])
    source_rows = list(source_reader)
    required = {
        "Date",
        "Sex",
        "Event",
        "ID",
        "Season",
        "Race",
        *PREVIOUS_COLUMNS.values(),
    }
    missing = sorted(required - set(header))
    if missing:
        raise PromotionError(f"{source} is missing columns: {missing}")

    increment_header, increment_rows = _read_increment(increments)
    if increment_header != header:
        raise PromotionError(f"increment header does not match {source}")

    existing_keys: set[tuple[int, int, str]] = set()
    max_existing_race = 0
    for row_number, row in enumerate(source_rows, start=2):
        if row["Sex"] != sex:
            raise PromotionError(
                f"{source}:{row_number}: expected sex {sex}, found {row['Sex']}"
            )
        if row["Event"] == "Offseason":
            continue
        key = (int(row["Season"]), int(row["Race"]), row["ID"])
        if key in existing_keys:
            raise PromotionError(f"duplicate accepted result: {key}")
        existing_keys.add(key)
        if int(row["Season"]) == expected_season:
            max_existing_race = max(max_existing_race, int(row["Race"]))

    new_keys: set[tuple[int, int, str]] = set()
    new_races: set[int] = set()
    for row_number, row in enumerate(increment_rows, start=2):
        if row["Sex"] != sex or int(row["Season"]) != expected_season:
            raise PromotionError(
                f"{increments}:{row_number}: expected {sex}/{expected_season}, "
                f"found {row['Sex']}/{row['Season']}"
            )
        if _season_for_date(row["Date"]) != expected_season:
            raise PromotionError(
                f"{increments}:{row_number}: date {row['Date']} is outside "
                f"season {expected_season}"
            )
        race = int(row["Race"])
        if race <= max_existing_race:
            raise PromotionError(
                f"{increments}:{row_number}: race {race} is not newer than accepted "
                f"race {max_existing_race}; replay the season from its first gap"
            )
        key = (expected_season, race, row["ID"])
        if key in existing_keys or key in new_keys:
            raise PromotionError(f"duplicate promoted result: {key}")
        new_keys.add(key)
        new_races.add(race)

    _validate_first_previous_ratings(
        source=source, rows=increment_rows, expected_season=expected_season
    )
    appended = "".join(
        _csv_line([row[field] for field in header]) for row in increment_rows
    ).encode("utf-8")
    data = source_bytes + appended
    if not data.startswith(source_bytes):
        raise PromotionError("candidate does not preserve accepted history bytes")
    _atomic_bytes(output, data)
    return {
        "sex": sex,
        "accepted_rows": len(source_rows),
        "appended_rows": len(increment_rows),
        "appended_races": len(new_races),
        "max_accepted_race": max_existing_race,
        "race_zero_rows_modified": 0,
        "historical_bytes_preserved": True,
        "source_sha256": _digest_bytes(source_bytes),
        "increment_sha256": hashlib.sha256(increments.read_bytes()).hexdigest(),
        "output_sha256": _digest_bytes(data),
        "output_file": output.name,
    }


def promote_results(
    *,
    men_chrono: Path,
    ladies_chrono: Path,
    men_rows: Path,
    ladies_rows: Path,
    expected_season: int,
    output_dir: Path,
) -> dict[str, object]:
    output_dir = _validate_output_dir(
        output_dir, (men_chrono, ladies_chrono, men_rows, ladies_rows)
    )
    outputs = [
        build_candidate_history(
            source=men_chrono,
            increments=men_rows,
            output=output_dir / "men_chrono.csv",
            sex="M",
            expected_season=expected_season,
        ),
        build_candidate_history(
            source=ladies_chrono,
            increments=ladies_rows,
            output=output_dir / "ladies_chrono.csv",
            sex="L",
            expected_season=expected_season,
        ),
    ]
    if sum(int(item["appended_rows"]) for item in outputs) == 0:
        raise PromotionError("promotion contains no new result rows")
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.cross-country-results-promotion/v1",
        "expected_season": expected_season,
        "status": "promotion_candidate",
        "accepted_files_modified": False,
        "outputs": outputs,
    }
    _atomic_bytes(
        output_dir / "report.json",
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-rows", required=True, type=Path)
    parser.add_argument("--ladies-rows", required=True, type=Path)
    parser.add_argument("--expected-season", required=True, type=int)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = promote_results(
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        men_rows=args.men_rows,
        ladies_rows=args.ladies_rows,
        expected_season=args.expected_season,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
