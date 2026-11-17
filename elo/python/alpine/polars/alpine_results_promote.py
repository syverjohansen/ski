#!/usr/bin/env python3
"""Build promotion-ready Alpine chrono histories from validated Elo rows.

The command writes full candidate histories to a separate output directory. It
updates the immediately preceding offseason rows exactly once, appends only new
race rows, and refuses duplicate or inconsistent promotion input.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import tempfile
from pathlib import Path
from typing import Iterable

from alpine_results_elo_replay import BASE_ELO, PREVIOUS_COLUMNS, ROLLOVER_DISCOUNT


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


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_promoted_history(
    *,
    source: Path,
    increments: Path,
    output: Path,
    sex: str,
    expected_season: int,
) -> dict[str, object]:
    source_lines = source.read_text(encoding="utf-8-sig").splitlines(keepends=True)
    if not source_lines:
        raise PromotionError(f"accepted history is empty: {source}")
    header = next(csv.reader([source_lines[0].rstrip("\n")]))
    field_index = {field: index for index, field in enumerate(header)}
    required = {"Date", "Sex", "Event", "ID", "Season", "Race", *PREVIOUS_COLUMNS}
    missing = sorted(required - set(header))
    if missing:
        raise PromotionError(f"{source} is missing columns: {missing}")

    with increments.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        increment_header = list(reader.fieldnames or [])
        increment_rows = list(reader)
    if increment_header != header:
        raise PromotionError(f"increment header does not match {source}")
    if not increment_rows:
        raise PromotionError(f"increment file is empty: {increments}")

    existing_keys: set[tuple[int, int, str, str]] = set()
    promoted_lines = [source_lines[0]]
    rollover_rows = 0
    rollover_values = 0
    for line_number, original_line in enumerate(source_lines[1:], start=2):
        values = next(csv.reader([original_line.rstrip("\n")]))
        if len(values) != len(header):
            raise PromotionError(
                f"{source}:{line_number}: expected {len(header)} columns, found {len(values)}"
            )
        row = dict(zip(header, values))
        if row["Event"] != "Offseason":
            existing_keys.add(
                (int(row["Season"]), int(row["Race"]), row["Date"], row["ID"])
            )
        changed = False
        if row["Event"] == "Offseason" and int(row["Season"]) == expected_season - 1:
            rollover_rows += 1
            for elo_column, pelo_column in PREVIOUS_COLUMNS.items():
                raw_previous = row[pelo_column].strip()
                if not raw_previous:
                    continue
                previous = float(raw_previous)
                desired = previous * ROLLOVER_DISCOUNT + BASE_ELO * (
                    1.0 - ROLLOVER_DISCOUNT
                )
                raw_current = row[elo_column].strip()
                current = float(raw_current) if raw_current else previous
                if math.isclose(current, desired, rel_tol=0.0, abs_tol=1e-9):
                    continue
                if not math.isclose(current, previous, rel_tol=0.0, abs_tol=1e-9):
                    raise PromotionError(
                        f"{source}:{line_number}: {elo_column} is neither pre-rollover "
                        "nor the expected discounted value"
                    )
                values[field_index[elo_column]] = str(desired)
                rollover_values += 1
                changed = True
        promoted_lines.append(_csv_line(values) if changed else original_line)

    if rollover_rows == 0:
        raise PromotionError(
            f"{source} has no offseason rows for season {expected_season - 1}"
        )

    new_keys: set[tuple[int, int, str, str]] = set()
    for row_number, row in enumerate(increment_rows, start=2):
        if row["Sex"] != sex or int(row["Season"]) != expected_season:
            raise PromotionError(
                f"{increments}:{row_number}: expected {sex}/{expected_season}, "
                f"found {row['Sex']}/{row['Season']}"
            )
        key = (int(row["Season"]), int(row["Race"]), row["Date"], row["ID"])
        if key in existing_keys or key in new_keys:
            raise PromotionError(f"duplicate promoted result: {key}")
        new_keys.add(key)
        promoted_lines.append(_csv_line([row[field] for field in header]))

    data = "".join(promoted_lines).encode("utf-8")
    _atomic_bytes(output, data)
    return {
        "sex": sex,
        "accepted_rows": len(source_lines) - 1,
        "appended_rows": len(increment_rows),
        "rollover_rows": rollover_rows,
        "rollover_values": rollover_values,
        "source_sha256": _digest(source),
        "increment_sha256": _digest(increments),
        "output_sha256": hashlib.sha256(data).hexdigest(),
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
        build_promoted_history(
            source=men_chrono,
            increments=men_rows,
            output=output_dir / "men_chrono.csv",
            sex="M",
            expected_season=expected_season,
        ),
        build_promoted_history(
            source=ladies_chrono,
            increments=ladies_rows,
            output=output_dir / "ladies_chrono.csv",
            sex="L",
            expected_season=expected_season,
        ),
    ]
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.alpine-results-promotion/v1",
        "expected_season": expected_season,
        "status": "promotion_candidate",
        "outputs": outputs,
    }
    report_path = output_dir / "report.json"
    _atomic_bytes(
        report_path, (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
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
