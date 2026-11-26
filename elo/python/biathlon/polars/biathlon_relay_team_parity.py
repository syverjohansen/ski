#!/usr/bin/env python3
"""Prove relay-team artifacts from official IBU candidate athlete ratings.

The harness injects the publication-disabled event ratings emitted by
``biathlon_elo_parity.py`` into in-memory copies of the accepted relay chronos,
calls the existing team aggregation functions unchanged, and compares all four
team artifacts. It never mutates accepted data or publishes.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable

import polars as pl

from ibu_source_contract import IBUContractError


REPORT_SCHEMA = "nordicnumbers.biathlon-relay-team-parity/v1"
ATHLETE_KEY_FIELDS = ["Date", "Season", "Race", "Place", "ID", "RaceType", "Leg"]
TEAM_KEY_FIELDS = ["Nation", "Season", "Race", "Place", "Date", "RaceType"]
RATING_FIELDS = ["Pelo", "Elo"]
CHRONO_RATING_FIELDS = [
    "Pelo", "Elo",
    "Individual_Pelo", "Individual_Elo",
    "Sprint_Pelo", "Sprint_Elo",
    "Pursuit_Pelo", "Pursuit_Elo",
    "MassStart_Pelo", "MassStart_Elo",
]
ARTIFACTS = {
    "men_relay": "men_relay_chrono.csv",
    "ladies_relay": "ladies_relay_chrono.csv",
    "mixed_relay": "mixed_relay_chrono.csv",
    "single_mixed_relay": "single_mixed_relay_chrono.csv",
}


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        temporary = Path(handle.name)
    temporary.replace(path)


def load_aggregator(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "nordicnumbers_biathlon_relay_chrono", path
    )
    if spec is None or spec.loader is None:
        raise IBUContractError(f"cannot load relay aggregator: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    required = {
        "create_combined_df", "process_relay", "process_mixed_relay",
        "process_single_mixed_relay",
    }
    missing = sorted(name for name in required if not callable(getattr(module, name, None)))
    if missing:
        raise IBUContractError(f"relay aggregator is missing functions: {missing}")
    return module


def _read_chrono(path: Path) -> pl.DataFrame:
    frame = pl.read_csv(
        path,
        schema_overrides={
            "Distance": pl.String,
            "ID": pl.String,
            **{field: pl.Float64 for field in CHRONO_RATING_FIELDS},
        },
    )
    expressions = [
        pl.col("Date").str.to_date(),
        pl.col("Season").cast(pl.Int64),
        pl.col("Race").cast(pl.Int64),
        pl.col("Place").cast(pl.Int64),
        pl.col("Leg").cast(pl.Int64),
        pl.col("ID").cast(pl.String),
    ]
    expressions.extend(pl.col(field).cast(pl.Float64) for field in CHRONO_RATING_FIELDS)
    return frame.with_columns(expressions)


def _read_event_ratings(path: Path) -> pl.DataFrame:
    return pl.read_csv(
        path,
        schema_overrides={"Distance": pl.String, "ID": pl.String},
    ).with_columns([
        pl.col("Date").str.to_date(),
        pl.col("Season").cast(pl.Int64),
        pl.col("Race").cast(pl.Int64),
        pl.col("Place").cast(pl.Int64),
        pl.col("Leg").cast(pl.Int64),
        pl.col("ID").cast(pl.String),
        pl.col("Pelo").cast(pl.Float64),
        pl.col("Elo").cast(pl.Float64),
    ])


def _athlete_key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(row[field]) for field in ATHLETE_KEY_FIELDS)


def inject_event_ratings(
    accepted_chrono: pl.DataFrame,
    candidate_ratings: pl.DataFrame,
    *,
    tolerance: float,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    candidate_rows = candidate_ratings.select(
        [*ATHLETE_KEY_FIELDS, *RATING_FIELDS]
    ).to_dicts()
    accepted_rows = accepted_chrono.select(
        [*ATHLETE_KEY_FIELDS, *RATING_FIELDS]
    ).to_dicts()
    candidate_by_key = {_athlete_key(row): row for row in candidate_rows}
    accepted_by_key = {_athlete_key(row): row for row in accepted_rows}
    duplicate_candidates = len(candidate_by_key) != len(candidate_rows)
    missing = set(candidate_by_key) - set(accepted_by_key)
    mismatches = []
    maximum_difference = 0.0
    for key in set(candidate_by_key) & set(accepted_by_key):
        differences = {
            field: abs(float(candidate_by_key[key][field]) - float(accepted_by_key[key][field]))
            for field in RATING_FIELDS
        }
        maximum_difference = max(maximum_difference, *differences.values())
        if any(not math.isclose(
            float(candidate_by_key[key][field]),
            float(accepted_by_key[key][field]),
            rel_tol=0.0,
            abs_tol=tolerance,
        ) for field in RATING_FIELDS):
            mismatches.append({
                "key": dict(zip(ATHLETE_KEY_FIELDS, key)),
                "differences": differences,
            })
    passed = (
        bool(candidate_rows)
        and not duplicate_candidates
        and not missing
        and not mismatches
    )
    report = {
        "status": "passed" if passed else "failed",
        "candidate_rows": len(candidate_rows),
        "matched_rows": len(set(candidate_by_key) & set(accepted_by_key)),
        "missing_rows": len(missing),
        "rating_mismatches": len(mismatches),
        "maximum_absolute_rating_difference": maximum_difference,
        "tolerance": tolerance,
        "duplicate_candidate_keys": duplicate_candidates,
        "missing_examples": [dict(zip(ATHLETE_KEY_FIELDS, key)) for key in sorted(missing)[:3]],
        "mismatch_examples": mismatches[:3],
    }
    if not passed:
        return accepted_chrono, report
    replacement = candidate_ratings.select([
        *ATHLETE_KEY_FIELDS,
        pl.col("Pelo").alias("candidate_Pelo"),
        pl.col("Elo").alias("candidate_Elo"),
    ])
    injected = accepted_chrono.join(
        replacement, on=ATHLETE_KEY_FIELDS, how="left", coalesce=True
    )
    injected = injected.with_columns([
        pl.coalesce([pl.col("candidate_Pelo"), pl.col("Pelo")]).alias("Pelo"),
        pl.coalesce([pl.col("candidate_Elo"), pl.col("Elo")]).alias("Elo"),
    ]).drop(["candidate_Pelo", "candidate_Elo"])
    return injected, report


def csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    result = frame.clone()
    for name, dtype in result.schema.items():
        if isinstance(dtype, pl.List):
            result = result.with_columns(
                pl.col(name).map_elements(
                    lambda values: ",".join(map(str, values)) if values is not None else "",
                    return_dtype=pl.String,
                ).alias(name)
            )
    return result


def _team_key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(row[field]) for field in TEAM_KEY_FIELDS)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def compare_team_artifact(
    generated: pl.DataFrame,
    accepted_path: Path,
    *,
    tolerance: float,
) -> dict[str, Any]:
    generated = csv_ready(generated)
    accepted = pl.read_csv(accepted_path, schema_overrides={"Distance": pl.String})
    generated_rows = generated.to_dicts()
    accepted_rows = accepted.to_dicts()
    generated_by_key = {_team_key(row): row for row in generated_rows}
    accepted_by_key = {_team_key(row): row for row in accepted_rows}
    duplicate_generated = len(generated_by_key) != len(generated_rows)
    duplicate_accepted = len(accepted_by_key) != len(accepted_rows)
    missing = set(accepted_by_key) - set(generated_by_key)
    unexpected = set(generated_by_key) - set(accepted_by_key)
    mismatches = []
    maximum_difference = 0.0
    common_columns = list(accepted.columns)
    if set(common_columns) != set(generated.columns):
        return {
            "status": "failed",
            "generated_rows": len(generated_rows),
            "accepted_rows": len(accepted_rows),
            "schema_mismatch": True,
            "generated_only_columns": sorted(set(generated.columns) - set(common_columns)),
            "accepted_only_columns": sorted(set(common_columns) - set(generated.columns)),
        }
    for key in set(generated_by_key) & set(accepted_by_key):
        row_differences = {}
        for field in common_columns:
            generated_value = generated_by_key[key][field]
            accepted_value = accepted_by_key[key][field]
            if _is_number(generated_value) and _is_number(accepted_value):
                difference = abs(float(generated_value) - float(accepted_value))
                maximum_difference = max(maximum_difference, difference)
                if not math.isclose(
                    float(generated_value), float(accepted_value),
                    rel_tol=0.0, abs_tol=tolerance,
                ):
                    row_differences[field] = difference
            elif str(generated_value) != str(accepted_value):
                row_differences[field] = {
                    "generated": str(generated_value),
                    "accepted": str(accepted_value),
                }
        if row_differences:
            mismatches.append({
                "key": dict(zip(TEAM_KEY_FIELDS, key)),
                "differences": row_differences,
            })
    passed = (
        not duplicate_generated
        and not duplicate_accepted
        and not missing
        and not unexpected
        and not mismatches
        and len(generated_rows) == len(accepted_rows)
    )
    return {
        "status": "passed" if passed else "failed",
        "generated_rows": len(generated_rows),
        "accepted_rows": len(accepted_rows),
        "schema_mismatch": False,
        "missing_rows": len(missing),
        "unexpected_rows": len(unexpected),
        "value_mismatches": len(mismatches),
        "maximum_absolute_numeric_difference": maximum_difference,
        "tolerance": tolerance,
        "duplicate_generated_keys": duplicate_generated,
        "duplicate_accepted_keys": duplicate_accepted,
        "missing_examples": [dict(zip(TEAM_KEY_FIELDS, key)) for key in sorted(missing)[:3]],
        "unexpected_examples": [dict(zip(TEAM_KEY_FIELDS, key)) for key in sorted(unexpected)[:3]],
        "mismatch_examples": mismatches[:3],
    }


def _selected_events(*rating_frames: pl.DataFrame) -> set[tuple[str, str]]:
    return {
        (str(row["Date"]), str(row["RaceType"]))
        for frame in rating_frames
        for row in frame.select(["Date", "RaceType"]).unique().to_dicts()
    }


def run_parity(
    *,
    event_ratings_dir: Path,
    accepted_dir: Path,
    output_dir: Path,
    aggregator_path: Path,
    tolerance: float = 1e-9,
) -> dict[str, Any]:
    accepted_dir = accepted_dir.resolve()
    output_dir = output_dir.resolve()
    if accepted_dir == output_dir or accepted_dir in output_dir.parents:
        raise IBUContractError("relay parity output must be outside accepted data directory")
    aggregator = load_aggregator(aggregator_path.resolve())
    men = _read_chrono(accepted_dir / "men_chrono.csv")
    ladies = _read_chrono(accepted_dir / "ladies_chrono.csv")
    men_ratings = _read_event_ratings(event_ratings_dir / "relay_M_overall.csv")
    ladies_ratings = _read_event_ratings(event_ratings_dir / "relay_L_overall.csv")
    men, men_injection = inject_event_ratings(men, men_ratings, tolerance=tolerance)
    ladies, ladies_injection = inject_event_ratings(
        ladies, ladies_ratings, tolerance=tolerance
    )
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "athlete_rating_injection": {"M": men_injection, "L": ladies_injection},
        "artifacts": {},
        "accepted_data_mutated": False,
        "publication": {"status": "disabled"},
    }
    if men_injection["status"] != "passed" or ladies_injection["status"] != "passed":
        report["status"] = "failed"
        _atomic_json(output_dir / "relay_team_parity_report.json", report)
        return report

    combined = aggregator.create_combined_df(men, ladies)
    generated = {
        "men_relay": aggregator.process_relay(men, "Men's"),
        "ladies_relay": aggregator.process_relay(ladies, "Ladies'"),
        "mixed_relay": aggregator.process_mixed_relay(combined),
        "single_mixed_relay": aggregator.process_single_mixed_relay(combined),
    }
    selected_events = _selected_events(men_ratings, ladies_ratings)
    for name, frame in generated.items():
        if frame is None:
            report["artifacts"][name] = {"status": "failed", "reason": "no rows"}
            report["status"] = "failed"
            continue
        comparison = compare_team_artifact(
            frame,
            accepted_dir / ARTIFACTS[name],
            tolerance=tolerance,
        )
        report["artifacts"][name] = comparison
        if comparison["status"] != "passed":
            report["status"] = "failed"
        event_rows = [
            row for row in csv_ready(frame).to_dicts()
            if (str(row["Date"]), str(row["RaceType"])) in selected_events
        ]
        if event_rows:
            _write_csv(
                output_dir / "team_candidates" / f"{name}.csv",
                event_rows,
                csv_ready(frame).columns,
            )
    _atomic_json(output_dir / "relay_team_parity_report.json", report)
    return report


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--event-ratings-dir", type=Path, required=True)
    command.add_argument(
        "--accepted-dir", type=Path, default=here / "relay" / "excel365"
    )
    command.add_argument(
        "--aggregator", type=Path, default=here / "relay" / "relay_chrono.py"
    )
    command.add_argument("--output-dir", type=Path, required=True)
    command.add_argument("--tolerance", type=float, default=1e-9)
    return command


def main() -> int:
    args = parser().parse_args()
    report = run_parity(
        event_ratings_dir=args.event_ratings_dir,
        accepted_dir=args.accepted_dir,
        output_dir=args.output_dir,
        aggregator_path=args.aggregator,
        tolerance=args.tolerance,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
