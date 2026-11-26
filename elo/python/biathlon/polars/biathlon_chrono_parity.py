#!/usr/bin/env python3
"""Deterministically rebuild and verify Biathlon individual/relay chronos.

The legacy chrono scripts are duplicated, hard-coded programs that write
directly into accepted data.  This module preserves their rating semantics in
a callable generator, verifies all four full chronos against accepted data,
and writes candidates only to a new isolated directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import tempfile
from pathlib import Path
from typing import Any

import polars as pl
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError

REPORT_SCHEMA = "nordicnumbers.biathlon-chrono-parity/v1"
COMMON_COLUMNS = [
    "Date",
    "City",
    "Country",
    "Sex",
    "Distance",
    "RaceType",
    "MassStart",
    "Event",
    "Place",
    "Skier",
    "Nation",
    "ID",
    "Season",
    "Race",
    "Birthday",
    "Age",
    "Exp",
    "Leg",
]
RATING_COLUMNS = [
    "Elo",
    "Individual_Elo",
    "Sprint_Elo",
    "Pursuit_Elo",
    "MassStart_Elo",
    "Pelo",
    "Individual_Pelo",
    "Sprint_Pelo",
    "Pursuit_Pelo",
    "MassStart_Pelo",
]
STREAMS = {
    "Individual": "Individual",
    "Sprint": "Sprint",
    "Pursuit": "Pursuit",
    "Mass_Start": "MassStart",
}
SEXES = {"M": "men", "L": "ladies"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read(path: Path) -> pl.DataFrame:
    if not path.is_file():
        raise IBUContractError(f"missing chrono input: {path}")
    return pl.read_csv(path, schema_overrides={"Distance": pl.String})


def _validate_stream(frame: pl.DataFrame, path: Path) -> None:
    required = set(COMMON_COLUMNS + ["Pelo", "Elo"])
    missing = required - set(frame.columns)
    if missing:
        raise IBUContractError(f"{path} is missing columns: {sorted(missing)}")
    duplicates = frame.select(COMMON_COLUMNS).is_duplicated().sum()
    if duplicates:
        raise IBUContractError(f"{path} has {duplicates} duplicate chrono keys")


def _apply_offseason_rules(frame: pl.DataFrame) -> pl.DataFrame:
    max_season = frame["Season"].max()
    expressions = []
    for elo in [column for column in RATING_COLUMNS if column.endswith("Elo")]:
        pelo = "Pelo" if elo == "Elo" else elo.replace("_Elo", "_Pelo")
        expressions.append(
            pl.when(
                (pl.col("RaceType") == "Offseason") & (pl.col("Season") == max_season)
            )
            .then(pl.col(pelo))
            .when(pl.col("RaceType") == "Offseason")
            .then(pl.col(pelo) * 0.85 + 1300 * 0.15)
            .otherwise(pl.col(elo))
            .alias(elo)
        )
    return frame.with_columns(expressions)


def build_chrono(input_dir: Path, sex: str) -> pl.DataFrame:
    """Build one full chrono with the established legacy rating semantics."""
    if sex not in SEXES:
        raise IBUContractError(f"unsupported chrono sex: {sex!r}")
    input_dir = input_dir.resolve()
    overall_path = input_dir / f"{sex}.csv"
    merged = _read(overall_path)
    _validate_stream(merged, overall_path)
    for filename, prefix in STREAMS.items():
        path = input_dir / f"{sex}_{filename}.csv"
        stream = _read(path)
        _validate_stream(stream, path)
        stream = stream.rename({"Pelo": f"{prefix}_Pelo", "Elo": f"{prefix}_Elo"})
        merged = merged.join(stream, on=COMMON_COLUMNS, how="left")

    ordered = merged.sort(["ID", "Date", "Race", "Place", "Leg"])
    ordered = ordered.with_columns(
        [
            pl.col(column).forward_fill().over("ID").alias(column)
            for column in RATING_COLUMNS
            if column in ordered.columns
        ]
    )
    for elo, pelo in zip(RATING_COLUMNS[:5], RATING_COLUMNS[5:], strict=True):
        ordered = ordered.with_columns(
            pl.when(pl.col(pelo).is_null())
            .then(pl.col(elo))
            .otherwise(pl.col(pelo))
            .alias(pelo)
        )
    ordered = _apply_offseason_rules(ordered)
    # The first three columns preserve chronological consumption; the remaining
    # keys make ties deterministic without changing any rating semantics.
    sort_columns = ["Date", "Race", "Place", "ID", "Leg", "RaceType", "Skier"]
    return ordered.select(RATING_COLUMNS + COMMON_COLUMNS).sort(sort_columns)


def compare_chrono(
    generated: pl.DataFrame, accepted_path: Path, tolerance: float
) -> dict[str, Any]:
    accepted = _read(accepted_path).select(RATING_COLUMNS + COMMON_COLUMNS)
    generated_keys = generated.select(COMMON_COLUMNS)
    accepted_keys = accepted.select(COMMON_COLUMNS)
    missing = accepted_keys.join(generated_keys, on=COMMON_COLUMNS, how="anti")
    unexpected = generated_keys.join(accepted_keys, on=COMMON_COLUMNS, how="anti")
    joined = generated.join(
        accepted,
        on=COMMON_COLUMNS,
        how="inner",
        suffix="_accepted",
    )
    mismatches = 0
    maximum = 0.0
    for column in RATING_COLUMNS:
        left = joined[column].to_list()
        right = joined[f"{column}_accepted"].to_list()
        for generated_value, accepted_value in zip(left, right, strict=True):
            if generated_value is None or accepted_value is None:
                mismatches += generated_value is not accepted_value
                continue
            difference = abs(float(generated_value) - float(accepted_value))
            if not math.isfinite(difference):
                mismatches += 1
                continue
            maximum = max(maximum, difference)
            mismatches += difference > tolerance
    passed = (
        generated.height == accepted.height
        and missing.is_empty()
        and unexpected.is_empty()
        and mismatches == 0
    )
    return {
        "status": "passed" if passed else "failed",
        "generated_rows": generated.height,
        "accepted_rows": accepted.height,
        "missing_rows": missing.height,
        "unexpected_rows": unexpected.height,
        "rating_mismatches": mismatches,
        "maximum_absolute_rating_difference": maximum,
        "tolerance": tolerance,
        "accepted_artifact": str(accepted_path.resolve()),
    }


def _safe_output(output_dir: Path, individual_dir: Path, relay_dir: Path) -> Path:
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise IBUContractError(f"chrono parity output already exists: {output_dir}")
    for accepted in (individual_dir.resolve(), relay_dir.resolve()):
        if output_dir == accepted or accepted in output_dir.parents:
            raise IBUContractError("chrono parity output must be outside accepted data")
    return output_dir


def run_parity(
    *,
    individual_dir: Path,
    relay_dir: Path,
    output_dir: Path,
    tolerance: float = 1e-9,
) -> dict[str, Any]:
    individual_dir = individual_dir.resolve()
    relay_dir = relay_dir.resolve()
    output_dir = _safe_output(output_dir, individual_dir, relay_dir)
    before = {
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "tolerance": tolerance,
        "policies": {},
        "accepted_fingerprints": {"before": before, "after": None},
        "accepted_data_mutated": False,
        "promotion": {"status": "disabled"},
        "publication": {"status": "disabled"},
    }
    try:
        for policy, accepted_dir in (
            ("individual", individual_dir),
            ("relay", relay_dir),
        ):
            policy_report: dict[str, Any] = {}
            for sex, label in SEXES.items():
                candidate = build_chrono(accepted_dir, sex)
                candidate_path = staging / policy / f"{label}_chrono.csv"
                candidate_path.parent.mkdir(parents=True, exist_ok=True)
                candidate.write_csv(candidate_path)
                comparison = compare_chrono(
                    candidate,
                    accepted_dir / f"{label}_chrono.csv",
                    tolerance,
                )
                comparison["candidate"] = {
                    "file": candidate_path.relative_to(staging).as_posix(),
                    "rows": candidate.height,
                    "sha256": _sha256(candidate_path),
                }
                policy_report[sex] = comparison
                if comparison["status"] != "passed":
                    report["status"] = "failed"
            report["policies"][policy] = policy_report

        after = {
            "individual": fingerprint_tree(individual_dir),
            "relay": fingerprint_tree(relay_dir),
        }
        report["accepted_fingerprints"]["after"] = after
        report["accepted_data_mutated"] = before != after
        if before != after:
            report["status"] = "failed"
        with (staging / "chrono_parity_report.json").open(
            "w", encoding="utf-8"
        ) as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        staging.replace(output_dir)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--individual-dir", type=Path, default=here / "excel365")
    parser.add_argument("--relay-dir", type=Path, default=here / "relay" / "excel365")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    args = parser.parse_args()
    try:
        report = run_parity(**vars(args))
    except IBUContractError as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
