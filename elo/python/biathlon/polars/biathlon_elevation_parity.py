#!/usr/bin/env python3
"""Deterministically add and verify elevation on Biathlon chronos."""

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
from biathlon_chrono_parity import (
    KEY_COLUMNS,
    _read,
    compare_chrono,
)
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError

REPORT_SCHEMA = "nordicnumbers.biathlon-elevation-parity/v1"
SEXES = ("men", "ladies")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_city(value: object) -> str | None:
    return value.strip().title() if isinstance(value, str) else None


def read_elevation_map(path: Path) -> dict[str, int]:
    if not path.is_file():
        raise IBUContractError(f"missing elevation reference: {path}")
    frame = pl.read_csv(path)
    required = {"City", "Elevation"}
    if not required.issubset(frame.columns):
        raise IBUContractError(f"elevation reference is missing {sorted(required)}")
    mapping: dict[str, int] = {}
    for row in frame.select("City", "Elevation").to_dicts():
        city = _normalized_city(row["City"])
        if not city or city in mapping:
            raise IBUContractError(f"invalid or duplicate elevation city: {city!r}")
        mapping[city] = int(row["Elevation"])
    if not mapping:
        raise IBUContractError("elevation reference contains no cities")
    return mapping


def add_elevation(chrono: pl.DataFrame, elevation_path: Path) -> pl.DataFrame:
    """Apply the legacy title-case city mapping and zero-default policy."""
    if "City" not in chrono.columns:
        raise IBUContractError("chrono candidate has no City column")
    mapping = read_elevation_map(elevation_path)
    cities = [_normalized_city(value) for value in chrono["City"].to_list()]
    elevations = [mapping.get(city, 0) if city is not None else 0 for city in cities]
    return chrono.with_columns(
        pl.Series("City", cities, dtype=pl.String),
        pl.Series("Elevation", elevations, dtype=pl.Int64),
        # The legacy pandas read/write converted numeric distances to floats and
        # treated relay ``N/A`` as null. Preserve that accepted-file contract
        # explicitly instead of depending on inference.
        pl.col("Distance").cast(pl.Float64, strict=False),
    )


def compare_elevation(
    generated: pl.DataFrame,
    accepted_path: Path,
    tolerance: float,
    allowed_additive_events: set[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    semantic = generated.with_columns(pl.col("Distance").cast(pl.String))
    base = compare_chrono(
        semantic.drop("Elevation"),
        accepted_path,
        tolerance,
        allowed_additive_events,
    )
    accepted = _read(accepted_path)
    if "Elevation" not in accepted.columns:
        raise IBUContractError(
            f"accepted elevation artifact lacks Elevation: {accepted_path}"
        )
    joined = semantic.select(KEY_COLUMNS + ["Elevation"]).join(
        accepted.select(KEY_COLUMNS + ["Elevation"]),
        on=KEY_COLUMNS,
        how="inner",
        suffix="_accepted",
        join_nulls=True,
    )
    differences = (
        joined.select((pl.col("Elevation") - pl.col("Elevation_accepted")).abs())
        .to_series()
        .to_list()
    )
    finite = [
        float(value)
        for value in differences
        if value is not None and math.isfinite(float(value))
    ]
    mismatches = sum(value > tolerance for value in finite)
    mismatches += len(differences) - len(finite)
    base.update(
        {
            "elevation_mismatches": mismatches,
            "maximum_absolute_elevation_difference": max(finite, default=0.0),
            "zero_elevation_rows": generated.filter(pl.col("Elevation") == 0).height,
        }
    )
    if mismatches:
        base["status"] = "failed"
    return base


def _safe_output(output_dir: Path, roots: tuple[Path, Path]) -> Path:
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise IBUContractError(f"elevation parity output already exists: {output_dir}")
    for root in roots:
        root = root.resolve()
        if output_dir == root or root in output_dir.parents:
            raise IBUContractError(
                "elevation parity output must be outside accepted data"
            )
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
    output_dir = _safe_output(output_dir, (individual_dir, relay_dir))
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
            for label in SEXES:
                generated = add_elevation(
                    _read(accepted_dir / f"{label}_chrono.csv"),
                    accepted_dir / "elevation.csv",
                )
                accepted_path = accepted_dir / f"{label}_chrono_elevation.csv"
                comparison = compare_elevation(generated, accepted_path, tolerance)
                artifact = staging / policy / f"{label}_chrono_elevation.csv"
                artifact.parent.mkdir(parents=True, exist_ok=True)
                generated.write_csv(artifact)
                comparison["candidate"] = {
                    "file": artifact.relative_to(staging).as_posix(),
                    "rows": generated.height,
                    "bytes": artifact.stat().st_size,
                    "sha256": _sha256(artifact),
                }
                policy_report[label] = comparison
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
        with (staging / "elevation_parity_report.json").open(
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
