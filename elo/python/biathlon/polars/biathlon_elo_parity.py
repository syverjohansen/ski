#!/usr/bin/env python3
"""Replay official IBU candidates through the existing Biathlon Elo engine.

This is a shadow-only parity harness. It replaces one captured event inside an
in-memory copy of accepted histories, calls the existing ``elo.py`` function
unchanged, and compares generated Elo/Pelo values with checked-in accepted
artifacts. It never writes to accepted data or publishes.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import logging
import math
import tempfile
from collections import Counter
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable

import polars as pl

from ibu_source_contract import IBUContractError


REPORT_SCHEMA = "nordicnumbers.biathlon-elo-parity/v1"
HISTORY_FIELDS = [
    "Date", "City", "Country", "Event", "Distance", "RaceType", "MassStart",
    "Season", "Race", "Place", "Skier", "Nation", "ID", "Sex", "Birthday",
    "Leg", "Age", "Exp",
]
INPUT_PARITY_FIELDS = [field for field in HISTORY_FIELDS if field != "Age"]
OUTPUT_KEY_FIELDS = ["Date", "Season", "Race", "Place", "ID", "RaceType", "Leg"]
RATING_FIELDS = ["Pelo", "Elo"]
STREAMS = {
    "overall": None,
    "individual": "Individual",
    "sprint": "Sprint",
    "pursuit": "Pursuit",
    "mass_start": "Mass Start",
}
ARTIFACT_NAMES = {
    "overall": {"M": "M.csv", "L": "L.csv"},
    "individual": {"M": "M_Individual.csv", "L": "L_Individual.csv"},
    "sprint": {"M": "M_Sprint.csv", "L": "L_Sprint.csv"},
    "pursuit": {"M": "M_Pursuit.csv", "L": "L_Pursuit.csv"},
    "mass_start": {"M": "M_Mass_Start.csv", "L": "L_Mass_Start.csv"},
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


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


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def load_engine(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("nordicnumbers_biathlon_elo", path)
    if spec is None or spec.loader is None:
        raise IBUContractError(f"cannot load existing Elo engine: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not callable(getattr(module, "elo", None)):
        raise IBUContractError(f"existing Elo engine has no elo() function: {path}")
    if isinstance(getattr(module, "logger", None), logging.Logger):
        module.logger.setLevel(logging.WARNING)
    return module


def _event_key(row: dict[str, str]) -> tuple[str, str]:
    return row["Date"], row["RaceType"]


def splice_candidates(
    accepted_rows: list[dict[str, str]],
    candidate_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Replace captured event rows and align target-specific race numbering."""
    if not candidate_rows:
        raise IBUContractError("official candidate contains no result rows")
    selected_events = {_event_key(row) for row in candidate_rows}
    accepted_event_rows = [
        row for row in accepted_rows if _event_key(row) in selected_events
    ]
    accepted_event_numbers: dict[tuple[str, str], set[str]] = {}
    for event in selected_events:
        accepted_event_numbers[event] = {
            row["Race"] for row in accepted_event_rows if _event_key(row) == event
        }
        if len(accepted_event_numbers[event]) != 1:
            raise IBUContractError(
                f"accepted history has ambiguous race numbering for {event}: "
                f"{sorted(accepted_event_numbers[event])}"
            )
    aligned = []
    for source in candidate_rows:
        row = dict(source)
        row["Race"] = next(iter(accepted_event_numbers[_event_key(row)]))
        aligned.append(row)

    candidate_keys = {
        tuple(row[field] for field in INPUT_PARITY_FIELDS) for row in aligned
    }
    accepted_keys = {
        tuple(row[field] for field in INPUT_PARITY_FIELDS) for row in accepted_event_rows
    }
    missing = accepted_keys - candidate_keys
    unexpected = candidate_keys - accepted_keys
    parity = (
        len(aligned) == len(accepted_event_rows)
        and not missing
        and not unexpected
    )
    report = {
        "status": "passed" if parity else "failed",
        "candidate_rows": len(aligned),
        "accepted_rows": len(accepted_event_rows),
        "missing_rows": len(missing),
        "unexpected_rows": len(unexpected),
        "age_excluded_from_identity": True,
        "missing_examples": [dict(zip(INPUT_PARITY_FIELDS, row)) for row in sorted(missing)[:3]],
        "unexpected_examples": [dict(zip(INPUT_PARITY_FIELDS, row)) for row in sorted(unexpected)[:3]],
    }
    if not parity:
        return [], report
    retained = [row for row in accepted_rows if _event_key(row) not in selected_events]
    return [*retained, *aligned], report


def history_frame(rows: list[dict[str, str]]) -> pl.DataFrame:
    frame = pl.DataFrame(rows, schema=HISTORY_FIELDS)
    return frame.with_columns([
        pl.col("Date").str.to_date(),
        pl.col("Birthday").str.to_datetime(),
        pl.col("Distance").cast(pl.String),
        pl.col("ID").cast(pl.String),
        pl.col("MassStart").cast(pl.Int64),
        pl.col("Season").cast(pl.Int64),
        pl.col("Race").cast(pl.Int64),
        pl.col("Place").cast(pl.Int64),
        pl.col("Leg").cast(pl.Int64),
        pl.col("Age").cast(pl.Float64),
        pl.col("Exp").cast(pl.Int64),
    ])


def accepted_output_frame(path: Path) -> pl.DataFrame:
    return pl.read_csv(
        path,
        schema_overrides={"Distance": pl.String, "ID": pl.String},
    ).with_columns([
        pl.col("Date").str.to_date(),
        pl.col("Season").cast(pl.Int64),
        pl.col("Race").cast(pl.Int64),
        pl.col("Place").cast(pl.Int64),
        pl.col("Leg").cast(pl.Int64),
        pl.col("Pelo").cast(pl.Float64),
        pl.col("Elo").cast(pl.Float64),
    ])


def major_events(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(
        pl.col("Event").str.contains("(?i)Olympic|World Cup|Championship")
    )


def stream_input(
    frame: pl.DataFrame,
    *,
    include_relays: bool,
    race_type: str | None,
) -> pl.DataFrame:
    frame = major_events(frame)
    if not include_relays:
        frame = frame.filter(~pl.col("RaceType").str.contains("Relay"))
    if race_type is not None:
        frame = frame.filter(pl.col("RaceType") == race_type)
    return frame


def _key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(row[field]) for field in OUTPUT_KEY_FIELDS)


def compare_ratings(
    generated: pl.DataFrame,
    accepted: pl.DataFrame,
    *,
    tolerance: float,
) -> dict[str, Any]:
    generated_rows = generated.select([*OUTPUT_KEY_FIELDS, *RATING_FIELDS]).to_dicts()
    accepted_rows = accepted.select([*OUTPUT_KEY_FIELDS, *RATING_FIELDS]).to_dicts()
    generated_by_key = {_key(row): row for row in generated_rows}
    accepted_by_key = {_key(row): row for row in accepted_rows}
    duplicate_generated = len(generated_by_key) != len(generated_rows)
    duplicate_accepted = len(accepted_by_key) != len(accepted_rows)
    missing = set(accepted_by_key) - set(generated_by_key)
    unexpected = set(generated_by_key) - set(accepted_by_key)
    mismatches = []
    maximum_difference = 0.0
    for key in set(generated_by_key) & set(accepted_by_key):
        differences = {
            field: abs(float(generated_by_key[key][field]) - float(accepted_by_key[key][field]))
            for field in RATING_FIELDS
        }
        maximum_difference = max(maximum_difference, *differences.values())
        if any(not math.isclose(
            float(generated_by_key[key][field]),
            float(accepted_by_key[key][field]),
            rel_tol=0.0,
            abs_tol=tolerance,
        ) for field in RATING_FIELDS):
            mismatches.append({
                "key": dict(zip(OUTPUT_KEY_FIELDS, key)),
                "differences": differences,
                "generated": {field: generated_by_key[key][field] for field in RATING_FIELDS},
                "accepted": {field: accepted_by_key[key][field] for field in RATING_FIELDS},
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
        "missing_rows": len(missing),
        "unexpected_rows": len(unexpected),
        "rating_mismatches": len(mismatches),
        "maximum_absolute_rating_difference": maximum_difference,
        "tolerance": tolerance,
        "duplicate_generated_keys": duplicate_generated,
        "duplicate_accepted_keys": duplicate_accepted,
        "missing_examples": [dict(zip(OUTPUT_KEY_FIELDS, row)) for row in sorted(missing)[:3]],
        "unexpected_examples": [dict(zip(OUTPUT_KEY_FIELDS, row)) for row in sorted(unexpected)[:3]],
        "mismatch_examples": mismatches[:3],
    }


def _event_output_rows(
    generated: pl.DataFrame,
    candidate_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    events = {_event_key(row) for row in candidate_rows}
    return [
        row for row in generated.to_dicts()
        if (str(row["Date"]), str(row["RaceType"])) in events
    ]


def run_parity(
    *,
    candidate_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
    output_dir: Path,
    engine_path: Path,
    tolerance: float = 1e-9,
) -> dict[str, Any]:
    candidate_dir = candidate_dir.resolve()
    individual_dir = individual_dir.resolve()
    relay_dir = relay_dir.resolve()
    output_dir = output_dir.resolve()
    if any(root == output_dir or root in output_dir.parents for root in (individual_dir, relay_dir)):
        raise IBUContractError("Elo parity output must be outside accepted data directories")
    engine = load_engine(engine_path.resolve())
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "engine": str(engine_path.resolve()),
        "tolerance": tolerance,
        "policies": {},
        "accepted_data_mutated": False,
        "publication": {"status": "disabled"},
    }

    for policy, accepted_dir in (
        ("individual", individual_dir),
        ("relay", relay_dir),
    ):
        policy_report: dict[str, Any] = {}
        for sex, prefix in (("M", "men"), ("L", "ladies")):
            accepted_history = _read_csv(accepted_dir / f"{prefix}_scrape_update.csv")
            candidates = _read_csv(candidate_dir / f"{prefix}_results_candidate.csv")
            spliced, input_report = splice_candidates(accepted_history, candidates)
            sex_report: dict[str, Any] = {"input_parity": input_report, "streams": {}}
            if input_report["status"] != "passed":
                report["status"] = "failed"
                policy_report[sex] = sex_report
                continue
            frame = history_frame(spliced)
            selected_streams = STREAMS if policy == "individual" else {"overall": None}
            for stream, race_type in selected_streams.items():
                inputs = stream_input(
                    frame,
                    include_relays=policy == "relay",
                    race_type=race_type,
                )
                generated = engine.elo(inputs)
                artifact = accepted_dir / ARTIFACT_NAMES[stream][sex]
                comparison = compare_ratings(
                    generated,
                    accepted_output_frame(artifact),
                    tolerance=tolerance,
                )
                comparison["accepted_artifact"] = str(artifact)
                sex_report["streams"][stream] = comparison
                if comparison["status"] != "passed":
                    report["status"] = "failed"
                event_rows = _event_output_rows(generated, candidates)
                if event_rows:
                    _write_csv(
                        output_dir / "event_ratings" / f"{policy}_{sex}_{stream}.csv",
                        event_rows,
                        generated.columns,
                    )
            policy_report[sex] = sex_report
        report["policies"][policy] = policy_report

    _atomic_json(output_dir / "elo_parity_report.json", report)
    return report


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--candidate-dir", type=Path, required=True)
    command.add_argument("--individual-dir", type=Path, default=here / "excel365")
    command.add_argument("--relay-dir", type=Path, default=here / "relay" / "excel365")
    command.add_argument("--engine", type=Path, default=here / "elo.py")
    command.add_argument("--output-dir", type=Path, required=True)
    command.add_argument("--tolerance", type=float, default=1e-9)
    return command


def main() -> int:
    args = parser().parse_args()
    report = run_parity(
        candidate_dir=args.candidate_dir,
        individual_dir=args.individual_dir,
        relay_dir=args.relay_dir,
        output_dir=args.output_dir,
        engine_path=args.engine,
        tolerance=args.tolerance,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
