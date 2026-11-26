#!/usr/bin/env python3
"""Materialize complete Biathlon rating and chrono promotion candidates.

This deliberately large, explicit step is separate from the lightweight hosted
shadow. It replaces one parity-proven event in isolated history copies, rebuilds
every individual and relay-inclusive rating stream, verifies those full outputs,
then feeds them into the deterministic chrono generator. Accepted data is never
written and application/publication remain disabled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from biathlon_chrono_parity import SEXES, build_chrono, compare_chrono
from biathlon_elo_parity import (
    ARTIFACT_NAMES,
    STREAMS,
    _read_csv,
    accepted_output_frame,
    compare_ratings,
    history_frame,
    load_engine,
    splice_candidates,
    stream_input,
)
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError

REPORT_SCHEMA = "nordicnumbers.biathlon-rating-chrono-candidate/v1"
POLICIES = {"individual": False, "relay": True}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_output(
    output_dir: Path,
    candidate_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
) -> Path:
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise IBUContractError(f"rating/chrono candidate output exists: {output_dir}")
    for protected in (
        candidate_dir.resolve(),
        individual_dir.resolve(),
        relay_dir.resolve(),
    ):
        if output_dir == protected or protected in output_dir.parents:
            raise IBUContractError(
                "rating/chrono candidate output must be outside source and accepted data"
            )
    return output_dir


def _candidate_receipt(path: Path, rows: int, staging: Path) -> dict[str, Any]:
    return {
        "file": path.relative_to(staging).as_posix(),
        "rows": rows,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def build_candidates(
    *,
    candidate_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
    engine_path: Path,
    output_dir: Path,
    tolerance: float = 1e-9,
) -> dict[str, Any]:
    candidate_dir = candidate_dir.expanduser().resolve()
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    output_dir = _safe_output(output_dir, candidate_dir, individual_dir, relay_dir)
    before = {
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }
    engine = load_engine(engine_path.expanduser().resolve())
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
        "apply_ready": False,
        "remaining_blockers": ["individual and relay elevation chrono candidates"],
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    try:
        accepted_dirs = {"individual": individual_dir, "relay": relay_dir}
        for policy, include_relays in POLICIES.items():
            accepted_dir = accepted_dirs[policy]
            policy_report: dict[str, Any] = {}
            ratings_dir = staging / "full_ratings" / policy
            for sex, prefix in (("M", "men"), ("L", "ladies")):
                accepted_history = _read_csv(
                    accepted_dir / f"{prefix}_scrape_update.csv"
                )
                event_candidates = _read_csv(
                    candidate_dir / f"{prefix}_results_candidate.csv"
                )
                spliced, input_report = splice_candidates(
                    accepted_history, event_candidates
                )
                sex_report: dict[str, Any] = {
                    "input_parity": input_report,
                    "streams": {},
                }
                if input_report["status"] != "passed":
                    report["status"] = "failed"
                    policy_report[sex] = sex_report
                    continue
                frame = history_frame(spliced)
                for stream, race_type in STREAMS.items():
                    generated = engine.elo(
                        stream_input(
                            frame,
                            include_relays=include_relays,
                            race_type=race_type,
                        )
                    )
                    accepted_path = accepted_dir / ARTIFACT_NAMES[stream][sex]
                    comparison = compare_ratings(
                        generated,
                        accepted_output_frame(accepted_path),
                        tolerance=tolerance,
                    )
                    comparison["accepted_artifact"] = str(accepted_path)
                    artifact = ratings_dir / ARTIFACT_NAMES[stream][sex]
                    artifact.parent.mkdir(parents=True, exist_ok=True)
                    generated.sort(
                        ["Date", "Race", "Place", "ID", "Leg", "RaceType"]
                    ).write_csv(artifact)
                    comparison["candidate"] = _candidate_receipt(
                        artifact, generated.height, staging
                    )
                    sex_report["streams"][stream] = comparison
                    if comparison["status"] != "passed":
                        report["status"] = "failed"
                policy_report[sex] = sex_report

            chrono_report: dict[str, Any] = {}
            if all(
                policy_report.get(sex, {}).get("input_parity", {}).get("status")
                == "passed"
                for sex in ("M", "L")
            ):
                for sex, label in SEXES.items():
                    chrono = build_chrono(ratings_dir, sex)
                    comparison = compare_chrono(
                        chrono,
                        accepted_dir / f"{label}_chrono.csv",
                        tolerance,
                    )
                    artifact = staging / "full_chronos" / policy / f"{label}_chrono.csv"
                    artifact.parent.mkdir(parents=True, exist_ok=True)
                    chrono.write_csv(artifact)
                    comparison["candidate"] = _candidate_receipt(
                        artifact, chrono.height, staging
                    )
                    chrono_report[sex] = comparison
                    if comparison["status"] != "passed":
                        report["status"] = "failed"
            policy_report["chronos"] = chrono_report
            report["policies"][policy] = policy_report

        after = {
            "individual": fingerprint_tree(individual_dir),
            "relay": fingerprint_tree(relay_dir),
        }
        report["accepted_fingerprints"]["after"] = after
        report["accepted_data_mutated"] = before != after
        if before != after:
            report["status"] = "failed"
        with (staging / "rating_chrono_candidate_report.json").open(
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
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument("--individual-dir", type=Path, default=here / "excel365")
    parser.add_argument("--relay-dir", type=Path, default=here / "relay" / "excel365")
    parser.add_argument(
        "--engine", dest="engine_path", type=Path, default=here / "elo.py"
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    args = parser.parse_args()
    try:
        report = build_candidates(**vars(args))
    except IBUContractError as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
