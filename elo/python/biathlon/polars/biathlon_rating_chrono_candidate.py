#!/usr/bin/env python3
"""Materialize complete Biathlon rating and chrono promotion candidates.

This deliberately large, explicit step is separate from the lightweight hosted
shadow. It replaces a parity-proven historical event or appends finalized new
events in isolated history copies, rebuilds every individual and relay-inclusive
rating stream, verifies the accepted rows remain identical, then feeds the full
outputs into the deterministic chrono generator. Accepted data is never written
and application/publication remain disabled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from biathlon_chrono_parity import (
    KEY_COLUMNS,
    RATING_COLUMNS,
    SEXES,
    build_chrono,
    compare_chrono,
)
from biathlon_elevation_parity import add_elevation, compare_elevation
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
from biathlon_relay_team_parity import (
    ARTIFACTS as TEAM_ARTIFACTS,
    compare_team_artifact,
    csv_ready,
    load_aggregator,
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


def _policy_candidate_dir(candidate_dir: Path, policy: str) -> Path:
    """Accept both the historical flat layout and the additive policy layout."""
    policy_dir = candidate_dir / policy
    return policy_dir if policy_dir.is_dir() else candidate_dir


def _allowed_derived_events(
    candidate_rows: list[dict[str, str]], mode: str
) -> set[tuple[str, str]] | None:
    if mode != "additive":
        return None
    events = {(row["Date"], row["RaceType"]) for row in candidate_rows}
    # The established engine writes a synthetic season snapshot after the final
    # race. Adding a current-season result may legitimately add, remove, or
    # update those Race=0 rows; no earlier event is authorized to change.
    events.update(
        (f"{int(row['Season']):04d}-05-01", "Offseason")
        for row in candidate_rows
    )
    return events


def _preserve_prior_offseason_archives(
    chrono: Any,
    accepted_path: Path,
    candidate_rows: list[dict[str, str]],
) -> Any:
    """Keep earlier Race=0 archive values stable when a new season starts.

    The chrono presentation intentionally exposes the latest undiscounted Elo
    for its current-season Race=0 page. Rebuilding after the first race of the
    next season must not retroactively rewrite the prior archive with the
    engine's discounted starting value.
    """
    if not candidate_rows:
        return chrono
    candidate_season = min(int(row["Season"]) for row in candidate_rows)
    import polars as pl

    accepted = pl.read_csv(
        accepted_path, schema_overrides={"Distance": pl.String}
    ).with_columns(
        *(pl.col(field).cast(chrono.schema[field]) for field in KEY_COLUMNS),
        *(pl.col(field).cast(pl.Float64) for field in RATING_COLUMNS),
    )
    archived = accepted.filter(
        (pl.col("RaceType") == "Offseason")
        & (pl.col("Season") < candidate_season)
    ).select(
        *KEY_COLUMNS,
        *(pl.col(field).alias(f"_accepted_{field}") for field in RATING_COLUMNS),
    )
    if archived.is_empty():
        return chrono
    merged = chrono.join(archived, on=KEY_COLUMNS, how="left", coalesce=True)
    merged = merged.with_columns(
        *(
            pl.coalesce([pl.col(f"_accepted_{field}"), pl.col(field)]).alias(field)
            for field in RATING_COLUMNS
        )
    )
    return merged.drop([f"_accepted_{field}" for field in RATING_COLUMNS])


def build_candidates(
    *,
    candidate_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
    engine_path: Path,
    output_dir: Path,
    aggregator_path: Path | None = None,
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
    source_before = fingerprint_tree(candidate_dir)
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
        "relay_team_artifacts": {},
        "source_candidate_fingerprints": {
            "before": source_before,
            "after": None,
        },
        "source_candidates_mutated": False,
        "accepted_fingerprints": {"before": before, "after": None},
        "accepted_data_mutated": False,
        "ready_for_promotion_bundle": False,
        "apply_ready": False,
        "remaining_blockers": [
            "integration with the checksummed promotion-review atomic set"
        ],
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    try:
        accepted_dirs = {"individual": individual_dir, "relay": relay_dir}
        relay_chronos: dict[str, Any] = {}
        relay_candidate_events: set[tuple[str, str]] = set()
        for policy, include_relays in POLICIES.items():
            accepted_dir = accepted_dirs[policy]
            policy_candidate_dir = _policy_candidate_dir(candidate_dir, policy)
            policy_report: dict[str, Any] = {}
            ratings_dir = staging / "full_ratings" / policy
            policy_has_candidates = False
            for sex, prefix in (("M", "men"), ("L", "ladies")):
                accepted_history = _read_csv(
                    accepted_dir / f"{prefix}_scrape_update.csv"
                )
                event_candidates = _read_csv(
                    policy_candidate_dir / f"{prefix}_results_candidate.csv"
                )
                if event_candidates:
                    policy_has_candidates = True
                    spliced, input_report = splice_candidates(
                        accepted_history, event_candidates
                    )
                else:
                    spliced = accepted_history
                    input_report = {
                        "status": "passed",
                        "mode": "no_change",
                        "candidate_rows": 0,
                        "accepted_rows": 0,
                        "missing_rows": 0,
                        "unexpected_rows": 0,
                        "age_excluded_from_identity": True,
                        "missing_examples": [],
                        "unexpected_examples": [],
                    }
                allowed_additive_events = _allowed_derived_events(
                    event_candidates, input_report["mode"]
                )
                if policy == "relay":
                    relay_candidate_events.update(
                        (row["Date"], row["RaceType"])
                        for row in event_candidates
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
                        allowed_additive_events=allowed_additive_events,
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

            if not policy_has_candidates:
                raise IBUContractError(
                    f"{policy} candidate set contains no result rows"
                )

            chrono_report: dict[str, Any] = {}
            elevation_report: dict[str, Any] = {}
            if all(
                policy_report.get(sex, {}).get("input_parity", {}).get("status")
                == "passed"
                for sex in ("M", "L")
            ):
                for sex, label in SEXES.items():
                    chrono = build_chrono(ratings_dir, sex)
                    candidate_rows = _read_csv(
                        policy_candidate_dir / f"{label}_results_candidate.csv"
                    )
                    if policy_report[sex]["input_parity"]["mode"] == "additive":
                        chrono = _preserve_prior_offseason_archives(
                            chrono,
                            accepted_dir / f"{label}_chrono.csv",
                            candidate_rows,
                        )
                    comparison = compare_chrono(
                        chrono,
                        accepted_dir / f"{label}_chrono.csv",
                        tolerance,
                        _allowed_derived_events(
                            candidate_rows,
                            policy_report[sex]["input_parity"]["mode"],
                        ),
                    )
                    artifact = staging / "full_chronos" / policy / f"{label}_chrono.csv"
                    artifact.parent.mkdir(parents=True, exist_ok=True)
                    chrono.write_csv(artifact)
                    if policy == "relay":
                        relay_chronos[sex] = chrono
                    comparison["candidate"] = _candidate_receipt(
                        artifact, chrono.height, staging
                    )
                    chrono_report[sex] = comparison
                    if comparison["status"] != "passed":
                        report["status"] = "failed"
                    elevation = add_elevation(chrono, accepted_dir / "elevation.csv")
                    elevation_comparison = compare_elevation(
                        elevation,
                        accepted_dir / f"{label}_chrono_elevation.csv",
                        tolerance,
                        _allowed_derived_events(
                            candidate_rows,
                            policy_report[sex]["input_parity"]["mode"],
                        ),
                    )
                    elevation_artifact = (
                        staging
                        / "full_elevation_chronos"
                        / policy
                        / f"{label}_chrono_elevation.csv"
                    )
                    elevation_artifact.parent.mkdir(parents=True, exist_ok=True)
                    elevation.write_csv(elevation_artifact)
                    elevation_comparison["candidate"] = _candidate_receipt(
                        elevation_artifact, elevation.height, staging
                    )
                    elevation_report[sex] = elevation_comparison
                    if elevation_comparison["status"] != "passed":
                        report["status"] = "failed"
            policy_report["chronos"] = chrono_report
            policy_report["elevation_chronos"] = elevation_report
            report["policies"][policy] = policy_report

        relay_event_types = {
            race_type
            for _, race_type in relay_candidate_events
            if "Relay" in race_type
        }
        if relay_event_types:
            if aggregator_path is None:
                raise IBUContractError(
                    "relay result candidates require the relay aggregator"
                )
            aggregator = load_aggregator(aggregator_path.expanduser().resolve())
            combined = aggregator.create_combined_df(
                relay_chronos["M"], relay_chronos["L"]
            )
            generated_teams = {
                "men_relay": aggregator.process_relay(relay_chronos["M"], "Men's"),
                "ladies_relay": aggregator.process_relay(relay_chronos["L"], "Ladies'"),
                "mixed_relay": aggregator.process_mixed_relay(combined),
                "single_mixed_relay": aggregator.process_single_mixed_relay(combined),
            }
            team_dir = staging / "full_relay_teams"
            for name, frame in generated_teams.items():
                if frame is None:
                    raise IBUContractError(
                        f"relay aggregator returned no {name} artifact"
                    )
                authorized = {
                    event
                    for event in relay_candidate_events
                    if (
                        (name in {"men_relay", "ladies_relay"} and event[1] == "Relay")
                        or (name == "mixed_relay" and event[1] == "Mixed Relay")
                        or (
                            name == "single_mixed_relay"
                            and event[1] == "Single Mixed Relay"
                        )
                    )
                }
                comparison = compare_team_artifact(
                    frame,
                    relay_dir / TEAM_ARTIFACTS[name],
                    tolerance=tolerance,
                    allowed_additive_events=authorized or None,
                )
                artifact = team_dir / TEAM_ARTIFACTS[name]
                artifact.parent.mkdir(parents=True, exist_ok=True)
                ready = csv_ready(frame)
                ready.write_csv(artifact)
                comparison["candidate"] = _candidate_receipt(
                    artifact, ready.height, staging
                )
                report["relay_team_artifacts"][name] = comparison
                if comparison["status"] != "passed":
                    report["status"] = "failed"

        after = {
            "individual": fingerprint_tree(individual_dir),
            "relay": fingerprint_tree(relay_dir),
        }
        source_after = fingerprint_tree(candidate_dir)
        report["accepted_fingerprints"]["after"] = after
        report["accepted_data_mutated"] = before != after
        report["source_candidate_fingerprints"]["after"] = source_after
        report["source_candidates_mutated"] = source_before != source_after
        if before != after or source_before != source_after:
            report["status"] = "failed"
        if report["status"] == "passed":
            report["ready_for_promotion_bundle"] = True
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
    parser.add_argument(
        "--aggregator",
        dest="aggregator_path",
        type=Path,
        default=here / "relay" / "relay_chrono.py",
    )
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
