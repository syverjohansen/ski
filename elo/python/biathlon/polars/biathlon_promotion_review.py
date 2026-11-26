#!/usr/bin/env python3
"""Build an isolated review bundle from a passed Biathlon shadow runtime.

This is deliberately not an apply command.  It verifies the retained runtime
and current accepted-data fingerprints, inventories every available event
delta, and records which full derived artifacts still need deterministic
materialization before promotion can be enabled.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from biathlon_runtime_shadow import REPORT_SCHEMA as RUNTIME_SCHEMA
from biathlon_runtime_shadow import fingerprint_tree

REVIEW_SCHEMA = "nordicnumbers.biathlon-promotion-review/v1"
STAGES = [
    (
        "capture_official_evidence",
        "evidence/report.json",
        "nordicnumbers.biathlon-ibu-source/v1",
    ),
    (
        "adapt_compatibility_candidates",
        "candidates/parity_report.json",
        "nordicnumbers.biathlon-official-adapter/v1",
    ),
    (
        "replay_athlete_ratings",
        "elo_parity/elo_parity_report.json",
        "nordicnumbers.biathlon-elo-parity/v1",
    ),
    (
        "rebuild_relay_team_artifacts",
        "relay_team_parity/relay_team_parity_report.json",
        "nordicnumbers.biathlon-relay-team-parity/v1",
    ),
]
REQUIRED_CANDIDATES = {
    "candidates/schedule_candidate.csv",
    "candidates/men_results_candidate.csv",
    "candidates/ladies_results_candidate.csv",
    "candidates/identity_map.csv",
    "elo_parity/event_ratings/relay_M_overall.csv",
    "elo_parity/event_ratings/relay_L_overall.csv",
}
COPY_ROOTS = ("evidence", "candidates", "elo_parity", "relay_team_parity")


class ReviewError(RuntimeError):
    """The retained shadow evidence cannot become a review candidate."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path, description: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReviewError(f"invalid {description}: {path}") from error
    if not isinstance(payload, dict):
        raise ReviewError(f"{description} must be a JSON object: {path}")
    return payload


def _csv_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _content_fingerprint(receipt: dict[str, Any]) -> dict[str, Any]:
    return {key: receipt.get(key) for key in ("file_count", "total_bytes", "sha256")}


def _validate_output_location(
    output_dir: Path, runtime_dir: Path, accepted_roots: tuple[Path, Path]
) -> Path:
    resolved = output_dir.expanduser().resolve()
    if resolved.exists():
        raise ReviewError(f"review output already exists: {resolved}")
    protected = (runtime_dir.resolve(), *(root.resolve() for root in accepted_roots))
    for root in protected:
        if resolved == root or root in resolved.parents:
            raise ReviewError(
                f"review output cannot be inside protected directory {root}"
            )
    return resolved


def _validate_runtime(
    runtime_dir: Path,
    expected_report_sha256: str,
    expected_code_commit: str,
    individual_dir: Path,
    relay_dir: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    report_path = runtime_dir / "runtime_report.json"
    observed_report_sha = _sha256(report_path)
    if observed_report_sha != expected_report_sha256:
        raise ReviewError(
            "runtime report SHA-256 mismatch: "
            f"expected {expected_report_sha256}, observed {observed_report_sha}"
        )
    runtime = _load_json(report_path, "runtime report")
    if runtime.get("schema_version") != RUNTIME_SCHEMA:
        raise ReviewError("unsupported runtime report schema")
    if runtime.get("status") != "passed":
        raise ReviewError("runtime did not pass")
    if runtime.get("code_commit") != expected_code_commit:
        raise ReviewError("runtime code commit does not match approved commit")
    if runtime.get("accepted_data_mutated") is not False:
        raise ReviewError("runtime did not preserve accepted data")
    if runtime.get("promotion") != {"status": "disabled"}:
        raise ReviewError("runtime promotion boundary was not disabled")
    if runtime.get("publication") != {"status": "disabled"}:
        raise ReviewError("runtime publication boundary was not disabled")

    outcomes = runtime.get("stages")
    if not isinstance(outcomes, list) or [item.get("name") for item in outcomes] != [
        item[0] for item in STAGES
    ]:
        raise ReviewError("runtime stage sequence is incomplete or unexpected")
    if any(item.get("status") != "passed" for item in outcomes):
        raise ReviewError("not every runtime stage passed")

    fingerprints = runtime.get("accepted_fingerprints")
    if not isinstance(fingerprints, dict) or fingerprints.get(
        "before"
    ) != fingerprints.get("after"):
        raise ReviewError(
            "runtime accepted-data fingerprints are incomplete or changed"
        )
    before = fingerprints.get("before")
    if not isinstance(before, dict):
        raise ReviewError("runtime has no accepted-data fingerprint evidence")
    current = {
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }
    for policy in ("individual", "relay"):
        expected = before.get(policy)
        if not isinstance(expected, dict) or _content_fingerprint(
            expected
        ) != _content_fingerprint(current[policy]):
            raise ReviewError(f"accepted {policy} data drifted after the shadow run")
    return runtime, current


def _validate_stage_reports(
    runtime_dir: Path, runtime: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    for stage, relative, schema in STAGES:
        report = _load_json(runtime_dir / relative, f"{stage} report")
        if report.get("schema_version") != schema or report.get("status") != "passed":
            raise ReviewError(f"{stage} report did not pass the expected schema")
        if report.get("accepted_data_mutated") is not False:
            raise ReviewError(
                f"{stage} report does not prove accepted data was preserved"
            )
        if report.get("publication") != {"status": "disabled"}:
            raise ReviewError(
                f"{stage} report does not preserve the publication boundary"
            )
        reports[stage] = report

    adapter = reports["adapt_compatibility_candidates"]
    for report_name in ("capture_official_evidence", "adapt_compatibility_candidates"):
        report = reports[report_name]
        if report.get("season_id") != runtime.get("season_id") or report.get(
            "event_id"
        ) != runtime.get("event_id"):
            raise ReviewError(f"{report_name} identity does not match the runtime")
    identity = adapter.get("identity")
    if not isinstance(identity, dict) or identity.get("unresolved") != 0:
        raise ReviewError("adapter has unresolved athlete identities")

    candidates = runtime_dir / "candidates"
    counts = {
        "schedule_candidate.csv": adapter.get("schedule_candidate_rows"),
        "men_results_candidate.csv": adapter.get("result_parity", {})
        .get("M", {})
        .get("candidate_rows"),
        "ladies_results_candidate.csv": adapter.get("result_parity", {})
        .get("L", {})
        .get("candidate_rows"),
        "identity_map.csv": identity.get("resolved"),
    }
    for filename, expected in counts.items():
        path = candidates / filename
        if not path.is_file() or _csv_rows(path) != expected:
            raise ReviewError(f"adapter candidate evidence drifted: {filename}")

    source = reports["capture_official_evidence"]
    race_ids: set[str] = set()
    for race in source.get("races", []):
        if not isinstance(race, dict) or not isinstance(race.get("race_id"), str):
            raise ReviewError("source report contains malformed race evidence")
        race_id = race["race_id"]
        if race_id in race_ids:
            raise ReviewError(f"duplicate source race evidence: {race_id}")
        race_ids.add(race_id)
        for suffix, receipt_key in (
            ("results", "results_sha256"),
            ("reports", "reports_sha256"),
        ):
            payload = _load_json(
                runtime_dir / "evidence" / "raw" / f"{race_id}-{suffix}.json",
                f"official {suffix} evidence",
            )
            if _json_sha256(payload) != race.get(receipt_key):
                raise ReviewError(f"official {suffix} evidence drifted for {race_id}")
    if set(runtime.get("race_ids", [])) != race_ids:
        raise ReviewError("source race evidence does not match the runtime selection")
    if not isinstance(source.get("competition_count"), int) or source[
        "competition_count"
    ] < len(race_ids):
        raise ReviewError(
            "source competition count is smaller than selected race evidence"
        )
    return reports


def _required_deltas(runtime_dir: Path) -> list[Path]:
    missing = [
        name
        for name in sorted(REQUIRED_CANDIDATES)
        if not (runtime_dir / name).is_file()
    ]
    if missing:
        raise ReviewError(
            f"runtime is missing required candidate deltas: {', '.join(missing)}"
        )
    event_ratings = sorted((runtime_dir / "elo_parity" / "event_ratings").glob("*.csv"))
    team_candidates = sorted(
        (runtime_dir / "relay_team_parity" / "team_candidates").glob("*.csv")
    )
    if not event_ratings or not team_candidates:
        raise ReviewError(
            "runtime has incomplete rating or relay-team candidate deltas"
        )
    fixed = [runtime_dir / name for name in sorted(REQUIRED_CANDIDATES)]
    return sorted(set(fixed + event_ratings + team_candidates))


def _target_plan(
    runtime: dict[str, Any], delta_paths: list[Path], runtime_dir: Path
) -> list[dict[str, Any]]:
    season = str(runtime["season_id"])
    season_year = f"20{season[2:]}"
    plan: list[dict[str, Any]] = []
    for path in delta_paths:
        relative = path.relative_to(runtime_dir).as_posix()
        name = path.name
        targets: list[str] = []
        operation = "evidence_only"
        if name == "schedule_candidate.csv":
            targets = [f"individual/races{season_year}.csv"]
            operation = "merge_event_rows"
        elif name == "men_results_candidate.csv":
            targets = [
                "individual/men_scrape_update.csv",
                "relay/men_scrape_update.csv",
            ]
            operation = "merge_event_rows"
        elif name == "ladies_results_candidate.csv":
            targets = [
                "individual/ladies_scrape_update.csv",
                "relay/ladies_scrape_update.csv",
            ]
            operation = "merge_event_rows"
        elif path.parent.name == "event_ratings":
            policy, sex, stream = name.removesuffix(".csv").split("_", 2)
            suffixes = {
                "overall": "",
                "individual": "_Individual",
                "sprint": "_Sprint",
                "pursuit": "_Pursuit",
                "mass_start": "_Mass_Start",
            }
            targets = [f"{policy}/{'M' if sex == 'M' else 'L'}{suffixes[stream]}.csv"]
            operation = "merge_event_rows"
        elif path.parent.name == "team_candidates":
            targets = [f"relay/{name.replace('.csv', '_chrono.csv')}"]
            operation = "merge_event_rows"
        plan.append({"artifact": relative, "operation": operation, "targets": targets})
    return plan


def build_review_candidate(
    *,
    runtime_dir: Path,
    expected_report_sha256: str,
    expected_code_commit: str,
    individual_dir: Path,
    relay_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    runtime_dir = runtime_dir.expanduser().resolve()
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    output_dir = _validate_output_location(
        output_dir, runtime_dir, (individual_dir, relay_dir)
    )
    runtime, current = _validate_runtime(
        runtime_dir,
        expected_report_sha256,
        expected_code_commit,
        individual_dir,
        relay_dir,
    )
    _validate_stage_reports(runtime_dir, runtime)
    delta_paths = _required_deltas(runtime_dir)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )
    try:
        bundle = staging / "runtime_evidence"
        for name in COPY_ROOTS:
            shutil.copytree(runtime_dir / name, bundle / name)
        shutil.copy2(
            runtime_dir / "runtime_report.json", bundle / "runtime_report.json"
        )
        artifacts: list[dict[str, Any]] = []
        for path in sorted(item for item in bundle.rglob("*") if item.is_file()):
            receipt: dict[str, Any] = {
                "file": path.relative_to(staging).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            if path.suffix == ".csv":
                receipt["rows"] = _csv_rows(path)
            artifacts.append(receipt)
        report: dict[str, Any] = {
            "schema_version": REVIEW_SCHEMA,
            "status": "promotion_review_candidate",
            "run_id": runtime["run_id"],
            "season_id": runtime["season_id"],
            "event_id": runtime["event_id"],
            "race_ids": runtime["race_ids"],
            "code_commit": runtime["code_commit"],
            "source_runtime": {
                "file": "runtime_evidence/runtime_report.json",
                "sha256": expected_report_sha256,
            },
            "accepted_current": current,
            "artifacts": artifacts,
            "target_plan": _target_plan(runtime, delta_paths, runtime_dir),
            "pending_materialization": [
                "full individual men_chrono.csv and ladies_chrono.csv",
                "full relay men_chrono.csv and ladies_chrono.csv",
                "full relay discipline Elo artifacts",
                "full individual and relay elevation chrono artifacts",
            ],
            "apply_ready": False,
            "accepted_files_modified": False,
            "application": {"status": "disabled", "performed": False},
            "publication": {"status": "disabled"},
        }
        with (staging / "review_report.json").open("w", encoding="utf-8") as handle:
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
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--expected-code-commit", required=True)
    parser.add_argument("--individual-dir", type=Path, default=here / "excel365")
    parser.add_argument("--relay-dir", type=Path, default=here / "relay" / "excel365")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = build_review_candidate(**vars(args))
    except ReviewError as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
