#!/usr/bin/env python3
"""Run the proven official-IBU Biathlon path as an isolated shadow.

The runtime captures official evidence, adapts it to compatibility candidates,
replays athlete ratings, and rebuilds relay-team artifacts in one fail-closed
sequence. It fingerprints accepted inputs before and after the run, writes only
under a unique output directory, and has no promotion or publication path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from biathlon_elo_parity import run_parity as run_elo_parity
from biathlon_official_adapter import adapt_evidence
from biathlon_relay_team_parity import run_parity as run_relay_team_parity
from ibu_source_contract import (
    IBUClient,
    IBUContractError,
    capture_contract,
    discover_public_api_key,
)


REPORT_SCHEMA = "nordicnumbers.biathlon-runtime-shadow/v1"
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
COMMIT = re.compile(r"[0-9a-f]{40}")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint_tree(path: Path) -> dict[str, Any]:
    """Return a stable content fingerprint for every accepted file."""
    path = path.resolve()
    if not path.is_dir():
        raise IBUContractError(f"accepted data directory does not exist: {path}")
    digest = hashlib.sha256()
    file_count = 0
    total_bytes = 0
    for candidate in sorted(item for item in path.rglob("*") if item.is_file()):
        relative = candidate.relative_to(path).as_posix()
        file_digest = _sha256_file(candidate)
        size = candidate.stat().st_size
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_digest.encode("ascii"))
        digest.update(b"\0")
        file_count += 1
        total_bytes += size
    return {
        "path": str(path),
        "file_count": file_count,
        "total_bytes": total_bytes,
        "sha256": digest.hexdigest(),
    }


def _resolve_commit(value: str | None, repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    checked_out = result.stdout.strip()
    if not COMMIT.fullmatch(checked_out):
        raise IBUContractError(f"invalid checked-out code commit: {checked_out!r}")
    if value is not None and value != checked_out:
        raise IBUContractError(
            f"declared code commit {value!r} does not match checkout {checked_out!r}"
        )
    return checked_out


def _safe_run_dir(
    output_root: Path,
    run_id: str,
    accepted_roots: tuple[Path, ...],
) -> Path:
    if not RUN_ID.fullmatch(run_id):
        raise IBUContractError(f"invalid run_id: {run_id!r}")
    run_dir = output_root.expanduser().resolve() / run_id
    for accepted in accepted_roots:
        accepted = accepted.resolve()
        if run_dir == accepted or accepted in run_dir.parents:
            raise IBUContractError("shadow output must be outside accepted data directories")
    if run_dir.exists():
        raise IBUContractError(f"shadow run directory already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    return run_dir


def _stage(
    name: str,
    report_path: Path,
    operation: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    started_at = _now()
    try:
        report = operation()
        status = str(report.get("status", "failed"))
        return {
            "name": name,
            "status": status,
            "started_at": started_at,
            "completed_at": _now(),
            "report": str(report_path),
        }
    except Exception as error:
        return {
            "name": name,
            "status": "failed",
            "started_at": started_at,
            "completed_at": _now(),
            "report": str(report_path),
            "error": {"type": type(error).__name__, "message": str(error)},
        }


def run_shadow(
    *,
    season_id: str,
    event_id: str,
    race_ids: list[str],
    output_root: Path,
    run_id: str,
    individual_dir: Path,
    relay_dir: Path,
    aliases_path: Path,
    engine_path: Path,
    aggregator_path: Path,
    repo_root: Path,
    code_commit: str | None = None,
    tolerance: float = 1e-9,
    client: IBUClient | None = None,
) -> dict[str, Any]:
    individual_dir = individual_dir.resolve()
    relay_dir = relay_dir.resolve()
    accepted_roots = (individual_dir, relay_dir)
    code_commit = _resolve_commit(code_commit, repo_root.resolve())
    run_dir = _safe_run_dir(output_root, run_id, accepted_roots)
    report_path = run_dir / "runtime_report.json"
    before = {
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }
    runtime: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "status": "running",
        "run_id": run_id,
        "season_id": season_id,
        "event_id": event_id,
        "race_ids": list(race_ids),
        "code_commit": code_commit,
        "started_at": _now(),
        "completed_at": None,
        "stages": [],
        "accepted_fingerprints": {"before": before, "after": None},
        "accepted_data_mutated": False,
        "promotion": {"status": "disabled"},
        "publication": {"status": "disabled"},
    }
    _atomic_json(report_path, runtime)

    evidence_dir = run_dir / "evidence"
    candidate_dir = run_dir / "candidates"
    elo_dir = run_dir / "elo_parity"
    relay_team_dir = run_dir / "relay_team_parity"
    if client is None:
        client = IBUClient(api_key=discover_public_api_key())

    operations: list[tuple[str, Path, Callable[[], dict[str, Any]]]] = [
        (
            "capture_official_evidence",
            evidence_dir / "report.json",
            lambda: capture_contract(
                client=client,
                season_id=season_id,
                event_id=event_id,
                race_ids=race_ids,
                output_dir=evidence_dir,
            ),
        ),
        (
            "adapt_compatibility_candidates",
            candidate_dir / "parity_report.json",
            lambda: adapt_evidence(
                evidence_dir=evidence_dir,
                accepted_dir=individual_dir,
                output_dir=candidate_dir,
                aliases_path=aliases_path,
            ),
        ),
        (
            "replay_athlete_ratings",
            elo_dir / "elo_parity_report.json",
            lambda: run_elo_parity(
                candidate_dir=candidate_dir,
                individual_dir=individual_dir,
                relay_dir=relay_dir,
                output_dir=elo_dir,
                engine_path=engine_path,
                tolerance=tolerance,
            ),
        ),
        (
            "rebuild_relay_team_artifacts",
            relay_team_dir / "relay_team_parity_report.json",
            lambda: run_relay_team_parity(
                event_ratings_dir=elo_dir / "event_ratings",
                accepted_dir=relay_dir,
                output_dir=relay_team_dir,
                aggregator_path=aggregator_path,
                tolerance=tolerance,
            ),
        ),
    ]

    try:
        for name, stage_report_path, operation in operations:
            outcome = _stage(name, stage_report_path, operation)
            runtime["stages"].append(outcome)
            _atomic_json(report_path, runtime)
            if outcome["status"] != "passed":
                raise IBUContractError(f"shadow stage failed: {name}")
        runtime["status"] = "passed"
    except Exception as error:  # final evidence must survive any stage failure
        runtime["status"] = "failed"
        runtime["error"] = {
            "type": type(error).__name__,
            "message": str(error),
        }
    finally:
        after = {
            "individual": fingerprint_tree(individual_dir),
            "relay": fingerprint_tree(relay_dir),
        }
        mutated = before != after
        runtime["accepted_fingerprints"]["after"] = after
        runtime["accepted_data_mutated"] = mutated
        if mutated:
            runtime["status"] = "failed"
            runtime["error"] = {
                "type": "AcceptedDataMutation",
                "message": "accepted Biathlon data changed during shadow run",
            }
        runtime["completed_at"] = _now()
        _atomic_json(report_path, runtime)
    return runtime


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--season-id", required=True)
    command.add_argument("--event-id", required=True)
    command.add_argument("--race-id", action="append", default=[])
    command.add_argument("--output-root", type=Path, required=True)
    command.add_argument("--run-id", required=True)
    command.add_argument("--code-commit")
    command.add_argument("--individual-dir", type=Path, default=here / "excel365")
    command.add_argument("--relay-dir", type=Path, default=here / "relay" / "excel365")
    command.add_argument("--aliases", type=Path, default=here / "ibu_identity_aliases.csv")
    command.add_argument("--engine", type=Path, default=here / "elo.py")
    command.add_argument(
        "--aggregator", type=Path, default=here / "relay" / "relay_chrono.py"
    )
    command.add_argument("--repo-root", type=Path, default=here.parents[3])
    command.add_argument("--tolerance", type=float, default=1e-9)
    return command


def main() -> int:
    args = parser().parse_args()
    report = run_shadow(
        season_id=args.season_id,
        event_id=args.event_id,
        race_ids=args.race_id,
        output_root=args.output_root,
        run_id=args.run_id,
        individual_dir=args.individual_dir,
        relay_dir=args.relay_dir,
        aliases_path=args.aliases,
        engine_path=args.engine,
        aggregator_path=args.aggregator,
        repo_root=args.repo_root,
        code_commit=args.code_commit,
        tolerance=args.tolerance,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
