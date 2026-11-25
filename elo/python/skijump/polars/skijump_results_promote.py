#!/usr/bin/env python3
"""Build an atomic promotion candidate from a validated ski-jumping replay.

The command re-checks every accepted input hash, validates the replay report
and mixed-team structure, reruns the replay in a temporary directory, and
copies only byte-identical fresh candidates into an isolated atomic output
directory.  Accepted histories are never modified by this command.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
from collections import Counter
from collections.abc import Iterable
from pathlib import Path

from skijump_results_elo_replay import ReplayError, run_replay

REPLAY_SCHEMA = "nordicnumbers.skijump-elo-replay/v1"
PROMOTION_SCHEMA = "nordicnumbers.skijump-results-promotion/v1"
CANDIDATE_FILES = (
    "candidate_men_chrono.csv",
    "candidate_ladies_chrono.csv",
    "candidate_mixed_team_chrono.csv",
)


class PromotionError(RuntimeError):
    """Raised when replay evidence cannot become a promotion candidate."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _row_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _resolved_hashes(evidence: object) -> dict[Path, str]:
    if not isinstance(evidence, dict):
        raise PromotionError("replay report has no input hash evidence")
    resolved: dict[Path, str] = {}
    for raw_path, digest in evidence.items():
        if not isinstance(raw_path, str) or not isinstance(digest, str):
            raise PromotionError("replay input hash evidence is malformed")
        path = Path(raw_path).expanduser().resolve()
        if path in resolved and resolved[path] != digest:
            raise PromotionError(f"conflicting replay hashes for {path}")
        resolved[path] = digest
    return resolved


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_path = source.expanduser().resolve()
        parent = source_path.parent
        if resolved == parent or parent in resolved.parents:
            raise PromotionError(
                f"promotion output {resolved} cannot be inside source directory {parent}"
            )
    return resolved


def _load_report(path: Path, expected_sha256: str) -> dict[str, object]:
    observed = _sha256(path)
    if observed != expected_sha256:
        raise PromotionError(
            f"replay report SHA-256 mismatch: expected {expected_sha256}, observed {observed}"
        )
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PromotionError(f"invalid replay report: {path}") from exc
    if report.get("schema_version") != REPLAY_SCHEMA:
        raise PromotionError("unsupported replay report schema")
    return report


def _require_report_output(
    report: dict[str, object], replay_dir: Path, filename: str
) -> tuple[Path, str]:
    outputs = report.get("outputs")
    if not isinstance(outputs, dict) or set(outputs) != set(CANDIDATE_FILES):
        raise PromotionError("replay report has incomplete candidate output evidence")
    receipt = outputs.get(filename)
    if not isinstance(receipt, dict):
        raise PromotionError(f"invalid replay output receipt for {filename}")
    path = replay_dir / filename
    reported_path = Path(str(receipt.get("path", ""))).expanduser().resolve()
    expected_hash = receipt.get("sha256")
    if reported_path != path.resolve():
        raise PromotionError(f"replay report points {filename} at a different file")
    if not isinstance(expected_hash, str) or _sha256(path) != expected_hash:
        raise PromotionError(f"replay output checksum mismatch: {filename}")
    return path, expected_hash


def _validate_event_classifications(report: dict[str, object]) -> set[str]:
    classifications = report.get("event_classifications")
    if not isinstance(classifications, list) or not classifications:
        raise PromotionError("replay report has no event classifications")
    statuses: Counter[str] = Counter()
    eligible_dates: set[str] = set()
    for item in classifications:
        if not isinstance(item, dict):
            raise PromotionError("malformed replay event classification")
        status = item.get("status")
        statuses[str(status)] += 1
        if status == "eligible":
            if item.get("sex") != "Mixed" or item.get("event_kind") != "team":
                raise PromotionError("replay includes an eligible non-mixed-team event")
            eligible_dates.add(str(item.get("date", "")))
        elif status not in {"already_accepted", "policy_excluded"}:
            raise PromotionError(f"unsupported replay event status: {status!r}")
    if not eligible_dates:
        raise PromotionError("replay contains no eligible mixed-team events")
    if report.get("eligible_mixed_team_events") != len(eligible_dates):
        raise PromotionError("eligible mixed-team count does not match classifications")
    return eligible_dates


def _split(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _validate_mixed_teams(path: Path, eligible_dates: set[str]) -> None:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise PromotionError("mixed-team replay output is empty")
    seen: set[tuple[str, str, str]] = set()
    dates: set[str] = set()
    for row_number, row in enumerate(rows, start=2):
        dates.add(row["Date"])
        key = (row["Date"], row["Nation"], row["Place"])
        if key in seen:
            raise PromotionError(f"duplicate mixed-team row: {key}")
        seen.add(key)
        members = _split(row["MemberIDs"])
        names = _split(row["TeamMembers"])
        sexes = _split(row["MemberSex"])
        if (
            row["Sex"] != "Mixed"
            or int(row["TeamSize"]) != 4
            or len(members) != 4
            or len(set(members)) != 4
            or len(names) != 4
            or Counter(sexes) != Counter({"M": 2, "L": 2})
        ):
            raise PromotionError(f"{path}:{row_number}: invalid four-member mixed team")
    if dates != eligible_dates:
        raise PromotionError(
            f"mixed-team output dates {sorted(dates)} do not match eligible dates "
            f"{sorted(eligible_dates)}"
        )


def _validate_current_inputs(
    report: dict[str, object], required_inputs: Iterable[Path]
) -> None:
    evidence = _resolved_hashes(report.get("input_sha256"))
    for source in required_inputs:
        resolved = source.expanduser().resolve()
        expected = evidence.get(resolved)
        if expected is None:
            raise PromotionError(f"replay report did not validate input {resolved}")
        observed = _sha256(resolved)
        if observed != expected:
            raise PromotionError(
                f"accepted replay input changed: {resolved}; expected {expected}, "
                f"observed {observed}"
            )


def build_promotion_candidate(
    *,
    replay_report: Path,
    expected_report_sha256: str,
    mapped_results: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_raw: Path,
    ladies_raw: Path,
    mixed_team_chrono: Path,
    expected_season: int,
    output_dir: Path,
) -> dict[str, object]:
    replay_report = replay_report.expanduser().resolve()
    replay_dir = replay_report.parent
    required_inputs = (
        mapped_results,
        men_chrono,
        ladies_chrono,
        men_raw,
        ladies_raw,
        mixed_team_chrono,
    )
    output_dir = _validate_output_dir(
        output_dir,
        (men_chrono, ladies_chrono, men_raw, ladies_raw, mixed_team_chrono),
    )
    if output_dir == replay_dir or replay_dir in output_dir.parents:
        raise PromotionError("promotion output cannot be inside the replay directory")
    if output_dir.exists():
        raise PromotionError(f"promotion output already exists: {output_dir}")
    report = _load_report(replay_report, expected_report_sha256)
    if report.get("season") != expected_season:
        raise PromotionError(f"replay report is not season {expected_season}")
    if report.get("accepted_histories_modified") is not False:
        raise PromotionError("replay did not preserve accepted histories")
    if report.get("promotion_performed") is not False:
        raise PromotionError("replay report claims promotion was already performed")
    eligible_dates = _validate_event_classifications(report)
    _validate_current_inputs(report, required_inputs)
    reported_outputs = {
        filename: _require_report_output(report, replay_dir, filename)
        for filename in CANDIDATE_FILES
    }
    _validate_mixed_teams(
        reported_outputs["candidate_mixed_team_chrono.csv"][0], eligible_dates
    )

    original_hashes = {
        path.expanduser().resolve(): _sha256(path.expanduser().resolve())
        for path in required_inputs
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.promotion.", dir=output_dir.parent)
    )
    fresh_dir = Path(tempfile.mkdtemp(prefix="skijump-fresh-replay-"))
    try:
        fresh_report = run_replay(
            mapped_results=mapped_results,
            men_chrono=men_chrono,
            ladies_chrono=ladies_chrono,
            men_raw=men_raw,
            ladies_raw=ladies_raw,
            mixed_team_chrono=mixed_team_chrono,
            output_dir=fresh_dir,
            season=expected_season,
        )
        outputs: dict[str, dict[str, object]] = {}
        for filename, (reported_path, expected_hash) in reported_outputs.items():
            fresh_path = fresh_dir / filename
            fresh_hash = _sha256(fresh_path)
            if fresh_hash != expected_hash:
                raise PromotionError(
                    f"fresh replay checksum differs from reviewed candidate: {filename}"
                )
            destination = staging / filename.removeprefix("candidate_")
            shutil.copyfile(reported_path, destination)
            copied_hash = _sha256(destination)
            if copied_hash != expected_hash:
                raise PromotionError(f"staged candidate checksum mismatch: {filename}")
            outputs[destination.name] = {
                "file": destination.name,
                "sha256": copied_hash,
                "rows": _row_count(destination),
            }
        if {path: _sha256(path) for path in original_hashes} != original_hashes:
            raise PromotionError(
                "accepted replay inputs changed during promotion build"
            )
        promotion_report: dict[str, object] = {
            "schema_version": PROMOTION_SCHEMA,
            "status": "promotion_candidate",
            "season": expected_season,
            "accepted_files_modified": False,
            "atomic_candidate_set": True,
            "source_replay_report": {
                "file": str(replay_report),
                "sha256": expected_report_sha256,
            },
            "eligible_mixed_team_dates": sorted(eligible_dates),
            "fresh_replay": {
                "schema_version": fresh_report["schema_version"],
                "candidate_mixed_team_rows": fresh_report["candidate_mixed_team_rows"],
                "inserted_member_rows": fresh_report["inserted_member_rows"],
            },
            "outputs": outputs,
        }
        (staging / "report.json").write_text(
            json.dumps(promotion_report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        staging.replace(output_dir)
        return promotion_report
    except (OSError, ReplayError) as exc:
        raise PromotionError(str(exc)) from exc
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        shutil.rmtree(fresh_dir, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-report", required=True, type=Path)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--mapped-results", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-raw", required=True, type=Path)
    parser.add_argument("--ladies-raw", required=True, type=Path)
    parser.add_argument("--mixed-team-chrono", required=True, type=Path)
    parser.add_argument("--expected-season", required=True, type=int)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = build_promotion_candidate(**vars(args))
    except PromotionError as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
