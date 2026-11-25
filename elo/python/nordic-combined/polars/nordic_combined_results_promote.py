#!/usr/bin/env python3
"""Build an isolated, atomic Nordic-combined results promotion candidate."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

from nordic_combined_results_replay import ReplayError, mapped_event, run_replay

REPLAY_SCHEMA = "nordicnumbers.nordic-combined-results-replay/v1"
PROMOTION_SCHEMA = "nordicnumbers.nordic-combined-results-promotion/v1"
REPLAY_FILES = {
    "candidate_men_chrono.csv": "men_chrono.csv",
    "candidate_ladies_chrono.csv": "ladies_chrono.csv",
    "candidate_team_sprint_chrono.csv": "team_sprint_chrono.csv",
}


class PromotionError(RuntimeError):
    """Raised when replay evidence cannot become a safe promotion candidate."""


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _load_report(path: Path, expected_sha256: str) -> dict[str, object]:
    observed = _sha(path)
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
    if (
        report.get("accepted_inputs_modified") is not False
        or report.get("promotion") != {"status": "candidate", "performed": False}
    ):
        raise PromotionError("replay report is not an isolated candidate")
    return report


def _validate_location(output_dir: Path, sources: list[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    if resolved.exists():
        raise PromotionError(f"promotion output already exists: {resolved}")
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise PromotionError(
                f"promotion output {resolved} cannot be inside source directory {parent}"
            )
    return resolved


def _validate_inputs(report: dict[str, object], sources: list[Path]) -> dict[str, str]:
    evidence = report.get("inputs")
    if not isinstance(evidence, dict):
        raise PromotionError("replay report has no input hashes")
    observed: dict[str, str] = {}
    for source in sources:
        path = str(source.expanduser().resolve())
        expected = evidence.get(path)
        if not isinstance(expected, str):
            raise PromotionError(f"replay report did not validate input {path}")
        digest = _sha(source)
        if digest != expected:
            raise PromotionError(
                f"accepted replay input changed: {path}; expected {expected}, observed {digest}"
            )
        observed[path] = digest
    return observed


def _validate_replay_outputs(report: dict[str, object], replay_dir: Path) -> None:
    evidence = report.get("outputs")
    if not isinstance(evidence, dict) or set(evidence) != set(REPLAY_FILES):
        raise PromotionError("replay report has incomplete output evidence")
    for filename in REPLAY_FILES:
        receipt = evidence[filename]
        path = replay_dir / filename
        if not isinstance(receipt, dict):
            raise PromotionError(f"malformed replay receipt for {filename}")
        if Path(str(receipt.get("path", ""))).resolve() != path.resolve():
            raise PromotionError(f"replay receipt points {filename} at another path")
        if receipt.get("rows") != _rows(path) or receipt.get("sha256") != _sha(path):
            raise PromotionError(f"replay output drift: {filename}")


def _validate_mapped(mapped_results: Path, season: int) -> tuple[int, int, dict[str, str]]:
    rows = _read(mapped_results)
    try:
        event = mapped_event(rows, season)
    except ReplayError as exc:
        raise PromotionError(str(exc)) from exc
    teams: dict[tuple[str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    athletes: set[tuple[str, str]] = set()
    for number, row in enumerate(rows, 2):
        if not row.get("canonical_athlete_id"):
            raise PromotionError(f"{mapped_results}:{number}: unsupported mapped result")
        athlete = (row["canonical_sex"], row["canonical_athlete_id"])
        if athlete in athletes:
            raise PromotionError(f"duplicate mapped athlete {athlete}")
        athletes.add(athlete)
        if event["event_kind"] == "team_sprint":
            key = (row["date"], row["team_name"], row["team_nation"], row["place"])
            teams[key].append(row)
    if event["event_kind"] == "team_sprint":
        for key, members in teams.items():
            if len(members) != 2 or Counter(row["canonical_sex"] for row in members) != Counter({"M": 1, "L": 1}):
                raise PromotionError(f"mixed team {key} is not exactly one man and one woman")
    return len(rows), len(teams), event


def _validate_candidates(
    candidate_dir: Path, mapped_results: Path, season: int, event: dict[str, str]
) -> None:
    mapped = _read(mapped_results)
    expected = {
        sex: {(row["date"], row["canonical_athlete_id"]) for row in mapped if row["canonical_sex"] == sex}
        for sex in ("M", "L")
    }
    for sex, filename in (("M", "men_chrono.csv"), ("L", "ladies_chrono.csv")):
        rows = _read(candidate_dir / filename)
        inserted = {
            (row["Date"], row["ID"])
            for row in rows
            if row["Season"] == str(season)
            and (row["Date"], row["ID"]) in expected[sex]
        }
        if inserted != expected[sex]:
            raise PromotionError(f"{filename} does not contain every mapped {sex} member")
    if event["event_kind"] == "individual":
        return
    team_rows = _read(candidate_dir / "team_sprint_chrono.csv")
    expected_teams = {
        (row["date"], row["canonical_nation"], row["place"]) for row in mapped
    }
    found: set[tuple[str, str, str]] = set()
    for number, row in enumerate(team_rows, 2):
        key = (row["Date"], row["Nation"], row["Place"])
        if row["Sex"] != "Mixed" or key not in expected_teams:
            continue
        members = [value for value in row["MemberIDs"].split(",") if value]
        if row["TeamSize"] != "2" or len(members) != 2 or len(set(members)) != 2:
            raise PromotionError(f"team_sprint_chrono.csv:{number}: invalid mixed team")
        if key in found:
            raise PromotionError(f"duplicate mixed team {key}")
        found.add(key)
    if found != expected_teams:
        raise PromotionError("team-sprint candidate does not contain every mapped team")


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def build_promotion_candidate(
    *, replay_report: Path, expected_report_sha256: str, mapped_results: Path,
    men_history: Path, ladies_history: Path, team_sprint_history: Path,
    expected_season: int, output_dir: Path,
) -> dict[str, object]:
    replay_report = replay_report.expanduser().resolve()
    replay_dir = replay_report.parent
    sources = [mapped_results, men_history, ladies_history, team_sprint_history]
    output_dir = _validate_location(output_dir, sources)
    report = _load_report(replay_report, expected_report_sha256)
    if report.get("season") != expected_season:
        raise PromotionError("replay season does not match approved candidate season")
    before = _validate_inputs(report, sources)
    _validate_replay_outputs(report, replay_dir)
    members, teams, event = _validate_mapped(mapped_results, expected_season)
    if report.get("inserted_member_rows") != members or report.get("inserted_team_rows") != teams:
        raise PromotionError("replay insertion counts do not match mapped evidence")
    if report.get("event") != event:
        raise PromotionError("replay event identity does not match mapped evidence")
    row_deltas = report.get("row_deltas")
    if not isinstance(row_deltas, dict) or set(row_deltas) != set(REPLAY_FILES.values()):
        raise PromotionError("replay report has incomplete row deltas")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        fresh = staging / "fresh-replay"
        try:
            fresh_report = run_replay(
                mapped_results=mapped_results, men_history=men_history,
                ladies_history=ladies_history, team_sprint_history=team_sprint_history,
                season=expected_season, output_dir=fresh,
            )
        except ReplayError as exc:
            raise PromotionError(f"fresh replay failed: {exc}") from exc
        if fresh_report["inserted_member_rows"] != members or fresh_report["inserted_team_rows"] != teams:
            raise PromotionError("fresh replay insertion counts changed")
        for replay_name, accepted_name in REPLAY_FILES.items():
            original = replay_dir / replay_name
            regenerated = fresh / replay_name
            if _sha(original) != _sha(regenerated):
                raise PromotionError(f"fresh replay is not byte-identical: {replay_name}")
            shutil.copyfile(regenerated, staging / accepted_name)
        if fresh_report.get("event") != event:
            raise PromotionError("fresh replay event identity changed")
        if fresh_report.get("row_deltas") != row_deltas:
            raise PromotionError("fresh replay row deltas changed")
        _validate_candidates(staging, mapped_results, expected_season, event)
        if before != _validate_inputs(report, sources):
            raise PromotionError("accepted inputs changed while building promotion candidate")
        promotion: dict[str, object] = {
            "schema_version": PROMOTION_SCHEMA,
            "status": "promotion_candidate",
            "season": expected_season,
            "event": event,
            "row_deltas": row_deltas,
            "atomic_candidate_set": True,
            "accepted_files_modified": False,
            "application_performed": False,
            "replay_report": {"file": str(replay_report), "sha256": expected_report_sha256},
            "mapped_member_rows": members,
            "mapped_team_rows": teams,
            "accepted_before": {
                Path(path).name: {"file": path, "sha256": digest}
                for path, digest in before.items() if Path(path) != mapped_results.resolve()
            },
            "outputs": {
                name: {"file": name, "rows": _rows(staging / name), "sha256": _sha(staging / name)}
                for name in REPLAY_FILES.values()
            },
        }
        shutil.rmtree(fresh)
        _atomic_json(staging / "report.json", promotion)
        staging.replace(output_dir)
        return promotion
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-report", required=True, type=Path)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--mapped-results", required=True, type=Path)
    parser.add_argument("--men-history", required=True, type=Path)
    parser.add_argument("--ladies-history", required=True, type=Path)
    parser.add_argument("--team-sprint-history", required=True, type=Path)
    parser.add_argument("--expected-season", required=True, type=int)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = build_promotion_candidate(**vars(args))
    except PromotionError as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
