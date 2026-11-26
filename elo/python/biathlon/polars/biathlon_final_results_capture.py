#!/usr/bin/env python3
"""Capture immutable evidence for finalized, missing IBU results.

The input is a passed Biathlon gap audit. Only races classified by that audit
as both missing and Final/OFFICIAL/FINISHED are fetched. Accepted histories and
the schedule are fingerprinted before and after; application and publication
remain disabled. A no-op capture is valid while the season has no final races.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import (
    IBUClient,
    IBUContractError,
    discover_public_api_key,
    is_final_competition,
    validate_competitions,
    validate_events,
    validate_reports,
    validate_results,
)


REPORT_SCHEMA = "nordicnumbers.biathlon-final-results-capture/v1"
GAP_SCHEMA = "nordicnumbers.biathlon-results-gap/v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise IBUContractError(f"invalid final-results input: {path}") from error


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _safe_output(
    output_dir: Path,
    evidence_dir: Path,
    schedule_path: Path,
    individual_dir: Path,
    relay_dir: Path,
) -> Path:
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise IBUContractError(f"final-results output already exists: {output_dir}")
    for protected in (
        evidence_dir.resolve(),
        schedule_path.resolve().parent,
        individual_dir.resolve(),
        relay_dir.resolve(),
    ):
        if output_dir == protected or protected in output_dir.parents:
            raise IBUContractError(
                "final-results output must be outside evidence and accepted data"
            )
    return output_dir


def _accepted_fingerprints(
    schedule_path: Path, individual_dir: Path, relay_dir: Path
) -> dict[str, Any]:
    return {
        "schedule": {"file": str(schedule_path), "sha256": _sha256(schedule_path)},
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }


def capture_final_results(
    *,
    client: IBUClient,
    evidence_dir: Path,
    schedule_path: Path,
    individual_dir: Path,
    relay_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    evidence_dir = evidence_dir.expanduser().resolve()
    schedule_path = schedule_path.expanduser().resolve()
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    output_dir = _safe_output(
        output_dir, evidence_dir, schedule_path, individual_dir, relay_dir
    )
    gap_path = evidence_dir / "gap_report.json"
    gap = _load_json(gap_path)
    if (
        gap.get("schema_version") != GAP_SCHEMA
        or gap.get("status") != "passed"
        or gap.get("accepted_data_mutated") is not False
        or gap.get("application") != {"status": "disabled", "performed": False}
        or gap.get("publication") != {"status": "disabled"}
    ):
        raise IBUContractError("gap report is not an immutable passed audit")

    before = _accepted_fingerprints(schedule_path, individual_dir, relay_dir)
    expected = gap.get("accepted_fingerprints", {})
    if expected.get("before") != expected.get("after") or expected.get("after") != before:
        raise IBUContractError("accepted Biathlon inputs drifted after the gap audit")

    raw_dir = evidence_dir / "raw"
    raw_events = _load_json(raw_dir / "events.json")
    if _json_sha256(raw_events) != gap["source"]["event_payload_sha256"]:
        raise IBUContractError("official event evidence hash mismatch")
    events = validate_events(raw_events, season_id=gap["season_id"], level=1)
    event_ids = {event["event_id"] for event in events}

    competitions: dict[str, dict[str, Any]] = {}
    for event_id in sorted(event_ids):
        payload = _load_json(raw_dir / f"{event_id}-competitions.json")
        expected_hash = gap["source"]["competition_payload_sha256"].get(event_id)
        if not expected_hash or _json_sha256(payload) != expected_hash:
            raise IBUContractError(
                f"official competition evidence hash mismatch: {event_id}"
            )
        for competition in validate_competitions(payload, event_id=event_id):
            competitions[competition["race_id"]] = competition

    coverage = {item["race_id"]: item for item in gap["coverage"]}
    if set(coverage) != set(competitions):
        raise IBUContractError("gap coverage does not match competition evidence")
    selected = list(gap["additive_candidates"]["final_result_race_ids"])
    if len(selected) != len(set(selected)):
        raise IBUContractError("gap report contains duplicate final result candidates")
    for race_id in selected:
        item = coverage.get(race_id)
        competition = competitions.get(race_id)
        if (
            not item
            or not competition
            or item.get("result_state") != "missing"
            or item.get("official_result_final") is not True
            or item.get("schedule_present") is not True
            or not is_final_competition(competition)
        ):
            raise IBUContractError(
                f"race is not an audited finalized additive result: {race_id}"
            )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    captured: list[dict[str, Any]] = []
    try:
        output_raw = staging / "raw"
        for race_id in selected:
            results = client.results(race_id)
            reports = client.reports(race_id)
            summary = validate_results(results, competitions[race_id])
            if not is_final_competition(
                {
                    "status_text": summary["status"],
                    "result_status": coverage[race_id]["result_status"],
                    "schedule_status": coverage[race_id]["schedule_status"],
                }
            ):
                raise IBUContractError(
                    f"fetched result is no longer Final/OFFICIAL/FINISHED: {race_id}"
                )
            start_list = validate_reports(reports, race_id=race_id)
            _atomic_json(output_raw / f"{race_id}-results.json", results)
            _atomic_json(output_raw / f"{race_id}-reports.json", reports)
            captured.append(
                {
                    **summary,
                    "event_id": competitions[race_id]["event_id"],
                    "start_list": start_list,
                    "results_sha256": _json_sha256(results),
                    "reports_sha256": _json_sha256(reports),
                }
            )

        after = _accepted_fingerprints(schedule_path, individual_dir, relay_dir)
        mutated = before != after
        report = {
            "schema_version": REPORT_SCHEMA,
            "status": "failed" if mutated else "passed",
            "season_id": gap["season_id"],
            "source": {
                "authority": "International Biathlon Union",
                "gap_report": str(gap_path),
                "gap_report_sha256": _sha256(gap_path),
                "api_key_persisted": False,
            },
            "summary": {
                "audited_final_missing": len(selected),
                "captured_results": len(captured),
                "waiting_for_first_final": not selected,
            },
            "races": captured,
            "accepted_fingerprints": {"before": before, "after": after},
            "accepted_data_mutated": mutated,
            "application": {"status": "disabled", "performed": False},
            "publication": {"status": "disabled"},
        }
        _atomic_json(staging / "capture_report.json", report)
        staging.replace(output_dir)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--evidence-dir", type=Path, required=True)
    command.add_argument("--schedule", dest="schedule_path", type=Path, default=here / "excel365/races.csv")
    command.add_argument("--individual-dir", type=Path, default=here / "excel365")
    command.add_argument("--relay-dir", type=Path, default=here / "relay/excel365")
    command.add_argument("--output-dir", type=Path, required=True)
    return command


def main() -> int:
    args = parser().parse_args()
    try:
        report = capture_final_results(
            client=IBUClient(api_key=discover_public_api_key()), **vars(args)
        )
    except IBUContractError as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
