#!/usr/bin/env python3
"""Audit official IBU competitions against accepted Biathlon event coverage.

The audit is read-only. It inventories the complete level-one season, compares
schedule and result-event identities in both individual and relay-inclusive
accepted trees, and separates final missing results from live/future races.
It writes only an isolated evidence bundle with publication disabled.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
from collections import Counter
from datetime import datetime
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
)

REPORT_SCHEMA = "nordicnumbers.biathlon-results-gap/v1"
RACE_TYPES = {
    "individual": "Individual",
    "short_individual": "Short Individual",
    "sprint": "Sprint",
    "pursuit": "Pursuit",
    "mass_start": "Mass Start",
    "relay": "Relay",
    "mixed_relay": "Mixed Relay",
    "single_mixed_relay": "Single Mixed Relay",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_csv(path: Path, required: set[str]) -> list[dict[str, str]]:
    if not path.is_file():
        raise IBUContractError(f"accepted Biathlon input does not exist: {path}")
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or not required.issubset(reader.fieldnames):
                raise IBUContractError(
                    f"{path} is missing required columns: "
                    f"{sorted(required - set(reader.fieldnames or []))}"
                )
            return list(reader)
    except (OSError, csv.Error) as error:
        raise IBUContractError(f"invalid accepted Biathlon CSV: {path}") from error


def _schedule_key(row: dict[str, str]) -> tuple[str, str, str]:
    try:
        day = datetime.strptime(row["Date"], "%m/%d/%Y").date().isoformat()
    except ValueError as error:
        raise IBUContractError(
            f"invalid accepted schedule date: {row['Date']!r}"
        ) from error
    return day, row["RaceType"], row["Sex"]


def _result_key(row: dict[str, str]) -> tuple[str, str, str]:
    try:
        day = datetime.fromisoformat(row["Date"]).date().isoformat()
    except ValueError as error:
        raise IBUContractError(
            f"invalid accepted result date: {row['Date']!r}"
        ) from error
    return day, row["RaceType"], row["Sex"]


def _accepted_schedule(path: Path) -> set[tuple[str, str, str]]:
    rows = _read_csv(path, {"Date", "RaceType", "Sex"})
    keys = [_schedule_key(row) for row in rows]
    duplicates = [key for key, count in Counter(keys).items() if count > 1]
    if duplicates:
        raise IBUContractError(
            f"accepted schedule has duplicate event identities: {duplicates[:3]}"
        )
    return set(keys)


def _accepted_results(root: Path) -> set[tuple[str, str, str]]:
    keys: set[tuple[str, str, str]] = set()
    for label, expected_sex in (("men", "M"), ("ladies", "L")):
        rows = _read_csv(
            root / f"{label}_scrape_update.csv",
            {"Date", "RaceType", "Sex", "ID"},
        )
        if any(row["Sex"] != expected_sex for row in rows):
            raise IBUContractError(
                f"accepted {label} history contains a different sex identity"
            )
        keys.update(_result_key(row) for row in rows)
    return keys


def _safe_output(
    output_dir: Path,
    schedule_path: Path,
    individual_dir: Path,
    relay_dir: Path,
) -> Path:
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise IBUContractError(
            f"Biathlon gap-audit output already exists: {output_dir}"
        )
    protected = (
        schedule_path.resolve(),
        individual_dir.resolve(),
        relay_dir.resolve(),
    )
    for root in protected:
        if output_dir == root or root in output_dir.parents:
            raise IBUContractError("gap-audit output must be outside accepted data")
    return output_dir


def audit_gaps(
    *,
    client: IBUClient,
    season_id: str,
    schedule_path: Path,
    individual_dir: Path,
    relay_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    schedule_path = schedule_path.expanduser().resolve()
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    output_dir = _safe_output(output_dir, schedule_path, individual_dir, relay_dir)
    before = {
        "schedule": {"file": str(schedule_path), "sha256": _sha256(schedule_path)},
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }
    accepted_schedule = _accepted_schedule(schedule_path)
    accepted_results = {
        "individual": _accepted_results(individual_dir),
        "relay": _accepted_results(relay_dir),
    }

    raw_events = client.events(season_id, level=1)
    events = validate_events(raw_events, season_id=season_id, level=1)
    raw_competitions: dict[str, list[dict[str, Any]]] = {}
    competitions: list[dict[str, Any]] = []
    for event in events:
        event_id = event["event_id"]
        payload = client.competitions(event_id)
        raw_competitions[event_id] = payload
        competitions.extend(validate_competitions(payload, event_id=event_id))

    coverage: list[dict[str, Any]] = []
    for competition in sorted(
        competitions, key=lambda item: (item["start_time"], item["race_id"])
    ):
        race_type = RACE_TYPES[competition["event_kind"]]
        day = datetime.fromisoformat(competition["start_time"]).date().isoformat()
        sex = competition["sex"]
        schedule_key = (day, race_type, sex)
        result_sexes = ("M", "L") if sex == "Mixed" else (sex,)
        result_keys = {(day, race_type, result_sex) for result_sex in result_sexes}
        individual_present = result_keys.issubset(accepted_results["individual"])
        relay_present = result_keys.issubset(accepted_results["relay"])
        final = is_final_competition(competition)
        results_present = individual_present and relay_present
        coverage.append(
            {
                **competition,
                "date": day,
                "race_type": race_type,
                "schedule_present": schedule_key in accepted_schedule,
                "official_result_final": final,
                "individual_results_present": individual_present,
                "relay_results_present": relay_present,
                "result_state": (
                    "not_final"
                    if not final
                    else "accepted"
                    if results_present
                    else "missing"
                ),
            }
        )

    schedule_missing = [item for item in coverage if not item["schedule_present"]]
    result_missing = [item for item in coverage if item["result_state"] == "missing"]
    result_pending = [item for item in coverage if item["result_state"] == "not_final"]
    after = {
        "schedule": {"file": str(schedule_path), "sha256": _sha256(schedule_path)},
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }
    mutated = before != after
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "status": "failed" if mutated else "passed",
        "season_id": season_id,
        "source": {
            "authority": "International Biathlon Union",
            "event_payload_sha256": _json_sha256(raw_events),
            "competition_payload_sha256": {
                event_id: _json_sha256(payload)
                for event_id, payload in sorted(raw_competitions.items())
            },
            "api_key_persisted": False,
        },
        "summary": {
            "official_events": len(events),
            "official_competitions": len(coverage),
            "schedule_missing": len(schedule_missing),
            "final_results_missing": len(result_missing),
            "results_not_final": len(result_pending),
            "fully_accepted": sum(
                item["schedule_present"] and item["result_state"] == "accepted"
                for item in coverage
            ),
        },
        "coverage": coverage,
        "additive_candidates": {
            "schedule_event_ids": sorted(
                {item["event_id"] for item in schedule_missing}
            ),
            "schedule_race_ids": [item["race_id"] for item in schedule_missing],
            "final_result_race_ids": [item["race_id"] for item in result_missing],
            "pending_result_race_ids": [item["race_id"] for item in result_pending],
        },
        "accepted_fingerprints": {"before": before, "after": after},
        "accepted_data_mutated": mutated,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )
    try:
        raw = staging / "raw"
        raw.mkdir()
        (raw / "events.json").write_text(
            json.dumps(raw_events, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for event_id, payload in sorted(raw_competitions.items()):
            (raw / f"{event_id}-competitions.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
        (staging / "gap_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        staging.replace(output_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return report


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--season-id", required=True)
    command.add_argument(
        "--schedule",
        dest="schedule_path",
        type=Path,
        default=here / "excel365/races.csv",
    )
    command.add_argument("--individual-dir", type=Path, default=here / "excel365")
    command.add_argument("--relay-dir", type=Path, default=here / "relay/excel365")
    command.add_argument("--output-dir", type=Path, required=True)
    return command


def main() -> int:
    args = parser().parse_args()
    try:
        report = audit_gaps(
            client=IBUClient(api_key=discover_public_api_key()),
            **vars(args),
        )
    except IBUContractError as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
