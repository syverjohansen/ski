#!/usr/bin/env python3
"""Build a review-only Biathlon schedule candidate from a gap-audit bundle."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import tempfile
from collections import Counter
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from ibu_source_contract import (
    IBUContractError,
    validate_competitions,
    validate_events,
)

REPORT_SCHEMA = "nordicnumbers.biathlon-schedule-candidate/v1"
RACE_FIELDS = [
    "Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
    "Race",
]
WEEKEND_FIELDS = [
    "Date",
    "Race_Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
]
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
CHAMPIONSHIP_CLASSIFICATIONS = {"BTSWRLCH", "BTSWRLOG", "BTSOWG"}
SUPPORTED_CLASSIFICATIONS = {"BTSWRLCP", *CHAMPIONSHIP_CLASSIFICATIONS}


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
        raise IBUContractError(f"invalid schedule-candidate input: {path}") from error


def _read_schedule(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise IBUContractError(f"accepted schedule does not exist: {path}")
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            required = {"Date", "RaceType", "Sex"}
            if not reader.fieldnames or not required.issubset(reader.fieldnames):
                raise IBUContractError("accepted schedule is missing identity columns")
            return list(reader)
    except (OSError, csv.Error) as error:
        raise IBUContractError(f"invalid accepted schedule: {path}") from error


def _schedule_identity(row: dict[str, str]) -> tuple[str, str, str]:
    try:
        day = datetime.strptime(row["Date"], "%m/%d/%Y").date().isoformat()
    except ValueError as error:
        raise IBUContractError(
            f"invalid accepted schedule date: {row['Date']!r}"
        ) from error
    return day, row["RaceType"], row["Sex"]


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _event_metadata(raw: dict[str, Any], normalized: dict[str, Any]) -> dict[str, Any]:
    classification = normalized["classification"]
    if classification not in SUPPORTED_CLASSIFICATIONS:
        raise IBUContractError(
            f"unsupported level-one event classification: {classification}"
        )
    city = str(raw.get("Organizer") or raw.get("ShortDescription") or "").strip()
    country = str(raw.get("NatLong") or "").strip()
    elevation = str(raw.get("Altitude") or "").strip()
    if not city or not country or not re.fullmatch(r"\d+(?:\.\d+)?", elevation):
        raise IBUContractError(
            f"incomplete official event metadata: {normalized['event_id']}"
        )
    return {
        "event_id": normalized["event_id"],
        "city": city,
        "country": country,
        "elevation": elevation,
        "championship": 1 if classification in CHAMPIONSHIP_CLASSIFICATIONS else 0,
    }


def _distance(raw: dict[str, Any], kind: str) -> str:
    if kind in {"relay", "mixed_relay", "single_mixed_relay"}:
        return "N/A"
    value = str(raw.get("km") or "").strip()
    if not re.fullmatch(r"\d+(?:\.\d+)?", value):
        raise IBUContractError(f"invalid official race distance: {value!r}")
    return value


def _balanced_periods(rows: list[dict[str, Any]]) -> None:
    championship_days = [row["day"] for row in rows if row["championship"] == 1]
    championship_end = max(championship_days) if championship_days else None

    for sex in ("M", "L"):
        sex_rows = [row for row in rows if row["sex"] == sex]
        pre = []
        for row in sex_rows:
            if row["championship"] == 1:
                row["period"] = 4
            elif championship_end and row["day"] > championship_end:
                row["period"] = 5
            else:
                pre.append(row)
        pre.sort(key=lambda row: (row["day"], row["race_id"]))
        quotient, remainder = divmod(len(pre), 3)
        sizes = [quotient + (index < remainder) for index in range(3)]
        cursor = 0
        for period, size in enumerate(sizes, start=1):
            for row in pre[cursor : cursor + size]:
                row["period"] = period
            cursor += size

    men = [row for row in rows if row["sex"] == "M"]
    if any(row["sex"] == "Mixed" for row in rows) and not men:
        raise IBUContractError("cannot assign mixed-event periods without men's races")
    for row in (item for item in rows if item["sex"] == "Mixed"):
        if row["championship"] == 1:
            row["period"] = 4
        elif championship_end and row["day"] > championship_end:
            row["period"] = 5
        else:
            nearest = min(
                men,
                key=lambda item: (
                    abs((item["day"] - row["day"]).days),
                    item["day"],
                    item["race_id"],
                ),
            )
            row["period"] = nearest["period"]


def _safe_output(output_dir: Path, evidence_dir: Path, schedule_path: Path) -> Path:
    output_dir = output_dir.expanduser().resolve()
    evidence_dir = evidence_dir.expanduser().resolve()
    schedule_root = schedule_path.expanduser().resolve().parent
    if output_dir.exists():
        raise IBUContractError(
            f"schedule-candidate output already exists: {output_dir}"
        )
    for protected in (evidence_dir, schedule_root):
        if output_dir == protected or protected in output_dir.parents:
            raise IBUContractError(
                "schedule-candidate output must be outside evidence and accepted data"
            )
    return output_dir


def build_schedule_candidate(
    *,
    evidence_dir: Path,
    schedule_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    evidence_dir = evidence_dir.expanduser().resolve()
    schedule_path = schedule_path.expanduser().resolve()
    output_dir = _safe_output(output_dir, evidence_dir, schedule_path)
    gap_report_path = evidence_dir / "gap_report.json"
    report = _load_json(gap_report_path)
    if (
        report.get("schema_version") != "nordicnumbers.biathlon-results-gap/v1"
        or report.get("status") != "passed"
        or report.get("accepted_data_mutated") is not False
        or report.get("application") != {"status": "disabled", "performed": False}
        or report.get("publication") != {"status": "disabled"}
    ):
        raise IBUContractError("gap report is not an immutable passed audit")

    accepted_hash = _sha256(schedule_path)
    expected_schedule = report["accepted_fingerprints"]["after"]["schedule"]
    if (
        expected_schedule.get("sha256") != accepted_hash
        or report["accepted_fingerprints"]["before"]["schedule"] != expected_schedule
    ):
        raise IBUContractError("accepted schedule drifted after the gap audit")

    raw_dir = evidence_dir / "raw"
    raw_events = _load_json(raw_dir / "events.json")
    if _json_sha256(raw_events) != report["source"]["event_payload_sha256"]:
        raise IBUContractError("official event evidence hash mismatch")
    events = validate_events(raw_events, season_id=report["season_id"], level=1)
    raw_events_by_id = {row["EventId"]: row for row in raw_events}
    metadata = {
        event["event_id"]: _event_metadata(raw_events_by_id[event["event_id"]], event)
        for event in events
    }

    rows: list[dict[str, Any]] = []
    raw_by_race: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = event["event_id"]
        path = raw_dir / f"{event_id}-competitions.json"
        payload = _load_json(path)
        expected_hash = report["source"]["competition_payload_sha256"].get(event_id)
        if not expected_hash or _json_sha256(payload) != expected_hash:
            raise IBUContractError(
                f"official competition evidence hash mismatch: {event_id}"
            )
        raw_by_id = {item["RaceId"]: item for item in payload}
        for competition in validate_competitions(payload, event_id=event_id):
            raw_by_race[competition["race_id"]] = raw_by_id[competition["race_id"]]
            rows.append(
                {
                    **competition,
                    **metadata[event_id],
                    "day": datetime.fromisoformat(competition["start_time"]).date(),
                }
            )

    rows.sort(key=lambda row: (row["day"], row["start_time"], row["race_id"]))
    if len({row["race_id"] for row in rows}) != len(rows):
        raise IBUContractError("duplicate RaceId across official events")
    coverage = {item["race_id"]: item for item in report["coverage"]}
    if set(coverage) != {row["race_id"] for row in rows}:
        raise IBUContractError(
            "gap coverage does not match official competition evidence"
        )

    accepted_rows = _read_schedule(schedule_path)
    accepted_identities = [_schedule_identity(row) for row in accepted_rows]
    duplicates = [
        identity
        for identity, count in Counter(accepted_identities).items()
        if count > 1
    ]
    if duplicates:
        raise IBUContractError(
            f"accepted schedule has duplicate identities: {duplicates[:3]}"
        )
    accepted = set(accepted_identities)

    race_counters = Counter()
    for row in rows:
        race_counters[row["sex"]] += 1
        row["race"] = race_counters[row["sex"]]
        row["race_type"] = RACE_TYPES[row["event_kind"]]
        row["distance"] = _distance(raw_by_race[row["race_id"]], row["event_kind"])
        identity = (row["day"].isoformat(), row["race_type"], row["sex"])
        schedule_present = identity in accepted
        audited = coverage[row["race_id"]]
        if (
            audited["date"] != identity[0]
            or audited["race_type"] != identity[1]
            or audited["sex"] != identity[2]
            or audited["schedule_present"] != schedule_present
        ):
            raise IBUContractError(f"gap coverage mismatch for {row['race_id']}")
        row["schedule_present"] = schedule_present

    audited_missing = set(report["additive_candidates"]["schedule_race_ids"])
    actual_missing = {row["race_id"] for row in rows if not row["schedule_present"]}
    if audited_missing != actual_missing or not actual_missing:
        raise IBUContractError("audited schedule gap no longer matches accepted data")

    _balanced_periods(rows)
    first_day = {
        event_id: min(row["day"] for row in rows if row["event_id"] == event_id)
        for event_id in metadata
    }
    missing = [row for row in rows if not row["schedule_present"]]
    races = []
    weekends = []
    for row in missing:
        common = {
            "Startlist": f"https://www.biathlonworld.com/startlist/{row['race_id']}",
            "Sex": row["sex"],
            "City": row["city"],
            "Country": row["country"],
            "Distance": row["distance"],
            "RaceType": row["race_type"],
            "Period": row["period"],
            "Elevation": row["elevation"],
            "Championship": row["championship"],
        }
        races.append(
            {"Date": row["day"].strftime("%m/%d/%Y"), **common, "Race": row["race"]}
        )
        weekends.append(
            {
                "Date": first_day[row["event_id"]].strftime("%m/%d/%Y"),
                "Race_Date": row["day"].strftime("%m/%d/%Y"),
                **common,
            }
        )

    candidate_report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "season_id": report["season_id"],
        "source": {
            "gap_report": str(gap_report_path),
            "gap_report_sha256": _sha256(gap_report_path),
            "official_event_payload_sha256": report["source"]["event_payload_sha256"],
            "official_competition_payload_sha256": report["source"][
                "competition_payload_sha256"
            ],
            "api_key_persisted": False,
        },
        "accepted_schedule": {
            "file": str(schedule_path),
            "sha256_before": accepted_hash,
            "sha256_after": _sha256(schedule_path),
            "mutated": False,
        },
        "summary": {
            "official_events": len(events),
            "official_competitions": len(rows),
            "accepted_competitions": len(rows) - len(missing),
            "candidate_competitions": len(missing),
            "candidate_by_sex": dict(
                sorted(Counter(row["sex"] for row in missing).items())
            ),
            "candidate_by_period": {
                str(key): value
                for key, value in sorted(
                    Counter(row["period"] for row in missing).items()
                )
            },
        },
        "candidate_race_ids": [row["race_id"] for row in missing],
        "candidate_files": {},
        "ready_for_review": True,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    if candidate_report["accepted_schedule"]["sha256_after"] != accepted_hash:
        raise IBUContractError("accepted schedule mutated while building candidate")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )
    try:
        year = 2000 + int(report["season_id"][2:])
        races_name = f"races{year}_candidate.csv"
        weekends_name = "weekends_candidate.csv"
        _write_csv(staging / races_name, races, RACE_FIELDS)
        _write_csv(staging / weekends_name, weekends, WEEKEND_FIELDS)
        candidate_report["candidate_files"] = {
            races_name: {
                "rows": len(races),
                "sha256": _sha256(staging / races_name),
            },
            weekends_name: {
                "rows": len(weekends),
                "sha256": _sha256(staging / weekends_name),
            },
        }
        (staging / "candidate_report.json").write_text(
            json.dumps(candidate_report, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        staging.replace(output_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return candidate_report


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--evidence-dir", type=Path, required=True)
    command.add_argument(
        "--schedule",
        dest="schedule_path",
        type=Path,
        default=here / "excel365/races.csv",
    )
    command.add_argument("--output-dir", type=Path, required=True)
    return command


def main() -> int:
    try:
        report = build_schedule_candidate(**vars(parser().parse_args()))
    except (IBUContractError, KeyError, TypeError, ValueError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
