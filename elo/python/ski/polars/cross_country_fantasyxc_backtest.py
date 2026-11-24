#!/usr/bin/env python3
"""Score pre-startlist FantasyXC candidates against official FIS startlists."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from cross_country_fantasyxc_resolve import CANDIDATES_SCHEMA, RESOLVER_VERSION
from cross_country_weekend_inputs import STARTLIST_FIELDS

REPORT_SCHEMA = "nordicnumbers.cross-country-announced-roster-backtest/v1"
CASE_FIELDS = (
    "candidate_id",
    "sex",
    "race_id",
    "canonical_athlete_id",
    "canonical_name",
    "suggested_status",
    "official_started",
    "correct",
)


class FantasyXCBacktestError(RuntimeError):
    """Candidates or official truth cannot be evaluated safely."""


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _read_candidates(path: Path, logical_date: date) -> tuple[dict[str, Any], bytes]:
    try:
        content = path.read_bytes()
        document = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FantasyXCBacktestError(f"cannot read candidate artifact: {exc}") from exc
    if not isinstance(document, dict) or document.get("schema_version") != CANDIDATES_SCHEMA:
        raise FantasyXCBacktestError("unsupported candidate schema")
    if document.get("resolver_version") != RESOLVER_VERSION:
        raise FantasyXCBacktestError("unsupported candidate resolver version")
    if document.get("logical_date") != logical_date.isoformat():
        raise FantasyXCBacktestError("candidate logical date does not match")
    if document.get("status") != "candidate_review":
        raise FantasyXCBacktestError("candidate artifact must remain candidate_review")
    if document.get("publication") != {"status": "disabled"}:
        raise FantasyXCBacktestError("candidate publication must remain disabled")
    rows = document.get("candidates")
    if not isinstance(rows, list) or not rows:
        raise FantasyXCBacktestError("candidate artifact contains no candidates")
    exceptions = document.get("review_exceptions")
    if not isinstance(exceptions, list):
        raise FantasyXCBacktestError("candidate review_exceptions must be a list")
    candidate_ids = [
        str(row.get("candidate_id", "")) for row in rows if isinstance(row, dict)
    ]
    if len(candidate_ids) != len(rows) or len(candidate_ids) != len(set(candidate_ids)):
        raise FantasyXCBacktestError("candidate IDs must be present and unique")
    return document, content


def _read_official(
    paths: tuple[Path, ...], logical_date: date
) -> tuple[set[tuple[str, str, str]], dict[tuple[str, str], set[str]], dict[str, str]]:
    official: set[tuple[str, str, str]] = set()
    events: dict[tuple[str, str], set[str]] = {}
    checksums: dict[str, str] = {}
    for path in paths:
        try:
            content = path.read_bytes()
            reader = csv.DictReader(content.decode("utf-8-sig").splitlines())
            if tuple(reader.fieldnames or ()) != STARTLIST_FIELDS:
                raise FantasyXCBacktestError(
                    f"unexpected official startlist columns: {path}"
                )
            rows = list(reader)
        except (OSError, UnicodeDecodeError, csv.Error) as exc:
            raise FantasyXCBacktestError(f"cannot read official startlist {path}: {exc}") from exc
        checksums[str(path)] = _sha256(content)
        for row_number, row in enumerate(rows, start=2):
            sex = row["sex"].strip()
            race_id = row["race_id"].strip()
            athlete_id = row["canonical_athlete_id"].strip()
            if sex not in {"M", "L"} or not race_id.isdigit() or not athlete_id:
                raise FantasyXCBacktestError(
                    f"{path}:{row_number}: invalid official identity"
                )
            try:
                group_date = datetime.strptime(row["group_date"], "%Y-%m-%d").date()
            except ValueError as exc:
                raise FantasyXCBacktestError(
                    f"{path}:{row_number}: invalid group date"
                ) from exc
            if group_date != logical_date:
                raise FantasyXCBacktestError(
                    f"{path}:{row_number}: official logical date does not match"
                )
            if (
                row["selection_status"] != "confirmed"
                or row["source_mode"] != "official_fis_startlist"
            ):
                raise FantasyXCBacktestError(
                    f"{path}:{row_number}: truth must come from an official FIS startlist"
                )
            key = (sex, race_id, athlete_id)
            if key in official:
                raise FantasyXCBacktestError(f"duplicate official starter: {key}")
            official.add(key)
            events.setdefault((sex, race_id), set()).add(athlete_id)
    if not official:
        raise FantasyXCBacktestError("official truth contains no starters")
    return official, events, checksums


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def backtest_candidates(
    *,
    candidates_path: Path,
    official_startlists: tuple[Path, ...],
    logical_date: date,
    output_dir: Path,
) -> dict[str, Any]:
    """Create event-level decision cases and research-only accuracy metrics."""

    candidates, candidate_content = _read_candidates(candidates_path, logical_date)
    official, events, official_checksums = _read_official(
        official_startlists, logical_date
    )
    cases: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for index, candidate in enumerate(candidates["candidates"], start=1):
        if not isinstance(candidate, dict):
            raise FantasyXCBacktestError(f"candidate {index} must be an object")
        candidate_id = str(candidate.get("candidate_id", ""))
        sex = str(candidate.get("sex", ""))
        athlete_id = str(candidate.get("canonical_athlete_id", ""))
        name = str(candidate.get("canonical_name", ""))
        status = str(candidate.get("suggested_status", ""))
        race_ids = candidate.get("race_ids")
        if (
            not candidate_id.startswith("candidate:")
            or sex not in {"M", "L"}
            or not athlete_id
            or not name
            or status not in {"confirmed", "excluded"}
            or not isinstance(race_ids, list)
            or not race_ids
        ):
            raise FantasyXCBacktestError(f"candidate {index} is incomplete")
        try:
            observed = datetime.fromisoformat(
                str(candidate.get("observed_at", "")).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise FantasyXCBacktestError(
                f"candidate {index} has an invalid observation timestamp"
            ) from exc
        if observed.tzinfo is None or observed.date() > logical_date:
            raise FantasyXCBacktestError(
                f"candidate {index} was not observed before the logical date"
            )
        expanded: list[str] = []
        for race_id in race_ids:
            race_id = str(race_id)
            if race_id == "*":
                expanded.extend(
                    event_race
                    for event_sex, event_race in events
                    if event_sex == sex
                )
            elif (sex, race_id) in events:
                expanded.append(race_id)
            else:
                raise FantasyXCBacktestError(
                    f"candidate {candidate_id} references unknown official event {race_id}"
                )
        for race_id in sorted(set(expanded), key=int):
            key = (sex, race_id, athlete_id)
            if key in seen:
                raise FantasyXCBacktestError(f"duplicate candidate decision: {key}")
            seen.add(key)
            started = key in official
            correct = started if status == "confirmed" else not started
            cases.append(
                {
                    "candidate_id": candidate_id,
                    "sex": sex,
                    "race_id": race_id,
                    "canonical_athlete_id": athlete_id,
                    "canonical_name": name,
                    "suggested_status": status,
                    "official_started": str(started).lower(),
                    "correct": str(correct).lower(),
                }
            )
    if not cases:
        raise FantasyXCBacktestError("candidate artifact produced no evaluable decisions")

    total = len(cases)
    correct = sum(row["correct"] == "true" for row in cases)
    confirmations = [row for row in cases if row["suggested_status"] == "confirmed"]
    exclusions = [row for row in cases if row["suggested_status"] == "excluded"]
    confirmed_correct = sum(row["correct"] == "true" for row in confirmations)
    excluded_correct = sum(row["correct"] == "true" for row in exclusions)
    confirmed_truth = {
        (row["sex"], row["race_id"], row["canonical_athlete_id"])
        for row in confirmations
        if row["official_started"] == "true"
    }
    case_output = io.StringIO(newline="")
    writer = csv.DictWriter(case_output, fieldnames=CASE_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(cases)
    cases_bytes = case_output.getvalue().encode()
    report = {
        "schema_version": REPORT_SCHEMA,
        "resolver_version": RESOLVER_VERSION,
        "status": "research_backtest",
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "recommendation": "insufficient_evidence",
        "inputs": {
            "candidates": {
                "path": str(candidates_path),
                "sha256": _sha256(candidate_content),
            },
            "official_startlists": official_checksums,
        },
        "metrics": {
            "decision_count": total,
            "correct_decisions": correct,
            "decision_accuracy": _rate(correct, total),
            "confirmed_decisions": len(confirmations),
            "confirmed_correct": confirmed_correct,
            "confirmed_precision": _rate(confirmed_correct, len(confirmations)),
            "excluded_decisions": len(exclusions),
            "excluded_correct": excluded_correct,
            "excluded_accuracy": _rate(excluded_correct, len(exclusions)),
            "official_starter_rows": len(official),
            "official_starter_rows_covered": len(confirmed_truth),
            "official_field_coverage": _rate(len(confirmed_truth), len(official)),
            "false_confirmations": len(confirmations) - confirmed_correct,
            "false_exclusions": len(exclusions) - excluded_correct,
        },
        "unresolved_exception_count": len(candidates.get("review_exceptions", [])),
        "outputs": {
            "fantasyxc-backtest-cases.csv": {
                "sha256": _sha256(cases_bytes),
                "rows": len(cases),
            }
        },
    }
    output_dir = output_dir.expanduser().resolve()
    cases_path = output_dir / "fantasyxc-backtest-cases.csv"
    report_path = output_dir / "fantasyxc-backtest-report.json"
    _atomic_write(cases_path, cases_bytes)
    _atomic_write(report_path, _json_bytes(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True, type=Path)
    parser.add_argument(
        "--official-startlist",
        required=True,
        action="append",
        type=Path,
    )
    parser.add_argument("--logical-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    report = backtest_candidates(
        candidates_path=args.candidates,
        official_startlists=tuple(args.official_startlist),
        logical_date=args.logical_date,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
