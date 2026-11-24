#!/usr/bin/env python3
"""Resolve FantasyXC feed evidence into shadow roster-review candidates.

This resolver deliberately stops before the reviewed announced-roster contract.
It proposes exact canonical athlete and race decisions, while preserving every
unknown, ambiguous, or conflicting claim as a review exception.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from cross_country_fantasyxc_feed import ITEMS_SCHEMA
from cross_country_results_identity import CanonicalAthlete, load_history, normalized_name
from cross_country_weekend_inputs import select_normal_weekend, validate_startlist_url

CANDIDATES_SCHEMA = "nordicnumbers.cross-country-announced-roster-candidates/v1"
REPORT_SCHEMA = "nordicnumbers.cross-country-announced-roster-resolution-report/v1"
POSITIVE_CUES = (
    "will be racing",
    "will race",
    "to race",
    "team selection",
    "announced",
    "line-up",
    "lineup",
    "roster",
    "represented by",
    "following athletes",
    "companions are",
    "racing for",
    "selected",
    "race ready",
    "comeback",
)
NEGATIVE_CUES = (
    "will not",
    "won't",
    "will miss",
    "miss out",
    "not travel",
    "out of",
)
INJURY_CUES = ("injury", "injured", "rehab", "rehabilitat", "recover")


class FantasyXCResolveError(RuntimeError):
    """Feed evidence cannot be resolved safely."""


class _LineParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.lines: list[str] = []
        self._parts: list[str] = []

    def _flush(self) -> None:
        text = " ".join("".join(self._parts).split())
        if text:
            self.lines.append(text)
        self._parts = []

    def handle_starttag(self, tag: str, _attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"br", "p", "div", "li"}:
            self._flush()

    def handle_endtag(self, tag: str) -> None:
        if tag in {"p", "div", "li"}:
            self._flush()

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def close(self) -> None:
        super().close()
        self._flush()


@dataclass(frozen=True)
class _Event:
    sex: str
    race_id: str
    distance: str


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _stable_id(prefix: str, value: dict[str, Any]) -> str:
    content = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return f"{prefix}:{_sha256(content)}"


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


def html_lines(value: str) -> list[str]:
    parser = _LineParser()
    parser.feed(value)
    parser.close()
    return parser.lines


def _timestamp(value: object, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise FantasyXCResolveError(f"{label} has an invalid timestamp") from exc
    if parsed.tzinfo is None:
        raise FantasyXCResolveError(f"{label} timestamp has no timezone")
    return parsed


def _read_feed(path: Path, logical_date: date) -> tuple[dict[str, Any], bytes]:
    try:
        content = path.read_bytes()
        document = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FantasyXCResolveError(f"cannot read normalized feed: {exc}") from exc
    if not isinstance(document, dict) or document.get("schema_version") != ITEMS_SCHEMA:
        raise FantasyXCResolveError("unsupported normalized feed schema")
    if document.get("logical_date") != logical_date.isoformat():
        raise FantasyXCResolveError("normalized feed logical date does not match")
    if document.get("status") != "candidate_review":
        raise FantasyXCResolveError("normalized feed must remain candidate_review")
    if document.get("publication") != {"status": "disabled"}:
        raise FantasyXCResolveError("normalized feed publication must be disabled")
    if not isinstance(document.get("items"), list):
        raise FantasyXCResolveError("normalized feed items must be a list")
    return document, content


def _events(schedule: Path, logical_date: date) -> list[_Event]:
    rows, _ignored = select_normal_weekend(schedule, logical_date)
    return [
        _Event(row["Sex"], validate_startlist_url(row["Startlist"]), row["Distance"])
        for row in rows
    ]


def _race_ids(line: str, sex: str, events: list[_Event]) -> tuple[list[str], str | None]:
    annotations = re.findall(r"\(([^()]*)\)", line)
    if not annotations:
        return ["*"], None
    tokens: set[str] = set()
    for annotation in annotations:
        tokens.update(part.strip().lower() for part in annotation.split("+") if part.strip())
    wanted: set[str] = set()
    unknown: list[str] = []
    for token in tokens:
        if token in {"s", "sprint"}:
            wanted.add("sprint")
        elif re.fullmatch(r"\d+(?:\.\d+)?k", token):
            wanted.add(token[:-1].lstrip("0") or "0")
        else:
            unknown.append(token)
    if unknown:
        return [], f"unknown race annotation: {', '.join(sorted(unknown))}"
    def distance_key(value: str) -> str:
        try:
            number = float(value)
        except ValueError:
            return value.lower()
        return str(int(number)) if number.is_integer() else str(number)

    matches: set[str] = set()
    missing: list[str] = []
    for wanted_distance in sorted(wanted):
        distance_matches = {
            event.race_id
            for event in events
            if event.sex == sex
            and (
                (wanted_distance == "sprint" and event.distance.lower() == "sprint")
                or distance_key(event.distance) == wanted_distance
            )
        }
        if not distance_matches:
            missing.append(wanted_distance)
        matches.update(distance_matches)
    if missing:
        return [], "race annotation did not match the weekend schedule: " + ", ".join(missing)
    return sorted(matches, key=int), None


def _candidate_athletes(
    line: str,
    sex: str | None,
    athletes: dict[tuple[str, str], CanonicalAthlete],
) -> list[tuple[str, CanonicalAthlete]]:
    line_tokens = set(normalized_name(re.sub(r"\([^)]*\)", " ", line)))
    matches: dict[tuple[str, str], CanonicalAthlete] = {}
    for (athlete_sex, athlete_id), athlete in athletes.items():
        if sex and athlete_sex != sex:
            continue
        name_tokens = set(normalized_name(athlete.name))
        if len(name_tokens) >= 2 and name_tokens <= line_tokens:
            matches[(athlete_sex, athlete_id)] = athlete
    # A longer exact full name dominates a shorter canonical alias in the same line.
    if matches:
        longest = max(len(normalized_name(athlete.name)) for athlete in matches.values())
        matches = {
            key: athlete
            for key, athlete in matches.items()
            if len(normalized_name(athlete.name)) == longest
        }
    return [(key[0], athlete) for key, athlete in sorted(matches.items())]


def _looks_name_like(line: str) -> bool:
    stripped = re.sub(r"\([^)]*\)", "", line).strip(" .:-")
    words = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ'’-]+", stripped)
    lower = stripped.lower()
    return (
        2 <= len(words) <= 5
        and not lower.startswith(("source", "photo", "read more", "more info"))
        and all(word[0].isupper() for word in words)
        and not any(cue in lower for cue in POSITIVE_CUES + NEGATIVE_CUES + INJURY_CUES)
    )


def resolve_feed(
    *,
    feed_items: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    schedule: Path,
    logical_date: date,
    output_path: Path,
    report_path: Path,
) -> dict[str, Any]:
    """Write deterministic candidates and review exceptions; never reviewed input."""

    feed, feed_bytes = _read_feed(feed_items, logical_date)
    athletes, _name_index, _names_by_id = load_history(men_chrono, ladies_chrono)
    events = _events(schedule, logical_date)
    items = sorted(feed["items"], key=lambda item: (item["published_at"], item["status_id"]))
    proposed: dict[tuple[str, str, str], dict[str, Any]] = {}
    conflicts: set[tuple[str, str, str]] = set()
    exceptions: list[dict[str, Any]] = []
    last_positive: datetime | None = None

    for item_index, item in enumerate(items, start=1):
        label = f"feed item {item_index}"
        if not isinstance(item, dict):
            raise FantasyXCResolveError(f"{label} must be an object")
        published = _timestamp(item.get("published_at"), label=label)
        source_url = str(item.get("source_url", "")).strip()
        if not re.fullmatch(r"https://x\.com/FantasyXC/status/\d+", source_url):
            raise FantasyXCResolveError(f"{label} has an unsupported source URL")
        lines = html_lines(str(item.get("description_html", "")))
        combined = " ".join([str(item.get("title", "")), *lines]).lower()
        positive_context = any(cue in combined for cue in POSITIVE_CUES)
        first_line_is_section = bool(
            lines
            and re.fullmatch(r"(?:men|women|ladies)\s*:", lines[0], re.I)
        )
        first_line_is_exact_annotated_athlete = bool(
            lines
            and len(_candidate_athletes(lines[0], None, athletes)) == 1
            and re.search(r"\([^()]+\)", lines[0])
        )
        if (
            not positive_context
            and (first_line_is_section or first_line_is_exact_annotated_athlete)
            and last_positive is not None
            and published - last_positive <= timedelta(minutes=15)
        ):
            positive_context = True
        if positive_context:
            last_positive = published
        active_sex: str | None = None
        for line in lines:
            section = re.fullmatch(r"(men|women|ladies)\s*:\s*(.*)", line, re.I)
            if section:
                active_sex = "M" if section.group(1).lower() == "men" else "L"
                line = section.group(2).strip()
                if not line:
                    continue
            matches = _candidate_athletes(line, active_sex, athletes)
            if len(matches) > 1:
                exceptions.append({
                    "reason": "ambiguous canonical athlete match",
                    "source_url": source_url,
                    "published_at": item["published_at"],
                    "evidence_text": line,
                    "candidate_ids": [athlete.athlete_id for _sex, athlete in matches],
                })
                continue
            if not matches:
                if positive_context and _looks_name_like(line):
                    exceptions.append({
                        "reason": "unknown athlete line",
                        "source_url": source_url,
                        "published_at": item["published_at"],
                        "evidence_text": line,
                        "sex": active_sex or "unknown",
                    })
                continue
            match_sex, athlete = matches[0]
            lower = line.lower()
            if any(cue in lower for cue in NEGATIVE_CUES):
                status = "excluded"
            elif any(cue in lower for cue in INJURY_CUES) and not any(
                cue in lower for cue in POSITIVE_CUES
            ):
                exceptions.append({
                    "reason": "injury prose has no explicit roster decision",
                    "source_url": source_url,
                    "published_at": item["published_at"],
                    "evidence_text": line,
                    "sex": match_sex,
                    "canonical_athlete_id": athlete.athlete_id,
                })
                continue
            elif positive_context or any(cue in lower for cue in POSITIVE_CUES):
                status = "confirmed"
            else:
                exceptions.append({
                    "reason": "athlete mention has no explicit roster decision",
                    "source_url": source_url,
                    "published_at": item["published_at"],
                    "evidence_text": line,
                    "sex": match_sex,
                    "canonical_athlete_id": athlete.athlete_id,
                })
                continue
            race_ids, race_error = _race_ids(line, match_sex, events)
            if race_error:
                exceptions.append({
                    "reason": race_error,
                    "source_url": source_url,
                    "published_at": item["published_at"],
                    "evidence_text": line,
                    "sex": match_sex,
                    "canonical_athlete_id": athlete.athlete_id,
                })
                continue
            for race_id in race_ids:
                key = (match_sex, athlete.athlete_id, race_id)
                candidate = {
                    "sex": match_sex,
                    "canonical_athlete_id": athlete.athlete_id,
                    "canonical_name": athlete.name,
                    "suggested_status": status,
                    "race_ids": [race_id],
                    "source_url": source_url,
                    "observed_at": item["published_at"],
                    "evidence_text": line,
                    "identity_match_method": "exact_canonical_name_tokens",
                    "confidence": "high",
                }
                existing = proposed.get(key)
                if existing and existing["suggested_status"] != status:
                    conflicts.add(key)
                    exceptions.append({
                        "reason": "conflicting roster decisions",
                        "source_url": source_url,
                        "published_at": item["published_at"],
                        "evidence_text": line,
                        "sex": match_sex,
                        "canonical_athlete_id": athlete.athlete_id,
                        "race_id": race_id,
                    })
                elif not existing:
                    proposed[key] = candidate

    for key in conflicts:
        proposed.pop(key, None)
    grouped: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in proposed.values():
        group_key = (
            row["sex"],
            row["canonical_athlete_id"],
            row["suggested_status"],
            row["source_url"],
            row["observed_at"],
            row["evidence_text"],
        )
        if group_key not in grouped:
            grouped[group_key] = {**row, "race_ids": []}
        grouped[group_key]["race_ids"].extend(row["race_ids"])
    candidates = sorted(
        grouped.values(),
        key=lambda row: (row["sex"], row["canonical_name"], row["race_ids"]),
    )
    for row in candidates:
        row["race_ids"] = sorted(set(row["race_ids"]), key=lambda value: (value != "*", value))
        row["candidate_id"] = _stable_id("candidate", row)
    for exception in exceptions:
        exception["exception_id"] = _stable_id("exception", exception)
    document = {
        "schema_version": CANDIDATES_SCHEMA,
        "status": "candidate_review",
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "source": {"path": str(feed_items), "sha256": _sha256(feed_bytes)},
        "candidates": candidates,
        "review_exceptions": exceptions,
    }
    output_bytes = _json_bytes(document)
    _atomic_write(output_path, output_bytes)
    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "candidate_review",
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "candidate_count": len(candidates),
        "exception_count": len(exceptions),
        "conflict_count": len(conflicts),
        "inputs": {"feed_items_sha256": _sha256(feed_bytes)},
        "outputs": {str(output_path): {"sha256": _sha256(output_bytes)}},
    }
    _atomic_write(report_path, _json_bytes(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed-items", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--logical-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    report = resolve_feed(
        feed_items=args.feed_items,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        schedule=args.schedule,
        logical_date=args.logical_date,
        output_path=args.output,
        report_path=args.report,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
