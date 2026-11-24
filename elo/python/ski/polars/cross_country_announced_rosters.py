#!/usr/bin/env python3
"""Validate reviewed cross-country weekend roster announcements.

This module does not scrape social media. It defines the stable handoff between
future acquisition/review tooling and weekend prediction inputs. Announcements
are exact canonical-athlete overlays: confirmed athletes are added, excluded
athletes are removed, and everyone not mentioned remains governed by the
existing fallback logic until an official FIS startlist supersedes the overlay.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from cross_country_results_identity import CanonicalAthlete

SCHEMA_VERSION = "nordicnumbers.cross-country-announced-rosters/v1"
AUTOMATED_SCHEMA_VERSION = "nordicnumbers.cross-country-announced-rosters/v2"
STATUSES = frozenset({"confirmed", "excluded"})


class AnnouncedRosterError(ValueError):
    """Reviewed announcement evidence does not satisfy the input contract."""


@dataclass(frozen=True)
class AnnouncedSelection:
    status: str
    source_url: str
    observed_at: str
    race_id: str


def _timestamp(value: object, *, label: str) -> str:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AnnouncedRosterError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise AnnouncedRosterError(f"{label} must include a timezone")
    return text


def _source_url(value: object, *, label: str) -> str:
    text = str(value).strip()
    parsed = urlparse(text)
    if parsed.scheme != "https" or not parsed.hostname:
        raise AnnouncedRosterError(f"{label} must be an HTTPS evidence URL")
    return text


def load_announced_rosters(
    path: Path,
    *,
    logical_date: date,
    athletes: dict[tuple[str, str], CanonicalAthlete],
) -> dict[tuple[str, str, str], AnnouncedSelection]:
    """Return selections keyed by ``(sex, race_id|*, canonical_athlete_id)``."""

    try:
        document: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AnnouncedRosterError(
            f"cannot read announced roster evidence: {exc}"
        ) from exc
    if not isinstance(document, dict):
        raise AnnouncedRosterError("announced roster evidence must be a JSON object")
    schema = document.get("schema_version")
    status = document.get("status")
    if schema not in {SCHEMA_VERSION, AUTOMATED_SCHEMA_VERSION}:
        raise AnnouncedRosterError("unsupported announced roster schema")
    if document.get("logical_date") != logical_date.isoformat():
        raise AnnouncedRosterError("announced roster logical date does not match")
    if document.get("publication") != {"status": "disabled"}:
        raise AnnouncedRosterError("announced roster publication must remain disabled")
    expected_status = "reviewed" if schema == SCHEMA_VERSION else "automated"
    if status != expected_status:
        raise AnnouncedRosterError(
            f"announced roster {schema} status must be {expected_status}"
        )
    if schema == AUTOMATED_SCHEMA_VERSION:
        automation = document.get("automation")
        unresolved = document.get("unresolved")
        if not isinstance(automation, dict) or not automation.get("policy_version"):
            raise AnnouncedRosterError(
                "automated announced roster must record its policy version"
            )
        if not isinstance(unresolved, list):
            raise AnnouncedRosterError(
                "automated announced roster must contain unresolved evidence"
            )
    entries = document.get("entries")
    if not isinstance(entries, list):
        raise AnnouncedRosterError("announced roster entries must be a list")
    if schema == SCHEMA_VERSION and not entries:
        raise AnnouncedRosterError("reviewed announced roster contains no entries")

    selections: dict[tuple[str, str, str], AnnouncedSelection] = {}
    for index, entry in enumerate(entries, start=1):
        label = f"announced roster entry {index}"
        if not isinstance(entry, dict):
            raise AnnouncedRosterError(f"{label} must be an object")
        sex = str(entry.get("sex", "")).strip().upper()
        athlete_id = str(entry.get("canonical_athlete_id", "")).strip()
        status = str(entry.get("status", "")).strip()
        race_ids = entry.get("race_ids")
        if sex not in {"M", "L"}:
            raise AnnouncedRosterError(f"{label} has invalid sex")
        if (sex, athlete_id) not in athletes:
            raise AnnouncedRosterError(
                f"{label} references an unknown canonical athlete"
            )
        if status not in STATUSES:
            raise AnnouncedRosterError(f"{label} has invalid status")
        if not isinstance(race_ids, list) or not race_ids:
            raise AnnouncedRosterError(f"{label} must contain race_ids")
        normalized_races = [str(race_id).strip() for race_id in race_ids]
        if any(
            race_id != "*" and not race_id.isdigit()
            for race_id in normalized_races
        ):
            raise AnnouncedRosterError(f"{label} has an invalid race ID")
        if len(set(normalized_races)) != len(normalized_races):
            raise AnnouncedRosterError(f"{label} contains duplicate race IDs")
        if "*" in normalized_races and len(normalized_races) != 1:
            raise AnnouncedRosterError(f"{label} cannot combine '*' with race IDs")
        source_url = _source_url(entry.get("source_url"), label=label)
        observed_at = _timestamp(entry.get("observed_at"), label=label)
        for race_id in normalized_races:
            key = (sex, race_id, athlete_id)
            if key in selections:
                raise AnnouncedRosterError(
                    f"duplicate announced roster selection: {key}"
                )
            selections[key] = AnnouncedSelection(
                status=status,
                source_url=source_url,
                observed_at=observed_at,
                race_id=race_id,
            )
    return selections


def event_selections(
    selections: dict[tuple[str, str, str], AnnouncedSelection],
    *,
    sex: str,
    race_id: str,
) -> dict[str, AnnouncedSelection]:
    """Resolve group-wide evidence first, then exact event-specific evidence."""

    result = {
        athlete_id: selection
        for (entry_sex, entry_race, athlete_id), selection in selections.items()
        if entry_sex == sex and entry_race == "*"
    }
    result.update(
        {
            athlete_id: selection
            for (entry_sex, entry_race, athlete_id), selection in selections.items()
            if entry_sex == sex and entry_race == race_id
        }
    )
    return result
