#!/usr/bin/env python3
"""Read-only source contract for the official IBU Datacenter.

This module deliberately stops at trustworthy source evidence. It discovers or
accepts the public Datacenter API key, downloads official JSON, validates event,
competition, result, relay-member, and start-list-report identities, and writes
an isolated evidence bundle. It never mutates accepted Nordic Numbers data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen


DATACENTER_URL = "https://biathlonresults.com/"
SPORT_API_BASE = "https://biathlonresults.com/modules/sportapi/api/"
OFFICIAL_DOC_HOST = "ibu.blob.core.windows.net"
REPORT_SCHEMA = "nordicnumbers.biathlon-ibu-source/v1"
IDENTIFIER = re.compile(r"^[A-Z0-9_]+$")
SEASON_ID = re.compile(r"^\d{4}$")
API_KEY = re.compile(r"window\.ApiKey\s*=\s*[\"']([^\"']+)")
SCRIPT_SRC = re.compile(
    r"<script[^>]+src=(?:\"([^\"]+)\"|'([^']+)'|([^\s>]+))",
    re.IGNORECASE,
)

DISCIPLINES = {
    "IN": "individual",
    "SI": "short_individual",
    "SP": "sprint",
    "PU": "pursuit",
    "MS": "mass_start",
}
SEXES = {"SM": "M", "SW": "L", "MX": "Mixed"}


class IBUContractError(RuntimeError):
    """Official IBU evidence violates the supported source contract."""


def is_final_competition(competition: dict[str, Any]) -> bool:
    """Return whether all three official IBU completion signals agree."""
    return (
        str(competition.get("status_text", "")).casefold() == "final"
        and str(competition.get("result_status", "")).upper() == "OFFICIAL"
        and str(competition.get("schedule_status", "")).upper() == "FINISHED"
    )


Fetcher = Callable[[str, dict[str, str]], bytes]


def _official_url(url: str, *, allowed_hosts: set[str]) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in allowed_hosts:
        raise IBUContractError(f"untrusted official source URL: {url}")


def fetch_bytes(
    url: str,
    headers: dict[str, str] | None = None,
    *,
    attempts: int = 3,
) -> bytes:
    """Fetch a public IBU resource with bounded transient retries."""
    _official_url(url, allowed_hosts={"biathlonresults.com", OFFICIAL_DOC_HOST})
    request_headers = {
        "User-Agent": "NordicNumbers-IBU-Source-Contract/1.0",
        **(headers or {}),
    }
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(Request(url, headers=request_headers), timeout=30) as response:
                if response.status != 200:
                    raise IBUContractError(
                        f"IBU returned HTTP {response.status} for {url}"
                    )
                return response.read()
        except HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == attempts:
                raise IBUContractError(
                    f"IBU request failed with HTTP {error.code} for {url}"
                ) from error
        except (TimeoutError, URLError) as error:
            if attempt == attempts:
                raise IBUContractError(f"IBU request failed for {url}: {error}") from error
        time.sleep(2 ** (attempt - 1))
    raise AssertionError("retry loop exited unexpectedly")


def discover_public_api_key(fetcher: Fetcher = fetch_bytes) -> str:
    """Read the API key published by the official Datacenter web client.

    The value is used only as a request header and is never written to reports.
    Operators may instead set ``IBU_API_KEY`` to bypass discovery.
    """
    configured = os.environ.get("IBU_API_KEY", "").strip()
    if configured:
        return configured
    html = fetcher(DATACENTER_URL, {}).decode("utf-8")
    sources = [next(value for value in match.groups() if value) for match in SCRIPT_SRC.finditer(html)]
    app_sources = [source for source in sources if Path(urlparse(source).path).name.startswith("app~")]
    for source in app_sources:
        url = urljoin(DATACENTER_URL, source)
        _official_url(url, allowed_hosts={"biathlonresults.com"})
        chunk = fetcher(url, {}).decode("utf-8")
        match = API_KEY.search(chunk)
        if match:
            return match.group(1)
    raise IBUContractError("official Datacenter client did not expose an API key")


def _identifier(value: str, *, field: str) -> str:
    if not value or not IDENTIFIER.fullmatch(value):
        raise IBUContractError(f"invalid {field}: {value!r}")
    return value


def _iso_datetime(value: str, *, field: str) -> str:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise IBUContractError(f"invalid {field}: {value!r}") from error
    return value


@dataclass
class IBUClient:
    api_key: str
    fetcher: Fetcher = fetch_bytes
    base_url: str = SPORT_API_BASE

    def _get(self, endpoint: str, **parameters: str | int) -> Any:
        if endpoint not in {"Events", "Competitions", "Results", "Reports2", "CISBios"}:
            raise IBUContractError(f"unsupported IBU endpoint: {endpoint}")
        url = f"{self.base_url}{endpoint}?{urlencode(parameters)}"
        _official_url(url, allowed_hosts={"biathlonresults.com"})
        try:
            return json.loads(self.fetcher(url, {"apikey": self.api_key}).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise IBUContractError(f"IBU returned invalid JSON for {endpoint}") from error

    def events(self, season_id: str, level: int = 1) -> list[dict[str, Any]]:
        if not SEASON_ID.fullmatch(season_id):
            raise IBUContractError(f"invalid season_id: {season_id!r}")
        payload = self._get("Events", SeasonId=season_id, Level=level)
        if not isinstance(payload, list):
            raise IBUContractError("IBU Events response is not a list")
        return payload

    def competitions(self, event_id: str) -> list[dict[str, Any]]:
        _identifier(event_id, field="event_id")
        payload = self._get("Competitions", EventId=event_id)
        if not isinstance(payload, list):
            raise IBUContractError("IBU Competitions response is not a list")
        return payload

    def results(self, race_id: str) -> dict[str, Any]:
        _identifier(race_id, field="race_id")
        payload = self._get("Results", RaceId=race_id)
        if not isinstance(payload, dict):
            raise IBUContractError("IBU Results response is not an object")
        return payload

    def reports(self, race_id: str) -> dict[str, Any]:
        _identifier(race_id, field="race_id")
        payload = self._get("Reports2", RaceId=race_id)
        if not isinstance(payload, dict):
            raise IBUContractError("IBU Reports response is not an object")
        return payload

    def bio(self, ibu_id: str) -> dict[str, Any]:
        """Return the official athlete biography for identity corroboration."""
        _identifier(ibu_id, field="ibu_id")
        payload = self._get("CISBios", IBUId=ibu_id)
        if not isinstance(payload, dict):
            raise IBUContractError("IBU CISBios response is not an object")
        if payload.get("IBUId") != ibu_id:
            raise IBUContractError(f"IBU CISBios identity mismatch for {ibu_id}")
        return payload


def validate_events(
    payload: list[dict[str, Any]], *, season_id: str, level: int
) -> list[dict[str, Any]]:
    if not payload:
        raise IBUContractError("IBU season contains no events")
    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for event in payload:
        event_id = _identifier(str(event.get("EventId", "")), field="EventId")
        if event_id in seen:
            raise IBUContractError(f"duplicate IBU EventId: {event_id}")
        seen.add(event_id)
        if event.get("SeasonId") != season_id or event.get("Level") != level:
            raise IBUContractError(f"IBU event scope mismatch: {event_id}")
        start = _iso_datetime(event.get("StartDate"), field=f"{event_id}.StartDate")
        end = _iso_datetime(event.get("EndDate"), field=f"{event_id}.EndDate")
        if end < start:
            raise IBUContractError(f"IBU event ends before it starts: {event_id}")
        normalized.append({
            "season_id": season_id,
            "event_id": event_id,
            "start_date": start,
            "end_date": end,
            "description": str(event.get("Description", "")).strip(),
            "venue": str(event.get("ShortDescription", "")).strip(),
            "nation": str(event.get("Nat", "")).strip(),
            "classification": _identifier(
                str(event.get("EventClassificationId", "")),
                field="EventClassificationId",
            ),
            "level": level,
        })
    return normalized


def competition_kind(competition: dict[str, Any]) -> tuple[str, str]:
    discipline = str(competition.get("DisciplineId", ""))
    category = str(competition.get("catId", ""))
    try:
        sex = SEXES[category]
    except KeyError as error:
        raise IBUContractError(f"unsupported IBU category: {category!r}") from error
    if discipline == "RL":
        return ("mixed_relay" if sex == "Mixed" else "relay", sex)
    if discipline == "SR" and sex == "Mixed":
        return "single_mixed_relay", sex
    try:
        return DISCIPLINES[discipline], sex
    except KeyError as error:
        raise IBUContractError(
            f"unsupported IBU discipline {discipline!r} for {category!r}"
        ) from error


def validate_competitions(
    payload: list[dict[str, Any]], *, event_id: str
) -> list[dict[str, Any]]:
    if not payload:
        raise IBUContractError(f"IBU event has no competitions: {event_id}")
    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for competition in payload:
        race_id = _identifier(str(competition.get("RaceId", "")), field="RaceId")
        if not race_id.startswith(event_id) or race_id in seen:
            raise IBUContractError(f"invalid or duplicate IBU RaceId: {race_id}")
        seen.add(race_id)
        kind, sex = competition_kind(competition)
        normalized.append({
            "event_id": event_id,
            "race_id": race_id,
            "start_time": _iso_datetime(
                competition.get("StartTime"), field=f"{race_id}.StartTime"
            ),
            "description": str(competition.get("ShortDescription", "")).strip(),
            "gender_order": str(competition.get("GenderOrder", "") or "").strip(),
            "discipline_id": str(competition.get("DisciplineId", "")),
            "event_kind": kind,
            "sex": sex,
            "status_id": int(competition.get("StatusId", 0)),
            "status_text": str(competition.get("StatusText", "")).strip(),
            "result_status": str(competition.get("ResultStatus", "")).strip(),
            "schedule_status": str(competition.get("ScheduleStatus", "")).strip(),
        })
    return normalized


def validate_results(payload: dict[str, Any], competition: dict[str, Any]) -> dict[str, Any]:
    race_id = competition["race_id"]
    if payload.get("RaceId") != race_id:
        raise IBUContractError(f"IBU Results RaceId mismatch for {race_id}")
    source_competition = payload.get("Competition")
    if not isinstance(source_competition, dict) or source_competition.get("RaceId") != race_id:
        raise IBUContractError(f"IBU Results competition mismatch for {race_id}")
    rows = payload.get("Results")
    if not isinstance(rows, list) or not rows:
        raise IBUContractError(f"IBU Results contains no rows for {race_id}")

    team_kind = competition["event_kind"] in {"relay", "mixed_relay", "single_mixed_relay"}
    athlete_rows = [row for row in rows if not row.get("IsTeam")]
    team_rows = [row for row in rows if row.get("IsTeam")]
    if team_kind and not team_rows:
        raise IBUContractError(f"IBU relay contains no team rows for {race_id}")
    if not team_kind and team_rows:
        raise IBUContractError(f"IBU individual race contains team rows for {race_id}")
    if not athlete_rows:
        raise IBUContractError(f"IBU Results contains no athlete rows for {race_id}")

    identities: set[tuple[str, int]] = set()
    team_keys = {
        (str(row.get("Bib", "")), str(row.get("Nat", ""))) for row in team_rows
    }
    if team_kind and len(team_keys) != len(team_rows):
        raise IBUContractError(f"duplicate IBU relay team identity for {race_id}")
    members: defaultdict[tuple[str, str], list[int]] = defaultdict(list)
    for row in athlete_rows:
        ibu_id = _identifier(str(row.get("IBUId", "")), field="IBUId")
        nation = str(row.get("Nat", ""))
        if not re.fullmatch(r"[A-Z]{3}", nation):
            raise IBUContractError(f"invalid IBU nation for {ibu_id}: {nation!r}")
        leg = int(row.get("Leg") or 0)
        identity = (ibu_id, leg if team_kind else 0)
        if identity in identities:
            raise IBUContractError(f"duplicate IBU result identity: {identity}")
        identities.add(identity)
        if team_kind:
            if leg < 1:
                raise IBUContractError(f"relay member has invalid leg: {ibu_id}")
            team_key = (str(row.get("Bib", "")), nation)
            if team_key not in team_keys:
                raise IBUContractError(
                    f"relay member has no matching team for {race_id}: {team_key}"
                )
            members[team_key].append(leg)

    if team_kind:
        expected = {1, 2, 3, 4}
        incomplete = {
            f"{team[0]}:{team[1]}": sorted(legs)
            for team, legs in members.items()
            if set(legs) != expected
        }
        missing = sorted(f"{team[0]}:{team[1]}" for team in team_keys - set(members))
        if missing:
            incomplete["missing_members"] = missing
        if incomplete:
            raise IBUContractError(f"incomplete IBU relay teams for {race_id}: {incomplete}")

    return {
        "race_id": race_id,
        "event_kind": competition["event_kind"],
        "sex": competition["sex"],
        "status_id": int(source_competition.get("StatusId", 0)),
        "status": str(source_competition.get("StatusText", "")),
        "result_rows": len(rows),
        "athlete_rows": len(athlete_rows),
        "team_rows": len(team_rows),
        "unique_athletes": len({row[0] for row in identities}),
    }


def validate_reports(payload: dict[str, Any], *, race_id: str) -> dict[str, str]:
    if payload.get("RaceId") != race_id:
        raise IBUContractError(f"IBU Reports RaceId mismatch for {race_id}")
    reports = payload.get("Reports")
    if not isinstance(reports, list):
        raise IBUContractError(f"IBU Reports is not a list for {race_id}")
    start_lists = [report for report in reports if report.get("Description") == "Start List"]
    if len(start_lists) != 1:
        raise IBUContractError(
            f"expected exactly one official Start List for {race_id}; found {len(start_lists)}"
        )
    url = str(start_lists[0].get("URL", ""))
    _official_url(url, allowed_hosts={OFFICIAL_DOC_HOST})
    if not urlparse(url).path.lower().endswith(".pdf"):
        raise IBUContractError(f"official Start List is not a PDF for {race_id}")
    return {"description": "Start List", "url": url}


def _sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _safe_output_dir(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if "excel365" in resolved.parts:
        raise IBUContractError("source evidence cannot be written inside accepted excel365 data")
    return resolved


def capture_contract(
    *,
    client: IBUClient,
    season_id: str,
    event_id: str,
    race_ids: Iterable[str],
    output_dir: Path,
) -> dict[str, Any]:
    """Capture and validate an immutable, publication-disabled evidence bundle."""
    output_dir = _safe_output_dir(output_dir)
    events_payload = client.events(season_id, level=1)
    events = validate_events(events_payload, season_id=season_id, level=1)
    if event_id not in {event["event_id"] for event in events}:
        raise IBUContractError(f"event is not in official season level 1: {event_id}")
    competitions_payload = client.competitions(event_id)
    competitions = validate_competitions(competitions_payload, event_id=event_id)
    by_race = {competition["race_id"]: competition for competition in competitions}
    selected = list(race_ids)
    if not selected:
        raise IBUContractError("at least one --race-id is required")
    unknown = sorted(set(selected) - set(by_race))
    if unknown:
        raise IBUContractError(f"races are not in official event {event_id}: {unknown}")
    not_final = sorted(
        race_id for race_id in selected if not is_final_competition(by_race[race_id])
    )
    if not_final:
        raise IBUContractError(
            "refusing to capture results before IBU marks them "
            f"Final/OFFICIAL/FINISHED: {not_final}"
        )

    raw_dir = output_dir / "raw"
    _atomic_json(raw_dir / "events.json", events_payload)
    _atomic_json(raw_dir / "competitions.json", competitions_payload)
    race_reports: list[dict[str, Any]] = []
    for race_id in selected:
        results_payload = client.results(race_id)
        reports_payload = client.reports(race_id)
        result_summary = validate_results(results_payload, by_race[race_id])
        start_list = validate_reports(reports_payload, race_id=race_id)
        _atomic_json(raw_dir / f"{race_id}-results.json", results_payload)
        _atomic_json(raw_dir / f"{race_id}-reports.json", reports_payload)
        race_reports.append({
            **result_summary,
            "start_list": start_list,
            "results_sha256": _sha256(results_payload),
            "reports_sha256": _sha256(reports_payload),
        })

    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "season_id": season_id,
        "event_id": event_id,
        "source": {
            "authority": "International Biathlon Union",
            "datacenter": DATACENTER_URL,
            "api": SPORT_API_BASE,
            "api_key_persisted": False,
        },
        "event_count": len(events),
        "competition_count": len(competitions),
        "discipline_counts": dict(sorted(Counter(item["event_kind"] for item in competitions).items())),
        "races": race_reports,
        "accepted_data_mutated": False,
        "publication": {"status": "disabled"},
    }
    _atomic_json(output_dir / "report.json", report)
    return report


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--season-id", required=True)
    command.add_argument("--event-id", required=True)
    command.add_argument("--race-id", action="append", default=[])
    command.add_argument("--output-dir", type=Path, required=True)
    return command


def main() -> int:
    args = parser().parse_args()
    client = IBUClient(api_key=discover_public_api_key())
    report = capture_contract(
        client=client,
        season_id=args.season_id,
        event_id=args.event_id,
        race_ids=args.race_id,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
