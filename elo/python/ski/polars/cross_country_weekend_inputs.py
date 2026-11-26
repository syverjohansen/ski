#!/usr/bin/env python3
"""Build an isolated input bundle for normal cross-country weekend picks.

The accepted weekend schedule, Elo histories, and reviewed FIS aliases are
read-only inputs. Official FIS startlists are preferred. When FIS has not yet
published a startlist, the bundle contains an explicitly labelled candidate
pool made from athletes who appear in the latest accepted season; it never
claims that those athletes are confirmed starters.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from cross_country_announced_rosters import (
    AnnouncedRosterError,
    AnnouncedSelection,
    event_selections,
    load_announced_rosters,
)
from cross_country_results_identity import (
    CanonicalAthlete,
    load_aliases,
    load_history,
    normalized_name,
)
from cross_country_world_cup_quotas import (
    QUOTA_FIELDS,
    QuotaContractError,
    load_quota_contract,
)
from schedule_contract import WEEKENDS_COLUMNS

SCHEDULE_FIELDS = WEEKENDS_COLUMNS
STARTLIST_FIELDS = (
    "group_date",
    "race_date",
    "race_id",
    "sex",
    "city",
    "country",
    "distance",
    "technique",
    "canonical_athlete_id",
    "canonical_name",
    "canonical_nation",
    "fis_competitor_id",
    "fis_code",
    "identity_match_method",
    "selection_status",
    "source_mode",
    "source_url",
    "source_sha256",
)
REPORT_SCHEMA = "nordicnumbers.cross-country-weekend-inputs/v1"


class WeekendInputError(RuntimeError):
    """A trustworthy normal-weekend input bundle cannot be produced."""


@dataclass(frozen=True)
class OfficialAthlete:
    competitor_id: str
    fis_code: str
    name: str
    nation: str


class FISStartlistParser(HTMLParser):
    """Extract athlete identities from FIS table rows without page scripts."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[dict[str, str]] = []
        self._row: dict[str, str] | None = None
        self._capture: tuple[str, str, list[str]] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        classes = set((attributes.get("class") or "").split())
        if tag == "a" and "table-row" in classes:
            self._row = {"href": attributes.get("href") or ""}
            return
        if self._row is None:
            return
        field = None
        if "athlete-name" in classes:
            field = "name"
        elif "country__name-short" in classes:
            field = "nation"
        elif {"justify-right", "gray", "pr-1"} <= classes:
            field = "fis_code"
        if field:
            self._capture = (tag, field, [])

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._capture[2].append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._capture and tag == self._capture[0]:
            _, field, parts = self._capture
            if self._row is not None:
                self._row[field] = " ".join("".join(parts).split())
            self._capture = None
        if tag == "a" and self._row is not None:
            self.rows.append(self._row)
            self._row = None


def parse_official_startlist(html: bytes) -> list[OfficialAthlete]:
    parser = FISStartlistParser()
    parser.feed(html.decode("utf-8", errors="replace"))
    parser.close()
    athletes: list[OfficialAthlete] = []
    seen: set[str] = set()
    for row in parser.rows:
        match = re.search(r"competitorid=(\d+)", row.get("href", ""))
        name = row.get("name", "").strip()
        if not match or not name:
            continue
        competitor_id = match.group(1)
        if competitor_id in seen:
            raise WeekendInputError(f"duplicate FIS competitor ID: {competitor_id}")
        seen.add(competitor_id)
        athletes.append(
            OfficialAthlete(
                competitor_id=competitor_id,
                fis_code=row.get("fis_code", "").strip(),
                name=name,
                nation=row.get("nation", "").strip(),
            )
        )
    return athletes


def validate_startlist_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    race_ids = query.get("raceid", [])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.fis-ski.com"
        or parsed.path != "/DB/general/results.html"
        or query.get("sectorcode") != ["CC"]
        or len(race_ids) != 1
        or not race_ids[0].isdigit()
    ):
        raise WeekendInputError(f"unsupported FIS cross-country URL: {url}")
    return race_ids[0]


def fetch_html(url: str, attempts: int = 3) -> bytes:
    validate_startlist_url(url)
    request = Request(url, headers={"User-Agent": "NordicNumbers-WeekendInputs/1.0"})
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise WeekendInputError(
                        f"FIS returned HTTP {response.status} for {url}"
                    )
                return response.read()
        except HTTPError as exc:
            if exc.code < 500 or attempt == attempts:
                raise WeekendInputError(f"FIS request failed for {url}: {exc}") from exc
        except (TimeoutError, URLError) as exc:
            if attempt == attempts:
                raise WeekendInputError(f"FIS request failed for {url}: {exc}") from exc
        time.sleep(2 ** (attempt - 1))
    raise AssertionError("retry loop exited unexpectedly")


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), [dict(row) for row in reader]


def _parse_group_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as exc:
        raise WeekendInputError(f"invalid group date: {value!r}") from exc


def select_normal_weekend(
    schedule: Path, group_date: date
) -> tuple[list[dict[str, str]], int]:
    fields, rows = _read_csv(schedule)
    if tuple(fields) != SCHEDULE_FIELDS:
        raise WeekendInputError(
            f"unexpected weekend schedule columns: {tuple(fields)}; "
            f"expected {SCHEDULE_FIELDS}"
        )
    matching = [row for row in rows if _parse_group_date(row["Date"]) == group_date]
    normal = [
        row
        for row in matching
        if row["Distance"] not in {"Rel", "Ts"} and row["Stage"] != "1"
    ]
    for row in normal:
        if row["Sex"] not in {"M", "L"}:
            raise WeekendInputError(f"invalid normal-weekend sex: {row['Sex']!r}")
        validate_startlist_url(row["Startlist"])
    return normal, len(matching) - len(normal)


def _has_team_event(schedule: Path, group_date: date) -> bool:
    fields, rows = _read_csv(schedule)
    if tuple(fields) != SCHEDULE_FIELDS:
        raise WeekendInputError(f"unexpected weekend schedule columns: {tuple(fields)}")
    return any(
        _parse_group_date(row["Date"]) == group_date
        and row["Distance"] in {"Rel", "Ts"}
        for row in rows
    )


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise WeekendInputError(
                f"bundle output {resolved} cannot be inside accepted source directory "
                f"{source_parent}"
            )
    return resolved


def _csv_text(rows: list[dict[str, str]], fields: Iterable[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _latest_season_candidates(path: Path, sex: str) -> list[dict[str, str]]:
    fields, rows = _read_csv(path)
    required = {"ID", "Skier", "Nation", "Season", "Date", "Race"}
    if missing := sorted(required - set(fields)):
        raise WeekendInputError(f"history {path} is missing fields: {missing}")
    try:
        latest_season = max(int(row["Season"]) for row in rows if row["Season"])
    except ValueError as exc:
        raise WeekendInputError(f"history {path} has an invalid Season") from exc
    latest: dict[str, dict[str, str]] = {}
    for row in rows:
        if int(row["Season"]) != latest_season:
            continue
        athlete_id = row["ID"].strip()
        if not athlete_id or not row["Skier"].strip():
            raise WeekendInputError(f"history {path} contains an incomplete athlete")
        previous = latest.get(athlete_id)
        order = (row["Date"], int(row["Race"] or 0))
        previous_order = (
            (previous["Date"], int(previous["Race"] or 0)) if previous else ("", -1)
        )
        if order >= previous_order:
            latest[athlete_id] = row
    if not latest:
        raise WeekendInputError(f"history {path} has no latest-season athletes")
    return [
        {
            "canonical_athlete_id": row["ID"].strip(),
            "canonical_name": row["Skier"].strip(),
            "canonical_nation": row["Nation"].strip(),
            "sex": sex,
        }
        for _, row in sorted(latest.items())
    ]


def _base_event(row: dict[str, str]) -> dict[str, str]:
    return {
        "group_date": _parse_group_date(row["Date"]).isoformat(),
        "race_date": _parse_group_date(row["Race_Date"]).isoformat(),
        "race_id": validate_startlist_url(row["Startlist"]),
        "sex": row["Sex"],
        "city": row["City"],
        "country": row["Country"],
        "distance": row["Distance"],
        "technique": row["Technique"],
    }


def _apply_announced_overlay(
    *,
    candidates: list[dict[str, str]],
    selections: dict[str, AnnouncedSelection],
    athletes: dict[tuple[str, str], CanonicalAthlete],
    sex: str,
    base: dict[str, str],
    fallback_url: str,
    fallback_sha: str,
    evidence_sha: str,
    exact_confirmed_athlete_ids: set[str],
) -> tuple[list[dict[str, str]], dict[str, object]]:
    selected: dict[str, dict[str, str]] = {
        athlete["canonical_athlete_id"]: {
            **base,
            **athlete,
            "fis_competitor_id": "",
            "fis_code": "",
            "identity_match_method": "accepted_history_id",
            "selection_status": "candidate_pool",
            "source_mode": "latest_season_fallback",
            "source_url": fallback_url,
            "source_sha256": fallback_sha,
        }
        for athlete in candidates
    }
    for athlete_id in exact_confirmed_athlete_ids - set(selections):
        selected.pop(athlete_id, None)
    confirmed = 0
    excluded = 0
    sources: set[str] = set()
    evidence: list[dict[str, str]] = []
    for athlete_id, selection in selections.items():
        sources.add(selection.source_url)
        evidence.append(
            {
                "canonical_athlete_id": athlete_id,
                "status": selection.status,
                "race_id": selection.race_id,
                "source_url": selection.source_url,
                "observed_at": selection.observed_at,
            }
        )
        if selection.status == "excluded":
            selected.pop(athlete_id, None)
            excluded += 1
            continue
        canonical = athletes[(sex, athlete_id)]
        event_specific = selection.race_id != "*"
        selected[athlete_id] = {
            **base,
            "canonical_athlete_id": canonical.athlete_id,
            "canonical_name": canonical.name,
            "canonical_nation": canonical.nation,
            "sex": sex,
            "fis_competitor_id": selection.fis_competitor_id,
            "fis_code": selection.fis_code,
            "identity_match_method": "reviewed_announced_roster_id",
            "selection_status": "announced" if event_specific else "candidate_pool",
            "source_mode": (
                "announced_roster_evidence"
                if event_specific
                else "announced_weekend_pool"
            ),
            "source_url": selection.source_url,
            "source_sha256": evidence_sha,
        }
        confirmed += 1
    return (
        [selected[athlete_id] for athlete_id in sorted(selected)],
        {
            "confirmed": confirmed,
            "excluded": excluded,
            "source_urls": sorted(sources),
            "evidence": sorted(
                evidence,
                key=lambda item: (item["canonical_athlete_id"], item["status"]),
            ),
        },
    )


def build_weekend_inputs(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    aliases_path: Path,
    group_date: date,
    output_dir: Path,
    fetcher: Callable[[str], bytes] = fetch_html,
    prediction_shadow: bool = False,
    announced_rosters_path: Path | None = None,
    quota_contract_path: Path | None = None,
    men_identity_history: Path | None = None,
    ladies_identity_history: Path | None = None,
) -> dict[str, object]:
    if (men_identity_history is None) != (ladies_identity_history is None):
        raise WeekendInputError(
            "men and ladies identity history must be supplied together"
        )
    sources = (schedule, men_chrono, ladies_chrono, aliases_path) + (
        (announced_rosters_path,) if announced_rosters_path is not None else ()
    ) + (
        (quota_contract_path,) if quota_contract_path is not None else ()
    ) + (
        (men_identity_history, ladies_identity_history)
        if men_identity_history is not None and ladies_identity_history is not None
        else ()
    )
    output_dir = _validate_output_dir(output_dir, sources)
    races, excluded_specialized = select_normal_weekend(schedule, group_date)
    team_event_weekend = _has_team_event(schedule, group_date)
    quota_contract = None
    if quota_contract_path is not None:
        try:
            quota_contract = load_quota_contract(
                quota_contract_path, logical_date=group_date
            )
        except QuotaContractError as exc:
            raise WeekendInputError(str(exc)) from exc
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "logical_date": group_date.isoformat(),
        "publication": {"status": "disabled"},
        "prediction_shadow": prediction_shadow,
        "input_sha256": {path.name: _sha256(path.read_bytes()) for path in sources},
        "excluded_specialized_races": excluded_specialized,
        "team_event_weekend": team_event_weekend,
    }
    if quota_contract is not None:
        report["quota_contract"] = {
            "version": quota_contract.contract_version,
            "season": quota_contract.season,
            "sha256": _sha256(quota_contract_path.read_bytes()),
            "personal_rights_are_non_transferable": True,
        }
    if not races:
        report.update({"status": "no_work", "race_count": 0, "startlist_rows": 0})
        _atomic_text(
            output_dir / "report.json",
            json.dumps(report, indent=2, sort_keys=True) + "\n",
        )
        return report

    accepted_athletes, name_index, names_by_id = load_history(
        men_chrono, ladies_chrono
    )
    athletes = dict(accepted_athletes)
    if men_identity_history is not None and ladies_identity_history is not None:
        full_history, _full_index, _full_names = load_history(
            men_identity_history, ladies_identity_history
        )
        athletes = {**full_history, **accepted_athletes}
    aliases = load_aliases(aliases_path, athletes, names_by_id)
    announced_selections = {}
    announced_sha = ""
    exact_confirmed_by_sex: dict[str, set[str]] = defaultdict(set)
    if announced_rosters_path is not None:
        try:
            announced_selections = load_announced_rosters(
                announced_rosters_path,
                logical_date=group_date,
                athletes=athletes,
            )
        except AnnouncedRosterError as exc:
            raise WeekendInputError(str(exc)) from exc
        announced_sha = _sha256(announced_rosters_path.read_bytes())
        for (entry_sex, entry_race, athlete_id), selection in (
            announced_selections.items()
        ):
            if entry_race != "*" and selection.status == "confirmed":
                exact_confirmed_by_sex[entry_sex].add(athlete_id)
    fallback = {
        "M": _latest_season_candidates(men_chrono, "M"),
        "L": _latest_season_candidates(ladies_chrono, "L"),
    }
    event_modes: list[dict[str, object]] = []
    output_rows: dict[str, list[dict[str, str]]] = defaultdict(list)

    for race in races:
        sex = race["Sex"]
        url = race["Startlist"]
        html = fetcher(url)
        source_sha = _sha256(html)
        official = parse_official_startlist(html)
        base = _base_event(race)
        event_announcements = event_selections(
            announced_selections, sex=sex, race_id=base["race_id"]
        )
        if official:
            for athlete in official:
                alias = aliases.get((sex, athlete.fis_code))
                if alias:
                    canonical_id = alias.canonical_id
                    method = "reviewed_alias"
                else:
                    candidates = name_index.get(
                        (sex, normalized_name(athlete.name)), set()
                    )
                    if len(candidates) > 1:
                        raise WeekendInputError(
                            f"ambiguous identity for {sex}/{athlete.competitor_id} "
                            f"{athlete.name!r}; candidates={sorted(candidates)}"
                        )
                    if not candidates:
                        if not prediction_shadow:
                            raise WeekendInputError(
                                f"missing identity for {sex}/{athlete.competitor_id} "
                                f"{athlete.name!r}; candidates=[]"
                            )
                        canonical_id = f"fis:{athlete.competitor_id}"
                        method = "prediction_only_fis_identity"
                    else:
                        canonical_id = next(iter(candidates))
                        reviewed_codes = {
                            reviewed.fis_code
                            for reviewed in aliases.values()
                            if reviewed.sex == sex
                            and reviewed.canonical_id == canonical_id
                        }
                        if reviewed_codes:
                            raise WeekendInputError(
                                "FIS code changed for reviewed athlete "
                                f"{sex}/{canonical_id}; observed={athlete.fis_code}, "
                                f"reviewed={sorted(reviewed_codes)}"
                            )
                        method = "unique_normalized_name"
                canonical = athletes.get((sex, canonical_id))
                output_rows[sex].append(
                    {
                        **base,
                        "canonical_athlete_id": (
                            canonical.athlete_id if canonical else canonical_id
                        ),
                        "canonical_name": canonical.name if canonical else athlete.name,
                        "canonical_nation": (
                            canonical.nation if canonical else athlete.nation
                        ),
                        "fis_competitor_id": athlete.competitor_id,
                        "fis_code": athlete.fis_code,
                        "identity_match_method": method,
                        "selection_status": "confirmed",
                        "source_mode": "official_fis_startlist",
                        "source_url": url,
                        "source_sha256": source_sha,
                    }
                )
            source_mode = "official_fis_startlist"
            event_row_count = len(official)
            announcement_summary = {
                "available": bool(event_announcements),
                "ignored_because_official": len(event_announcements),
                "confirmed": 0,
                "excluded": 0,
                "source_urls": [],
                "evidence": [],
            }
        elif event_announcements or exact_confirmed_by_sex[sex]:
            rows, overlay = _apply_announced_overlay(
                candidates=fallback[sex],
                selections=event_announcements,
                athletes=athletes,
                sex=sex,
                base=base,
                fallback_url=url,
                fallback_sha=source_sha,
                evidence_sha=announced_sha,
                exact_confirmed_athlete_ids=exact_confirmed_by_sex[sex],
            )
            output_rows[sex].extend(rows)
            source_mode = "announced_roster_overlay"
            event_row_count = len(rows)
            announcement_summary = {
                "available": True,
                "ignored_because_official": 0,
                **overlay,
            }
        else:
            for athlete in fallback[sex]:
                output_rows[sex].append(
                    {
                        **base,
                        **athlete,
                        "fis_competitor_id": "",
                        "fis_code": "",
                        "identity_match_method": "accepted_history_id",
                        "selection_status": "candidate_pool",
                        "source_mode": "latest_season_fallback",
                        "source_url": url,
                        "source_sha256": source_sha,
                    }
                )
            source_mode = "latest_season_fallback"
            event_row_count = len(fallback[sex])
            announcement_summary = {
                "available": False,
                "ignored_because_official": 0,
                "confirmed": 0,
                "excluded": 0,
                "source_urls": [],
                "evidence": [],
            }
        event_modes.append(
            {
                "race_id": base["race_id"],
                "sex": sex,
                "source_mode": source_mode,
                "athlete_rows": event_row_count,
                "source_sha256": source_sha,
                "announced_rosters": announcement_summary,
            }
        )

    startlist_fields = STARTLIST_FIELDS
    if quota_contract is not None:
        for rows in output_rows.values():
            for row in rows:
                row.update(
                    quota_contract.assignment(
                        athlete_name=row["canonical_name"],
                        nation=row["canonical_nation"],
                        sex=row["sex"],
                        event_country=row["country"],
                        logical_date=group_date,
                        team_event_weekend=team_event_weekend,
                    )
                )
        startlist_fields = (*STARTLIST_FIELDS, *QUOTA_FIELDS)

    schedule_text = _csv_text(races, SCHEDULE_FIELDS)
    output_text = {
        "schedule.csv": schedule_text,
        "men_startlist.csv": _csv_text(output_rows["M"], startlist_fields),
        "ladies_startlist.csv": _csv_text(output_rows["L"], startlist_fields),
    }
    for name, text in output_text.items():
        _atomic_text(output_dir / name, text)
    report.update(
        {
            "status": "candidate",
            "race_count": len(races),
            "startlist_rows": sum(len(rows) for rows in output_rows.values()),
            "events": event_modes,
            "outputs": {
                name: {"sha256": _sha256(text.encode()), "rows": text.count("\n") - 1}
                for name, text in output_text.items()
            },
        }
    )
    _atomic_text(
        output_dir / "report.json", json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--group-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--announced-rosters",
        type=Path,
        help="Reviewed JSON overlay of confirmed/excluded weekend athletes",
    )
    parser.add_argument(
        "--prediction-shadow",
        action="store_true",
        help="Represent unmatched official FIS starters with private fis: IDs",
    )
    parser.add_argument(
        "--quota-contract",
        type=Path,
        help=(
            "Versioned FIS quota JSON. When supplied, append audited shared and "
            "nominative quota fields; omitted preserves the v1 startlist schema."
        ),
    )
    parser.add_argument("--men-identity-history", type=Path)
    parser.add_argument("--ladies-identity-history", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_weekend_inputs(
        schedule=args.schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        aliases_path=args.aliases,
        group_date=args.group_date,
        output_dir=args.output_dir,
        prediction_shadow=args.prediction_shadow,
        announced_rosters_path=args.announced_rosters,
        quota_contract_path=args.quota_contract,
        men_identity_history=args.men_identity_history,
        ladies_identity_history=args.ladies_identity_history,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
