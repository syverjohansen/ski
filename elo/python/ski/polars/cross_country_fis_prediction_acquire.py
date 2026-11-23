#!/usr/bin/env python3
"""Acquire lower-tier FIS cross-country results as isolated prediction evidence.

The accepted A-tier schedule and histories are read-only. Calendar categories
are explicitly allowlisted by the prediction source contract, every request is
bounded to an official FIS cross-country URL, and discovered A-tier race IDs
are excluded before result acquisition.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

from cross_country_prediction_sources import (
    PREDICTION_ONLY_FIS_CATEGORIES,
    classify_fis_category,
    prediction_calendar_url,
)
from cross_country_results_backfill import (
    parse_classified_results,
    parse_fis_page,
    validate_results_url,
)
from cross_country_schedule_scraper import (
    canonical_result_url,
    parse_distance,
    parse_technique,
)

REPORT_SCHEMA = "nordicnumbers.fis-prediction-evidence/v1"
RESULT_FIELDS = (
    "source_categories",
    "date",
    "sex",
    "distance",
    "technique",
    "mass_start",
    "fis_race_id",
    "fis_competitor_id",
    "fis_code",
    "name",
    "nation",
    "place",
    "source_url",
    "source_sha256",
)


class FisPredictionAcquireError(RuntimeError):
    """Raised when lower-tier FIS evidence cannot be acquired safely."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(payload)
        temporary = Path(handle.name)
    temporary.replace(path)


def _atomic_text(path: Path, text: str) -> None:
    _atomic_bytes(path, text.encode("utf-8"))


def _csv_text(rows: list[dict[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=RESULT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _validate_output_dir(
    output_dir: Path, sources: Iterable[Path], *, resume: bool = False
) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise FisPredictionAcquireError(
                "prediction evidence output cannot overlap accepted sources"
            )
    existing = list(resolved.iterdir()) if resolved.exists() else []
    if existing and not resume:
        raise FisPredictionAcquireError(
            "prediction evidence output directory must be empty"
        )
    if resume and any(path.name != "raw" for path in existing):
        raise FisPredictionAcquireError(
            "resumed output may contain only the raw evidence cache"
        )
    return resolved


def _validate_fis_url(url: str, allowed_paths: set[str]) -> None:
    parsed = urlparse(url)
    sectors = parse_qs(parsed.query).get("sectorcode", [])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.fis-ski.com"
        or parsed.path not in allowed_paths
        or sectors != ["CC"]
    ):
        raise FisPredictionAcquireError(f"unsupported FIS URL: {url}")


def fetch_html(url: str, attempts: int = 3) -> bytes:
    _validate_fis_url(
        url,
        {
            "/DB/cross-country/calendar-results.html",
            "/DB/general/event-details.html",
            "/DB/general/results.html",
        },
    )
    request = Request(url, headers={"User-Agent": "NordicNumbers-Prediction/1.0"})
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise FisPredictionAcquireError(
                        f"FIS returned HTTP {response.status} for {url}"
                    )
                return response.read()
        except HTTPError as exc:
            if exc.code < 500 or attempt == attempts:
                raise FisPredictionAcquireError(
                    f"FIS request failed for {url}: {exc}"
                ) from exc
        except (TimeoutError, URLError) as exc:
            if attempt == attempts:
                raise FisPredictionAcquireError(
                    f"FIS request failed for {url}: {exc}"
                ) from exc
        time.sleep(2 ** (attempt - 1))
    raise AssertionError("retry loop exited unexpectedly")


def _soup(payload: bytes):
    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise FisPredictionAcquireError(
            "beautifulsoup4 is required for FIS prediction acquisition"
        ) from exc
    return BeautifulSoup(payload, "html.parser")


def _event_id(url: str) -> str:
    _validate_fis_url(url, {"/DB/general/event-details.html"})
    values = parse_qs(urlparse(url).query).get("eventid", [])
    if len(values) != 1 or not values[0].isdigit():
        raise FisPredictionAcquireError(f"FIS event URL has no eventid: {url}")
    return values[0]


def discover_event_urls(calendar_html: bytes) -> list[str]:
    soup = _soup(calendar_html)
    calendar = soup.find("div", id="calendardata")
    if calendar is None:
        raise FisPredictionAcquireError("FIS category page has no calendar data")
    event_urls: list[str] = []
    for link in calendar.find_all(
        "a", href=lambda value: value and "event-details.html" in value
    ):
        url = urljoin("https://www.fis-ski.com", link.get("href"))
        _event_id(url)
        if url not in event_urls:
            event_urls.append(url)
    return event_urls


def _gender(row) -> str:
    for item in row.find_all(
        "div", class_=lambda value: value and "gender__item" in value
    ):
        classes = item.get("class", [])
        if "gender__item_m" in classes:
            return "M"
        if "gender__item_l" in classes:
            return "L"
    return ""


def _race_label(row) -> str:
    labels = []
    for item in row.find_all("div", class_="clip"):
        text = " ".join(item.get_text(" ", strip=True).split())
        if any(
            token in text
            for token in ("km", "Sprint", "Relay", "Skiathlon", "Classic", "Free")
        ):
            labels.append(text)
    return max(labels, key=len) if labels else ""


def discover_result_races(event_html: bytes, *, as_of: date) -> list[dict[str, object]]:
    soup = _soup(event_html)
    content = soup.find("div", id="eventdetailscontent")
    if content is None:
        raise FisPredictionAcquireError("FIS event page has no race details")
    races = []
    for row in content.find_all("div", class_="table-row"):
        if row.find(
            "span", class_=lambda value: value and "status__item_cancelled" in value
        ):
            continue
        date_element = row.find("div", class_="timezone-date")
        raw_date = date_element.get("data-date") if date_element else None
        result = row.find(
            "a",
            href=lambda value: value and "results.html" in value and "raceid=" in value,
        )
        sex = _gender(row)
        if not raw_date or result is None or not sex:
            continue
        try:
            raced = date.fromisoformat(raw_date)
        except ValueError as exc:
            raise FisPredictionAcquireError(
                f"invalid FIS prediction race date: {raw_date!r}"
            ) from exc
        if raced > as_of:
            continue
        label = _race_label(row)
        team_sprint_qualification = "TSPQ" in row.get_text(" ", strip=True)
        distance = parse_distance(
            label, team_sprint_qualification=team_sprint_qualification
        )
        if not distance or distance in {"Rel", "Ts"}:
            continue
        technique = parse_technique(label, distance)
        if not technique:
            raise FisPredictionAcquireError(
                f"cannot parse technique from FIS race label: {label!r}"
            )
        source_url = canonical_result_url(result.get("href"))
        race_id = validate_results_url(source_url)
        races.append(
            {
                "date": raced.isoformat(),
                "sex": sex,
                "distance": distance,
                "technique": technique,
                "mass_start": "1"
                if "Mass Start" in label or "Skiathlon" in label
                else "0",
                "fis_race_id": race_id,
                "source_url": source_url,
            }
        )
    return races


def _result_unavailable_reason(result_html: bytes) -> tuple[str, str] | None:
    """Return an auditable reason when FIS provides no classified result."""
    results = _soup(result_html).find("div", id="events-info-results")
    if results is None:
        return None
    text = " ".join(results.get_text(" ", strip=True).split())
    if text.casefold() == "no results found for this competition.":
        return "fis_explicitly_reports_no_results", text
    athlete_rows = [
        row
        for row in parse_fis_page(result_html).rows
        if "competitorid=" in row.get("href", "") and row.get("name")
    ]
    if len(athlete_rows) >= 3 and not any(
        row.get("place", "").isdigit() for row in athlete_rows
    ):
        return (
            "fis_page_has_unranked_athletes",
            f"FIS returned {len(athlete_rows)} athlete rows without classified ranks",
        )
    return None


def _accepted_a_tier_race_ids(schedule: Path) -> set[str]:
    race_ids = set()
    with schedule.open(newline="", encoding="utf-8-sig") as handle:
        for line, row in enumerate(csv.DictReader(handle), start=2):
            source_url = (row.get("Startlist") or "").strip()
            if not source_url:
                raise FisPredictionAcquireError(
                    f"accepted schedule line {line} has no FIS result URL"
                )
            try:
                race_ids.add(validate_results_url(source_url))
            except Exception as exc:
                raise FisPredictionAcquireError(
                    f"accepted schedule line {line} has an invalid result URL"
                ) from exc
    return race_ids


def acquire_fis_prediction_evidence(
    *,
    season: int,
    categories: Iterable[str],
    accepted_schedule: Path,
    as_of: date,
    output_dir: Path,
    fetcher: Callable[[str], bytes] = fetch_html,
    resume: bool = False,
) -> dict[str, object]:
    """Acquire classified lower-tier results without modifying accepted history."""
    output_dir = _validate_output_dir(output_dir, (accepted_schedule,), resume=resume)
    cache_hits = 0

    def fetch_evidence(url: str, path: Path) -> bytes:
        nonlocal cache_hits
        if resume and path.is_file():
            cache_hits += 1
            return path.read_bytes()
        payload = fetcher(url)
        _atomic_bytes(path, payload)
        return payload

    normalized_categories = tuple(
        sorted({category.strip().upper() for category in categories})
    )
    if not normalized_categories:
        raise FisPredictionAcquireError("at least one FIS category is required")
    for category in normalized_categories:
        if classify_fis_category(category) != "prediction_only":
            raise FisPredictionAcquireError(
                f"category {category!r} is not prediction-only"
            )

    accepted_race_ids = _accepted_a_tier_race_ids(accepted_schedule)
    event_categories: dict[str, set[str]] = defaultdict(set)
    category_event_counts = {}
    for category in normalized_categories:
        calendar_url = prediction_calendar_url(season, category)
        calendar_html = fetch_evidence(
            calendar_url,
            output_dir / "raw" / "calendars" / f"{category}.html",
        )
        event_urls = discover_event_urls(calendar_html)
        category_event_counts[category] = len(event_urls)
        for event_url in event_urls:
            event_categories[event_url].add(category)

    discovered: dict[str, dict[str, object]] = {}
    excluded_a_tier = set()
    for event_url in sorted(event_categories):
        event_html = fetch_evidence(
            event_url,
            output_dir / "raw" / "events" / f"{_event_id(event_url)}.html",
        )
        for race in discover_result_races(event_html, as_of=as_of):
            race_id = str(race["fis_race_id"])
            if race_id in accepted_race_ids:
                excluded_a_tier.add(race_id)
                continue
            categories_for_race = set(event_categories[event_url])
            previous = discovered.get(race_id)
            if previous is not None:
                comparable = {
                    key: value for key, value in previous.items() if key != "categories"
                }
                if comparable != race:
                    raise FisPredictionAcquireError(
                        f"conflicting FIS metadata for race {race_id}"
                    )
                previous["categories"].update(categories_for_race)
            else:
                discovered[race_id] = {**race, "categories": categories_for_race}

    all_rows: list[dict[str, object]] = []
    race_reports = []
    skipped_races = []
    for race_id in sorted(discovered, key=int):
        race = discovered[race_id]
        source_url = str(race["source_url"])
        result_path = output_dir / "raw" / "results" / f"{race_id}.html"
        result_html = fetch_evidence(source_url, result_path)
        digest = _sha256(result_html)
        unavailable = _result_unavailable_reason(result_html)
        if unavailable:
            unavailable_reason, source_message = unavailable
            skipped_races.append(
                {
                    "fis_race_id": race_id,
                    "date": race["date"],
                    "sex": race["sex"],
                    "source_categories": sorted(race["categories"]),
                    "reason": unavailable_reason,
                    "source_message": source_message,
                    "source_sha256": digest,
                }
            )
            continue
        # Two finishers still provide one useful head-to-head observation. The
        # accepted-history backfill retains its stricter default of three.
        classified = parse_classified_results(result_html, minimum_results=2)
        categories_text = ";".join(sorted(race["categories"]))
        rows = [
            {
                "source_categories": categories_text,
                "date": race["date"],
                "sex": race["sex"],
                "distance": race["distance"],
                "technique": race["technique"],
                "mass_start": race["mass_start"],
                "fis_race_id": race_id,
                **result,
                "source_url": source_url,
                "source_sha256": digest,
            }
            for result in classified
        ]
        all_rows.extend(rows)
        race_reports.append(
            {
                "fis_race_id": race_id,
                "date": race["date"],
                "sex": race["sex"],
                "source_categories": sorted(race["categories"]),
                "result_rows": len(rows),
                "source_sha256": digest,
            }
        )

    if not race_reports:
        raise FisPredictionAcquireError(
            "selected FIS categories yielded no completed prediction races"
        )
    all_rows.sort(
        key=lambda row: (
            str(row["date"]),
            str(row["sex"]),
            int(row["fis_race_id"]),
            int(row["place"]),
        )
    )
    _atomic_text(output_dir / "fis-prediction-results.csv", _csv_text(all_rows))
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "season": int(season),
        "as_of": as_of.isoformat(),
        "categories": list(normalized_categories),
        "category_event_counts": category_event_counts,
        "unique_event_pages": len(event_categories),
        "acquired_races": len(race_reports),
        "skipped_races": skipped_races,
        "acquired_result_rows": len(all_rows),
        "raw_cache_hits": cache_hits,
        "excluded_accepted_a_tier_race_ids": sorted(excluded_a_tier, key=int),
        "races": race_reports,
        "accepted_history_modified": False,
        "publication_eligible": False,
        "status": "acquired_prediction_evidence_only",
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument(
        "--category",
        action="append",
        choices=sorted(PREDICTION_ONLY_FIS_CATEGORIES),
        dest="categories",
    )
    parser.add_argument("--accepted-schedule", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.allow_network:
        raise FisPredictionAcquireError("network acquisition requires --allow-network")
    report = acquire_fis_prediction_evidence(
        season=args.season,
        categories=args.categories or sorted(PREDICTION_ONLY_FIS_CATEGORIES),
        accepted_schedule=args.accepted_schedule,
        as_of=args.as_of,
        output_dir=args.output_dir,
        resume=args.resume,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
