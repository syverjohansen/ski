"""Acquire canonical cross-country prediction races from FIS calendars."""

from __future__ import annotations

import csv
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from schedule_sources import calendar_sources, championship_value

COUNTRIES = {
    "AUT": "Austria",
    "CAN": "Canada",
    "CHN": "China",
    "CZE": "Czechia",
    "EST": "Estonia",
    "FIN": "Finland",
    "FRA": "France",
    "GER": "Germany",
    "ITA": "Italy",
    "JPN": "Japan",
    "KOR": "South Korea",
    "NOR": "Norway",
    "POL": "Poland",
    "RUS": "Russia",
    "SLO": "Slovenia",
    "SUI": "Switzerland",
    "SVK": "Slovakia",
    "SWE": "Sweden",
    "USA": "USA",
}
VENUE_ALIASES = {
    "lake placid": "lake placid, ny",
    "milano cortina": "val di fiemme",
    "oslo": "holmenkollen",
    "toblach": "toblach / dobbiaco",
    "toblach/dobbiaco": "toblach / dobbiaco",
}


def load_elevations(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    elevations = {
        row["City"].strip().casefold(): row["Elevation"].strip()
        for row in rows
        if row.get("City", "").strip() and row.get("Elevation", "").strip()
    }
    if not elevations:
        raise RuntimeError(f"elevation reference contains no venues: {path}")
    return elevations


def elevation_for_city(city, elevations):
    key = city.strip().casefold()
    key = VENUE_ALIASES.get(key, key)
    if key not in elevations:
        raise RuntimeError(
            f"no exact elevation mapping for FIS venue {city!r}; add it explicitly"
        )
    return elevations[key]


def parse_distance(label, *, team_sprint_qualification=False):
    normalized = " ".join(label.split())
    if team_sprint_qualification or "Team Sprint Qualification" in normalized:
        return "Ts"
    if "Team Sprint" in normalized or (
        "Sprint" in normalized and "Qualification" not in normalized
    ):
        return ""
    if normalized.startswith("4x") or " Relay" in normalized:
        return "Rel"
    if "Sprint Qualification" in normalized:
        return "Sprint"
    if "Skiathlon" in normalized:
        for token in normalized.split():
            if "+" in token and "km" in token:
                parts = token.replace("km", "").split("+")
                if all(part.replace(".", "", 1).isdigit() for part in parts):
                    return str(sum(float(part) for part in parts)).removesuffix(".0")
        return "20"
    match = re.search(r"(\d+(?:\.\d+)?)\s*km", normalized, flags=re.IGNORECASE)
    if match:
        return match.group(1).removesuffix(".0")
    return ""


def parse_technique(label, distance):
    if distance == "Rel":
        return "N/A"
    if "Skiathlon" in label:
        return "P"
    if "Classic" in label:
        return "C"
    if "Free" in label:
        return "F"
    return ""


def canonical_result_url(url):
    parsed = urlparse(urljoin("https://www.fis-ski.com", url))
    race_ids = parse_qs(parsed.query).get("raceid", [])
    if not race_ids or not race_ids[0].strip():
        raise RuntimeError(f"FIS result URL has no raceid: {url}")
    return (
        "https://www.fis-ski.com/DB/general/results.html?"
        + urlencode({"sectorcode": "CC", "raceid": race_ids[0].strip()})
    )


def _response_soup(url, *, request_fn, soup_factory):
    response = request_fn(url, timeout=30)
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
    content = getattr(response, "content", None)
    if content is None:
        raise RuntimeError(f"FIS response had no content: {url}")
    return soup_factory(content, "html.parser")


def _gender(row):
    for item in row.find_all(
        "div", class_=lambda value: value and "gender__item" in value
    ):
        classes = item.get("class", [])
        if "gender__item_m" in classes:
            return "M"
        if "gender__item_l" in classes:
            return "L"
    return ""


def _race_label(row):
    labels = []
    for item in row.find_all("div", class_="clip"):
        text = " ".join(item.get_text(" ", strip=True).split())
        if any(
            token in text
            for token in ("km", "Sprint", "Relay", "Skiathlon", "Classic", "Free")
        ):
            labels.append(text)
    return max(labels, key=len) if labels else ""


def scrape_event(event_url, source_category, *, request_fn, soup_factory, elevations):
    soup = _response_soup(event_url, request_fn=request_fn, soup_factory=soup_factory)
    header = soup.find("h1", class_=lambda value: value and "event-header__name" in value)
    if header is None:
        raise RuntimeError(f"FIS event has no venue header: {event_url}")
    header_text = " ".join(header.get_text(" ", strip=True).split())
    if "(" not in header_text or ")" not in header_text:
        raise RuntimeError(f"cannot parse FIS event venue: {header_text!r}")
    city = header_text.rsplit("(", 1)[0].strip()
    country_code = header_text.rsplit("(", 1)[1].split(")", 1)[0].strip()
    # FIS publishes a Tour de Ski umbrella event in addition to each physical
    # venue. It contains overall standings, not independent prediction races,
    # but its date provides a stable start boundary for the stage block.
    if header_text == "FIS Tour de Ski (FIS)":
        content = soup.find("div", id="eventdetailscontent")
        dates = []
        if content is not None:
            for row in content.find_all("div", class_="table-row"):
                element = row.find("div", class_="timezone-date")
                raw_date = element.get("data-date") if element else None
                if raw_date:
                    dates.append(datetime.strptime(raw_date, "%Y-%m-%d").date())
        if not dates:
            raise RuntimeError("FIS Tour de Ski umbrella event has no start date")
        return [], min(dates)
    if country_code not in COUNTRIES:
        raise RuntimeError(
            f"unknown FIS event country code {country_code!r} in "
            f"{header_text!r}: {event_url}"
        )
    elevation = elevation_for_city(city, elevations)

    content = soup.find("div", id="eventdetailscontent")
    if content is None:
        raise RuntimeError(f"FIS event has no race details: {event_url}")
    races = []
    for row in content.find_all("div", class_="table-row"):
        cancelled = row.find("span", class_=lambda value: value and "status__item_cancelled" in value)
        if cancelled is not None:
            continue
        date_element = row.find("div", class_="timezone-date")
        raw_date = date_element.get("data-date") if date_element else None
        if not raw_date:
            continue
        try:
            race_date = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%m/%d/%Y")
        except ValueError as exc:
            raise RuntimeError(f"invalid FIS race date: {raw_date!r}") from exc
        result = row.find(
            "a", href=lambda value: value and "results.html" in value and "raceid=" in value
        )
        sex = _gender(row)
        if result is None or not sex:
            continue
        label = _race_label(row)
        team_sprint_qualification = "TSPQ" in row.get_text(" ", strip=True)
        distance = parse_distance(
            label, team_sprint_qualification=team_sprint_qualification
        )
        if not distance:
            continue
        technique = parse_technique(label, distance)
        if not technique:
            raise RuntimeError(f"cannot parse technique from FIS race label: {label!r}")
        races.append(
            {
                "date": race_date,
                "startlist_url": canonical_result_url(result.get("href")),
                "sex": sex,
                "city": city,
                "country": COUNTRIES[country_code],
                "distance": distance,
                "technique": technique,
                "ms": "1" if "Mass Start" in label or "Skiathlon" in label else "0",
                "pursuit": "1" if "Pursuit" in label else "0",
                "elevation": elevation,
                "championship": championship_value(source_category),
            }
        )
    return races, None


def mark_tour_stages(races, tour_start):
    """Mark the dynamic Tour window through its Val di Fiemme Final Climb."""
    if tour_start is None:
        return races
    final_climbs = []
    for race in races:
        race_date = datetime.strptime(race["date"], "%m/%d/%Y").date()
        if (
            race_date >= tour_start
            and "val di fiemme" in race["city"].casefold()
            and race["ms"] == "1"
            and race["technique"] == "F"
        ):
            final_climbs.append(race_date)
    if not final_climbs:
        raise RuntimeError("FIS Tour de Ski has no identifiable Final Climb")
    tour_end = max(final_climbs)
    for race in races:
        race_date = datetime.strptime(race["date"], "%m/%d/%Y").date()
        race["tour_stage"] = "1" if tour_start <= race_date <= tour_end else "0"
    return races


def scrape_cross_country_races(
    *,
    season,
    include_championships=True,
    elevation_file=None,
    request_fn=None,
    soup_factory=None,
    sleep_fn=time.sleep,
):
    """Scrape every selected FIS source, failing closed on missing source data."""
    if elevation_file is None:
        elevation_file = Path(__file__).resolve().parent / "excel365" / "elevation.csv"
    elevations = load_elevations(elevation_file)
    if request_fn is None:
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError("requests is required for FIS acquisition") from exc
        request_fn = requests.get
    if soup_factory is None:
        try:
            from bs4 import BeautifulSoup
        except ImportError as exc:
            raise RuntimeError("beautifulsoup4 is required for FIS acquisition") from exc
        soup_factory = BeautifulSoup

    races = []
    seen_events = set()
    tour_start = None
    for source_url, source_category in calendar_sources(season, include_championships):
        soup = _response_soup(
            source_url, request_fn=request_fn, soup_factory=soup_factory
        )
        calendar = soup.find("div", id="calendardata")
        if calendar is None:
            raise RuntimeError(f"FIS {source_category} source contained no calendar data")
        event_urls = []
        for link in calendar.find_all(
            "a", href=lambda value: value and "event-details.html" in value
        ):
            event_url = urljoin("https://www.fis-ski.com", link.get("href"))
            if event_url not in seen_events:
                seen_events.add(event_url)
                event_urls.append(event_url)
        if not event_urls:
            raise RuntimeError(f"FIS {source_category} source contained no events")
        source_races = []
        for event_url in event_urls:
            event_races, event_tour_start = scrape_event(
                event_url,
                source_category,
                request_fn=request_fn,
                soup_factory=soup_factory,
                elevations=elevations,
            )
            source_races.extend(event_races)
            if event_tour_start is not None:
                if tour_start is not None and tour_start != event_tour_start:
                    raise RuntimeError("FIS returned conflicting Tour de Ski start dates")
                tour_start = event_tour_start
            sleep_fn(1)
        if not source_races:
            raise RuntimeError(f"FIS {source_category} source contained no prediction races")
        races.extend(source_races)

    mark_tour_stages(races, tour_start)
    identities = [parse_qs(urlparse(race["startlist_url"]).query)["raceid"][0] for race in races]
    if len(identities) != len(set(identities)):
        raise RuntimeError("FIS acquisition returned duplicate race identities")
    return races
