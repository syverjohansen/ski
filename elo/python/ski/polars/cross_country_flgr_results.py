"""Build isolated, validated FLGR result candidates for the Russian Elo bridge."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup

SCHEMA_VERSION = "nordicnumbers.flgr-results-candidate/v1"
SOURCE_DOMAIN = "flgr-results.ru"
RACE_PATH = re.compile(r"^/results/(?P<competition_id>\d+)/(?P<race_id>\d+)/?$")
ATHLETE_PATH = re.compile(r"^/athletes/(?P<competitor_id>\d+)/?$")
SOURCE_DATE = re.compile(
    r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})(?:\.(?P<year>\d{4}))?"
)
DISTANCE = re.compile(r"(?P<distance>\d+(?:[.,]\d+)?)\s*км", re.IGNORECASE)


class FlgrContractError(ValueError):
    """Raised when FLGR evidence is unsafe to use as a bridge candidate."""


@dataclass(frozen=True)
class SourceIdentity:
    competition_id: str
    race_id: str

    @property
    def event_id(self) -> str:
        return f"flgr:{self.competition_id}:{self.race_id}"


def parse_source_url(source_url: str) -> SourceIdentity:
    parsed = urlparse(source_url)
    if parsed.scheme != "https" or parsed.hostname != SOURCE_DOMAIN:
        raise FlgrContractError("source URL must be an HTTPS flgr-results.ru URL")
    match = RACE_PATH.fullmatch(parsed.path)
    if not match:
        raise FlgrContractError("source URL must identify one FLGR result race")
    return SourceIdentity(**match.groupdict())


def parse_season_date(source_text: str, season: int) -> date:
    match = SOURCE_DATE.search(source_text)
    if not match:
        raise FlgrContractError(f"could not parse FLGR race date: {source_text!r}")
    day = int(match.group("day"))
    month = int(match.group("month"))
    explicit_year = match.group("year")
    year = int(explicit_year) if explicit_year else season - 1 if month >= 7 else season
    try:
        parsed = date(year, month, day)
    except ValueError as exc:
        raise FlgrContractError(f"invalid FLGR race date: {source_text!r}") from exc
    expected_season = parsed.year + 1 if parsed.month >= 7 else parsed.year
    if expected_season != season:
        raise FlgrContractError(
            f"race date {parsed.isoformat()} does not belong to season {season}"
        )
    return parsed


def parse_race_label(label: str) -> dict[str, object]:
    normalized = " ".join(label.split())
    lowered = normalized.lower()
    if "командный спринт" in lowered or "эстафета" in lowered:
        raise FlgrContractError("team events are outside the individual bridge contract")
    if "квалиф" in lowered or "полуфинал" in lowered:
        raise FlgrContractError("qualification and semifinal rows are not classified races")
    if "лыжероллер" in lowered:
        raise FlgrContractError("roller-ski races are outside the bridge contract")

    if "спринт" in lowered:
        distance = "Sprint"
    elif match := DISTANCE.search(normalized):
        distance = match.group("distance").replace(",", ".")
    else:
        raise FlgrContractError(f"could not determine race distance: {label!r}")

    if "кл" in lowered:
        technique = "C"
    elif "св" in lowered:
        technique = "F"
    elif "скиатлон" in lowered:
        technique = "P"
    else:
        raise FlgrContractError(f"could not determine race technique: {label!r}")

    return {
        "distance": distance,
        "technique": technique,
        "mass_start": "масс" in lowered or " мс" in lowered,
    }


def _source_name(athlete_link) -> str:
    name = " ".join(athlete_link.get_text(" ", strip=True).split())
    if not name:
        raise FlgrContractError("classified result has an empty athlete name")
    return name


def parse_classified_results(soup: BeautifulSoup) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    seen_competitors: set[str] = set()
    sanctions = soup.find("div", class_="race-sanctions")
    sanction_rows = {id(row) for row in sanctions.find_all("tr")} if sanctions else set()

    for row in soup.find_all("tr", class_="ant-table-row"):
        if id(row) in sanction_rows or "separator" in str(row.get("data-row-key", "")):
            continue
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        try:
            place = int(cells[0].get_text(strip=True))
        except ValueError:
            continue
        if place <= 0:
            continue
        athlete_link = row.find("a", href=ATHLETE_PATH)
        if athlete_link is None:
            continue
        match = ATHLETE_PATH.fullmatch(athlete_link["href"])
        competitor_id = match.group("competitor_id")
        if competitor_id in seen_competitors:
            raise FlgrContractError(
                f"duplicate athlete flgr:{competitor_id} in classified results"
            )
        seen_competitors.add(competitor_id)

        birth_year = None
        for cell in cells:
            text = cell.get_text(strip=True)
            if text.isdigit() and 1950 <= int(text) <= 2015:
                birth_year = int(text)
                break
        results.append(
            {
                "athlete_id": f"flgr:{competitor_id}",
                "source_competitor_id": competitor_id,
                "source_name": _source_name(athlete_link),
                "birth_year": birth_year,
                "place": place,
            }
        )

    if len(results) < 2:
        raise FlgrContractError("race does not contain at least two classified results")
    return sorted(results, key=lambda result: (result["place"], result["athlete_id"]))


def build_race_candidate(
    html: bytes,
    *,
    source_url: str,
    season: int,
    sex: str,
    as_of: date,
    retrieved_at: datetime,
) -> dict[str, object]:
    if sex not in {"M", "L"}:
        raise FlgrContractError("sex must be M or L")
    identity = parse_source_url(source_url)
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("title")
    if title is None:
        raise FlgrContractError("race page is missing a title")
    source_title = " ".join(title.get_text(" ", strip=True).split())

    date_element = soup.find("div", class_="race-info__date")
    if date_element is None:
        raise FlgrContractError("race page is missing its race date")
    race_date = parse_season_date(date_element.get_text(" ", strip=True), season)
    if race_date > as_of:
        raise FlgrContractError(
            f"future-dated race {race_date.isoformat()} exceeds as-of {as_of.isoformat()}"
        )
    if race_date.month in {6, 7, 8, 9}:
        raise FlgrContractError("summer events are outside the snow-race bridge contract")

    place_element = soup.find("div", class_="race-info__place")
    source_city = (
        " ".join(place_element.get_text(" ", strip=True).split())
        if place_element
        else None
    )
    race_fields = parse_race_label(source_title)
    results = parse_classified_results(soup)
    source_sha256 = hashlib.sha256(html).hexdigest()

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": identity.event_id,
        "competition_id": identity.competition_id,
        "source_race_id": identity.race_id,
        "source_url": source_url,
        "source_sha256": source_sha256,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "date": race_date.isoformat(),
        "season": season,
        "sex": sex,
        "source_title": source_title,
        "source_city": source_city,
        **race_fields,
        "classified_result_count": len(results),
        "results": results,
    }


def validate_candidate_set(
    candidates: list[dict[str, object]],
    *,
    accepted_checksums: dict[str, str] | None = None,
) -> None:
    event_ids: set[str] = set()
    for candidate in candidates:
        event_id = str(candidate["event_id"])
        if event_id in event_ids:
            raise FlgrContractError(f"duplicate candidate event: {event_id}")
        event_ids.add(event_id)
        if (
            accepted_checksums
            and event_id in accepted_checksums
            and candidate["source_sha256"] != accepted_checksums[event_id]
        ):
            raise FlgrContractError(
                f"accepted source changed for {event_id}; explicit review is required"
            )


def write_candidate(candidate: dict[str, object], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise FlgrContractError("output directory must be empty")
    manifest_path = output_dir / "flgr-candidate.json"
    results_path = output_dir / "flgr-results.csv"
    manifest_path.write_text(
        json.dumps(candidate, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    fieldnames = [
        "event_id",
        "date",
        "season",
        "sex",
        "distance",
        "technique",
        "mass_start",
        "athlete_id",
        "source_competitor_id",
        "source_name",
        "birth_year",
        "place",
        "source_url",
        "source_sha256",
    ]
    with results_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for result in candidate["results"]:
            writer.writerow(
                {
                    **{key: candidate[key] for key in fieldnames if key in candidate},
                    **result,
                }
            )
    return manifest_path, results_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--race-html", type=Path, required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--sex", choices=("M", "L"), required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    candidate = build_race_candidate(
        args.race_html.read_bytes(),
        source_url=args.source_url,
        season=args.season,
        sex=args.sex,
        as_of=args.as_of,
        retrieved_at=datetime.now(UTC),
    )
    validate_candidate_set([candidate])
    write_candidate(candidate, args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
