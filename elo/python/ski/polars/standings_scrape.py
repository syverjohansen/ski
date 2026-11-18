#!/usr/bin/env python3
"""Scrape validated cross-country World Cup standings from the official FIS site."""

from __future__ import annotations

import argparse
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from standings_contract import (
    atomic_write_standings,
    canonicalize_standings,
    season_for_date,
    validate_standings,
)

LOGGER = logging.getLogger(__name__)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def standings_url(season: int, gender: str) -> str:
    gender_code = "W" if gender == "ladies" else "M"
    return (
        "https://www.fis-ski.com/DB/cross-country/cup-standings.html"
        f"?cupcode=WC&disciplinecode=ALL&gendercode={gender_code}"
        f"&nationcode=&seasoncode={season}&sectorcode=CC"
    )


def _athlete_id(href: str) -> str | None:
    match = re.search(r"competitorid=(\d+)", href)
    return match.group(1) if match else None


def parse_standings(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    records = []
    for row in soup.select("a.table-row"):
        name = row.select_one(".justify-left.bold.align-xs-top")
        nation = row.select_one(".country__name-short")
        values = row.select(".justify-right.bold")
        if name is None or nation is None or len(values) < 2:
            continue
        try:
            ranking = int(values[0].get_text(strip=True))
            points = int(values[1].get_text(strip=True).replace("'", ""))
        except ValueError:
            continue
        records.append(
            {
                "Skier": name.get_text(" ", strip=True),
                "ID": _athlete_id(row.get("href", "")),
                "Nation": nation.get_text(strip=True),
                "Ranking": ranking,
                "Points": points,
            }
        )
    return records


def scrape(url: str, session: requests.Session | None = None) -> list[dict]:
    client = session or requests.Session()
    response = client.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return parse_standings(response.text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    accepted = Path(__file__).resolve().parent / "excel365"
    parser.add_argument("--season", type=int, default=season_for_date(date.today()))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "excel365",
    )
    parser.add_argument("--men-history", type=Path, default=accepted / "men_chrono.csv")
    parser.add_argument(
        "--ladies-history", type=Path, default=accepted / "ladies_chrono.csv"
    )
    parser.add_argument(
        "--identity-aliases",
        type=Path,
        default=accepted / "fis_identity_aliases.csv",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args()
    genders = ("men", "ladies")
    histories = {"men": args.men_history, "ladies": args.ladies_history}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            gender: executor.submit(scrape, standings_url(args.season, gender))
            for gender in genders
        }
        records = {
            gender: canonicalize_standings(
                future.result(), histories[gender], gender, args.identity_aliases
            )
            for gender, future in futures.items()
        }
    for gender in genders:
        validate_standings(records[gender], gender)
    for gender, filename in (
        ("men", "men_standings.csv"),
        ("ladies", "ladies_standings.csv"),
    ):
        count = atomic_write_standings(
            records[gender], args.output_dir / filename, gender
        )
        LOGGER.info("Published %s validated %s rows", count, gender)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
