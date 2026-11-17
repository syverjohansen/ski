#!/usr/bin/env python3
"""Fetch audited Alpine results gaps into an isolated candidate artifact.

The command never modifies the accepted schedule, scrape, or chrono files.  It
uses those files only to discover completed gaps, then writes raw FIS evidence
under an operator-provided output directory for later identity mapping and Elo
replay.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import tempfile
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from alpine_results_gap import MissingRace, find_missing_races


RESULT_FIELDS = (
    "date",
    "sex",
    "season",
    "race",
    "city",
    "country",
    "distance",
    "place",
    "fis_competitor_id",
    "fis_code",
    "name",
    "nation",
    "source_url",
    "source_sha256",
)


class BackfillError(RuntimeError):
    """Raised when acquisition cannot produce a trustworthy candidate."""


@dataclass(frozen=True)
class RaceEvidence:
    date: str
    sex: str
    season: int
    race: int
    source_url: str
    source_sha256: str
    result_rows: int
    output_file: str


def validate_results_url(url: str) -> str:
    parsed = urlparse(url)
    race_ids = parse_qs(parsed.query).get("raceid", [])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.fis-ski.com"
        or parsed.path != "/DB/general/results.html"
        or len(race_ids) != 1
        or not race_ids[0].isdigit()
    ):
        raise BackfillError(f"unsupported FIS results URL: {url}")
    return race_ids[0]


def fetch_html(url: str, attempts: int = 3) -> bytes:
    validate_results_url(url)
    request = Request(url, headers={"User-Agent": "NordicNumbers-Backfill/1.0"})
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise BackfillError(f"FIS returned HTTP {response.status} for {url}")
                return response.read()
        except HTTPError as exc:
            if exc.code < 500 or attempt == attempts:
                raise BackfillError(f"FIS request failed for {url}: {exc}") from exc
        except (TimeoutError, URLError) as exc:
            if attempt == attempts:
                raise BackfillError(f"FIS request failed for {url}: {exc}") from exc
        time.sleep(2 ** (attempt - 1))
    raise AssertionError("retry loop exited unexpectedly")


def _normalized(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(character for character in decomposed if character.isalnum()).lower()


def validate_page_identity(html: bytes, gap: MissingRace) -> None:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    season_label = f"{gap.season - 1}/{gap.season}"
    if _normalized(gap.city) not in _normalized(title) or season_label not in title:
        raise BackfillError(
            f"FIS page identity mismatch for {gap.sex} race {gap.race}: {title!r}"
        )


def parse_classified_results(html: bytes) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#events-info-results")
    rows = container.select("a.table-row") if container else soup.select("a.table-row")
    results: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    for row in rows:
        rank_element = row.select_one(".justify-right.pr-1.bold")
        name_element = row.select_one(".athlete-name")
        href = row.get("href", "")
        id_match = re.search(r"competitorid=(\d+)", href)
        rank_text = rank_element.get_text(strip=True) if rank_element else ""
        if not rank_text.isdigit() or not name_element or not id_match:
            continue
        competitor_id = id_match.group(1)
        if competitor_id in seen_ids:
            raise BackfillError(f"duplicate FIS competitor ID: {competitor_id}")
        seen_ids.add(competitor_id)
        fis_code_element = row.select_one(".justify-right.gray.pr-1")
        nation_element = row.select_one(".country__name-short")
        results.append(
            {
                "place": int(rank_text),
                "fis_competitor_id": competitor_id,
                "fis_code": (
                    fis_code_element.get_text(strip=True) if fis_code_element else ""
                ),
                "name": name_element.get_text(" ", strip=True),
                "nation": nation_element.get_text(strip=True) if nation_element else "",
            }
        )
    if len(results) < 3:
        raise BackfillError(f"FIS page yielded only {len(results)} classified results")
    return results


def validate_output_dir(output_dir: Path, source_paths: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in source_paths:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise BackfillError(
                f"candidate output {resolved} cannot be inside accepted source directory "
                f"{source_parent}"
            )
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]]) -> str:
    import io

    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=RESULT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def acquire_backfill(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    as_of: date,
    output_dir: Path,
    fetcher: Callable[[str], bytes] = fetch_html,
) -> dict[str, object]:
    output_dir = validate_output_dir(
        output_dir, (schedule, men_chrono, ladies_chrono)
    )
    missing, completed_count = find_missing_races(
        schedule, men_chrono, ladies_chrono, as_of
    )
    if not missing:
        raise BackfillError("no completed results gaps were found")

    all_rows: list[dict[str, object]] = []
    evidence: list[RaceEvidence] = []
    for gap in missing:
        validate_results_url(gap.results_url)
        html = fetcher(gap.results_url)
        validate_page_identity(html, gap)
        digest = hashlib.sha256(html).hexdigest()
        parsed = parse_classified_results(html)
        rows = [
            {
                "date": gap.date,
                "sex": gap.sex,
                "season": gap.season,
                "race": gap.race,
                "city": gap.city,
                "country": gap.country,
                "distance": gap.distance,
                **result,
                "source_url": gap.results_url,
                "source_sha256": digest,
            }
            for result in parsed
        ]
        relative_file = f"races/{gap.sex}-{gap.season}-{gap.race:03d}.csv"
        _atomic_text(output_dir / relative_file, _csv_text(rows))
        all_rows.extend(rows)
        evidence.append(
            RaceEvidence(
                date=gap.date,
                sex=gap.sex,
                season=gap.season,
                race=gap.race,
                source_url=gap.results_url,
                source_sha256=digest,
                result_rows=len(rows),
                output_file=relative_file,
            )
        )

    _atomic_text(output_dir / "results.csv", _csv_text(all_rows))
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.alpine-results-backfill/v1",
        "as_of": as_of.isoformat(),
        "completed_schedule_races": completed_count,
        "acquired_race_count": len(evidence),
        "acquired_result_rows": len(all_rows),
        "identity_status": "unmapped",
        "races": [asdict(item) for item in evidence],
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="Required acknowledgement before contacting FIS",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.allow_network:
        raise BackfillError("network acquisition requires --allow-network")
    report = acquire_backfill(
        schedule=args.schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        as_of=args.as_of,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
