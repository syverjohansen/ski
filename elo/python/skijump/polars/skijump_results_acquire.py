#!/usr/bin/env python3
"""Acquire ski-jumping result gaps into an isolated evidence bundle.

The command is intentionally unable to mutate accepted histories. It writes
raw source HTML, normalized result rows, and a checksummed report beneath an
operator-provided output directory for later identity mapping and Elo replay.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
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

from skijump_results_gap import MissingRace, find_missing_races, validate_results_url


RESULT_FIELDS = (
    "date",
    "sex",
    "season",
    "race",
    "event_kind",
    "city",
    "country",
    "hill_size",
    "race_type",
    "championship",
    "place",
    "team_name",
    "team_nation",
    "member_order",
    "fis_competitor_id",
    "fis_code",
    "name",
    "birth_year",
    "nation",
    "source_url",
    "source_sha256",
)


class AcquisitionError(RuntimeError):
    """Raised when acquisition cannot create trustworthy evidence."""


@dataclass(frozen=True)
class RaceEvidence:
    date: str
    sex: str
    season: int
    race: int
    event_kind: str
    fis_race_id: str
    status: str
    source_url: str
    source_sha256: str
    source_file: str
    result_rows: int
    output_file: str | None


def fetch_html(url: str, attempts: int = 3) -> bytes:
    validate_results_url(url)
    request = Request(url, headers={"User-Agent": "NordicNumbers-Backfill/1.0"})
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise AcquisitionError(
                        f"FIS returned HTTP {response.status} for {url}"
                    )
                return response.read()
        except HTTPError as exc:
            if exc.code < 500 or attempt == attempts:
                raise AcquisitionError(f"FIS request failed for {url}: {exc}") from exc
        except (TimeoutError, URLError) as exc:
            if attempt == attempts:
                raise AcquisitionError(f"FIS request failed for {url}: {exc}") from exc
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
        raise AcquisitionError(
            f"FIS page identity mismatch for {gap.sex} race {gap.race}: {title!r}"
        )


def _row_cells(row: object) -> list[object]:
    container = row.select_one(".g-row.justify-sb")
    return container.find_all("div", recursive=False) if container else []


def _competitor_id(row: object) -> str:
    values = parse_qs(urlparse(row.get("href", "")).query).get("competitorid", [])
    return values[0] if len(values) == 1 and values[0].isdigit() else ""


def parse_individual_results(html: bytes) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#events-info-results")
    rows = container.select("a.table-row") if container else soup.select("a.table-row")
    results: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if "table-row_theme_additional" in (row.get("class") or []):
            continue
        cells = _row_cells(row)
        if len(cells) < 6:
            continue
        rank = cells[0].get_text(" ", strip=True)
        competitor_id = _competitor_id(row)
        if not rank.isdigit() or not competitor_id:
            continue
        if competitor_id in seen:
            raise AcquisitionError(f"duplicate FIS competitor ID: {competitor_id}")
        seen.add(competitor_id)
        results.append(
            {
                "place": int(rank),
                "team_name": "",
                "team_nation": "",
                "member_order": "",
                "fis_competitor_id": competitor_id,
                "fis_code": cells[2].get_text(" ", strip=True),
                "name": cells[3].get_text(" ", strip=True),
                "birth_year": cells[4].get_text(" ", strip=True),
                "nation": cells[5].get_text(" ", strip=True),
            }
        )
    if results and len(results) < 3:
        raise AcquisitionError(
            f"FIS individual page yielded only {len(results)} classified results"
        )
    return results


def parse_team_results(html: bytes) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#events-info-results")
    rows = container.select("a.table-row") if container else soup.select("a.table-row")
    results: list[dict[str, object]] = []
    seen: set[str] = set()
    current_team: dict[str, object] | None = None
    team_member_counts: dict[tuple[int, str], int] = {}
    for row in rows:
        classes = row.get("class") or []
        cells = _row_cells(row)
        if len(cells) < 6:
            continue
        if "table-row_theme_main" in classes:
            rank = cells[0].get_text(" ", strip=True)
            if not rank.isdigit():
                current_team = None
                continue
            current_team = {
                "place": int(rank),
                "team_name": cells[3].get_text(" ", strip=True),
                "team_nation": cells[5].get_text(" ", strip=True),
            }
            team_member_counts[(int(rank), str(current_team["team_nation"]))] = 0
            continue
        if "table-row_theme_additional" not in classes or current_team is None:
            continue
        competitor_id = _competitor_id(row)
        if not competitor_id:
            continue
        if competitor_id in seen:
            raise AcquisitionError(f"duplicate FIS competitor ID: {competitor_id}")
        seen.add(competitor_id)
        bib_order = cells[1].get_text(" ", strip=True)
        order_match = re.search(r"-(\d+)$", bib_order)
        if not order_match:
            raise AcquisitionError(f"invalid FIS team-member order: {bib_order!r}")
        key = (int(current_team["place"]), str(current_team["team_nation"]))
        team_member_counts[key] += 1
        results.append(
            {
                **current_team,
                "member_order": int(order_match.group(1)),
                "fis_competitor_id": competitor_id,
                "fis_code": cells[2].get_text(" ", strip=True),
                "name": cells[3].get_text(" ", strip=True),
                "birth_year": cells[4].get_text(" ", strip=True),
                "nation": str(current_team["team_nation"]),
            }
        )
    if results:
        if len(team_member_counts) < 2:
            raise AcquisitionError("FIS team page yielded fewer than two teams")
        incomplete = [key for key, count in team_member_counts.items() if count < 2]
        if incomplete:
            raise AcquisitionError(f"FIS team page has incomplete teams: {incomplete}")
    return results


def page_status(html: bytes, event_kind: str) -> tuple[str, list[dict[str, object]]]:
    results = (
        parse_team_results(html)
        if event_kind == "team"
        else parse_individual_results(html)
    )
    if results:
        return "classified", results
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).casefold()
    if "cancelled" in text or "canceled" in text:
        return "cancelled", []
    return "no_results", []


def validate_output_dir(output_dir: Path, source_paths: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in source_paths:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise AcquisitionError(
                f"evidence output {resolved} cannot be inside accepted source directory "
                f"{source_parent}"
            )
    return resolved


def _atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def _atomic_text(path: Path, content: str) -> None:
    _atomic_bytes(path, content.encode("utf-8"))


def _csv_text(rows: list[dict[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=RESULT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def acquire_results(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_team_chrono: Path,
    ladies_team_chrono: Path,
    mixed_team_chrono: Path,
    as_of: date,
    output_dir: Path,
    fetcher: Callable[[str], bytes] = fetch_html,
) -> dict[str, object]:
    sources = (
        schedule,
        men_chrono,
        ladies_chrono,
        men_team_chrono,
        ladies_team_chrono,
        mixed_team_chrono,
    )
    output_dir = validate_output_dir(output_dir, sources)
    missing, completed_count = find_missing_races(
        schedule=schedule,
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        men_team_chrono=men_team_chrono,
        ladies_team_chrono=ladies_team_chrono,
        mixed_team_chrono=mixed_team_chrono,
        as_of=as_of,
    )
    evidence: list[RaceEvidence] = []
    all_rows: list[dict[str, object]] = []
    for gap in missing:
        fis_race_id = validate_results_url(gap.results_url)
        html = fetcher(gap.results_url)
        validate_page_identity(html, gap)
        digest = hashlib.sha256(html).hexdigest()
        basename = f"{gap.sex}-{gap.season}-{gap.race:03d}-{fis_race_id}"
        source_file = f"sources/{basename}.html"
        _atomic_bytes(output_dir / source_file, html)
        status, parsed = page_status(html, gap.event_kind)
        rows = [
            {
                "date": gap.date,
                "sex": gap.sex,
                "season": gap.season,
                "race": gap.race,
                "event_kind": gap.event_kind,
                "city": gap.city,
                "country": gap.country,
                "hill_size": gap.hill_size,
                "race_type": gap.race_type,
                "championship": int(gap.championship),
                **result,
                "source_url": gap.results_url,
                "source_sha256": digest,
            }
            for result in parsed
        ]
        output_file = f"races/{basename}.csv" if rows else None
        if output_file:
            _atomic_text(output_dir / output_file, _csv_text(rows))
        all_rows.extend(rows)
        evidence.append(
            RaceEvidence(
                date=gap.date,
                sex=gap.sex,
                season=gap.season,
                race=gap.race,
                event_kind=gap.event_kind,
                fis_race_id=fis_race_id,
                status=status,
                source_url=gap.results_url,
                source_sha256=digest,
                source_file=source_file,
                result_rows=len(rows),
                output_file=output_file,
            )
        )

    _atomic_text(output_dir / "results.csv", _csv_text(all_rows))
    unresolved = sum(item.status == "no_results" for item in evidence)
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.skijump-results-acquisition/v1",
        "as_of": as_of.isoformat(),
        "completed_schedule_races": completed_count,
        "gap_count": len(missing),
        "classified_race_count": sum(
            item.status == "classified" for item in evidence
        ),
        "cancelled_race_count": sum(item.status == "cancelled" for item in evidence),
        "unresolved_race_count": unresolved,
        "acquired_result_rows": len(all_rows),
        "identity_status": "unmapped",
        "accepted_histories_modified": False,
        "replay_eligible": bool(evidence) and unresolved == 0,
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
    parser.add_argument("--men-team-chrono", required=True, type=Path)
    parser.add_argument("--ladies-team-chrono", required=True, type=Path)
    parser.add_argument("--mixed-team-chrono", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--fail-on-unresolved", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.allow_network:
        raise AcquisitionError("network acquisition requires --allow-network")
    report = acquire_results(
        schedule=args.schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        men_team_chrono=args.men_team_chrono,
        ladies_team_chrono=args.ladies_team_chrono,
        mixed_team_chrono=args.mixed_team_chrono,
        as_of=args.as_of,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if args.fail_on_unresolved and report["unresolved_race_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
