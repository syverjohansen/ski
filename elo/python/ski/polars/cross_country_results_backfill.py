#!/usr/bin/env python3
"""Fetch audited cross-country result gaps into an isolated candidate artifact.

Accepted schedule and chrono files are read-only inputs. The command writes raw
FIS evidence only below an operator-provided output directory so identity
mapping and Elo replay can be reviewed separately.
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
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from cross_country_results_gap import ScheduleRace, audit_results

RESULT_FIELDS = (
    "date",
    "sex",
    "season",
    "race",
    "city",
    "country",
    "distance",
    "technique",
    "mass_start",
    "period",
    "pursuit",
    "stage",
    "final_climb",
    "elevation",
    "championship",
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


class FISResultsParser(HTMLParser):
    """Extract the page title and classified rows from FIS result HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.rows: list[dict[str, str]] = []
        self._in_title = False
        self._row: dict[str, str] | None = None
        self._capture: tuple[str, str, list[str]] | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = dict(attrs)
        classes = set((attributes.get("class") or "").split())
        if tag == "title":
            self._in_title = True
        if tag == "a" and "table-row" in classes:
            self._row = {"href": attributes.get("href") or ""}
            return
        if self._row is None:
            return
        field = None
        if {"justify-right", "pr-1", "bold"} <= classes:
            field = "place"
        elif {"justify-right", "gray", "pr-1"} <= classes:
            field = "fis_code"
        elif "athlete-name" in classes:
            field = "name"
        elif "country__name-short" in classes:
            field = "nation"
        if field:
            self._capture = (tag, field, [])

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
        if self._capture:
            self._capture[2].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if self._capture and tag == self._capture[0]:
            _, field, parts = self._capture
            if self._row is not None:
                self._row[field] = " ".join("".join(parts).split())
            self._capture = None
        if tag == "a" and self._row is not None:
            self.rows.append(self._row)
            self._row = None

    @property
    def title(self) -> str:
        return " ".join("".join(self.title_parts).split())


def parse_fis_page(html: bytes) -> FISResultsParser:
    parser = FISResultsParser()
    parser.feed(html.decode("utf-8", errors="replace"))
    parser.close()
    return parser


def validate_results_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    race_ids = query.get("raceid", [])
    sectors = query.get("sectorcode", [])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.fis-ski.com"
        or parsed.path != "/DB/general/results.html"
        or len(race_ids) != 1
        or not race_ids[0].isdigit()
        or len(sectors) != 1
        or sectors[0].upper() != "CC"
    ):
        raise BackfillError(f"unsupported FIS cross-country results URL: {url}")
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


def validate_page_identity(html: bytes, gap: ScheduleRace) -> None:
    title = parse_fis_page(html).title
    season_label = f"{gap.season - 1}/{gap.season}"
    if _normalized(gap.city) not in _normalized(title) or season_label not in title:
        raise BackfillError(
            f"FIS page identity mismatch for {gap.sex} race {gap.race}: {title!r}"
        )


def parse_classified_results(html: bytes) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    for row in parse_fis_page(html).rows:
        href = row.get("href", "")
        id_match = re.search(r"competitorid=(\d+)", href)
        rank_text = row.get("place", "")
        name = row.get("name", "")
        if not rank_text.isdigit() or not name or not id_match:
            continue
        competitor_id = id_match.group(1)
        if competitor_id in seen_ids:
            raise BackfillError(f"duplicate FIS competitor ID: {competitor_id}")
        seen_ids.add(competitor_id)
        results.append(
            {
                "place": int(rank_text),
                "fis_competitor_id": competitor_id,
                "fis_code": row.get("fis_code", ""),
                "name": name,
                "nation": row.get("nation", ""),
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
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=RESULT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _schedule_metadata(gap: ScheduleRace) -> dict[str, object]:
    return {
        "date": gap.date,
        "sex": gap.sex,
        "season": gap.season,
        "race": gap.race,
        "city": gap.city,
        "country": gap.country,
        "distance": gap.distance,
        "technique": gap.technique,
        "mass_start": gap.mass_start,
        "period": gap.period,
        "pursuit": gap.pursuit,
        "stage": gap.stage,
        "final_climb": gap.final_climb,
        "elevation": gap.elevation,
        "championship": gap.championship,
    }


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
    audit = audit_results(schedule, men_chrono, ladies_chrono, as_of)
    if not audit.missing:
        raise BackfillError("no completed individual results gaps were found")

    all_rows: list[dict[str, object]] = []
    evidence: list[RaceEvidence] = []
    for gap in audit.missing:
        validate_results_url(gap.results_url)
        html = fetcher(gap.results_url)
        validate_page_identity(html, gap)
        digest = hashlib.sha256(html).hexdigest()
        parsed = parse_classified_results(html)
        rows = [
            {
                **_schedule_metadata(gap),
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
        "schema_version": "nordicnumbers.cross-country-results-backfill/v1",
        "as_of": as_of.isoformat(),
        "completed_individual_schedule_races": audit.completed_individual,
        "completed_team_events_excluded": audit.completed_team_events,
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
