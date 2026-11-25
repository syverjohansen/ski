#!/usr/bin/env python3
"""Acquire exact-date official FIS Nordic Combined startlist evidence.

The command writes only to an isolated evidence directory. It never selects a
nearby event, invents entrants, resolves Nordic Numbers IDs, or mutates the
accepted startlist CSVs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
import time
import unicodedata
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from nordic_combined_results_gap import event_kind, validate_results_url


STARTLIST_FIELDS = (
    "logical_date", "sex", "event_kind", "race", "city", "country",
    "distance", "race_type", "source_order", "team_name", "team_nation",
    "member_order", "fis_competitor_id", "fis_code", "name", "birth_year",
    "nation", "source_url", "source_sha256",
)


class StartlistAcquisitionError(RuntimeError):
    """Raised when trustworthy exact-date evidence cannot be produced."""


@dataclass(frozen=True)
class EventEvidence:
    logical_date: str
    sex: str
    event_kind: str
    race: int
    fis_race_id: str | None
    status: str
    source_url: str
    source_sha256: str | None
    source_file: str | None
    entrant_rows: int
    output_file: str | None


def fetch_html(url: str, attempts: int = 3) -> bytes:
    validate_results_url(url)
    request = Request(url, headers={"User-Agent": "NordicNumbers-Startlist/1.0"})
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise StartlistAcquisitionError(
                        f"FIS returned HTTP {response.status} for {url}"
                    )
                return response.read()
        except HTTPError as error:
            if error.code < 500 or attempt == attempts:
                raise StartlistAcquisitionError(
                    f"FIS request failed for {url}: {error}"
                ) from error
        except (TimeoutError, URLError) as error:
            if attempt == attempts:
                raise StartlistAcquisitionError(
                    f"FIS request failed for {url}: {error}"
                ) from error
        time.sleep(2 ** (attempt - 1))
    raise AssertionError("retry loop exited unexpectedly")


def _normalized(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(character for character in decomposed if character.isalnum()).lower()


def _schedule_date(value: str) -> date:
    for pattern in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            pass
    raise StartlistAcquisitionError(f"unsupported schedule date: {value!r}")


def _season_label(logical_date: date) -> str:
    start_year = logical_date.year if logical_date.month >= 7 else logical_date.year - 1
    return f"{start_year}/{start_year + 1}"


def validate_page_identity(html: bytes, *, city: str, logical_date: date) -> None:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    if _normalized(city) not in _normalized(title) or _season_label(logical_date) not in title:
        raise StartlistAcquisitionError(
            f"FIS page identity mismatch for {city} on {logical_date}: {title!r}"
        )


def _row_cells(row: object) -> list[object]:
    container = row.select_one(".g-row.justify-sb")
    return container.find_all("div", recursive=False) if container else []


def _competitor_id(row: object) -> str:
    values = parse_qs(urlparse(row.get("href", "")).query).get("competitorid", [])
    return values[0] if len(values) == 1 and values[0].isdigit() else ""


def parse_individual_entrants(html: bytes) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#events-info-results")
    rows = container.select("a.table-row") if container else soup.select("a.table-row")
    entrants: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if "table-row_theme_additional" in (row.get("class") or []):
            continue
        cells = _row_cells(row)
        if len(cells) < 6:
            continue
        source_order = cells[0].get_text(" ", strip=True)
        competitor_id = _competitor_id(row)
        if not source_order.isdigit() or not competitor_id:
            continue
        if competitor_id in seen:
            raise StartlistAcquisitionError(f"duplicate FIS competitor ID: {competitor_id}")
        seen.add(competitor_id)
        entrants.append({
            "source_order": int(source_order), "team_name": "", "team_nation": "",
            "member_order": "", "fis_competitor_id": competitor_id,
            "fis_code": cells[2].get_text(" ", strip=True),
            "name": cells[3].get_text(" ", strip=True),
            "birth_year": cells[4].get_text(" ", strip=True),
            "nation": cells[5].get_text(" ", strip=True),
        })
    if entrants and len(entrants) < 3:
        raise StartlistAcquisitionError(
            f"FIS individual page yielded only {len(entrants)} entrants"
        )
    return entrants


def parse_team_entrants(html: bytes) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#events-info-results")
    rows = container.select("a.table-row") if container else soup.select("a.table-row")
    entrants: list[dict[str, object]] = []
    seen: set[str] = set()
    current: dict[str, object] | None = None
    counts: dict[tuple[int, str], int] = {}
    for row in rows:
        classes = row.get("class") or []
        cells = _row_cells(row)
        if len(cells) < 6:
            continue
        if "table-row_theme_main" in classes:
            order = cells[0].get_text(" ", strip=True)
            if not order.isdigit():
                current = None
                continue
            current = {
                "source_order": int(order),
                "team_name": cells[3].get_text(" ", strip=True),
                "team_nation": cells[5].get_text(" ", strip=True),
            }
            counts[(int(order), str(current["team_nation"]))] = 0
            continue
        if "table-row_theme_additional" not in classes or current is None:
            continue
        competitor_id = _competitor_id(row)
        if not competitor_id:
            continue
        if competitor_id in seen:
            raise StartlistAcquisitionError(f"duplicate FIS competitor ID: {competitor_id}")
        seen.add(competitor_id)
        bib = cells[1].get_text(" ", strip=True)
        try:
            member_order = int(bib.rsplit("-", 1)[1])
        except (IndexError, ValueError) as error:
            raise StartlistAcquisitionError(f"invalid FIS team-member order: {bib!r}") from error
        key = (int(current["source_order"]), str(current["team_nation"]))
        counts[key] += 1
        entrants.append({
            **current, "member_order": member_order,
            "fis_competitor_id": competitor_id,
            "fis_code": cells[2].get_text(" ", strip=True),
            "name": cells[3].get_text(" ", strip=True),
            "birth_year": cells[4].get_text(" ", strip=True),
            "nation": str(current["team_nation"]),
        })
    if entrants:
        if len(counts) < 2:
            raise StartlistAcquisitionError("FIS team page yielded fewer than two teams")
        incomplete = [key for key, count in counts.items() if count < 2]
        if incomplete:
            raise StartlistAcquisitionError(f"FIS team page has incomplete teams: {incomplete}")
    return entrants


def _atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=STARTLIST_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise StartlistAcquisitionError(
                f"evidence output {resolved} cannot be inside accepted source directory {parent}"
            )
    return resolved


def acquire_startlists(
    *, schedule: Path, logical_date: date, output_dir: Path,
    protected_sources: Iterable[Path] = (),
    fetcher: Callable[[str], bytes] = fetch_html,
) -> dict[str, object]:
    output_dir = validate_output_dir(output_dir, (schedule, *protected_sources))
    with schedule.open(encoding="utf-8", newline="") as handle:
        schedule_rows = list(csv.DictReader(handle))
    selected = [row for row in schedule_rows if _schedule_date(row.get("Date", "")) == logical_date]
    evidence: list[EventEvidence] = []
    all_rows: list[dict[str, object]] = []
    for index, row in enumerate(selected, 1):
        sex = row.get("Sex", "").strip()
        try:
            kind = event_kind(row.get("RaceType", ""))
        except ValueError as error:
            raise StartlistAcquisitionError(str(error)) from error
        if sex == "Mixed" and kind == "individual":
            raise StartlistAcquisitionError("Mixed schedule rows must be team events")
        race = int(row.get("Race") or index)
        url = row.get("Startlist", "").strip()
        if not url:
            evidence.append(EventEvidence(logical_date.isoformat(), sex, kind, race, None,
                "not_available", "", None, None, 0, None))
            continue
        try:
            fis_race_id = validate_results_url(url)
        except ValueError as error:
            raise StartlistAcquisitionError(str(error)) from error
        html = fetcher(url)
        validate_page_identity(html, city=row.get("City", ""), logical_date=logical_date)
        digest = hashlib.sha256(html).hexdigest()
        basename = f"{sex.lower()}-{logical_date.isoformat()}-{race:03d}-{fis_race_id}"
        source_file = f"sources/{basename}.html"
        _atomic_bytes(output_dir / source_file, html)
        parsed = parse_team_entrants(html) if kind in {"team", "team_sprint"} else parse_individual_entrants(html)
        rows = [{
            "logical_date": logical_date.isoformat(), "sex": sex,
            "event_kind": kind, "race": race, "city": row.get("City", ""),
            "country": row.get("Country", ""), "distance": row.get("Distance", ""),
            "race_type": row.get("RaceType", ""), **entrant,
            "source_url": url, "source_sha256": digest,
        } for entrant in parsed]
        output_file = f"events/{basename}.csv" if rows else None
        if output_file:
            _atomic_bytes(output_dir / output_file, _csv_text(rows).encode())
        all_rows.extend(rows)
        evidence.append(EventEvidence(logical_date.isoformat(), sex, kind, race, fis_race_id,
            "official" if rows else "not_available", url, digest, source_file,
            len(rows), output_file))
    startlists = output_dir / "startlists.csv"
    _atomic_bytes(startlists, _csv_text(all_rows).encode())
    unresolved = sum(item.status != "official" for item in evidence)
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.nordic-combined-startlist-acquisition/v1",
        "logical_date": logical_date.isoformat(),
        "scheduled_event_count": len(selected),
        "official_event_count": sum(item.status == "official" for item in evidence),
        "unresolved_event_count": unresolved,
        "entrant_row_count": len(all_rows),
        "startlists_sha256": hashlib.sha256(startlists.read_bytes()).hexdigest(),
        "status": "no_work" if not selected else ("ready" if unresolved == 0 else "waiting"),
        "accepted_startlists_modified": False,
        "normalization_status": "source_identities_only",
        "prediction_eligible": bool(selected) and unresolved == 0,
        "events": [asdict(item) for item in evidence],
    }
    _atomic_bytes(output_dir / "report.json", (json.dumps(report, indent=2, sort_keys=True) + "\n").encode())
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--logical-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--protect-source", action="append", type=Path, default=[])
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--fail-if-waiting", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.allow_network:
        raise StartlistAcquisitionError("network acquisition requires --allow-network")
    report = acquire_startlists(
        schedule=args.schedule, logical_date=args.logical_date,
        output_dir=args.output_dir, protected_sources=args.protect_source,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if args.fail_if_waiting and report["status"] == "waiting" else 0


if __name__ == "__main__":
    raise SystemExit(main())
