#!/usr/bin/env python3
"""Acquire bounded FantasyXC roster evidence from its public RSS mirror."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

FEED_URL = "https://fxtwitter.com/FantasyXC/feed.xml"
FEED_URLS = (
    FEED_URL,
    "https://fixupx.com/FantasyXC/feed.xml",
)
REPORT_SCHEMA = "nordicnumbers.cross-country-fantasyxc-feed-report/v1"
ITEMS_SCHEMA = "nordicnumbers.cross-country-fantasyxc-feed-items/v1"
MAX_FEED_BYTES = 2 * 1024 * 1024
USER_AGENT = (
    "Mozilla/5.0 (compatible; NordicNumbers/1.0; +https://nordicnumbers.com)"
)


class FantasyXCFeedError(RuntimeError):
    """The public roster-evidence feed could not be acquired safely."""


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    ).encode()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def validate_feed_url(url: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"fxtwitter.com", "fixupx.com"}
        or parsed.path != "/FantasyXC/feed.xml"
        or parsed.query
        or parsed.fragment
    ):
        raise FantasyXCFeedError(f"unsupported FantasyXC feed URL: {url}")


def fetch_feed(
    *,
    url: str = FEED_URL,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
    attempts: int = 3,
    timeout_seconds: int = 20,
) -> bytes:
    """Fetch the fixed feed with bounded transient retries and a size cap."""

    validate_feed_url(url)
    if attempts < 1:
        raise ValueError("attempts must be positive")
    request = Request(
        url,
        headers={"Accept": "application/rss+xml", "User-Agent": USER_AGENT},
    )
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with opener(request, timeout=timeout_seconds) as response:
                status = getattr(response, "status", 200)
                if status != 200:
                    raise FantasyXCFeedError(f"FantasyXC feed returned HTTP {status}")
                declared = response.headers.get("Content-Length")
                if declared and int(declared) > MAX_FEED_BYTES:
                    raise FantasyXCFeedError("FantasyXC feed exceeds the size limit")
                payload = response.read(MAX_FEED_BYTES + 1)
                if not payload:
                    raise FantasyXCFeedError("FantasyXC feed returned an empty body")
                if len(payload) > MAX_FEED_BYTES:
                    raise FantasyXCFeedError("FantasyXC feed exceeds the size limit")
                return payload
        except HTTPError as exc:
            last_error = exc
            exc.close()
            if exc.code != 429 and not 500 <= exc.code <= 599:
                break
        except (TimeoutError, URLError) as exc:
            last_error = exc
        except FantasyXCFeedError:
            raise
        if attempt < attempts:
            sleep(float(2 ** (attempt - 1)))
    raise FantasyXCFeedError(
        f"FantasyXC feed failed after {attempts} attempts: {last_error}"
    )


def fetch_feed_with_fallback(
    *,
    urls: tuple[str, ...] = FEED_URLS,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[bytes, str]:
    """Try fixed FxEmbed realms in order and retain the successful realm."""

    if not urls:
        raise FantasyXCFeedError("no FantasyXC feed URLs are configured")
    failures: list[str] = []
    for url in urls:
        validate_feed_url(url)
        try:
            return fetch_feed(url=url, opener=opener, sleep=sleep), url
        except FantasyXCFeedError as exc:
            failures.append(f"{url}: {exc}")
    raise FantasyXCFeedError(
        "all configured FantasyXC feed realms failed: " + "; ".join(failures)
    )


def _required_text(item: ElementTree.Element, tag: str, *, index: int) -> str:
    value = (item.findtext(tag) or "").strip()
    if not value:
        raise FantasyXCFeedError(f"feed item {index} has no {tag}")
    return value


def parse_feed(
    payload: bytes,
    *,
    logical_date: date,
    as_of: date,
    lookback_days: int,
) -> list[dict[str, str]]:
    """Parse and date-bound RSS items without interpreting roster claims."""

    if lookback_days < 1 or lookback_days > 30:
        raise FantasyXCFeedError("lookback_days must be between 1 and 30")
    if as_of > logical_date + timedelta(days=7):
        raise FantasyXCFeedError("as_of is outside the logical weekend window")
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        raise FantasyXCFeedError(f"FantasyXC feed is invalid XML: {exc}") from exc
    channel = root.find("channel") if root.tag == "rss" else None
    if channel is None:
        raise FantasyXCFeedError("FantasyXC feed must be RSS 2.0")
    lower_bound = logical_date - timedelta(days=lookback_days)
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, item in enumerate(channel.findall("item"), start=1):
        link = _required_text(item, "link", index=index)
        match = re.fullmatch(r"https://x\.com/FantasyXC/status/(\d+)", link)
        if not match:
            raise FantasyXCFeedError(f"feed item {index} has an unsupported link")
        status_id = match.group(1)
        if status_id in seen:
            raise FantasyXCFeedError(f"duplicate FantasyXC status ID: {status_id}")
        seen.add(status_id)
        try:
            published = parsedate_to_datetime(
                _required_text(item, "pubDate", index=index)
            )
            if published.tzinfo is None:
                raise ValueError("pubDate has no timezone")
            published = published.astimezone(UTC)
        except (TypeError, ValueError) as exc:
            raise FantasyXCFeedError(
                f"feed item {index} has an invalid pubDate"
            ) from exc
        if not lower_bound <= published.date() <= as_of:
            continue
        items.append(
            {
                "status_id": status_id,
                "source_url": link,
                "published_at": published.isoformat().replace("+00:00", "Z"),
                "title": _required_text(item, "title", index=index),
                "description_html": _required_text(
                    item, "description", index=index
                ),
            }
        )
    return sorted(items, key=lambda item: (item["published_at"], item["status_id"]))


def acquire_feed(
    *,
    logical_date: date,
    as_of: date,
    output_dir: Path,
    report_path: Path,
    lookback_days: int = 14,
    allow_network: bool = False,
    payload: bytes | None = None,
    source_url: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Persist raw and normalized evidence; never emit reviewed roster decisions."""

    if payload is None:
        if not allow_network:
            raise FantasyXCFeedError("network acquisition requires --allow-network")
        payload, source_url = fetch_feed_with_fallback()
    elif source_url is None:
        source_url = FEED_URL
    if source_url is None:
        raise AssertionError("feed source URL was not recorded")
    validate_feed_url(source_url)
    items = parse_feed(
        payload,
        logical_date=logical_date,
        as_of=as_of,
        lookback_days=lookback_days,
    )
    output_dir = output_dir.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    raw_path = output_dir / "fantasyxc-feed.xml"
    items_path = output_dir / "fantasyxc-feed-items.json"
    items_document = {
        "schema_version": ITEMS_SCHEMA,
        "status": "candidate_review",
        "logical_date": logical_date.isoformat(),
        "as_of": as_of.isoformat(),
        "publication": {"status": "disabled"},
        "source": {"url": source_url, "sha256": _sha256(payload)},
        "items": items,
    }
    items_bytes = _json_bytes(items_document)
    _atomic_write(raw_path, payload)
    _atomic_write(items_path, items_bytes)
    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "candidate_review",
        "logical_date": logical_date.isoformat(),
        "as_of": as_of.isoformat(),
        "retrieved_at": now().astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "publication": {"status": "disabled"},
        "source": {"url": source_url, "sha256": _sha256(payload)},
        "item_count": len(items),
        "outputs": {
            raw_path.name: {"sha256": _sha256(payload), "bytes": len(payload)},
            items_path.name: {
                "schema_version": ITEMS_SCHEMA,
                "sha256": _sha256(items_bytes),
                "items": len(items),
            },
        },
    }
    _atomic_write(report_path, _json_bytes(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logical-date", required=True, type=date.fromisoformat)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--allow-network", action="store_true")
    args = parser.parse_args()
    report = acquire_feed(
        logical_date=args.logical_date,
        as_of=args.as_of,
        output_dir=args.output_dir,
        report_path=args.report,
        lookback_days=args.lookback_days,
        allow_network=args.allow_network,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
