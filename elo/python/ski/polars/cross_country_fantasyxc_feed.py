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
from html import escape
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
THREAD_API_ROOT = "https://api.fxtwitter.com/2/thread"
PROFILE_API_ROOT = "https://api.fxtwitter.com/2/profile/FantasyXC/statuses"
REPORT_SCHEMA = "nordicnumbers.cross-country-fantasyxc-feed-report/v1"
ITEMS_SCHEMA = "nordicnumbers.cross-country-fantasyxc-feed-items/v1"
MAX_FEED_BYTES = 2 * 1024 * 1024
MAX_THREAD_BYTES = 2 * 1024 * 1024
MAX_THREAD_STATUSES = 100
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


def validate_thread_url(url: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "api.fxtwitter.com"
        or not re.fullmatch(r"/2/thread/\d+", parsed.path)
        or parsed.query
        or parsed.fragment
    ):
        raise FantasyXCFeedError(f"unsupported FantasyXC thread URL: {url}")


def validate_profile_url(url: str) -> None:
    parsed = urlparse(url)
    query = dict(
        part.split("=", 1)
        for part in parsed.query.split("&")
        if "=" in part
    )
    if (
        parsed.scheme != "https"
        or parsed.hostname != "api.fxtwitter.com"
        or parsed.path != "/2/profile/FantasyXC/statuses"
        or parsed.fragment
        or set(query) != {"count", "with_replies", "since"}
        or query.get("count") != "100"
        or query.get("with_replies") != "1"
        or not re.fullmatch(r"\d+", query.get("since", ""))
    ):
        raise FantasyXCFeedError(f"unsupported FantasyXC profile URL: {url}")


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


def fetch_thread(
    status_id: str,
    *,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
    attempts: int = 3,
    timeout_seconds: int = 20,
) -> bytes:
    """Fetch an author's self-reply thread with bounded retries and size."""

    if not re.fullmatch(r"\d+", status_id):
        raise FantasyXCFeedError(f"invalid FantasyXC status ID: {status_id}")
    if attempts < 1:
        raise ValueError("attempts must be positive")
    url = f"{THREAD_API_ROOT}/{status_id}"
    validate_thread_url(url)
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with opener(request, timeout=timeout_seconds) as response:
                status = getattr(response, "status", 200)
                if status != 200:
                    raise FantasyXCFeedError(
                        f"FantasyXC thread returned HTTP {status}"
                    )
                declared = response.headers.get("Content-Length")
                if declared and int(declared) > MAX_THREAD_BYTES:
                    raise FantasyXCFeedError("FantasyXC thread exceeds the size limit")
                payload = response.read(MAX_THREAD_BYTES + 1)
                if not payload:
                    raise FantasyXCFeedError("FantasyXC thread returned an empty body")
                if len(payload) > MAX_THREAD_BYTES:
                    raise FantasyXCFeedError("FantasyXC thread exceeds the size limit")
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
        f"FantasyXC thread failed after {attempts} attempts: {last_error}"
    )


def fetch_profile(
    *,
    logical_date: date,
    lookback_days: int,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
    attempts: int = 3,
    timeout_seconds: int = 20,
) -> tuple[bytes, str]:
    """Fetch a bounded FantasyXC timeline including the account's replies."""

    if lookback_days < 1 or lookback_days > 30:
        raise FantasyXCFeedError("lookback_days must be between 1 and 30")
    since = int(
        datetime.combine(
            logical_date - timedelta(days=lookback_days),
            datetime.min.time(),
            tzinfo=UTC,
        ).timestamp()
    )
    url = f"{PROFILE_API_ROOT}?count=100&with_replies=1&since={since}"
    validate_profile_url(url)
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with opener(request, timeout=timeout_seconds) as response:
                status = getattr(response, "status", 200)
                if status != 200:
                    raise FantasyXCFeedError(
                        f"FantasyXC profile returned HTTP {status}"
                    )
                declared = response.headers.get("Content-Length")
                if declared and int(declared) > MAX_FEED_BYTES:
                    raise FantasyXCFeedError("FantasyXC profile exceeds the size limit")
                payload = response.read(MAX_FEED_BYTES + 1)
                if not payload:
                    raise FantasyXCFeedError("FantasyXC profile returned an empty body")
                if len(payload) > MAX_FEED_BYTES:
                    raise FantasyXCFeedError("FantasyXC profile exceeds the size limit")
                return payload, url
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
        f"FantasyXC profile failed after {attempts} attempts: {last_error}"
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


def parse_thread(
    payload: bytes,
    *,
    seed_status_id: str,
    logical_date: date,
    as_of: date,
    lookback_days: int,
) -> list[dict[str, str]]:
    """Normalize FantasyXC's self-reply chain into ordinary feed items."""

    if lookback_days < 1 or lookback_days > 30:
        raise FantasyXCFeedError("lookback_days must be between 1 and 30")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FantasyXCFeedError("FantasyXC thread returned invalid JSON") from exc
    statuses = document.get("thread") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or document.get("code") != 200
        or not isinstance(statuses, list)
    ):
        raise FantasyXCFeedError("FantasyXC thread has an unsupported response")
    if len(statuses) > MAX_THREAD_STATUSES:
        raise FantasyXCFeedError("FantasyXC thread contains too many statuses")

    lower_bound = logical_date - timedelta(days=lookback_days)
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, status in enumerate(statuses, start=1):
        if not isinstance(status, dict) or status.get("type") != "status":
            continue
        status_id = str(status.get("id", ""))
        author = status.get("author")
        if (
            not re.fullmatch(r"\d+", status_id)
            or not isinstance(author, dict)
            or author.get("screen_name") != "FantasyXC"
        ):
            continue
        if status_id in seen:
            raise FantasyXCFeedError(
                f"duplicate FantasyXC thread status ID: {status_id}"
            )
        seen.add(status_id)
        expected_url = f"https://x.com/FantasyXC/status/{status_id}"
        if status.get("url") != expected_url:
            raise FantasyXCFeedError(
                f"FantasyXC thread status {index} has an unsupported URL"
            )
        try:
            published = datetime.fromtimestamp(
                int(status["created_timestamp"]), tz=UTC
            )
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise FantasyXCFeedError(
                f"FantasyXC thread status {index} has an invalid timestamp"
            ) from exc
        if not lower_bound <= published.date() <= as_of:
            continue
        text = str(status.get("text", "")).strip()
        if not text:
            raise FantasyXCFeedError(
                f"FantasyXC thread status {index} has no text"
            )
        lines = text.splitlines()
        normalized.append(
            {
                "status_id": status_id,
                "source_url": expected_url,
                "published_at": published.isoformat().replace("+00:00", "Z"),
                "title": " ".join(lines).strip(),
                "description_html": "<p>"
                + "<br />".join(escape(line) for line in lines)
                + "</p>",
                "thread_root_id": seed_status_id,
            }
        )
    if seed_status_id not in seen:
        raise FantasyXCFeedError("FantasyXC thread does not contain its seed status")
    return sorted(
        normalized, key=lambda item: (item["published_at"], item["status_id"])
    )


def parse_profile(
    payload: bytes,
    *,
    logical_date: date,
    as_of: date,
    lookback_days: int,
) -> list[dict[str, str]]:
    """Normalize the bounded v2 FantasyXC profile timeline."""

    if lookback_days < 1 or lookback_days > 30:
        raise FantasyXCFeedError("lookback_days must be between 1 and 30")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FantasyXCFeedError("FantasyXC profile returned invalid JSON") from exc
    results = document.get("results") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or document.get("code") != 200
        or not isinstance(results, list)
    ):
        raise FantasyXCFeedError("FantasyXC profile has an unsupported response")
    statuses: list[dict[str, Any]] = []
    for result in results:
        if not isinstance(result, dict):
            continue
        if result.get("type") == "status":
            statuses.append(result)
        elif result.get("type") == "thread" and isinstance(
            result.get("statuses"), list
        ):
            statuses.extend(
                status
                for status in result["statuses"]
                if isinstance(status, dict)
            )
    synthetic = _json_bytes({"code": 200, "thread": statuses})
    if not statuses:
        return []
    seed_status_id = str(statuses[0].get("id", ""))
    return parse_thread(
        synthetic,
        seed_status_id=seed_status_id,
        logical_date=logical_date,
        as_of=as_of,
        lookback_days=lookback_days,
    )


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
    thread_fetcher: Callable[[str], bytes] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Persist raw and normalized evidence; never emit reviewed roster decisions."""

    if payload is None:
        if not allow_network:
            raise FantasyXCFeedError("network acquisition requires --allow-network")
        try:
            payload, source_url = fetch_feed_with_fallback()
        except FantasyXCFeedError as feed_error:
            try:
                payload, source_url = fetch_profile(
                    logical_date=logical_date,
                    lookback_days=lookback_days,
                )
            except FantasyXCFeedError as profile_error:
                raise FantasyXCFeedError(
                    f"FantasyXC RSS and profile acquisition failed: "
                    f"{feed_error}; {profile_error}"
                ) from profile_error
    elif source_url is None:
        source_url = FEED_URL
    if source_url is None:
        raise AssertionError("feed source URL was not recorded")
    if source_url.startswith(PROFILE_API_ROOT):
        validate_profile_url(source_url)
        items = parse_profile(
            payload,
            logical_date=logical_date,
            as_of=as_of,
            lookback_days=lookback_days,
        )
        source_kind = "profile_v2"
    else:
        validate_feed_url(source_url)
        items = parse_feed(
            payload,
            logical_date=logical_date,
            as_of=as_of,
            lookback_days=lookback_days,
        )
        source_kind = "rss"
    thread_payloads: dict[str, bytes] = {}
    if allow_network:
        if thread_fetcher is None:
            thread_fetcher = fetch_thread
        by_status = {item["status_id"]: item for item in items}
        for item in list(items):
            # FantasyXC marks posts that continue in a self-reply thread with
            # this literal marker. Expanding only those posts keeps requests
            # bounded while retaining every roster page.
            if "(...)" not in f"{item['title']} {item['description_html']}":
                continue
            seed_status_id = item["status_id"]
            thread_payload = thread_fetcher(seed_status_id)
            thread_payloads[seed_status_id] = thread_payload
            for thread_item in parse_thread(
                thread_payload,
                seed_status_id=seed_status_id,
                logical_date=logical_date,
                as_of=as_of,
                lookback_days=lookback_days,
            ):
                by_status.setdefault(thread_item["status_id"], thread_item)
        items = sorted(
            by_status.values(),
            key=lambda item: (item["published_at"], item["status_id"]),
        )
    output_dir = output_dir.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    raw_path = output_dir / (
        "fantasyxc-profile.json" if source_kind == "profile_v2" else "fantasyxc-feed.xml"
    )
    items_path = output_dir / "fantasyxc-feed-items.json"
    thread_sources = [
        {
            "status_id": status_id,
            "url": f"{THREAD_API_ROOT}/{status_id}",
            "sha256": _sha256(thread_payloads[status_id]),
        }
        for status_id in sorted(thread_payloads)
    ]
    source = {
        "kind": source_kind,
        "url": source_url,
        "sha256": _sha256(payload),
        "threads": thread_sources,
    }
    items_document = {
        "schema_version": ITEMS_SCHEMA,
        "status": "candidate_review",
        "logical_date": logical_date.isoformat(),
        "as_of": as_of.isoformat(),
        "publication": {"status": "disabled"},
        "source": source,
        "items": items,
    }
    items_bytes = _json_bytes(items_document)
    _atomic_write(raw_path, payload)
    for status_id, thread_payload in thread_payloads.items():
        _atomic_write(
            output_dir / "threads" / f"fantasyxc-thread-{status_id}.json",
            thread_payload,
        )
    _atomic_write(items_path, items_bytes)
    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "candidate_review",
        "logical_date": logical_date.isoformat(),
        "as_of": as_of.isoformat(),
        "retrieved_at": now().astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "publication": {"status": "disabled"},
        "source": source,
        "item_count": len(items),
        "thread_count": len(thread_payloads),
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
