import json
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError

from cross_country_fantasyxc_feed import (
    FEED_URL,
    FantasyXCFeedError,
    acquire_feed,
    fetch_feed,
    fetch_feed_with_fallback,
    parse_feed,
    validate_feed_url,
)


def rss(*items: tuple[str, str, str, str]) -> bytes:
    entries = "".join(
        "<item>"
        f"<title>{title}</title>"
        f"<link>https://x.com/FantasyXC/status/{status_id}</link>"
        f"<guid>https://x.com/FantasyXC/status/{status_id}</guid>"
        f"<pubDate>{published}</pubDate>"
        f"<description><![CDATA[{description}]]></description>"
        "</item>"
        for status_id, published, title, description in items
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel><title>FantasyXC</title>'
        f"{entries}</channel></rss>"
    ).encode()


PAYLOAD = rss(
    (
        "100",
        "Mon, 23 Nov 2026 20:04:04 GMT",
        "Team Finland will race in Ruka",
        "<p>Women:<br />Jasmi Joensuu (10K+s)</p>",
    ),
    (
        "101",
        "Tue, 10 Nov 2026 09:00:00 GMT",
        "Old unrelated post",
        "<p>Outside the evidence window</p>",
    ),
)


class FakeResponse:
    status = 200

    def __init__(self, payload: bytes):
        self.payload = payload
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, amount: int) -> bytes:
        return self.payload[:amount]


class CrossCountryFantasyXCFeedTests(unittest.TestCase):
    def test_parses_only_items_inside_bounded_window(self) -> None:
        items = parse_feed(
            PAYLOAD,
            logical_date=date(2026, 11, 27),
            as_of=date(2026, 11, 23),
            lookback_days=14,
        )
        self.assertEqual(1, len(items))
        self.assertIn("100", {item["status_id"] for item in items})
        self.assertIn("Jasmi Joensuu", items[0]["description_html"])

    def test_acquisition_retains_raw_and_normalized_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = acquire_feed(
                logical_date=date(2026, 11, 27),
                as_of=date(2026, 11, 23),
                output_dir=root / "evidence",
                report_path=root / "reports/feed.json",
                payload=PAYLOAD,
                now=lambda: datetime(2026, 11, 23, 21, tzinfo=UTC),
            )
            self.assertEqual("candidate_review", report["status"])
            self.assertEqual({"status": "disabled"}, report["publication"])
            self.assertEqual(1, report["item_count"])
            raw = root / "evidence/fantasyxc-feed.xml"
            normalized = root / "evidence/fantasyxc-feed-items.json"
            self.assertEqual(PAYLOAD, raw.read_bytes())
            document = json.loads(normalized.read_text())
            self.assertEqual("candidate_review", document["status"])
            self.assertEqual("100", document["items"][0]["status_id"])
            self.assertTrue((root / "reports/feed.json").is_file())

    def test_network_is_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(FantasyXCFeedError, "--allow-network"):
                acquire_feed(
                    logical_date=date(2026, 11, 27),
                    as_of=date(2026, 11, 23),
                    output_dir=root / "evidence",
                    report_path=root / "report.json",
                )

    def test_transient_fetch_retries_then_succeeds(self) -> None:
        calls = []

        def opener(*_args, **_kwargs):
            calls.append(True)
            if len(calls) < 3:
                raise URLError("temporary")
            return FakeResponse(PAYLOAD)

        self.assertEqual(
            PAYLOAD,
            fetch_feed(opener=opener, sleep=lambda _seconds: None),
        )
        self.assertEqual(3, len(calls))

    def test_feed_url_is_fixed_to_fantasyxc(self) -> None:
        validate_feed_url(FEED_URL)
        validate_feed_url("https://fixupx.com/FantasyXC/feed.xml")
        with self.assertRaisesRegex(FantasyXCFeedError, "unsupported"):
            validate_feed_url("https://example.test/FantasyXC/feed.xml")

    def test_terminal_primary_failure_uses_fixed_fallback_realm(self) -> None:
        requested = []

        def opener(request, **_kwargs):
            requested.append(request.full_url)
            self.assertIn("Mozilla/5.0", request.get_header("User-agent"))
            self.assertIn("NordicNumbers/1.0", request.get_header("User-agent"))
            if request.full_url.startswith("https://fxtwitter.com/"):
                raise HTTPError(request.full_url, 404, "missing", {}, None)
            return FakeResponse(PAYLOAD)

        payload, source_url = fetch_feed_with_fallback(
            opener=opener,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(PAYLOAD, payload)
        self.assertEqual("https://fixupx.com/FantasyXC/feed.xml", source_url)
        self.assertEqual(
            [
                "https://fxtwitter.com/FantasyXC/feed.xml",
                "https://fixupx.com/FantasyXC/feed.xml",
            ],
            requested,
        )

    def test_acquisition_records_fallback_realm_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            acquire_feed(
                logical_date=date(2026, 11, 27),
                as_of=date(2026, 11, 23),
                output_dir=root / "evidence",
                report_path=root / "report.json",
                payload=PAYLOAD,
                source_url="https://fixupx.com/FantasyXC/feed.xml",
            )
            document = json.loads(
                (root / "evidence/fantasyxc-feed-items.json").read_text()
            )
            report = json.loads((root / "report.json").read_text())
            self.assertEqual(
                "https://fixupx.com/FantasyXC/feed.xml",
                document["source"]["url"],
            )
            self.assertEqual(document["source"], report["source"])

    def test_future_evidence_is_excluded(self) -> None:
        future = rss(
            (
                "102",
                "Tue, 24 Nov 2026 09:00:00 GMT",
                "Future post",
                "<p>Not available at the as-of boundary</p>",
            )
        )
        self.assertEqual(
            [],
            parse_feed(
                future,
                logical_date=date(2026, 11, 27),
                as_of=date(2026, 11, 23),
                lookback_days=14,
            ),
        )

    def test_active_weekend_as_of_may_follow_group_date(self) -> None:
        items = parse_feed(
            PAYLOAD,
            logical_date=date(2026, 11, 22),
            as_of=date(2026, 11, 23),
            lookback_days=14,
        )
        self.assertIn("100", {item["status_id"] for item in items})

    def test_duplicate_status_id_fails_closed(self) -> None:
        duplicate = rss(
            (
                "100",
                "Mon, 23 Nov 2026 20:04:04 GMT",
                "One",
                "<p>One</p>",
            ),
            (
                "100",
                "Mon, 23 Nov 2026 20:05:04 GMT",
                "Two",
                "<p>Two</p>",
            ),
        )
        with self.assertRaisesRegex(FantasyXCFeedError, "duplicate"):
            parse_feed(
                duplicate,
                logical_date=date(2026, 11, 27),
                as_of=date(2026, 11, 23),
                lookback_days=14,
            )


if __name__ == "__main__":
    unittest.main()
