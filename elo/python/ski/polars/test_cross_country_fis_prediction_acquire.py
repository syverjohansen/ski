import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_fis_prediction_acquire import (
    FisPredictionAcquireError,
    acquire_fis_prediction_evidence,
    discover_event_urls,
    discover_result_races,
)
from cross_country_prediction_sources import prediction_calendar_url
from cross_country_results_backfill import BackfillError

ROOT = Path(__file__).resolve().parent
RESULTS_FIXTURE = ROOT / "tests" / "fixtures" / "cc-results.html"

EVENT_URL = (
    "https://www.fis-ski.com/DB/general/event-details.html?"
    "sectorcode=CC&eventid=700&seasoncode=2027"
)
RESULT_URL = "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=500"
A_TIER_RESULT_URL = (
    "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=999"
)


def calendar_html() -> bytes:
    return f"""
    <html><body><div id="calendardata">
      <a href="{EVENT_URL}">Test lower-tier event</a>
      <a href="{EVENT_URL}">Duplicate presentation link</a>
    </div></body></html>
    """.encode()


def event_html() -> bytes:
    return f"""
    <html><body><div id="eventdetailscontent">
      <div class="table-row">
        <div class="timezone-date" data-date="2026-11-20"></div>
        <div class="gender__item gender__item_m"></div>
        <div class="clip">10 km Interval Start Classic</div>
        <a href="{RESULT_URL}">Results</a>
      </div>
      <div class="table-row">
        <div class="timezone-date" data-date="2026-11-20"></div>
        <div class="gender__item gender__item_m"></div>
        <div class="clip">4x7.5 km Relay</div>
        <a href="/DB/general/results.html?sectorcode=CC&amp;raceid=501">Results</a>
      </div>
      <div class="table-row">
        <div class="timezone-date" data-date="2026-12-20"></div>
        <div class="gender__item gender__item_l"></div>
        <div class="clip">Sprint Qualification Free</div>
        <a href="/DB/general/results.html?sectorcode=CC&amp;raceid=502">Results</a>
      </div>
      <div class="table-row">
        <div class="timezone-date" data-date="2026-11-21"></div>
        <div class="gender__item gender__item_l"></div>
        <div class="clip">Sprint Qualification Classic</div>
        <a href="{A_TIER_RESULT_URL}">Results</a>
      </div>
    </div></body></html>
    """.encode()


def no_results_html() -> bytes:
    return b"""
    <html><body>
      <div id="events-info-results">
        <div class="table-row"><div>No results found for this competition.</div></div>
      </div>
    </body></html>
    """


def unranked_results_html() -> bytes:
    rows = "".join(
        f"""
        <a class="table-row" href="https://www.fis-ski.com/DB/general/athlete-biography.html?sectorcode=cc&amp;competitorid={competitor_id}">
          <div class="justify-right pr-1 bold"></div>
          <div class="justify-right gray pr-1">{3500000 + competitor_id}</div>
          <div class="justify-left bold">Athlete {competitor_id}</div>
          <span class="country__name-short">NOR</span>
        </a>
        """
        for competitor_id in range(1, 4)
    )
    return f'<div id="events-info-results">{rows}</div>'.encode()


def write_schedule(path: Path) -> None:
    path.write_text(
        "Date,Startlist,Sex,City,Country,Distance,Technique,MS,Period,"
        "Pursuit,Stage,Final_Climb,Elevation,Race,Championship\n"
        f"11/21/2026,{A_TIER_RESULT_URL},L,Ruka,Finland,Sprint,C,0,1,0,0,0,337,1,0\n",
        encoding="utf-8",
    )


class CrossCountryFisPredictionAcquireTests(unittest.TestCase):
    def test_discovers_bounded_event_and_completed_individual_races(self):
        self.assertEqual(discover_event_urls(calendar_html()), [EVENT_URL])
        races = discover_result_races(event_html(), as_of=date(2026, 11, 23))
        self.assertEqual([race["fis_race_id"] for race in races], ["500", "999"])
        self.assertEqual(races[0]["distance"], "10")
        self.assertEqual(races[0]["technique"], "C")

    def test_acquires_each_shared_event_once_and_excludes_accepted_a_tier(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "accepted" / "races.csv"
            schedule.parent.mkdir()
            write_schedule(schedule)
            fetched = []
            sources = {
                prediction_calendar_url(2027, "FIS"): calendar_html(),
                prediction_calendar_url(2027, "SCAN"): calendar_html(),
                EVENT_URL: event_html(),
                RESULT_URL: RESULTS_FIXTURE.read_bytes(),
            }

            def fetch(url: str) -> bytes:
                fetched.append(url)
                return sources[url]

            report = acquire_fis_prediction_evidence(
                season=2027,
                categories=["SCAN", "FIS", "SCAN"],
                accepted_schedule=schedule,
                as_of=date(2026, 11, 23),
                output_dir=root / "output",
                fetcher=fetch,
            )

            self.assertEqual(fetched.count(EVENT_URL), 1)
            self.assertEqual(fetched.count(RESULT_URL), 1)
            self.assertEqual(report["categories"], ["FIS", "SCAN"])
            self.assertEqual(report["unique_event_pages"], 1)
            self.assertEqual(report["acquired_races"], 1)
            self.assertEqual(report["acquired_result_rows"], 3)
            self.assertEqual(report["excluded_accepted_a_tier_race_ids"], ["999"])
            self.assertFalse(report["accepted_history_modified"])
            self.assertFalse(report["publication_eligible"])

            with (root / "output" / "fis-prediction-results.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertEqual({row["source_categories"] for row in rows}, {"FIS;SCAN"})
            self.assertEqual({row["fis_race_id"] for row in rows}, {"500"})
            self.assertTrue(
                (root / "output" / "raw" / "results" / "500.html").is_file()
            )

    def test_rejects_a_tier_categories_and_overlapping_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "accepted" / "races.csv"
            schedule.parent.mkdir()
            write_schedule(schedule)
            with self.assertRaisesRegex(
                FisPredictionAcquireError, "not prediction-only"
            ):
                acquire_fis_prediction_evidence(
                    season=2027,
                    categories=["WC"],
                    accepted_schedule=schedule,
                    as_of=date(2026, 11, 23),
                    output_dir=root / "output",
                    fetcher=lambda _: b"",
                )
            with self.assertRaisesRegex(FisPredictionAcquireError, "overlap"):
                acquire_fis_prediction_evidence(
                    season=2027,
                    categories=["FIS"],
                    accepted_schedule=schedule,
                    as_of=date(2026, 11, 23),
                    output_dir=schedule.parent / "candidate",
                    fetcher=lambda _: b"",
                )

    def test_missing_calendar_and_empty_completed_slice_fail_closed(self):
        with self.assertRaisesRegex(FisPredictionAcquireError, "calendar data"):
            discover_event_urls(b"<html></html>")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "accepted" / "races.csv"
            schedule.parent.mkdir()
            write_schedule(schedule)
            sources = {
                prediction_calendar_url(2027, "FIS"): calendar_html(),
                EVENT_URL: b'<div id="eventdetailscontent"></div>',
            }
            with self.assertRaisesRegex(
                FisPredictionAcquireError, "no completed prediction races"
            ):
                acquire_fis_prediction_evidence(
                    season=2027,
                    categories=["FIS"],
                    accepted_schedule=schedule,
                    as_of=date(2026, 11, 23),
                    output_dir=root / "output",
                    fetcher=sources.__getitem__,
                )

    def test_resumes_from_raw_cache_without_refetching(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "accepted" / "races.csv"
            schedule.parent.mkdir()
            write_schedule(schedule)
            sources = {
                prediction_calendar_url(2027, "FIS"): calendar_html(),
                EVENT_URL: event_html(),
                RESULT_URL: RESULTS_FIXTURE.read_bytes(),
            }
            output = root / "output"
            acquire_fis_prediction_evidence(
                season=2027,
                categories=["FIS"],
                accepted_schedule=schedule,
                as_of=date(2026, 11, 23),
                output_dir=output,
                fetcher=sources.__getitem__,
            )
            (output / "fis-prediction-results.csv").unlink()
            (output / "report.json").unlink()

            report = acquire_fis_prediction_evidence(
                season=2027,
                categories=["FIS"],
                accepted_schedule=schedule,
                as_of=date(2026, 11, 23),
                output_dir=output,
                fetcher=lambda url: self.fail(f"unexpected fetch: {url}"),
                resume=True,
            )

            self.assertEqual(report["raw_cache_hits"], 3)
            self.assertEqual(report["acquired_races"], 1)
            self.assertTrue((output / "fis-prediction-results.csv").is_file())

    def test_records_explicit_fis_no_results_but_rejects_unparseable_pages(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "accepted" / "races.csv"
            schedule.parent.mkdir()
            write_schedule(schedule)
            event_with_two_races = event_html().replace(
                b"</div></body></html>",
                b"""
                <div class="table-row">
                  <div class="timezone-date" data-date="2026-11-19"></div>
                  <div class="gender__item gender__item_l"></div>
                  <div class="clip">5 km Interval Start Free</div>
                  <a href="/DB/general/results.html?sectorcode=CC&amp;raceid=503">Results</a>
                </div></div></body></html>
                """,
            )
            sources = {
                prediction_calendar_url(2027, "FIS"): calendar_html(),
                EVENT_URL: event_with_two_races,
                RESULT_URL: RESULTS_FIXTURE.read_bytes(),
                "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=503": no_results_html(),
            }
            report = acquire_fis_prediction_evidence(
                season=2027,
                categories=["FIS"],
                accepted_schedule=schedule,
                as_of=date(2026, 11, 23),
                output_dir=root / "output",
                fetcher=sources.__getitem__,
            )

            self.assertEqual(report["acquired_races"], 1)
            self.assertEqual(
                report["skipped_races"][0]["reason"],
                "fis_explicitly_reports_no_results",
            )

            unranked_sources = dict(sources)
            unranked_sources[
                "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=503"
            ] = unranked_results_html()
            unranked_report = acquire_fis_prediction_evidence(
                season=2027,
                categories=["FIS"],
                accepted_schedule=schedule,
                as_of=date(2026, 11, 23),
                output_dir=root / "unranked-output",
                fetcher=unranked_sources.__getitem__,
            )
            self.assertEqual(
                unranked_report["skipped_races"][0]["reason"],
                "fis_page_has_unranked_athletes",
            )

            broken_sources = dict(sources)
            broken_sources[
                "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=503"
            ] = b'<div id="events-info-results"><div>Unexpected markup</div></div>'
            with self.assertRaisesRegex(
                BackfillError, "yielded only 0 classified results"
            ):
                acquire_fis_prediction_evidence(
                    season=2027,
                    categories=["FIS"],
                    accepted_schedule=schedule,
                    as_of=date(2026, 11, 23),
                    output_dir=root / "broken-output",
                    fetcher=broken_sources.__getitem__,
                )


if __name__ == "__main__":
    unittest.main()
