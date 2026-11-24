import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_prediction_elo_pipeline import (
    PredictionEloPipelineError,
    build_prediction_elo_ledger,
)
from cross_country_prediction_sources import prediction_calendar_url
from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS

ROOT = Path(__file__).resolve().parent
RESULTS_FIXTURE = ROOT / "tests" / "fixtures" / "cc-results.html"
EVENT_URL = (
    "https://www.fis-ski.com/DB/general/event-details.html?"
    "sectorcode=CC&eventid=700&seasoncode=2027"
)
RESULT_URL = "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=500"
A_TIER_URL = "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=999"


def write_csv(path: Path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def history_row(athlete_id: str, name: str, rating: int) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "2026-03-01",
        "Event": "World Cup",
        "Season": "2026",
        "Race": "1",
        "ID": athlete_id,
        "Skier": name,
        "Nation": "Norway",
    }
    for stream in STREAMS:
        row[stream] = rating
        row[PREVIOUS_COLUMNS[stream]] = rating
    return row


def calendar_html() -> bytes:
    return f'<div id="calendardata"><a href="{EVENT_URL}">Event</a></div>'.encode()


def event_html() -> bytes:
    return f"""
    <div id="eventdetailscontent">
      <div class="table-row">
        <div class="timezone-date" data-date="2026-11-20"></div>
        <div class="gender__item gender__item_m"></div>
        <div class="clip">10 km Interval Start Classic</div>
        <a href="{RESULT_URL}">Results</a>
      </div>
      <div class="table-row">
        <div class="timezone-date" data-date="2026-11-21"></div>
        <div class="gender__item gender__item_m"></div>
        <div class="clip">Sprint Qualification Classic</div>
        <a href="{A_TIER_URL}">Results</a>
      </div>
    </div>
    """.encode()


def prepare_sources(root: Path):
    source = root / "accepted"
    schedule = source / "races.csv"
    men = source / "men_chrono.csv"
    ladies = source / "ladies_chrono.csv"
    aliases = source / "aliases.csv"
    policy = source / "policy.json"
    source.mkdir()
    schedule.write_text(
        "Date,Startlist,Sex\n" f"11/21/2026,{A_TIER_URL},M\n",
        encoding="utf-8",
    )
    history_fields = [
        "Date",
        "Event",
        "Season",
        "Race",
        "ID",
        "Skier",
        "Nation",
        *STREAMS,
        *PREVIOUS_COLUMNS.values(),
    ]
    write_csv(
        men,
        history_fields,
        [
            history_row("10", "Iivo Niskanen", 1600),
            history_row("20", "Johannes Hoesflot Klaebo", 1700),
        ],
    )
    write_csv(ladies, history_fields, [])
    write_csv(
        aliases,
        [
            "sex",
            "fis_competitor_id",
            "fis_code",
            "fis_name",
            "canonical_id",
            "canonical_name",
        ],
        [],
    )
    source_policy = json.loads(
        (ROOT / "cross_country_prediction_elo_policy.json").read_text(encoding="utf-8")
    )
    policy.write_text(json.dumps(source_policy), encoding="utf-8")
    return schedule, men, ladies, aliases, policy


class PredictionEloPipelineTests(unittest.TestCase):
    def test_builds_checksumned_k4_ledger_without_mutating_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule, men, ladies, aliases, policy = prepare_sources(root)
            sources = (schedule, men, ladies, aliases, policy)
            mtimes = {path: path.stat().st_mtime_ns for path in sources}
            responses = {
                prediction_calendar_url(2027, "FIS"): calendar_html(),
                EVENT_URL: event_html(),
                RESULT_URL: RESULTS_FIXTURE.read_bytes(),
            }
            report = build_prediction_elo_ledger(
                season=2027,
                as_of=date(2026, 11, 23),
                accepted_schedule=schedule,
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                policy_path=policy,
                output_dir=root / "output",
                categories=["FIS"],
                fetcher=responses.__getitem__,
            )

            with (root / "output/fis-prediction-ledger.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["prediction_athlete_id"], "fis:1003")
            self.assertEqual(rows[0]["k_factor"], "4.0")
            self.assertLess(float(rows[0]["Elo"]), 1300.0)
            self.assertEqual(report["status"], "private_ledger_ready")
            self.assertEqual(report["publication"], {"status": "disabled"})
            self.assertEqual(report["unknown_vs_unknown_comparisons"], 0)
            self.assertEqual(report["verified_anchor_updates"], 0)
            self.assertEqual(report["ledger"]["rows"], 1)
            self.assertEqual(
                mtimes, {path: path.stat().st_mtime_ns for path in sources}
            )

    def test_rejects_policy_k_other_than_calibrated_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule, men, ladies, aliases, policy = prepare_sources(root)
            document = json.loads(policy.read_text(encoding="utf-8"))
            document["prediction_elo"]["selected_k_factor"] = 5.0
            policy.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(PredictionEloPipelineError, "selected K=4"):
                build_prediction_elo_ledger(
                    season=2027,
                    as_of=date(2026, 11, 23),
                    accepted_schedule=schedule,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    aliases_path=aliases,
                    policy_path=policy,
                    output_dir=root / "output",
                    categories=["FIS"],
                    fetcher=lambda _url: b"",
                )

    def test_rejects_output_overlapping_accepted_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule, men, ladies, aliases, policy = prepare_sources(root)
            with self.assertRaisesRegex(PredictionEloPipelineError, "overlap"):
                build_prediction_elo_ledger(
                    season=2027,
                    as_of=date(2026, 11, 23),
                    accepted_schedule=schedule,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    aliases_path=aliases,
                    policy_path=policy,
                    output_dir=schedule.parent / "output",
                    categories=["FIS"],
                    fetcher=lambda _url: b"",
                )


if __name__ == "__main__":
    unittest.main()
