import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_weekend_inputs import (
    WeekendInputError,
    build_weekend_inputs,
    parse_official_startlist,
)
from schedule_contract import WEEKENDS_COLUMNS

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "tests" / "fixtures" / "cc-results.html"


def write_csv(
    path: Path, fields: list[str] | tuple[str, ...], rows: list[dict[str, str]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def race(
    *, sex: str, race_id: str, stage: str = "0", distance: str = "10"
) -> dict[str, str]:
    return {
        "Date": "11/27/2026",
        "Race_Date": "11/28/2026",
        "Startlist": (
            "https://www.fis-ski.com/DB/general/results.html?"
            f"sectorcode=CC&raceid={race_id}"
        ),
        "Sex": sex,
        "City": "Ruka",
        "Country": "Finland",
        "Distance": distance,
        "Technique": "C",
        "MS": "0",
        "Period": "1",
        "Pursuit": "0",
        "Stage": stage,
        "Final_Climb": "0",
        "Elevation": "337",
        "Race": "1",
        "Championship": "0",
    }


class WeekendInputFixture:
    def __init__(self, root: Path) -> None:
        self.sources = root / "accepted"
        self.schedule = self.sources / "weekends.csv"
        self.men = self.sources / "men_chrono.csv"
        self.ladies = self.sources / "ladies_chrono.csv"
        self.aliases = self.sources / "fis_identity_aliases.csv"
        history_fields = ["Date", "Season", "Race", "ID", "Skier", "Nation"]
        write_csv(
            self.men,
            history_fields,
            [
                {
                    "Date": "2026-03-01",
                    "Season": "2026",
                    "Race": "1",
                    "ID": "11",
                    "Skier": "Iivo Niskanen",
                    "Nation": "Finland",
                },
                {
                    "Date": "2026-03-02",
                    "Season": "2026",
                    "Race": "2",
                    "ID": "12",
                    "Skier": "Johannes Hoesflot Klaebo",
                    "Nation": "Norway",
                },
                {
                    "Date": "2026-03-03",
                    "Season": "2026",
                    "Race": "3",
                    "ID": "13",
                    "Skier": "William Poromaa",
                    "Nation": "Sweden",
                },
            ],
        )
        write_csv(
            self.ladies,
            history_fields,
            [
                {
                    "Date": "2026-03-01",
                    "Season": "2026",
                    "Race": "1",
                    "ID": "21",
                    "Skier": "Jessie Diggins",
                    "Nation": "United States",
                },
                {
                    "Date": "2025-03-01",
                    "Season": "2025",
                    "Race": "1",
                    "ID": "22",
                    "Skier": "Retired Skier",
                    "Nation": "Norway",
                },
            ],
        )
        write_csv(
            self.aliases,
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


class CrossCountryWeekendInputTests(unittest.TestCase):
    def test_parses_official_fis_rows(self) -> None:
        athletes = parse_official_startlist(FIXTURE.read_bytes())
        self.assertEqual(len(athletes), 3)
        self.assertEqual(athletes[0].competitor_id, "1001")
        self.assertEqual(athletes[0].name, "NISKANEN Iivo")

    def test_builds_official_bundle_without_mutating_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            mtimes = {
                path: path.stat().st_mtime_ns
                for path in (data.schedule, data.men, data.ladies, data.aliases)
            }

            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                fetcher=lambda _url: FIXTURE.read_bytes(),
            )

            self.assertEqual(report["status"], "candidate")
            self.assertEqual(report["startlist_rows"], 3)
            self.assertEqual(
                report["events"][0]["source_mode"], "official_fis_startlist"
            )
            with (root / "bundle" / "men_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["canonical_athlete_id"], "11")
            self.assertEqual(rows[0]["selection_status"], "confirmed")
            self.assertEqual(rows[0]["race_id"], "52085")
            self.assertEqual(mtimes, {path: path.stat().st_mtime_ns for path in mtimes})

    def test_missing_official_list_uses_explicit_latest_season_pool(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="L", race_id="52084")])

            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                fetcher=lambda _url: (
                    b"<html><body>Start list not published</body></html>"
                ),
            )

            self.assertEqual(
                report["events"][0]["source_mode"], "latest_season_fallback"
            )
            with (root / "bundle" / "ladies_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual([row["canonical_athlete_id"] for row in rows], ["21"])
            self.assertEqual(rows[0]["selection_status"], "candidate_pool")
            self.assertEqual(rows[0]["fis_competitor_id"], "")

    def test_excludes_team_and_tour_work_from_normal_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(
                data.schedule,
                WEEKENDS_COLUMNS,
                [
                    race(sex="M", race_id="1", distance="Rel"),
                    race(sex="L", race_id="2", stage="1"),
                ],
            )
            fetched: list[str] = []
            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                fetcher=lambda url: fetched.append(url) or b"",
            )
            self.assertEqual(report["status"], "no_work")
            self.assertEqual(report["excluded_specialized_races"], 2)
            self.assertEqual(fetched, [])

    def test_unknown_official_identity_fails_closed(self) -> None:
        html = b'<a class="table-row" href="?competitorid=999"><div class="athlete-name">UNKNOWN Athlete</div><span class="country__name-short">NOR</span></a>'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            with self.assertRaisesRegex(WeekendInputError, "missing identity"):
                build_weekend_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    group_date=date(2026, 11, 27),
                    output_dir=root / "bundle",
                    fetcher=lambda _url: html,
                )

    def test_rejects_output_inside_accepted_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            with self.assertRaisesRegex(WeekendInputError, "accepted source"):
                build_weekend_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    group_date=date(2026, 11, 27),
                    output_dir=data.sources / "candidate",
                    fetcher=lambda _url: FIXTURE.read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
