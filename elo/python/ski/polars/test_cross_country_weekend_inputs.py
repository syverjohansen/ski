import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_weekend_inputs import (
    STARTLIST_FIELDS,
    WeekendInputError,
    build_weekend_inputs,
    parse_official_startlist,
)
from cross_country_announced_rosters import SCHEMA_VERSION as ANNOUNCED_SCHEMA
from cross_country_world_cup_quotas import DEFAULT_CONTRACT_PATH, QUOTA_FIELDS
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


def write_announcements(
    path: Path,
    entries: list[dict[str, object]],
    *,
    logical_date: str = "2026-11-27",
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": ANNOUNCED_SCHEMA,
                "status": "reviewed",
                "logical_date": logical_date,
                "publication": {"status": "disabled"},
                "entries": entries,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def announcement(
    athlete_id: str,
    status: str,
    *,
    sex: str = "M",
    race_ids: list[str] | None = None,
) -> dict[str, object]:
    return {
        "sex": sex,
        "canonical_athlete_id": athlete_id,
        "status": status,
        "race_ids": race_ids or ["*"],
        "source_url": "https://example.test/fantasyxc/team-news",
        "observed_at": "2026-11-26T18:30:00Z",
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

    def test_optional_official_quota_contract_annotates_candidate_pool(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])

            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                quota_contract_path=DEFAULT_CONTRACT_PATH,
                fetcher=lambda _url: b"<html>Not published</html>",
            )

            with (root / "bundle" / "men_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(
                    tuple(reader.fieldnames or ()),
                    (*STARTLIST_FIELDS, *QUOTA_FIELDS),
                )
                rows = {row["canonical_athlete_id"]: row for row in reader}
            niskanen = rows["11"]
            klaebo = rows["12"]
            self.assertEqual("6", niskanen["base_nation_quota"])
            self.assertEqual("10", niskanen["shared_event_quota"])
            self.assertEqual("1", niskanen["is_host_nation"])
            self.assertEqual("overall_world_cup", klaebo["personal_start_right"])
            self.assertEqual("1", klaebo["personal_quota"])
            self.assertEqual("6", klaebo["shared_event_quota"])
            self.assertTrue(
                report["quota_contract"]["personal_rights_are_non_transferable"]
            )

    def test_quota_contract_applies_team_event_weekend_minimum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(
                data.men,
                ["Date", "Season", "Race", "ID", "Skier", "Nation"],
                [
                    {
                        "Date": "2026-03-01",
                        "Season": "2026",
                        "Race": "1",
                        "ID": "31",
                        "Skier": "Andorran Athlete",
                        "Nation": "Andorra",
                    }
                ],
            )
            write_csv(
                data.schedule,
                WEEKENDS_COLUMNS,
                [
                    race(sex="M", race_id="52085"),
                    race(sex="M", race_id="52086", distance="Ts"),
                ],
            )

            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                quota_contract_path=DEFAULT_CONTRACT_PATH,
                fetcher=lambda _url: b"<html>Not published</html>",
            )

            with (root / "bundle" / "men_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                row = next(csv.DictReader(handle))
            self.assertTrue(report["team_event_weekend"])
            self.assertEqual(1, report["excluded_specialized_races"])
            self.assertEqual("3", row["base_nation_quota"])
            self.assertEqual("4", row["shared_event_quota"])
            self.assertEqual("1", row["team_event_weekend_exception"])

    def test_reviewed_announcements_overlay_history_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            announced = data.sources / "announced.json"
            write_announcements(
                announced,
                [announcement("11", "excluded"), announcement("12", "confirmed")],
            )

            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                announced_rosters_path=announced,
                fetcher=lambda _url: b"<html>Not published</html>",
            )

            self.assertEqual(
                "announced_roster_overlay", report["events"][0]["source_mode"]
            )
            self.assertEqual(1, report["events"][0]["announced_rosters"]["confirmed"])
            self.assertEqual(1, report["events"][0]["announced_rosters"]["excluded"])
            with (root / "bundle/men_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(
                ["12", "13"], [row["canonical_athlete_id"] for row in rows]
            )
            selected = {row["canonical_athlete_id"]: row for row in rows}
            self.assertEqual("announced", selected["12"]["selection_status"])
            self.assertEqual(
                "announced_roster_evidence", selected["12"]["source_mode"]
            )
            self.assertEqual("candidate_pool", selected["13"]["selection_status"])

    def test_official_startlist_supersedes_announced_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            announced = data.sources / "announced.json"
            write_announcements(announced, [announcement("11", "excluded")])

            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                announced_rosters_path=announced,
                fetcher=lambda _url: FIXTURE.read_bytes(),
            )

            self.assertEqual(
                "official_fis_startlist", report["events"][0]["source_mode"]
            )
            self.assertEqual(
                1,
                report["events"][0]["announced_rosters"][
                    "ignored_because_official"
                ],
            )
            with (root / "bundle/men_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(3, len(rows))
            self.assertIn("11", {row["canonical_athlete_id"] for row in rows})

    def test_event_specific_announcement_overrides_weekend_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            announced = data.sources / "announced.json"
            write_announcements(
                announced,
                [
                    announcement("11", "excluded"),
                    announcement("11", "confirmed", race_ids=["52085"]),
                ],
            )
            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                announced_rosters_path=announced,
                fetcher=lambda _url: b"<html>Not published</html>",
            )
            with (root / "bundle/men_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            selected = {row["canonical_athlete_id"]: row for row in rows}
            self.assertEqual("announced", selected["11"]["selection_status"])
            self.assertEqual(
                "confirmed",
                report["events"][0]["announced_rosters"]["evidence"][0]["status"],
            )

    def test_announced_roster_unknown_identity_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            announced = data.sources / "announced.json"
            write_announcements(announced, [announcement("999", "confirmed")])
            with self.assertRaisesRegex(WeekendInputError, "unknown canonical athlete"):
                build_weekend_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    group_date=date(2026, 11, 27),
                    output_dir=root / "bundle",
                    announced_rosters_path=announced,
                    fetcher=lambda _url: b"<html>Not published</html>",
                )

    def test_announced_roster_date_must_match_weekend(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            announced = data.sources / "announced.json"
            write_announcements(
                announced,
                [announcement("11", "confirmed")],
                logical_date="2026-12-04",
            )
            with self.assertRaisesRegex(WeekendInputError, "logical date"):
                build_weekend_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    group_date=date(2026, 11, 27),
                    output_dir=root / "bundle",
                    announced_rosters_path=announced,
                    fetcher=lambda _url: b"<html>Not published</html>",
                )

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

    def test_shadow_mode_namespaces_unknown_official_identity(self) -> None:
        html = b'<a class="table-row" href="?competitorid=999"><div class="athlete-name">UNKNOWN Athlete</div><span class="country__name-short">NOR</span><span class="justify-right gray pr-1">3999999</span></a>'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = WeekendInputFixture(root)
            write_csv(data.schedule, WEEKENDS_COLUMNS, [race(sex="M", race_id="52085")])
            report = build_weekend_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                group_date=date(2026, 11, 27),
                output_dir=root / "bundle",
                fetcher=lambda _url: html,
                prediction_shadow=True,
            )
            with (root / "bundle" / "men_startlist.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                row = next(csv.DictReader(handle))
            self.assertTrue(report["prediction_shadow"])
            self.assertEqual(row["canonical_athlete_id"], "fis:999")
            self.assertEqual(row["fis_code"], "3999999")
            self.assertEqual(
                row["identity_match_method"], "prediction_only_fis_identity"
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
