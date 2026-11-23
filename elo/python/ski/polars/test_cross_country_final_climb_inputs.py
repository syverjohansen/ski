import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_final_climb_inputs import build_final_climb_inputs
from cross_country_tour_inputs import TourInputError

HERE = Path(__file__).resolve().parent
SCHEDULE = HERE / "tests" / "fixtures" / "cross_country_tour_contract" / "2027-tour.csv"


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fis_html(*athletes: tuple[str, str, str, str]) -> bytes:
    rows = [
        f'<a class="table-row" href="?competitorid={competitor_id}">'
        f'<span class="athlete-name">{name}</span>'
        f'<span class="country__name-short">{nation}</span>'
        f'<span class="justify-right gray pr-1">{fis_code}</span></a>'
        for competitor_id, fis_code, name, nation in athletes
    ]
    return ("<html><body>" + "".join(rows) + "</body></html>").encode()


class FinalClimbFixture:
    def __init__(self, root: Path) -> None:
        self.accepted = root / "accepted"
        self.schedule = self.accepted / "weekends.csv"
        self.schedule.parent.mkdir(parents=True)
        self.schedule.write_bytes(SCHEDULE.read_bytes())
        self.men = self.accepted / "men_chrono.csv"
        self.ladies = self.accepted / "ladies_chrono.csv"
        self.aliases = self.accepted / "fis_identity_aliases.csv"
        fields = ["Date", "Season", "Race", "ID", "Skier", "Nation"]
        write_csv(
            self.men,
            fields,
            [
                {
                    "Date": "2026-03-01",
                    "Season": "2026",
                    "Race": "1",
                    "ID": "11",
                    "Skier": "Johannes Hoesflot Klaebo",
                    "Nation": "Norway",
                }
            ],
        )
        write_csv(
            self.ladies,
            fields,
            [
                {
                    "Date": "2026-03-01",
                    "Season": "2026",
                    "Race": "1",
                    "ID": "21",
                    "Skier": "Jessie Diggins",
                    "Nation": "United States",
                }
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

    def fetch(self, url: str) -> bytes:
        if "raceid=52129" in url:
            return fis_html(("1001", "3421320", "Johannes Hoesflot Klaebo", "NOR"))
        if "raceid=52128" in url:
            return fis_html(("2001", "3535410", "Jessie Diggins", "USA"))
        raise AssertionError(f"unexpected fetch: {url}")


class CrossCountryFinalClimbInputTests(unittest.TestCase):
    def test_builds_official_bundle_without_mutating_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = FinalClimbFixture(root)
            sources = (data.schedule, data.men, data.ladies, data.aliases)
            before = {path: path.read_bytes() for path in sources}
            report = build_final_climb_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 9),
                output_dir=root / "bundle",
                fetcher=data.fetch,
            )
            self.assertEqual("candidate", report["status"])
            self.assertEqual(2, report["startlist_rows"])
            self.assertEqual("ready", report["plan"]["status"])
            with (root / "bundle" / "schedule.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                self.assertEqual(2, len(list(csv.DictReader(handle))))
            with (root / "bundle" / "startlist_final_climb_men.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                men = list(csv.DictReader(handle))
            self.assertEqual("11", men[0]["ID"])
            self.assertEqual("0", men[0]["Price"])
            self.assertEqual(before, {path: path.read_bytes() for path in sources})

    def test_missing_list_clears_stale_candidate_and_waits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = FinalClimbFixture(root)
            output = root / "bundle"
            build_final_climb_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 9),
                output_dir=output,
                fetcher=data.fetch,
            )

            def missing_ladies(url: str) -> bytes:
                return (
                    b"<html>not published</html>" if "52128" in url else data.fetch(url)
                )

            report = build_final_climb_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 10),
                output_dir=output,
                fetcher=missing_ladies,
            )
            self.assertEqual("waiting_for_startlists", report["status"])
            self.assertFalse((output / "startlist_final_climb_men.csv").exists())

    def test_outside_window_is_no_work_without_fetching(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = FinalClimbFixture(root)
            fetched: list[str] = []
            report = build_final_climb_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 8),
                output_dir=root / "bundle",
                fetcher=lambda url: fetched.append(url) or b"",
            )
            self.assertEqual("no_work", report["status"])
            self.assertEqual([], fetched)

    def test_unknown_official_athlete_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = FinalClimbFixture(root)

            def unknown_men(url: str) -> bytes:
                if "raceid=52129" in url:
                    return fis_html(("9999", "9999999", "Unknown Athlete", "NOR"))
                return data.fetch(url)

            with self.assertRaisesRegex(TourInputError, "missing identity"):
                build_final_climb_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    logical_date=date(2027, 1, 9),
                    output_dir=root / "bundle",
                    fetcher=unknown_men,
                )

    def test_rejects_output_inside_accepted_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = FinalClimbFixture(root)
            with self.assertRaisesRegex(TourInputError, "accepted source"):
                build_final_climb_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    logical_date=date(2027, 1, 9),
                    output_dir=data.accepted / "bundle",
                    fetcher=data.fetch,
                )


if __name__ == "__main__":
    unittest.main()
