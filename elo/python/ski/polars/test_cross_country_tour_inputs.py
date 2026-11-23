import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_tour_inputs import TourInputError, build_tour_inputs

HERE = Path(__file__).resolve().parent
SCHEDULE = (
    HERE / "tests" / "fixtures" / "cross_country_tour_contract" / "2027-tour.csv"
)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fis_html(*athletes: tuple[str, str, str, str]) -> bytes:
    rows = []
    for competitor_id, fis_code, name, nation in athletes:
        rows.append(
            f'<a class="table-row" href="?competitorid={competitor_id}">'
            f'<span class="athlete-name">{name}</span>'
            f'<span class="country__name-short">{nation}</span>'
            f'<span class="justify-right gray pr-1">{fis_code}</span></a>'
        )
    return ("<html><body>" + "".join(rows) + "</body></html>").encode()


class TourInputFixture:
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
                },
                {
                    "Date": "2026-03-02",
                    "Season": "2026",
                    "Race": "2",
                    "ID": "12",
                    "Skier": "Harald Oestberg Amundsen",
                    "Nation": "Norway",
                },
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
            [
                {
                    "sex": "M",
                    "fis_competitor_id": "1001",
                    "fis_code": "3421320",
                    "fis_name": "KLAEBO Johannes Hoesflot",
                    "canonical_id": "11",
                    "canonical_name": "Johannes Hoesflot Klaebo",
                }
            ],
        )

    def fetch(self, url: str) -> bytes:
        if "raceid=52111" in url:
            return fis_html(
                ("1001", "3421320", "KLAEBO Johannes Hoesflot", "NOR"),
                ("1002", "3421507", "AMUNDSEN Harald Oestberg", "NOR"),
            )
        if "raceid=52110" in url:
            return fis_html(("2001", "3535410", "DIGGINS Jessie", "USA"))
        raise AssertionError(f"unexpected fetch: {url}")


class CrossCountryTourInputTests(unittest.TestCase):
    def test_builds_official_opening_bundle_without_mutating_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = TourInputFixture(root)
            sources = (data.schedule, data.men, data.ladies, data.aliases)
            before = {path: path.read_bytes() for path in sources}
            report = build_tour_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 1),
                output_dir=root / "bundle",
                fetcher=data.fetch,
            )
            self.assertEqual("candidate", report["status"])
            self.assertEqual(3, report["startlist_rows"])
            self.assertEqual("ready", report["plan"]["status"])
            self.assertEqual(
                "zero_not_used_by_overall_model", report["price_policy"]
            )
            with (root / "bundle" / "startlist_tds_men.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                men = list(csv.DictReader(handle))
            self.assertEqual(["11", "12"], [row["ID"] for row in men])
            self.assertEqual("0", men[0]["Price"])
            with (root / "bundle" / "opening_startlist_provenance.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                provenance = list(csv.DictReader(handle))
            self.assertEqual("reviewed_alias", provenance[0]["identity_match_method"])
            self.assertEqual(
                "unique_normalized_name", provenance[1]["identity_match_method"]
            )
            self.assertEqual(before, {path: path.read_bytes() for path in sources})

    def test_unpublished_list_writes_waiting_report_and_no_model_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = TourInputFixture(root)

            def one_missing(url: str) -> bytes:
                if "raceid=52110" in url:
                    return b"<html><body>Not published</body></html>"
                return data.fetch(url)

            output = root / "bundle"
            ready = build_tour_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 1),
                output_dir=output,
                fetcher=data.fetch,
            )
            self.assertEqual("candidate", ready["status"])
            self.assertTrue((output / "startlist_tds_men.csv").exists())
            report = build_tour_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 1),
                output_dir=output,
                fetcher=one_missing,
            )
            self.assertEqual("waiting_for_startlists", report["status"])
            self.assertEqual(("52110",), report["plan"]["missing_startlist_race_ids"])
            self.assertTrue((output / "report.json").exists())
            self.assertFalse((output / "startlist_tds_men.csv").exists())
            self.assertFalse((output / "startlist_tds_ladies.csv").exists())

    def test_non_opening_date_is_no_work_and_does_not_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = TourInputFixture(root)
            fetched: list[str] = []
            report = build_tour_inputs(
                schedule=data.schedule,
                men_chrono=data.men,
                ladies_chrono=data.ladies,
                aliases_path=data.aliases,
                logical_date=date(2027, 1, 2),
                output_dir=root / "bundle",
                fetcher=lambda url: fetched.append(url) or b"",
            )
            self.assertEqual("no_work", report["status"])
            self.assertEqual([], fetched)

    def test_unknown_official_identity_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = TourInputFixture(root)

            def unknown(url: str) -> bytes:
                if "raceid=52111" in url:
                    return fis_html(("9999", "9999999", "UNKNOWN Athlete", "NOR"))
                return data.fetch(url)

            with self.assertRaisesRegex(TourInputError, "missing identity"):
                build_tour_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    logical_date=date(2027, 1, 1),
                    output_dir=root / "bundle",
                    fetcher=unknown,
                )

    def test_rejects_output_inside_accepted_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = TourInputFixture(root)
            with self.assertRaisesRegex(TourInputError, "accepted source"):
                build_tour_inputs(
                    schedule=data.schedule,
                    men_chrono=data.men,
                    ladies_chrono=data.ladies,
                    aliases_path=data.aliases,
                    logical_date=date(2027, 1, 1),
                    output_dir=data.accepted / "bundle",
                    fetcher=data.fetch,
                )


if __name__ == "__main__":
    unittest.main()
