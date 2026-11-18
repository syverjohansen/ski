import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_team_results_identity import (
    TeamIdentityError,
    map_team_identities,
    validate_team_memberships,
)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


TEAM_FIELDS = [
    "date",
    "sex",
    "season",
    "race",
    "distance",
    "place",
    "team_name",
    "nation",
    "team_bib",
    "member_order",
    "fis_competitor_id",
    "fis_code",
    "name",
]


class CrossCountryTeamResultsIdentityTests(unittest.TestCase):
    def build_sources(self, root: Path) -> tuple[Path, Path, Path, Path]:
        sources = root / "sources"
        sources.mkdir()
        men = sources / "men.csv"
        ladies = sources / "ladies.csv"
        aliases = sources / "aliases.csv"
        results = sources / "team_results.csv"
        history_fields = ["Date", "ID", "Skier", "Nation"]
        history = [
            {"Date": "2026-05-01", "ID": number, "Skier": f"Athlete {number}", "Nation": "Norway"}
            for number in range(1, 7)
        ]
        write_csv(men, history_fields, history)
        write_csv(ladies, history_fields, [])
        write_csv(
            aliases,
            ["sex", "fis_competitor_id", "fis_code", "fis_name", "canonical_id", "canonical_name"],
            [],
        )
        rows = []
        athlete = 1
        for place, (team, nation, bib) in enumerate(
            (("NORWAY", "NOR", 2), ("SWEDEN", "SWE", 4), ("FINLAND", "FIN", 6)),
            start=1,
        ):
            for order in (1, 2):
                rows.append(
                    {
                        "date": "2026-02-18",
                        "sex": "M",
                        "season": 2026,
                        "race": 12,
                        "distance": "Ts",
                        "place": place,
                        "team_name": team,
                        "nation": nation,
                        "team_bib": bib,
                        "member_order": order,
                        "fis_competitor_id": 1000 + athlete,
                        "fis_code": 3000000 + athlete,
                        "name": f"ATHLETE {athlete}",
                    }
                )
                athlete += 1
        write_csv(results, TEAM_FIELDS, rows)
        return results, men, ladies, aliases

    def test_maps_complete_team_memberships(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = self.build_sources(root)
            report = map_team_identities(
                results=results,
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                output_dir=root / "output",
            )
            self.assertEqual(report["event_count"], 1)
            self.assertEqual(report["team_count"], 3)
            self.assertEqual(report["member_rows"], 6)
            self.assertEqual(report["team_sprint_teams"], 3)
            self.assertEqual(report["team_membership_status"], "validated")
            with (root / "output" / "mapped_results.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                mapped = list(csv.DictReader(handle))
            self.assertEqual(
                [row["canonical_athlete_id"] for row in mapped],
                [str(number) for number in range(1, 7)],
            )

    def test_rejects_incomplete_team_before_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, _, _, _ = self.build_sources(root)
            fields, rows = self.read_rows(results)
            write_csv(results, fields, rows[:-1])
            with self.assertRaisesRegex(TeamIdentityError, "1 members; expected 2"):
                validate_team_memberships(results)

    def test_rejects_athlete_on_multiple_teams(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, _, _, _ = self.build_sources(root)
            fields, rows = self.read_rows(results)
            rows[-1]["fis_competitor_id"] = rows[0]["fis_competitor_id"]
            write_csv(results, fields, rows)
            with self.assertRaisesRegex(TeamIdentityError, "appears more than once"):
                validate_team_memberships(results)

    @staticmethod
    def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return list(reader.fieldnames or []), list(reader)


if __name__ == "__main__":
    unittest.main()
