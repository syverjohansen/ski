import csv
import tempfile
import unittest
from pathlib import Path

from alpine_results_identity import IdentityError, map_identities, normalized_name


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class AlpineResultsIdentityTest(unittest.TestCase):
    def test_normalization_ignores_name_order_and_diacritics(self) -> None:
        self.assertEqual(normalized_name("AICHER Emma"), normalized_name("Emma Aicher"))
        self.assertEqual(normalized_name("Lenz Hächler"), ("hachler", "lenz"))

    def test_maps_unique_name_and_reviewed_alias(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            men = sources / "men.csv"
            ladies = sources / "ladies.csv"
            results = sources / "results.csv"
            aliases = sources / "aliases.csv"
            history_fields = ["Date", "ID", "Skier", "Nation"]
            write_csv(men, history_fields, [])
            write_csv(
                ladies,
                history_fields,
                [
                    {"Date": "2026-05-01", "ID": 4194, "Skier": "Emma Aicher", "Nation": "Germany"},
                    {"Date": "2026-05-01", "ID": 28, "Skier": "Lena Dürr", "Nation": "Germany"},
                ],
            )
            result_fields = ["date", "sex", "fis_competitor_id", "fis_code", "name", "nation"]
            write_csv(
                results,
                result_fields,
                [
                    {"date": "2026-10-24", "sex": "L", "fis_competitor_id": 237841, "fis_code": 507168, "name": "AICHER Emma", "nation": "GER"},
                    {"date": "2026-10-24", "sex": "L", "fis_competitor_id": 126526, "fis_code": 206355, "name": "DUERR Lena", "nation": "GER"},
                ],
            )
            alias_fields = ["sex", "fis_competitor_id", "fis_code", "fis_name", "canonical_id", "canonical_name"]
            write_csv(
                aliases,
                alias_fields,
                [{"sex": "L", "fis_competitor_id": 126526, "fis_code": 206355, "fis_name": "DUERR Lena", "canonical_id": 28, "canonical_name": "Lena Dürr"}],
            )
            report = map_identities(
                results=results,
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                output_dir=root / "output",
            )
            self.assertEqual(report["result_rows"], 2)
            self.assertEqual(
                report["match_methods"],
                {"reviewed_alias": 1, "unique_normalized_name": 1},
            )
            with (root / "output" / "mapped_results.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                mapped = list(csv.DictReader(handle))
            self.assertEqual(
                [row["canonical_athlete_id"] for row in mapped], ["4194", "28"]
            )

    def test_fails_closed_for_unknown_athlete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            fields = ["Date", "ID", "Skier", "Nation"]
            write_csv(sources / "men.csv", fields, [])
            write_csv(sources / "ladies.csv", fields, [])
            write_csv(
                sources / "results.csv",
                ["sex", "fis_competitor_id", "fis_code", "name", "nation"],
                [{"sex": "L", "fis_competitor_id": 1, "fis_code": 2, "name": "UNKNOWN Athlete", "nation": "NOR"}],
            )
            write_csv(
                sources / "aliases.csv",
                ["sex", "fis_competitor_id", "fis_code", "fis_name", "canonical_id", "canonical_name"],
                [],
            )
            with self.assertRaisesRegex(IdentityError, "missing identity"):
                map_identities(
                    results=sources / "results.csv",
                    men_chrono=sources / "men.csv",
                    ladies_chrono=sources / "ladies.csv",
                    aliases_path=sources / "aliases.csv",
                    output_dir=root / "output",
                )


if __name__ == "__main__":
    unittest.main()
