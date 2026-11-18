import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_results_identity import (
    IdentityError,
    load_aliases,
    load_history,
    map_identities,
    normalized_name,
)

HERE = Path(__file__).resolve().parent


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryResultsIdentityTests(unittest.TestCase):
    def test_normalization_handles_name_order_and_nordic_transliteration(self) -> None:
        canonical = normalized_name("Johannes Høsflot Klæbo")
        self.assertEqual(canonical, normalized_name("KLAEBO Johannes Hoesflot"))
        self.assertEqual(normalized_name("Frida Karlsson"), ("frida", "karlsson"))

    def test_real_histories_load_as_canonical_identity_sources(self) -> None:
        data = HERE / "excel365"
        athletes, name_index, _ = load_history(
            data / "men_chrono.csv", data / "ladies_chrono.csv"
        )
        self.assertGreater(len(athletes), 8_000)
        self.assertEqual(
            name_index[("M", normalized_name("KLAEBO Johannes Hoesflot"))],
            {"5164"},
        )

    def test_reviewed_fis_aliases_resolve_to_real_history(self) -> None:
        data = HERE / "excel365"
        athletes, _, names_by_id = load_history(
            data / "men_chrono.csv", data / "ladies_chrono.csv"
        )
        aliases = load_aliases(
            data / "fis_identity_aliases.csv", athletes, names_by_id
        )
        self.assertGreaterEqual(len(aliases), 7)
        self.assertEqual(aliases[("L", "210291")].canonical_id, "6639")
        self.assertEqual(aliases[("L", "261973")].canonical_id, "10268")

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
            write_csv(
                men,
                history_fields,
                [
                    {
                        "Date": "2026-05-01",
                        "ID": 5164,
                        "Skier": "Johannes Høsflot Klæbo",
                        "Nation": "Norway",
                    },
                    {
                        "Date": "2026-05-01",
                        "ID": 1470,
                        "Skier": "Iivo Niskanen",
                        "Nation": "Finland",
                    },
                ],
            )
            write_csv(ladies, history_fields, [])
            result_fields = [
                "date",
                "sex",
                "fis_competitor_id",
                "fis_code",
                "name",
                "nation",
            ]
            write_csv(
                results,
                result_fields,
                [
                    {
                        "date": "2026-11-27",
                        "sex": "M",
                        "fis_competitor_id": 1002,
                        "fis_code": 3420011,
                        "name": "KLAEBO Johannes Hoesflot",
                        "nation": "NOR",
                    },
                    {
                        "date": "2026-11-27",
                        "sex": "M",
                        "fis_competitor_id": 1001,
                        "fis_code": 3100010,
                        "name": "NISKANEN Iivo M",
                        "nation": "FIN",
                    },
                ],
            )
            alias_fields = [
                "sex",
                "fis_competitor_id",
                "fis_code",
                "fis_name",
                "canonical_id",
                "canonical_name",
            ]
            write_csv(
                aliases,
                alias_fields,
                [
                    {
                        "sex": "M",
                        "fis_competitor_id": 1001,
                        "fis_code": 3100010,
                        "fis_name": "NISKANEN Iivo M",
                        "canonical_id": 1470,
                        "canonical_name": "Iivo Niskanen",
                    }
                ],
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
                [row["canonical_athlete_id"] for row in mapped], ["5164", "1470"]
            )

    def test_fails_closed_for_unknown_or_ambiguous_athlete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            fields = ["Date", "ID", "Skier", "Nation"]
            write_csv(
                sources / "men.csv",
                fields,
                [
                    {"Date": "2026-01-01", "ID": 1, "Skier": "Same Name", "Nation": "A"},
                    {"Date": "2026-01-01", "ID": 2, "Skier": "Same Name", "Nation": "B"},
                ],
            )
            write_csv(sources / "ladies.csv", fields, [])
            result_fields = [
                "sex",
                "fis_competitor_id",
                "fis_code",
                "name",
                "nation",
            ]
            alias_fields = [
                "sex",
                "fis_competitor_id",
                "fis_code",
                "fis_name",
                "canonical_id",
                "canonical_name",
            ]
            write_csv(sources / "aliases.csv", alias_fields, [])
            for athlete_name, expected in (
                ("Unknown Athlete", "missing identity"),
                ("Same Name", "ambiguous identity"),
            ):
                write_csv(
                    sources / "results.csv",
                    result_fields,
                    [
                        {
                            "sex": "M",
                            "fis_competitor_id": 9,
                            "fis_code": 99,
                            "name": athlete_name,
                            "nation": "NOR",
                        }
                    ],
                )
                with self.assertRaisesRegex(IdentityError, expected):
                    map_identities(
                        results=sources / "results.csv",
                        men_chrono=sources / "men.csv",
                        ladies_chrono=sources / "ladies.csv",
                        aliases_path=sources / "aliases.csv",
                        output_dir=root / f"output-{athlete_name.split()[0]}",
                    )


if __name__ == "__main__":
    unittest.main()
