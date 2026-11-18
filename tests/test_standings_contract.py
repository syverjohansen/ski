import importlib.util
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from elo.python.standings_contract import (
    StandingsContractError,
    atomic_write_standings,
    canonicalize_standings,
    normalized_name,
    season_for_date,
    validate_standings,
)

ROOT = Path(__file__).resolve().parents[1]


def _load_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ALPINE = _load_script(
    "alpine_standings",
    ROOT / "elo/python/alpine/polars/standings_scrape.py",
)
CROSS_COUNTRY = _load_script(
    "cross_country_standings",
    ROOT / "elo/python/ski/polars/standings_scrape.py",
)

HTML = """
<a class="table-row" href="athlete-biography.html?competitorid=123">
  <div class="justify-left bold align-xs-top">LOVELACE Ada</div>
  <span class="country__name-short">NOR</span>
  <div class="justify-right bold">1</div>
  <div class="justify-right bold">250</div>
</a>
<a class="table-row" href="athlete-biography.html?competitorid=456">
  <div class="justify-left bold align-xs-top">HOPPER Grace</div>
  <span class="country__name-short">USA</span>
  <div class="justify-right bold">2</div>
  <div class="justify-right bold">190</div>
</a>
"""


class StandingsContractTests(unittest.TestCase):
    def test_derives_fis_season_ending_year(self) -> None:
        self.assertEqual(season_for_date(date(2026, 6, 30)), 2026)
        self.assertEqual(season_for_date(date(2026, 7, 1)), 2027)

    def test_normalizes_fis_transliterated_names(self) -> None:
        self.assertEqual(
            normalized_name("HAECHLER Lenz"), normalized_name("Lenz Hächler")
        )
        self.assertEqual(normalized_name("HURT A J"), normalized_name("AJ Hurt"))

    def test_urls_include_explicit_season_and_gender(self) -> None:
        self.assertEqual(
            ALPINE.standings_url(2027, "ladies"),
            "https://www.fis-ski.com/DB/alpine-skiing/cup-standings.html"
            "?cupcode=WC&disciplinecode=ALL&gendercode=W"
            "&nationcode=&seasoncode=2027&sectorcode=AL",
        )
        self.assertEqual(
            CROSS_COUNTRY.standings_url(2027, "men"),
            "https://www.fis-ski.com/DB/cross-country/cup-standings.html"
            "?cupcode=WC&disciplinecode=ALL&gendercode=M"
            "&nationcode=&seasoncode=2027&sectorcode=CC",
        )
        self.assertEqual(
            CROSS_COUNTRY.standings_url(2027, "ladies"),
            "https://www.fis-ski.com/DB/cross-country/cup-standings.html"
            "?cupcode=WC&disciplinecode=ALL&gendercode=W"
            "&nationcode=&seasoncode=2027&sectorcode=CC",
        )

    def test_parsers_extract_complete_ranked_rows(self) -> None:
        for module in (ALPINE, CROSS_COUNTRY):
            rows = module.parse_standings(HTML)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["ID"], "123")
            self.assertEqual(rows[0]["Points"], 250)
            self.assertEqual(rows[1]["Ranking"], 2)

    def test_canonicalizes_official_fis_names_to_internal_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            history = Path(directory) / "men_chrono.csv"
            pd.DataFrame(
                [
                    {
                        "ID": "9001",
                        "Skier": "Ada Lovelace",
                        "Nation": "GBR",
                        "Date": "2026-03-01",
                    }
                ]
            ).to_csv(history, index=False)
            rows = canonicalize_standings(
                [ALPINE.parse_standings(HTML)[0]], history, "men"
            )
            self.assertEqual(rows[0]["ID"], "9001")
            self.assertEqual(rows[0]["Skier"], "Ada Lovelace")
            self.assertEqual(rows[0]["Nation"], "GBR")

    def test_uses_reviewed_fis_id_alias_before_name_matching(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "ladies_chrono.csv"
            aliases = root / "aliases.csv"
            pd.DataFrame(
                [
                    {
                        "ID": "4212",
                        "Skier": "Brett Richardson",
                        "Nation": "Canada",
                        "Date": "2026-10-24",
                    }
                ]
            ).to_csv(history, index=False)
            pd.DataFrame(
                [
                    {
                        "sex": "L",
                        "fis_competitor_id": "240548",
                        "fis_code": "108461",
                        "fis_name": "RICHARDSON Britt",
                        "canonical_id": "4212",
                        "canonical_name": "Brett Richardson",
                    }
                ]
            ).to_csv(aliases, index=False)
            rows = canonicalize_standings(
                [
                    {
                        "ID": "240548",
                        "Skier": "RICHARDSON Britt",
                        "Nation": "CAN",
                        "Ranking": 1,
                        "Points": 100,
                    }
                ],
                history,
                "ladies",
                aliases,
            )
            self.assertEqual(rows[0]["ID"], "4212")
            self.assertEqual(rows[0]["Skier"], "Brett Richardson")

    def test_empty_or_duplicate_results_fail_validation(self) -> None:
        with self.assertRaisesRegex(StandingsContractError, "no men standings"):
            validate_standings([], "men")
        records = ALPINE.parse_standings(HTML)
        records[1]["ID"] = records[0]["ID"]
        with self.assertRaisesRegex(StandingsContractError, "duplicate athlete IDs"):
            validate_standings(records, "men")

    def test_tied_rankings_are_valid(self) -> None:
        records = ALPINE.parse_standings(HTML)
        records[1]["Ranking"] = records[0]["Ranking"]
        self.assertEqual(len(validate_standings(records, "men")), 2)

    def test_invalid_candidate_does_not_replace_accepted_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "men_standings.csv"
            output.write_text("accepted\n", encoding="utf-8")
            with self.assertRaises(StandingsContractError):
                atomic_write_standings([], output, "men")
            self.assertEqual(output.read_text(encoding="utf-8"), "accepted\n")

            count = atomic_write_standings(ALPINE.parse_standings(HTML), output, "men")
            self.assertEqual(count, 2)
            frame = pd.read_csv(output)
            self.assertEqual(frame["ID"].astype(str).tolist(), ["123", "456"])

    def test_one_empty_gender_prevents_both_publications(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            men = output / "men_standings.csv"
            ladies = output / "ladies_standings.csv"
            men.write_text("accepted-men\n", encoding="utf-8")
            ladies.write_text("accepted-ladies\n", encoding="utf-8")
            with (
                patch.object(
                    ALPINE,
                    "parse_args",
                    return_value=SimpleNamespace(
                        season=2027,
                        output_dir=output,
                        men_history=None,
                        ladies_history=None,
                        identity_aliases=None,
                    ),
                ),
                patch.object(
                    ALPINE,
                    "scrape",
                    side_effect=[ALPINE.parse_standings(HTML), []],
                ),
                patch.object(
                    ALPINE,
                    "canonicalize_standings",
                    side_effect=lambda records, *_: records,
                ),
                self.assertRaisesRegex(RuntimeError, "no ladies standings"),
            ):
                ALPINE.main()
            self.assertEqual(men.read_text(encoding="utf-8"), "accepted-men\n")
            self.assertEqual(ladies.read_text(encoding="utf-8"), "accepted-ladies\n")


if __name__ == "__main__":
    unittest.main()
