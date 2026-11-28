from pathlib import Path
import tempfile
import unittest

from cross_country_identity_repair import repair, read, write
from cross_country_results_elo_replay import STREAMS, PREVIOUS_COLUMNS


class IdentityRepairTests(unittest.TestCase):
    def test_replays_duplicate_from_history_and_preserves_real_debut_seed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sources"
            source.mkdir()
            base = dict(
                Date="2026-05-01",
                City="Ruka",
                Country="Finland",
                Sex="M",
                Distance="10",
                Event="Offseason",
                MS="0",
                Technique="C",
                Place="1",
                Skier="Historical Athlete",
                Nation="Norway",
                ID="100",
                Season="2026",
                Race="0",
                Birthday="1990-01-01",
                Age="36",
                Exp="20",
                **{field: "1600" for field in [*STREAMS, *PREVIOUS_COLUMNS.values()]},
            )
            duplicate = dict(
                base,
                Date="2026-11-27",
                Event="World Cup",
                Season="2027",
                Race="1",
                ID="3999999",
                Skier="Renamed Athlete",
                Exp="1",
                **{field: "1300" for field in [*STREAMS, *PREVIOUS_COLUMNS.values()]},
            )
            debut = dict(
                duplicate,
                ID="3888888",
                Skier="Real Debut",
                Place="2",
                **{field: "1440" for field in [*STREAMS, *PREVIOUS_COLUMNS.values()]},
            )
            men, ladies, aliases = [
                source / name for name in ["men.csv", "ladies.csv", "aliases.csv"]
            ]
            write(men, list(base), [base, duplicate, debut])
            write(ladies, list(base), [])
            alias = dict(
                sex="M",
                fis_competitor_id="999",
                fis_code="3999999",
                fis_name="Renamed Athlete",
                canonical_id="100",
                canonical_name="Historical Athlete",
            )
            write(aliases, list(alias), [alias])
            original = men.read_bytes()
            report = repair(
                men=men,
                ladies=ladies,
                aliases=aliases,
                season=2027,
                output=root / "output",
            )
            _, rows = read(root / "output/men_chrono.csv")
            self.assertEqual(rows[0], base)
            by_id = {r["ID"]: r for r in rows if r["Season"] == "2027"}
            self.assertEqual(set(by_id), {"100", "3888888"})
            self.assertAlmostEqual(float(by_id["100"]["Pelo"]), 1555)
            self.assertAlmostEqual(float(by_id["3888888"]["Pelo"]), 1440)
            self.assertEqual(by_id["100"]["Exp"], "21")
            self.assertEqual(men.read_bytes(), original)
            self.assertTrue(report["prior_seasons_preserved"])
