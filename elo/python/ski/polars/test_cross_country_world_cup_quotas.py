import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_world_cup_quotas import (
    DEFAULT_CONTRACT_PATH,
    QuotaContractError,
    load_quota_contract,
)


class CrossCountryWorldCupQuotaTests(unittest.TestCase):
    def test_loads_official_2026_27_basic_quotas(self) -> None:
        contract = load_quota_contract(
            DEFAULT_CONTRACT_PATH, logical_date=date(2026, 11, 27)
        )

        self.assertEqual(6, contract.base_quota("Norway", "M"))
        self.assertEqual(5, contract.base_quota("France", "L"))
        self.assertEqual(4, contract.base_quota("Spain", "M"))
        self.assertEqual(2, contract.base_quota("Brazil", "L"))

    def test_host_shared_quota_is_capped_at_ten(self) -> None:
        contract = load_quota_contract(logical_date=date(2026, 11, 27))

        assignment = contract.assignment(
            athlete_name="Iivo Niskanen",
            nation="Finland",
            sex="M",
            event_country="Finland",
            logical_date=date(2026, 11, 27),
        )

        self.assertEqual("6", assignment["base_nation_quota"])
        self.assertEqual("10", assignment["shared_event_quota"])
        self.assertEqual("1", assignment["is_host_nation"])
        self.assertEqual("10", assignment["max_nation_starters"])

    def test_overall_and_coc_rights_are_nominative(self) -> None:
        contract = load_quota_contract(logical_date=date(2026, 11, 27))

        klaebo = contract.assignment(
            athlete_name="Johannes Høsflot Klæbo",
            nation="Norway",
            sex="M",
            event_country="Finland",
            logical_date=date(2026, 11, 27),
        )
        michelsen = contract.assignment(
            athlete_name="Even Solem Michelsen",
            nation="Norway",
            sex="M",
            event_country="Finland",
            logical_date=date(2026, 11, 27),
        )
        nyenget = contract.assignment(
            athlete_name="Martin Loewstroem Nyenget",
            nation="Norway",
            sex="M",
            event_country="Finland",
            logical_date=date(2026, 11, 27),
        )

        self.assertEqual("overall_world_cup", klaebo["personal_start_right"])
        self.assertEqual("1", klaebo["personal_quota"])
        self.assertEqual("continental_cup", michelsen["personal_start_right"])
        self.assertEqual("1", michelsen["personal_quota"])
        self.assertEqual("", nyenget["personal_start_right"])
        self.assertEqual("0", nyenget["personal_quota"])
        # Neither personal right is added to Norway's shared six places.
        self.assertEqual("6", klaebo["shared_event_quota"])
        self.assertEqual("6", michelsen["shared_event_quota"])

    def test_team_event_weekend_raises_small_shared_quota_to_four(self) -> None:
        contract = load_quota_contract(logical_date=date(2026, 11, 27))

        assignment = contract.assignment(
            athlete_name="Test Athlete",
            nation="Andorra",
            sex="M",
            event_country="Finland",
            logical_date=date(2026, 11, 27),
            team_event_weekend=True,
        )

        self.assertEqual("3", assignment["base_nation_quota"])
        self.assertEqual("4", assignment["shared_event_quota"])
        self.assertEqual("1", assignment["team_event_weekend_exception"])

    def test_period_one_right_expires_without_changing_basic_quota(self) -> None:
        contract = load_quota_contract(logical_date=date(2027, 1, 10))

        assignment = contract.assignment(
            athlete_name="Even Solem Michelsen",
            nation="NOR",
            sex="M",
            event_country="GER",
            logical_date=date(2027, 1, 10),
        )

        self.assertEqual("6", assignment["base_nation_quota"])
        self.assertEqual("0", assignment["personal_quota"])

    def test_rejects_a_contract_outside_its_season(self) -> None:
        with self.assertRaisesRegex(QuotaContractError, "does not cover"):
            load_quota_contract(logical_date=date(2027, 11, 24))

    def test_rejects_transferable_personal_right_policy(self) -> None:
        document = json.loads(DEFAULT_CONTRACT_PATH.read_text(encoding="utf-8"))
        document["rules"]["personal_rights_are_non_transferable"] = False
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quotas.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(QuotaContractError, "non-transferable"):
                load_quota_contract(path)


if __name__ == "__main__":
    unittest.main()
