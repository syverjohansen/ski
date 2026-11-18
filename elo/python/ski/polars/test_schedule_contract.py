import copy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from schedule_contract import (
    RACES_COLUMNS,
    WEEKENDS_COLUMNS,
    ScheduleContractError,
    assess_transition,
    build_shadow,
    championship_category,
    derive_weekends,
    read_csv,
    validate_races,
    validate_relationship,
    validate_season_structure,
    write_csv_atomic,
)

HERE = Path(__file__).resolve().parent
ACCEPTED_RACES = HERE / "excel365/races.csv"
ACCEPTED_WEEKENDS = HERE / "excel365/weekends.csv"


def accepted_races():
    return read_csv(ACCEPTED_RACES, RACES_COLUMNS)


def with_new_race_id(url, offset=100_000):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["raceid"] = [str(int(query["raceid"][0]) + offset)]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def next_season(rows):
    candidate = copy.deepcopy(rows)
    for row in candidate:
        race_date = datetime.strptime(row["Date"], "%m/%d/%Y")
        row["Date"] = race_date.replace(year=race_date.year + 1).strftime("%m/%d/%Y")
        row["Startlist"] = with_new_race_id(row["Startlist"])
        row["Championship"] = "0"
        if row["Period"] == "4":
            row["Period"] = "3"

    target_season = max(
        datetime.strptime(row["Date"], "%m/%d/%Y").year for row in candidate
    )
    if championship_category(target_season):
        # Reuse a coherent group in this synthetic fixture so the structural
        # championship gate can be exercised when the next season has one.
        championship_city = candidate[-1]["City"]
        for row in candidate:
            if row["City"] == championship_city:
                row["Championship"] = "1"
                row["Period"] = "4"
    return candidate


class CrossCountryScheduleContractTests(unittest.TestCase):
    def test_current_accepted_schedule_and_compatibility_view_pass(self):
        races = accepted_races()
        weekends = read_csv(ACCEPTED_WEEKENDS, WEEKENDS_COLUMNS)

        self.assertEqual(validate_races(races)["rows"], len(races))
        structure = validate_season_structure(races)
        self.assertEqual(
            structure["championship_category"],
            championship_category(structure["season"]),
        )
        self.assertGreater(structure["sex_counts"]["L"], 0)
        self.assertGreater(structure["sex_counts"]["M"], 0)
        self.assertEqual(derive_weekends(races), weekends)
        self.assertEqual(validate_relationship(races, weekends)["status"], "passed")

    def test_shadow_reproduces_accepted_files_byte_for_byte(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = build_shadow(
                ACCEPTED_RACES,
                ACCEPTED_RACES,
                Path(temporary),
                accepted_weekends=ACCEPTED_WEEKENDS,
            )

            candidate = Path(temporary) / "candidate"
            self.assertEqual((candidate / "races.csv").read_bytes(), ACCEPTED_RACES.read_bytes())
            self.assertEqual(
                (candidate / "weekends.csv").read_bytes(),
                ACCEPTED_WEEKENDS.read_bytes(),
            )
            self.assertEqual(report["promotion"], {"status": "eligible", "performed": False})
            self.assertEqual(report["season_transition"]["kind"], "same_season")

    def test_tour_de_ski_stages_each_keep_their_own_prediction_date(self):
        races = accepted_races()
        weekends = derive_weekends(races)
        stage_rows = [row for row in weekends if row["Period"] == "2"]

        self.assertGreater(len(stage_rows), 0)
        self.assertTrue(all(row["Date"] == row["Race_Date"] for row in stage_rows))

    def test_relay_and_team_sprint_windows_are_not_merged_with_individuals(self):
        races = accepted_races()
        weekends = derive_weekends(races)
        by_city = {}
        for row in weekends:
            by_city.setdefault(row["City"], []).append(row)

        mixed_city = next(
            rows
            for rows in by_city.values()
            if {row["Distance"] for row in rows} & {"Rel", "Ts"}
            and any(row["Distance"] not in {"Rel", "Ts"} for row in rows)
        )
        starts_by_type = {}
        for row in mixed_city:
            event_type = (
                "relay"
                if row["Distance"] == "Rel"
                else "team_sprint"
                if row["Distance"] == "Ts"
                else "individual"
            )
            starts_by_type.setdefault(event_type, set()).add(row["Date"])

        self.assertIn("individual", starts_by_type)
        self.assertTrue({"relay", "team_sprint"} & starts_by_type.keys())
        self.assertGreater(len(set().union(*starts_by_type.values())), 1)

    def test_schema_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "races.csv"
            path.write_text("Date,Sex\n11/01/2025,M\n", encoding="utf-8")
            with self.assertRaisesRegex(ScheduleContractError, "unexpected columns"):
                read_csv(path, RACES_COLUMNS)

    def test_duplicate_race_identity_is_rejected(self):
        rows = accepted_races()
        rows.append(copy.deepcopy(rows[0]))
        with self.assertRaisesRegex(ScheduleContractError, "duplicate race identity"):
            validate_races(rows)

    def test_invalid_cross_country_startlist_url_is_rejected(self):
        rows = accepted_races()
        rows[0]["Startlist"] = rows[0]["Startlist"].replace("sectorcode=CC", "sectorcode=AL")
        with self.assertRaisesRegex(ScheduleContractError, "invalid FIS Startlist URL"):
            validate_races(rows)

    def test_same_season_large_shrink_is_blocked(self):
        with tempfile.TemporaryDirectory() as temporary:
            candidate = Path(temporary) / "candidate.csv"
            rows = accepted_races()
            reduced = rows[:4] + [row for row in rows if row["Championship"] == "1"] + rows[-4:]
            write_csv_atomic(
                candidate,
                RACES_COLUMNS,
                reduced,
                lineterminator="\r\n",
            )
            report = build_shadow(
                candidate,
                ACCEPTED_RACES,
                Path(temporary) / "out",
                accepted_weekends=ACCEPTED_WEEKENDS,
            )

            self.assertEqual(report["promotion"]["status"], "blocked")
            self.assertFalse(report["comparison"]["within_removal_limit"])

    def test_complete_next_season_requires_approval(self):
        accepted = accepted_races()
        candidate = next_season(accepted)
        transition = assess_transition(
            candidate,
            accepted,
            validate_season_structure(candidate),
            validate_season_structure(accepted),
        )

        self.assertEqual(transition["kind"], "next_season")
        self.assertTrue(transition["valid"])
        self.assertTrue(transition["approval_required"])

    def test_incomplete_next_season_is_blocked(self):
        accepted = accepted_races()
        full_candidate = next_season(accepted)
        candidate = full_candidate[:15] + full_candidate[-25:]
        transition = assess_transition(
            candidate,
            accepted,
            validate_season_structure(candidate),
            validate_season_structure(accepted),
        )

        self.assertFalse(transition["valid"])
        self.assertFalse(transition["checks"]["row_count_sufficient"])

    def test_inconsistent_final_climb_is_rejected(self):
        rows = accepted_races()
        row = next(row for row in rows if row["Final_Climb"] == "1")
        row["MS"] = "0"
        with self.assertRaisesRegex(ScheduleContractError, "Final Climb flags"):
            validate_races(rows)


if __name__ == "__main__":
    unittest.main()
