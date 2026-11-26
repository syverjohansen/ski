import csv
import tempfile
import unittest
from pathlib import Path

from cross_country_fis_prediction_identity import (
    FisPredictionIdentityError,
    classify_fis_prediction_identities,
)
from cross_country_results_elo_replay import PREVIOUS_COLUMNS, STREAMS


def write_csv(path: Path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


HISTORY_FIELDS = [
    "Date",
    "Event",
    "Season",
    "Race",
    "ID",
    "Skier",
    "Nation",
    *STREAMS,
    *PREVIOUS_COLUMNS.values(),
]
RESULT_FIELDS = [
    "source_categories",
    "date",
    "sex",
    "distance",
    "technique",
    "mass_start",
    "fis_race_id",
    "fis_competitor_id",
    "fis_code",
    "name",
    "nation",
    "place",
    "source_url",
    "source_sha256",
]
ALIAS_FIELDS = [
    "sex",
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "canonical_id",
    "canonical_name",
]


def history_row(**overrides):
    row = {
        "Date": "2026-03-01",
        "Event": "World Cup",
        "Season": 2026,
        "Race": 1,
        "ID": "10",
        "Skier": "Anchor One",
        "Nation": "Norway",
    }
    for stream in STREAMS:
        row[stream] = 1600
        row[PREVIOUS_COLUMNS[stream]] = 1600
    row.update(overrides)
    return row


def result_row(**overrides):
    row = {
        "source_categories": "SCAN",
        "date": "2026-11-20",
        "sex": "M",
        "distance": "10",
        "technique": "C",
        "mass_start": "0",
        "fis_race_id": "500",
        "fis_competitor_id": "100",
        "fis_code": "3400001",
        "name": "Anchor One",
        "nation": "NOR",
        "place": "58",
        "source_url": "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=500",
        "source_sha256": "a" * 64,
    }
    row.update(overrides)
    return row


def prepare_sources(root: Path):
    sources = root / "sources"
    results = sources / "results.csv"
    men = sources / "men.csv"
    ladies = sources / "ladies.csv"
    aliases = sources / "aliases.csv"
    history = [
        history_row(),
        history_row(
            Date="2026-05-01",
            Event="Offseason",
            Race=0,
            Elo=1555,
            Pelo=1600,
        ),
        history_row(
            ID="20",
            Skier="Anchor Two",
            Elo=1400,
            Pelo=1400,
            **{stream: 1400 for stream in STREAMS if stream != "Elo"},
            **{
                previous: 1400
                for stream, previous in PREVIOUS_COLUMNS.items()
                if stream != "Elo"
            },
        ),
        history_row(
            Date="2026-12-01",
            Season=2027,
            ID="30",
            Skier="Future Debut",
            Elo=1450,
            Pelo=1400,
            **{stream: 1450 for stream in STREAMS if stream != "Elo"},
            **{
                previous: 1400
                for stream, previous in PREVIOUS_COLUMNS.items()
                if stream != "Elo"
            },
        ),
    ]
    rows = [
        result_row(),
        result_row(
            fis_competitor_id="300",
            fis_code="3400003",
            name="Future Debut",
            place="59",
        ),
        result_row(
            fis_competitor_id="400",
            fis_code="3400004",
            name="Unknown Athlete",
            place="60",
        ),
        result_row(
            fis_competitor_id="200",
            fis_code="3400002",
            name="ANCHOR TWO FIS",
            place="61",
        ),
        result_row(
            date="2026-12-20",
            fis_race_id="501",
            fis_competitor_id="300",
            fis_code="3400003",
            name="Future Debut",
            place="1",
            source_url="https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=501",
            source_sha256="b" * 64,
        ),
        result_row(
            date="2026-12-20",
            fis_race_id="501",
            fis_competitor_id="400",
            fis_code="3400004",
            name="Unknown Athlete",
            place="2",
            source_url="https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=501",
            source_sha256="b" * 64,
        ),
    ]
    write_csv(results, RESULT_FIELDS, rows)
    write_csv(men, HISTORY_FIELDS, history)
    write_csv(ladies, HISTORY_FIELDS, [])
    write_csv(
        aliases,
        ALIAS_FIELDS,
        [
            {
                "sex": "M",
                "fis_competitor_id": "200",
                "fis_code": "3400002",
                "fis_name": "ANCHOR TWO FIS",
                "canonical_id": "20",
                "canonical_name": "Anchor Two",
            }
        ],
    )
    return results, men, ladies, aliases


def read_csv(path: Path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class CrossCountryFisPredictionIdentityTests(unittest.TestCase):
    def test_classifies_verification_strictly_as_of_each_race(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = prepare_sources(root)
            report = classify_fis_prediction_identities(
                results=results,
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                output_dir=root / "output",
            )

            participants = read_csv(root / "output" / "fis-prediction-participants.csv")
            future_rows = [
                row for row in participants if row["fis_competitor_id"] == "300"
            ]
            self.assertEqual(
                [row["a_tier_verified"] for row in future_rows], ["false", "true"]
            )
            self.assertEqual(
                [row["prediction_athlete_id"] for row in future_rows],
                ["fis:300", "canonical:30"],
            )
            self.assertEqual(report["verified_anchor_appearances"], 3)
            self.assertEqual(report["never_a_tier_appearances"], 3)
            self.assertEqual(report["unknown_vs_unknown_comparisons"], 0)
            self.assertFalse(report["accepted_histories_modified"])

    def test_unknowns_in_same_anchor_interval_have_identical_comparisons(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = prepare_sources(root)
            classify_fis_prediction_identities(
                results=sources[0],
                men_chrono=sources[1],
                ladies_chrono=sources[2],
                aliases_path=sources[3],
                output_dir=root / "output",
            )
            comparisons = read_csv(root / "output" / "fis-anchor-comparisons.csv")
            first_race = [row for row in comparisons if row["fis_race_id"] == "500"]
            by_target = {}
            for row in first_race:
                by_target.setdefault(row["target_fis_competitor_id"], []).append(
                    (
                        row["anchor_canonical_athlete_id"],
                        row["anchor_place"],
                        row["score"],
                        row["anchor_Elo"],
                    )
                )
            self.assertEqual(by_target["300"], by_target["400"])
            self.assertEqual(
                by_target["300"],
                [("10", "58", "0.0", "1555.0"), ("20", "61", "1.0", "1385.0")],
            )
            self.assertEqual(len(comparisons), 5)

    def test_name_match_must_be_globally_unique_before_becoming_anchor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = prepare_sources(root)
            history = read_csv(men)
            history.append(
                history_row(
                    Date="2027-01-01",
                    Season=2027,
                    ID="99",
                    Skier="Anchor One",
                )
            )
            write_csv(men, HISTORY_FIELDS, history)
            classify_fis_prediction_identities(
                results=results,
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                output_dir=root / "output",
            )
            participants = read_csv(root / "output" / "fis-prediction-participants.csv")
            source_identity = next(
                row for row in participants if row["fis_competitor_id"] == "100"
            )
            self.assertEqual(source_identity["a_tier_verified"], "false")
            self.assertEqual(source_identity["prediction_athlete_id"], "fis:100")

    def test_alias_anchor_is_fixed_and_not_a_comparison_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = prepare_sources(root)
            classify_fis_prediction_identities(
                results=sources[0],
                men_chrono=sources[1],
                ladies_chrono=sources[2],
                aliases_path=sources[3],
                output_dir=root / "output",
            )
            participants = read_csv(root / "output" / "fis-prediction-participants.csv")
            alias = next(
                row for row in participants if row["fis_competitor_id"] == "200"
            )
            self.assertEqual(alias["verification_method"], "reviewed_alias")
            self.assertEqual(alias["canonical_athlete_id"], "20")
            comparisons = read_csv(root / "output" / "fis-anchor-comparisons.csv")
            self.assertNotIn(
                "200", {row["target_fis_competitor_id"] for row in comparisons}
            )

    def test_reviewed_alias_bridges_name_change_but_not_fis_code_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = prepare_sources(root)
            rows = read_csv(results)
            for row in rows:
                if row["fis_competitor_id"] == "200":
                    row["name"] = "ANCHOR TWO CURRENT"
                    row["fis_competitor_id"] = "999"
            write_csv(results, RESULT_FIELDS, rows)
            classify_fis_prediction_identities(
                results=results,
                men_chrono=men,
                ladies_chrono=ladies,
                aliases_path=aliases,
                output_dir=root / "renamed-output",
            )

            for row in rows:
                if row["fis_competitor_id"] == "999":
                    row["fis_code"] = "9999999"
                    row["name"] = "Anchor Two"
            write_csv(results, RESULT_FIELDS, rows)
            with self.assertRaisesRegex(
                FisPredictionIdentityError, "FIS code changed"
            ):
                classify_fis_prediction_identities(
                    results=results,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    aliases_path=aliases,
                    output_dir=root / "changed-code-output",
                )

    def test_sources_are_immutable_and_outputs_are_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = prepare_sources(root)
            before = {path: path.read_bytes() for path in sources}
            for output in (root / "first", root / "second"):
                classify_fis_prediction_identities(
                    results=sources[0],
                    men_chrono=sources[1],
                    ladies_chrono=sources[2],
                    aliases_path=sources[3],
                    output_dir=output,
                )
            self.assertEqual(before, {path: path.read_bytes() for path in sources})
            for filename in (
                "fis-prediction-participants.csv",
                "fis-anchor-comparisons.csv",
                "report.json",
            ):
                self.assertEqual(
                    (root / "first" / filename).read_bytes(),
                    (root / "second" / filename).read_bytes(),
                )

    def test_rejects_duplicate_event_athlete_and_nonempty_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, men, ladies, aliases = prepare_sources(root)
            rows = read_csv(results)
            rows.append(rows[0])
            write_csv(results, RESULT_FIELDS, rows)
            with self.assertRaisesRegex(
                FisPredictionIdentityError, "duplicate athlete"
            ):
                classify_fis_prediction_identities(
                    results=results,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    aliases_path=aliases,
                    output_dir=root / "duplicate",
                )
            occupied = root / "occupied"
            occupied.mkdir()
            (occupied / "old").write_text("old", encoding="utf-8")
            with self.assertRaisesRegex(FisPredictionIdentityError, "must be empty"):
                classify_fis_prediction_identities(
                    results=results,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    aliases_path=aliases,
                    output_dir=occupied,
                )


if __name__ == "__main__":
    unittest.main()
