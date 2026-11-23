import csv
import json
import tempfile
import unittest
from pathlib import Path

from cross_country_flgr_identity import (
    CROSSWALK_FIELDS,
    FlgrIdentityError,
    load_canonical_history,
    load_reviewed_crosswalk,
    normalized_name,
    resolve_flgr_identities,
)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_sources(root: Path) -> tuple[Path, Path, Path, Path]:
    sources = root / "sources"
    sources.mkdir()
    men = sources / "men.csv"
    ladies = sources / "ladies.csv"
    crosswalk = sources / "crosswalk.csv"
    candidate = sources / "candidate.json"
    history_fields = ["Date", "ID", "Skier", "Nation", "Birthday"]
    write_csv(
        men,
        history_fields,
        [
            {
                "Date": "2022-02-20",
                "ID": "10",
                "Skier": "Ivan Ivanov",
                "Nation": "Russia",
                "Birthday": "1999-04-10",
            },
            {
                "Date": "2022-02-20",
                "ID": "20",
                "Skier": "Alexey Novikov",
                "Nation": "RUS",
                "Birthday": "2003-07-12",
            },
            {
                "Date": "2022-02-20",
                "ID": "30",
                "Skier": "Unrelated Norwegian",
                "Nation": "Norway",
                "Birthday": "2000-01-01",
            },
        ],
    )
    write_csv(ladies, history_fields, [])
    candidate.write_text(
        json.dumps(
            {
                "schema_version": "nordicnumbers.flgr-results-candidate/v1",
                "event_id": "flgr:77:88",
                "date": "2026-11-23",
                "season": 2027,
                "sex": "M",
                "distance": "10",
                "technique": "C",
                "mass_start": True,
                "source_sha256": "a" * 64,
                "results": [
                    {
                        "athlete_id": "flgr:101",
                        "source_competitor_id": "101",
                        "source_name": "ИВАНОВ Иван",
                        "birth_year": 1999,
                        "place": 1,
                    },
                    {
                        "athlete_id": "flgr:202",
                        "source_competitor_id": "202",
                        "source_name": "НОВИКОВ Алексей",
                        "birth_year": 2003,
                        "place": 2,
                    },
                ],
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return men, ladies, crosswalk, candidate


def reviewed_row(**overrides) -> dict[str, object]:
    row = {
        "sex": "M",
        "flgr_competitor_id": "101",
        "flgr_name": "ИВАНОВ Иван",
        "birth_year": "1999",
        "canonical_athlete_id": "10",
        "canonical_name": "Ivan Ivanov",
        "status": "reviewed",
        "evidence": "FLGR profile and accepted FIS history reviewed 2026-11-23",
    }
    row.update(overrides)
    return row


class FlgrIdentityContractTests(unittest.TestCase):
    def test_normalization_transliterates_cyrillic_and_ignores_name_order(self):
        self.assertEqual(normalized_name("ИВАНОВ Иван"), normalized_name("Ivan Ivanov"))

    def test_reviewed_mapping_is_applied_and_exact_suggestion_is_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            men, ladies, crosswalk, candidate = write_sources(root)
            write_csv(crosswalk, list(CROSSWALK_FIELDS), [reviewed_row()])

            report = resolve_flgr_identities(
                candidate_path=candidate,
                men_chrono=men,
                ladies_chrono=ladies,
                crosswalk_path=crosswalk,
                output_dir=root / "output",
            )

            self.assertEqual(report["reviewed_crosswalk_rows"], 1)
            self.assertEqual(report["flgr_only_rows"], 1)
            self.assertTrue(report["suggestions_are_non_publishing"])
            suggestion = report["suggestions"][0]["suggestions"][0]
            self.assertEqual(suggestion["canonical_athlete_id"], "20")
            self.assertGreater(suggestion["score"], 0.95)
            self.assertFalse(suggestion["applied"])

            with (root / "output" / "flgr-identity-results.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["athlete_id"], "flgr:101")
            self.assertEqual(rows[0]["canonical_athlete_id"], "10")
            self.assertEqual(rows[0]["identity_status"], "reviewed_crosswalk")
            self.assertEqual(rows[1]["athlete_id"], "flgr:202")
            self.assertEqual(rows[1]["canonical_athlete_id"], "")
            self.assertEqual(rows[1]["identity_status"], "flgr_only")

    def test_crosswalk_rejects_unknown_or_reused_canonical_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            men, ladies, crosswalk, _ = write_sources(root)
            canonical = load_canonical_history(men, ladies)
            for rows, expected in (
                ([reviewed_row(canonical_athlete_id="999")], "unknown canonical"),
                (
                    [
                        reviewed_row(),
                        reviewed_row(
                            flgr_competitor_id="202",
                            flgr_name="НОВИКОВ Алексей",
                        ),
                    ],
                    "mapped more than once",
                ),
            ):
                with self.subTest(expected=expected):
                    write_csv(crosswalk, list(CROSSWALK_FIELDS), rows)
                    with self.assertRaisesRegex(FlgrIdentityError, expected):
                        load_reviewed_crosswalk(crosswalk, canonical)

    def test_crosswalk_rejects_name_birth_year_or_status_conflicts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            men, ladies, crosswalk, _ = write_sources(root)
            canonical = load_canonical_history(men, ladies)
            for row, expected in (
                (reviewed_row(canonical_name="Wrong Person"), "canonical name"),
                (reviewed_row(birth_year="2000"), "birth year"),
                (reviewed_row(status="suggested"), "incomplete reviewed mapping"),
            ):
                with self.subTest(expected=expected):
                    write_csv(crosswalk, list(CROSSWALK_FIELDS), [row])
                    with self.assertRaisesRegex(FlgrIdentityError, expected):
                        load_reviewed_crosswalk(crosswalk, canonical)

    def test_reviewed_source_identity_change_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            men, ladies, crosswalk, candidate = write_sources(root)
            write_csv(
                crosswalk,
                list(CROSSWALK_FIELDS),
                [reviewed_row(flgr_name="Different Athlete")],
            )
            with self.assertRaisesRegex(FlgrIdentityError, "FLGR name changed"):
                resolve_flgr_identities(
                    candidate_path=candidate,
                    men_chrono=men,
                    ladies_chrono=ladies,
                    crosswalk_path=crosswalk,
                    output_dir=root / "output",
                )

    def test_output_cannot_mix_with_source_or_existing_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            men, ladies, crosswalk, candidate = write_sources(root)
            write_csv(crosswalk, list(CROSSWALK_FIELDS), [])
            for output in (root / "sources" / "output", root / "existing"):
                if output.name == "existing":
                    output.mkdir()
                    (output / "old.txt").write_text("old", encoding="utf-8")
                with (
                    self.subTest(output=output),
                    self.assertRaises(FlgrIdentityError),
                ):
                    resolve_flgr_identities(
                        candidate_path=candidate,
                        men_chrono=men,
                        ladies_chrono=ladies,
                        crosswalk_path=crosswalk,
                        output_dir=output,
                    )


if __name__ == "__main__":
    unittest.main()
