from __future__ import annotations

import csv
import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from nordic_combined_results_identity import ALIAS_FIELDS, load_aliases, load_athletes
from nordic_combined_startlist_acquire import STARTLIST_FIELDS
from nordic_combined_startlist_normalize import (
    FILENAMES,
    StartlistNormalizationError,
    normalize_startlists,
)


HISTORY_FIELDS = [
    "Elo", "Individual_Elo", "IndividualCompact_Elo", "Sprint_Elo",
    "MassStart_Elo", "Date", "City", "Country", "Sex", "Distance",
    "RaceType", "Place", "Skier", "Nation", "ID", "Season", "Race",
]
LOGICAL_DATE = date(2026, 2, 10)
HERE = Path(__file__).resolve().parent


def write_csv(path: Path, fields: list[str] | tuple[str, ...], rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def history(athlete_id: int, name: str, sex: str, elo: int, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Elo": elo, "Individual_Elo": elo + 1, "IndividualCompact_Elo": elo + 2,
        "Sprint_Elo": elo + 3, "MassStart_Elo": elo + 4,
        "Date": "2026-02-09", "City": "Seefeld", "Country": "Austria",
        "Sex": sex, "Distance": "10", "RaceType": "Individual", "Place": "1",
        "Skier": name, "Nation": "Norway" if sex == "M" else "Germany",
        "ID": athlete_id, "Season": "2026", "Race": "1",
    }
    row.update(overrides)
    return row


def entrant(
    athlete_id: int, sex: str, kind: str, race: int, *, team: str = "", order: int | str = ""
) -> dict[str, object]:
    nation = "NOR" if sex in {"M", "Mixed"} else "GER"
    return {
        "logical_date": LOGICAL_DATE.isoformat(), "sex": sex, "event_kind": kind,
        "race": race, "city": "Predazzo", "country": "Italy", "distance": "10",
        "race_type": "Team Sprint" if kind == "team_sprint" else "Team" if kind == "team" else "Individual",
        "source_order": 1 if team else athlete_id, "team_name": team,
        "team_nation": nation if team else "", "member_order": order,
        "fis_competitor_id": str(1000 + athlete_id), "fis_code": str(7000 + athlete_id),
        "name": f"ATHLETE {athlete_id}", "birth_year": "2000", "nation": nation,
        "source_url": "https://www.fis-ski.com/DB/general/results.html?sectorcode=NK&raceid=1",
        "source_sha256": "a" * 64,
    }


def prepared(tmp_path: Path, rows: list[dict[str, object]], *, omit_history_id: int | None = None):
    accepted = tmp_path / "accepted"
    men_rows = [history(i, f"ATHLETE {i}", "M", 1300 + i) for i in range(1, 9) if i != omit_history_id]
    ladies_rows = [history(i, f"ATHLETE {i}", "L", 1400 + i) for i in range(9, 17) if i != omit_history_id]
    men = write_csv(accepted / "men.csv", HISTORY_FIELDS, men_rows)
    ladies = write_csv(accepted / "ladies.csv", HISTORY_FIELDS, ladies_rows)
    aliases = write_csv(accepted / "aliases.csv", list(ALIAS_FIELDS), [])
    acquired = write_csv(tmp_path / "evidence/startlists.csv", STARTLIST_FIELDS, rows)
    report = {
        "schema_version": "nordicnumbers.nordic-combined-startlist-acquisition/v1",
        "logical_date": LOGICAL_DATE.isoformat(), "status": "ready",
        "prediction_eligible": True,
        "startlists_sha256": hashlib.sha256(acquired.read_bytes()).hexdigest(),
    }
    report_path = tmp_path / "evidence/report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    return {
        "acquired_startlists": acquired, "acquisition_report": report_path,
        "men_history": men, "ladies_history": ladies, "aliases_path": aliases,
    }


def all_category_rows() -> list[dict[str, object]]:
    rows = [entrant(1, "M", "individual", 1), entrant(2, "M", "individual", 1)]
    rows += [entrant(9, "L", "individual", 2), entrant(10, "L", "individual", 2)]
    rows += [entrant(i, "M", "team", 3, team="NOR A", order=j) for j, i in enumerate(range(1, 5), 1)]
    rows += [entrant(i, "L", "team", 4, team="GER A", order=j) for j, i in enumerate(range(9, 13), 1)]
    rows += [entrant(i, "M", "team_sprint", 5, team="NOR TS", order=j) for j, i in enumerate((5, 6), 1)]
    rows += [entrant(i, "L", "team_sprint", 6, team="GER TS", order=j) for j, i in enumerate((13, 14), 1)]
    mixed = [(1, "M"), (9, "L"), (2, "M"), (10, "L")]
    rows += [entrant(i, "Mixed", "team", 7, team="MIX A", order=j) for j, (i, _) in enumerate(mixed, 1)]
    mixed_sprint = [(3, "M"), (11, "L")]
    rows += [entrant(i, "Mixed", "team_sprint", 8, team="MIX TS", order=j) for j, (i, _) in enumerate(mixed_sprint, 1)]
    return rows


def test_builds_all_eight_model_candidates_with_as_of_ratings(tmp_path: Path) -> None:
    inputs = prepared(tmp_path, all_category_rows())
    output = tmp_path / "normalized"
    report = normalize_startlists(**inputs, logical_date=LOGICAL_DATE, output_dir=output)
    assert report["status"] == "ready"
    assert report["prediction_eligible"] is True
    assert report["accepted_startlists_modified"] is False
    assert set(report["outputs"]) == set(FILENAMES)
    for category, filename in FILENAMES.items():
        assert (output / "candidates" / filename).is_file(), category
    with (output / "candidates/startlist_races_men.csv").open(newline="") as handle:
        men = list(csv.DictReader(handle))
    assert {row["ID"] for row in men} == {"1", "2"}
    assert {row["Race1_Prob"] for row in men} == {"1.0"}
    assert {row["Race_Date"] for row in men} == {"02/10/2026"}
    with (output / "candidates/startlist_mixed_team_races_teams.csv").open(newline="") as handle:
        mixed = list(csv.DictReader(handle))
    assert mixed[0]["Member_1_Sex"] == "M"
    assert mixed[0]["Member_2_Sex"] == "L"


def test_unknown_identity_blocks_without_writing_candidates(tmp_path: Path) -> None:
    inputs = prepared(tmp_path, [entrant(1, "M", "individual", 1)], omit_history_id=1)
    output = tmp_path / "normalized"
    report = normalize_startlists(**inputs, logical_date=LOGICAL_DATE, output_dir=output)
    assert report["status"] == "blocked"
    assert report["prediction_eligible"] is False
    assert report["unresolved_identities"] == 1
    assert not (output / "candidates").exists()


def test_future_ratings_are_not_used(tmp_path: Path) -> None:
    rows = [entrant(1, "M", "individual", 1), entrant(2, "M", "individual", 1)]
    inputs = prepared(tmp_path, rows)
    with inputs["men_history"].open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDS, lineterminator="\n")
        writer.writerow(history(1, "ATHLETE 1", "M", 9999, Date="2026-02-10", Race="2"))
    output = tmp_path / "normalized"
    normalize_startlists(**inputs, logical_date=LOGICAL_DATE, output_dir=output)
    with (output / "candidates/startlist_races_men.csv").open(newline="") as handle:
        candidates = {row["ID"]: row for row in csv.DictReader(handle)}
    assert candidates["1"]["Elo"] == "1301.0"


def test_checksum_drift_and_unsafe_output_fail_closed(tmp_path: Path) -> None:
    inputs = prepared(tmp_path, [entrant(1, "M", "individual", 1)])
    inputs["acquired_startlists"].write_text("tampered", encoding="utf-8")
    with pytest.raises(StartlistNormalizationError, match="checksum"):
        normalize_startlists(**inputs, logical_date=LOGICAL_DATE, output_dir=tmp_path / "normalized")
    clean = prepared(tmp_path / "second", [entrant(1, "M", "individual", 1)])
    with pytest.raises(StartlistNormalizationError, match="cannot be inside"):
        normalize_startlists(
            **clean, logical_date=LOGICAL_DATE,
            output_dir=clean["men_history"].parent / "candidate",
        )


def test_reviewed_aliases_cover_live_lahti_identity_drift() -> None:
    accepted = HERE / "excel365"
    athletes, _, names = load_athletes(
        accepted / "men_chrono_elevation.csv",
        accepted / "ladies_chrono_elevation.csv",
    )
    aliases = load_aliases(
        HERE / "nordic_combined_results_identity_aliases.csv", athletes, names
    )
    assert {
        fis_id: (row["canonical_sex"], row["canonical_id"], row["canonical_name"])
        for fis_id, row in aliases.items()
    } == {
        "153563": ("M", "345", "Espen Andersen"),
        "182614": ("M", "1329", "Jens Lurås Oftebro"),
        "200998": ("M", "1217", "Einar Lurås Oftebro"),
        "255089": ("L", "176", "Lily Grossmann"),
    }
