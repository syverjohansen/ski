from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

from nordic_combined_results_identity import IdentityError, map_identities


HISTORY_FIELDS = ["Date", "Sex", "Skier", "Nation", "ID"]
RESULT_FIELDS = [
    "date", "city", "country", "sex", "season", "race", "event_kind",
    "distance", "race_type", "championship", "place", "team_name",
    "team_nation", "member_order", "fis_competitor_id", "fis_code",
    "name", "nation",
]
ALIAS_FIELDS = [
    "fis_competitor_id", "fis_code", "fis_name", "canonical_sex",
    "canonical_id", "canonical_name",
]


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def result(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "date": "2026-03-07", "city": "Lahti", "country": "Finland",
        "sex": "Mixed", "season": "2026", "race": "1",
        "event_kind": "team_sprint", "distance": "2x7.5",
        "race_type": "Team Sprint", "championship": "0", "place": "1",
        "team_name": "NORWAY I", "team_nation": "Norway",
        "member_order": "1", "fis_competitor_id": "fis-1",
        "fis_code": "100001", "name": "HAGEN Ida Marie", "nation": "NOR",
    }
    row.update(overrides)
    return row


def identity_inputs(tmp_path: Path, *, aliases: list[dict[str, object]] | None = None) -> dict[str, Path]:
    accepted = tmp_path / "accepted"
    men = write_csv(accepted / "men.csv", HISTORY_FIELDS, [
        {"Date": "2026-02-01", "Sex": "M", "Skier": "Jørgen Graabak", "Nation": "Norway", "ID": "10"},
    ])
    ladies = write_csv(accepted / "ladies.csv", HISTORY_FIELDS, [
        {"Date": "2026-02-01", "Sex": "L", "Skier": "Ida Marie Hagen", "Nation": "Norway", "ID": "20"},
    ])
    alias_file = write_csv(accepted / "aliases.csv", ALIAS_FIELDS, aliases or [])
    results = write_csv(accepted / "results.csv", RESULT_FIELDS, [
        result(),
        result(member_order="2", fis_competitor_id="fis-2", fis_code="100002", name="GRAABAK Joergen"),
    ])
    return {"results": results, "men_history": men, "ladies_history": ladies, "aliases_path": alias_file}


def test_mixed_team_sprint_maps_sex_and_reviewed_alias_without_mutating_sources(tmp_path: Path) -> None:
    inputs = identity_inputs(tmp_path, aliases=[{
        "fis_competitor_id": "fis-2", "fis_code": "100002",
        "fis_name": "GRAABAK Joergen", "canonical_sex": "M",
        "canonical_id": "10", "canonical_name": "Jørgen Graabak",
    }])
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs.values()}
    output = tmp_path / "candidate" / "identity"
    report = map_identities(**inputs, output_dir=output)
    assert report["replay_eligible"] is True
    assert report["match_methods"] == {"reviewed_alias": 1, "unique_normalized_name": 1}
    with (output / "mapped_results.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [(row["canonical_athlete_id"], row["canonical_sex"]) for row in rows] == [("20", "L"), ("10", "M")]
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs.values()} == before


def test_reviewed_alias_rejects_changed_fis_identity(tmp_path: Path) -> None:
    inputs = identity_inputs(tmp_path, aliases=[{
        "fis_competitor_id": "fis-2", "fis_code": "wrong",
        "fis_name": "GRAABAK Joergen", "canonical_sex": "M",
        "canonical_id": "10", "canonical_name": "Jørgen Graabak",
    }])
    with pytest.raises(IdentityError, match="FIS identity changed"):
        map_identities(**inputs, output_dir=tmp_path / "candidate")


def test_team_sprint_requires_exactly_two_members(tmp_path: Path) -> None:
    inputs = identity_inputs(tmp_path)
    write_csv(inputs["results"], RESULT_FIELDS, [result()])
    with pytest.raises(IdentityError, match="has 1 members"):
        map_identities(**inputs, output_dir=tmp_path / "candidate")


def test_unresolved_identity_blocks_replay_but_preserves_evidence(tmp_path: Path) -> None:
    inputs = identity_inputs(tmp_path)
    report = map_identities(**inputs, output_dir=tmp_path / "candidate")
    assert report["status"] == "blocked"
    assert report["unresolved_fis_athletes"] == 1
    assert report["replay_eligible"] is False
    assert (tmp_path / "candidate/unresolved_identities.csv").is_file()
