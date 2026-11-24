from __future__ import annotations

import csv
from pathlib import Path

import pytest

from skijump_results_identity import IdentityError, map_identities


RESULT_FIELDS = [
    "date", "sex", "season", "race", "event_kind", "city", "country",
    "hill_size", "race_type", "championship", "place", "team_name",
    "team_nation", "member_order", "fis_competitor_id", "fis_code", "name",
    "birth_year", "nation", "source_url", "source_sha256",
]
HISTORY_FIELDS = ["Date", "Skier", "Nation", "ID"]
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


def result_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "date": "2025-11-22",
        "sex": "M",
        "season": "2026",
        "race": "1",
        "event_kind": "individual",
        "city": "Lillehammer",
        "country": "Norway",
        "hill_size": "140",
        "race_type": "Large",
        "championship": "0",
        "place": "1",
        "team_name": "",
        "team_nation": "",
        "member_order": "",
        "fis_competitor_id": "101",
        "fis_code": "7001",
        "name": "JUMPER One",
        "birth_year": "1998",
        "nation": "AUT",
        "source_url": "https://example.invalid/results",
        "source_sha256": "a" * 64,
    }
    row.update(overrides)
    return row


def source_bundle(
    tmp_path: Path,
    *,
    results: list[dict[str, object]],
    men: list[dict[str, object]],
    ladies: list[dict[str, object]],
    aliases: list[dict[str, object]] | None = None,
) -> dict[str, Path]:
    source = tmp_path / "source"
    return {
        "results": write_csv(source / "results.csv", RESULT_FIELDS, results),
        "men_history": write_csv(source / "men.csv", HISTORY_FIELDS, men),
        "ladies_history": write_csv(source / "ladies.csv", HISTORY_FIELDS, ladies),
        "aliases_path": write_csv(
            source / "aliases.csv", ALIAS_FIELDS, aliases or []
        ),
    }


def history(name: str, athlete_id: str, nation: str = "Austria") -> dict[str, str]:
    return {"Date": "2025-01-01", "Skier": name, "Nation": nation, "ID": athlete_id}


def team_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    members = [
        ("TEAM ONE", "AUT", "1", "201", "8001", "MEMBER One"),
        ("TEAM ONE", "AUT", "2", "202", "8002", "MEMBER Two"),
        ("TEAM TWO", "GER", "1", "203", "8003", "MEMBER Three"),
        ("TEAM TWO", "GER", "2", "204", "8004", "MEMBER Four"),
    ]
    for index, (team, nation, order, fis_id, fis_code, name) in enumerate(members):
        rows.append(
            result_row(
                sex="Mixed",
                race="2",
                event_kind="team",
                place="1" if index < 2 else "2",
                team_name=team,
                team_nation=nation,
                member_order=order,
                fis_competitor_id=fis_id,
                fis_code=fis_code,
                name=name,
                nation=nation,
            )
        )
    return rows


def test_maps_individual_and_infers_mixed_member_sexes(tmp_path: Path) -> None:
    sources = source_bundle(
        tmp_path,
        results=[result_row(), *team_rows()],
        men=[
            history("JUMPER One", "1"),
            history("MEMBER Two", "2"),
            history("MEMBER Four", "4", "Germany"),
        ],
        ladies=[
            history("MEMBER One", "11"),
            history("MEMBER Three", "13", "Germany"),
        ],
    )
    report = map_identities(
        **sources, output_dir=tmp_path / "identity"
    )
    assert report["status"] == "mapped"
    assert report["replay_eligible"] is True
    assert report["resolved_fis_athletes"] == 5
    with (tmp_path / "identity" / "mapped_results.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert [row["canonical_sex"] for row in rows[1:]] == ["L", "M", "L", "M"]


def test_reviewed_alias_requires_exact_fis_identity(tmp_path: Path) -> None:
    alias = {
        "fis_competitor_id": "101",
        "fis_code": "7001",
        "fis_name": "JUMPER One",
        "canonical_sex": "M",
        "canonical_id": "9",
        "canonical_name": "Canonical Jumper",
    }
    sources = source_bundle(
        tmp_path,
        results=[result_row()],
        men=[history("Canonical Jumper", "9")],
        ladies=[],
        aliases=[alias],
    )
    report = map_identities(**sources, output_dir=tmp_path / "identity")
    assert report["match_methods"] == {"reviewed_alias": 1}

    changed = source_bundle(
        tmp_path / "changed",
        results=[result_row(name="CHANGED NAME")],
        men=[history("Canonical Jumper", "9")],
        ladies=[],
        aliases=[alias],
    )
    with pytest.raises(IdentityError, match="FIS identity changed"):
        map_identities(**changed, output_dir=tmp_path / "changed-output")


def test_unresolved_identity_is_reported_and_blocks_replay(tmp_path: Path) -> None:
    sources = source_bundle(
        tmp_path,
        results=[result_row(name="UNKNOWN Athlete")],
        men=[history("Someone Else", "1")],
        ladies=[],
    )
    report = map_identities(**sources, output_dir=tmp_path / "identity")
    assert report["status"] == "blocked"
    assert report["unresolved_fis_athletes"] == 1
    assert report["replay_eligible"] is False
    with (tmp_path / "identity" / "unresolved_identities.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        unresolved = list(csv.DictReader(handle))
    assert unresolved[0]["reason"] == "missing"


def test_same_name_in_both_sexes_is_ambiguous_for_mixed_event(tmp_path: Path) -> None:
    rows = team_rows()
    rows[0]["name"] = "SAME Name"
    sources = source_bundle(
        tmp_path,
        results=rows,
        men=[
            history("SAME Name", "1"),
            history("MEMBER Two", "2"),
            history("MEMBER Four", "4"),
        ],
        ladies=[
            history("SAME Name", "11"),
            history("MEMBER Three", "13"),
        ],
    )
    report = map_identities(**sources, output_dir=tmp_path / "identity")
    assert report["unresolved_fis_athletes"] == 1
    with (tmp_path / "identity" / "unresolved_identities.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        unresolved = list(csv.DictReader(handle))
    assert unresolved[0]["reason"] == "ambiguous"
    assert "M:1:SAME Name" in unresolved[0]["candidate_identities"]
    assert "L:11:SAME Name" in unresolved[0]["candidate_identities"]


def test_malformed_team_membership_fails_closed(tmp_path: Path) -> None:
    rows = team_rows()
    rows[1]["fis_competitor_id"] = rows[0]["fis_competitor_id"]
    sources = source_bundle(
        tmp_path,
        results=rows,
        men=[history("MEMBER Two", "2"), history("MEMBER Four", "4")],
        ladies=[history("MEMBER One", "11"), history("MEMBER Three", "13")],
    )
    with pytest.raises(IdentityError, match="appears twice"):
        map_identities(**sources, output_dir=tmp_path / "identity")


def test_output_cannot_be_inside_source_directory(tmp_path: Path) -> None:
    sources = source_bundle(
        tmp_path,
        results=[result_row()],
        men=[history("JUMPER One", "1")],
        ladies=[],
    )
    with pytest.raises(IdentityError, match="cannot be inside"):
        map_identities(
            **sources,
            output_dir=sources["results"].parent / "identity",
        )
