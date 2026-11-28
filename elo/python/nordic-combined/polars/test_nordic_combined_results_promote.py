from __future__ import annotations

import csv
import hashlib
import json
import shutil
from pathlib import Path

import pytest

from nordic_combined_results_promote import PromotionError, build_promotion_candidate
from nordic_combined_results_replay import run_replay


ROOT = Path(__file__).resolve().parent
ACCEPTED = ROOT / "relay/excel365"
REFERENCES = ROOT / "excel365"
MAPPED_FIELDS = [
    "date", "city", "country", "sex", "season", "race", "event_kind",
    "distance", "race_type", "championship", "place", "team_name",
    "team_nation", "member_order", "fis_competitor_id", "fis_code",
    "name", "nation", "canonical_athlete_id", "canonical_name",
    "canonical_nation", "canonical_sex", "identity_match_method",
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def member(
    athlete_id: str, name: str, nation: str, sex: str, place: int, order: int
) -> dict[str, object]:
    return {
        "date": "2026-03-08", "city": "Lahti", "country": "Finland",
        "sex": "Mixed", "season": "2026", "race": "1",
        "event_kind": "team_sprint", "distance": "2x7.5",
        "race_type": "Team Sprint", "championship": "0", "place": place,
        "team_name": f"{nation.upper()} I", "team_nation": nation,
        "member_order": order, "fis_competitor_id": f"fis-{athlete_id}",
        "fis_code": f"code-{athlete_id}", "name": name, "nation": nation[:3].upper(),
        "canonical_athlete_id": athlete_id, "canonical_name": name,
        "canonical_nation": nation, "canonical_sex": sex,
        "identity_match_method": "test_fixture",
    }


def write_mapped(path: Path) -> None:
    rows = [
        member("1329", "Jens Lurås Oftebro", "Norway", "M", 1, 1),
        member("60", "Ida Marie Hagen", "Norway", "L", 1, 2),
        member("1348", "Julian Schmid", "Germany", "M", 2, 1),
        member("168", "Nathalie Armbruster", "Germany", "L", 2, 2),
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_individual_mapped(path: Path) -> None:
    rows = []
    for athlete_id, name, nation, place in (
        ("1329", "Jens Lurås Oftebro", "Norway", 1),
        ("1348", "Julian Schmid", "Germany", 2),
    ):
        rows.append({
            "date": "2026-03-08", "city": "Oslo", "country": "Norway",
            "sex": "M", "season": "2026", "race": "1",
            "event_kind": "individual", "distance": "10",
            "race_type": "Individual", "championship": "0", "place": place,
            "fis_competitor_id": f"fis-{athlete_id}", "fis_code": f"code-{athlete_id}",
            "name": name, "nation": nation[:3].upper(),
            "canonical_athlete_id": athlete_id, "canonical_name": name,
            "canonical_nation": nation, "canonical_sex": "M",
            "identity_match_method": "test_fixture",
        })
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_men_team_mapped(path: Path, event_kind: str) -> None:
    team_size = 4 if event_kind == "team" else 2
    teams = {
        "Austria": [
            ("1469", "Johannes Lamparter"), ("72", "Franz-Josef Rehrl"),
            ("1467", "Thomas Rettenegger"), ("1775", "Stefan Rettenegger"),
        ],
        "Germany": [
            ("1348", "Julian Schmid"), ("14", "Johannes Rydzek"),
            ("1137", "Terence Weber"), ("1468", "David Mach"),
        ],
    }
    rows = []
    for place, (nation, athletes) in enumerate(teams.items(), 1):
        for order, (athlete_id, name) in enumerate(athletes[:team_size], 1):
            rows.append({
                "date": "2026-03-08", "city": "Oslo", "country": "Norway",
                "sex": "M", "season": "2026", "race": "1",
                "event_kind": event_kind,
                "distance": "4x5" if event_kind == "team" else "2x7.5",
                "race_type": "Team" if event_kind == "team" else "Team Sprint",
                "championship": "0", "place": place,
                "team_name": f"{nation.upper()} I", "team_nation": nation,
                "member_order": order, "fis_competitor_id": f"fis-{athlete_id}",
                "fis_code": f"code-{athlete_id}", "name": name,
                "nation": nation[:3].upper(), "canonical_athlete_id": athlete_id,
                "canonical_name": name, "canonical_nation": nation,
                "canonical_sex": "M", "identity_match_method": "test_fixture",
            })
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture()
def prepared(tmp_path: Path) -> dict[str, Path]:
    mapped = tmp_path / "evidence/mapped.csv"
    mapped.parent.mkdir()
    write_mapped(mapped)
    replay = tmp_path / "replay"
    inputs = {
        "mapped_results": mapped,
        "men_history": ACCEPTED / "men_chrono.csv",
        "ladies_history": ACCEPTED / "ladies_chrono.csv",
        "team_history": ACCEPTED / "men_team_chrono.csv",
        "team_sprint_history": ACCEPTED / "team_sprint_chrono.csv",
        "men_reference_history": REFERENCES / "all_M.csv",
        "ladies_reference_history": REFERENCES / "all_L.csv",
    }
    run_replay(**inputs, season=2026, output_dir=replay)
    return {**inputs, "replay": replay}


def options(prepared: dict[str, Path], output: Path) -> dict[str, object]:
    report = prepared["replay"] / "replay_report.json"
    return {
        "replay_report": report,
        "expected_report_sha256": digest(report),
        "mapped_results": prepared["mapped_results"],
        "men_history": prepared["men_history"],
        "ladies_history": prepared["ladies_history"],
        "team_history": prepared["team_history"],
        "team_sprint_history": prepared["team_sprint_history"],
        "men_reference_history": prepared.get("men_reference_history"),
        "ladies_reference_history": prepared.get("ladies_reference_history"),
        "expected_season": 2026,
        "output_dir": output,
    }


def test_builds_atomic_candidate_without_modifying_accepted(
    prepared: dict[str, Path], tmp_path: Path
) -> None:
    sources = [prepared[name] for name in (
        "mapped_results", "men_history", "ladies_history", "team_history",
        "team_sprint_history"
    )]
    before = {path: digest(path) for path in sources}
    output = tmp_path / "promotion"
    report = build_promotion_candidate(**options(prepared, output))
    assert report["status"] == "promotion_candidate"
    assert report["atomic_candidate_set"] is True
    assert report["accepted_files_modified"] is False
    assert report["application_performed"] is False
    assert report["mapped_member_rows"] == 4
    assert report["mapped_team_rows"] == 2
    assert set(report["outputs"]) == {
        "men_chrono.csv", "ladies_chrono.csv", "men_team_chrono.csv",
        "team_sprint_chrono.csv",
    }
    assert set(report["accepted_before"]) == {
        "men_chrono.csv", "ladies_chrono.csv", "men_team_chrono.csv",
        "team_sprint_chrono.csv",
    }
    assert set(report["reference_inputs"]) == {"all_M.csv", "all_L.csv"}
    assert (output / "report.json").is_file()
    assert {path: digest(path) for path in sources} == before


def test_rejects_wrong_report_hash(prepared: dict[str, Path], tmp_path: Path) -> None:
    args = options(prepared, tmp_path / "promotion")
    args["expected_report_sha256"] = "0" * 64
    with pytest.raises(PromotionError, match="report SHA-256 mismatch"):
        build_promotion_candidate(**args)


def test_rejects_accepted_input_drift(prepared: dict[str, Path], tmp_path: Path) -> None:
    accepted_copy = tmp_path / "accepted-copy/men.csv"
    accepted_copy.parent.mkdir()
    shutil.copyfile(prepared["men_history"], accepted_copy)
    args = options(prepared, tmp_path / "promotion")
    args["men_history"] = accepted_copy
    with pytest.raises(PromotionError, match="did not validate input"):
        build_promotion_candidate(**args)


def test_fresh_replay_must_match_pinned_candidate(
    prepared: dict[str, Path], tmp_path: Path
) -> None:
    replay = tmp_path / "tampered-replay"
    shutil.copytree(prepared["replay"], replay)
    candidate = replay / "candidate_men_chrono.csv"
    candidate.write_bytes(candidate.read_bytes() + b"\n")
    report_path = replay / "replay_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    receipt = report["outputs"]["candidate_men_chrono.csv"]
    receipt["path"] = str(candidate.resolve())
    receipt["sha256"] = digest(candidate)
    for filename, item in report["outputs"].items():
        item["path"] = str((replay / filename).resolve())
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args = options(prepared, tmp_path / "promotion")
    args["replay_report"] = report_path
    args["expected_report_sha256"] = digest(report_path)
    with pytest.raises(PromotionError, match="fresh replay is not byte-identical"):
        build_promotion_candidate(**args)


def test_output_cannot_be_inside_accepted_data(prepared: dict[str, Path]) -> None:
    with pytest.raises(PromotionError, match="cannot be inside source directory"):
        build_promotion_candidate(
            **options(prepared, ACCEPTED / "promotion-candidate")
        )


def test_builds_regular_individual_promotion_candidate(tmp_path: Path) -> None:
    mapped = tmp_path / "evidence/mapped.csv"
    mapped.parent.mkdir()
    write_individual_mapped(mapped)
    replay = tmp_path / "replay"
    inputs = {
        "mapped_results": mapped,
        "men_history": ACCEPTED / "men_chrono.csv",
        "ladies_history": ACCEPTED / "ladies_chrono.csv",
        "team_history": ACCEPTED / "men_team_chrono.csv",
        "team_sprint_history": ACCEPTED / "team_sprint_chrono.csv",
    }
    run_replay(**inputs, season=2026, output_dir=replay)
    prepared = {**inputs, "replay": replay}
    report = build_promotion_candidate(**options(prepared, tmp_path / "promotion"))
    assert report["event"]["event_kind"] == "individual"
    assert report["event"]["sex"] == "M"
    assert report["mapped_member_rows"] == 2
    assert report["mapped_team_rows"] == 0
    assert report["row_deltas"]["men_chrono.csv"] >= 2
    assert report["row_deltas"]["ladies_chrono.csv"] == 0
    assert report["row_deltas"]["men_team_chrono.csv"] == 0
    assert report["row_deltas"]["team_sprint_chrono.csv"] == 0


@pytest.mark.parametrize(
    ("event_kind", "aggregate_file"),
    [("team", "men_team_chrono.csv"), ("team_sprint", "team_sprint_chrono.csv")],
)
def test_builds_same_sex_team_promotion_candidate(
    tmp_path: Path, event_kind: str, aggregate_file: str
) -> None:
    mapped = tmp_path / "evidence/mapped.csv"
    mapped.parent.mkdir()
    write_men_team_mapped(mapped, event_kind)
    replay = tmp_path / "replay"
    inputs = {
        "mapped_results": mapped,
        "men_history": ACCEPTED / "men_chrono.csv",
        "ladies_history": ACCEPTED / "ladies_chrono.csv",
        "team_history": ACCEPTED / "men_team_chrono.csv",
        "team_sprint_history": ACCEPTED / "team_sprint_chrono.csv",
    }
    run_replay(**inputs, season=2026, output_dir=replay)
    report = build_promotion_candidate(
        **options({**inputs, "replay": replay}, tmp_path / "promotion")
    )
    assert report["event"]["event_kind"] == event_kind
    assert report["event"]["sex"] == "M"
    assert report["mapped_team_rows"] == 2
    assert report["row_deltas"][aggregate_file] == 2
