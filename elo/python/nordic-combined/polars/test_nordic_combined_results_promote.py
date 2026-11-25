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
        "date": "2026-03-07", "city": "Lahti", "country": "Finland",
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
        "team_sprint_history": ACCEPTED / "team_sprint_chrono.csv",
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
        "team_sprint_history": prepared["team_sprint_history"],
        "expected_season": 2026,
        "output_dir": output,
    }


def test_builds_atomic_candidate_without_modifying_accepted(
    prepared: dict[str, Path], tmp_path: Path
) -> None:
    sources = [prepared[name] for name in (
        "mapped_results", "men_history", "ladies_history", "team_sprint_history"
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
        "men_chrono.csv", "ladies_chrono.csv", "team_sprint_chrono.csv"
    }
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
