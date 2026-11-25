from __future__ import annotations

import csv
import hashlib
import json
import shutil
from pathlib import Path

import pytest
from skijump_results_elo_replay import run_replay
from skijump_results_promote import PromotionError, build_promotion_candidate

ROOT = Path(__file__).resolve().parent
ACCEPTED = ROOT / "relay" / "excel365"
RAW = ROOT / "excel365"
MAPPED_FIELDS = [
    "date",
    "sex",
    "season",
    "race",
    "event_kind",
    "city",
    "country",
    "hill_size",
    "race_type",
    "championship",
    "place",
    "team_name",
    "team_nation",
    "member_order",
    "fis_competitor_id",
    "fis_code",
    "name",
    "birth_year",
    "nation",
    "source_url",
    "source_sha256",
    "canonical_athlete_id",
    "canonical_name",
    "canonical_nation",
    "canonical_sex",
    "identity_match_method",
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def mapped_member(
    athlete_id: str, name: str, sex: str, order: int
) -> dict[str, object]:
    return {
        "date": "2025-11-20",
        "sex": "Mixed",
        "season": 2026,
        "race": 1,
        "event_kind": "team",
        "city": "Lillehammer",
        "country": "Norway",
        "hill_size": "140",
        "race_type": "Team Large",
        "championship": 0,
        "place": 1,
        "team_name": "NORWAY",
        "team_nation": "NOR",
        "member_order": order,
        "fis_competitor_id": f"test-{athlete_id}",
        "fis_code": f"code-{athlete_id}",
        "name": name,
        "birth_year": 2000,
        "nation": "NOR",
        "source_url": "https://example.invalid/test",
        "source_sha256": "a" * 64,
        "canonical_athlete_id": athlete_id,
        "canonical_name": name,
        "canonical_nation": "Norway",
        "canonical_sex": sex,
        "identity_match_method": "test_fixture",
    }


def write_mapped(path: Path) -> None:
    rows = [
        mapped_member("2566", "Sølve Jokerud Strand", "M", 1),
        mapped_member("2535", "Adrian Thon Gundersrud", "M", 2),
        mapped_member("479", "Ingvild Synnøve Midtskogen", "L", 3),
        mapped_member("354", "Eirin Maria Kvandal", "L", 4),
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_individual_mapped(path: Path) -> None:
    rows = [
        {
            **mapped_member("2566", "Sølve Jokerud Strand", "M", 0),
            "date": "2025-11-19",
            "sex": "M",
            "event_kind": "individual",
            "race_type": "Large",
            "place": 1,
            "team_name": "",
            "team_nation": "",
            "member_order": "",
        },
        {
            **mapped_member("2535", "Adrian Thon Gundersrud", "M", 0),
            "date": "2025-11-19",
            "sex": "M",
            "event_kind": "individual",
            "race_type": "Large",
            "place": 2,
            "team_name": "",
            "team_nation": "",
            "member_order": "",
        },
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_same_sex_team_mapped(path: Path, sex: str) -> None:
    identities = (
        [
            ("2566", "Sølve Jokerud Strand"),
            ("2535", "Adrian Thon Gundersrud"),
            ("432", "Stefan Kraft"),
            ("2416", "Jan Hörl"),
        ]
        if sex == "M"
        else [
            ("479", "Ingvild Synnøve Midtskogen"),
            ("354", "Eirin Maria Kvandal"),
            ("400", "Julia Mühlbacher"),
            ("337", "Lisa Eder"),
        ]
    )
    rows = [
        {
            **mapped_member(athlete_id, name, sex, order),
            "date": "2025-11-19",
            "sex": sex,
            "event_kind": "team",
            "race_type": "Team Large",
            "place": 1,
        }
        for order, (athlete_id, name) in enumerate(identities, start=1)
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="module")
def prepared(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    root = tmp_path_factory.mktemp("skijump-promotion")
    mapped = root / "mapped.csv"
    write_mapped(mapped)
    replay_dir = root / "replay"
    inputs = {
        "mapped_results": mapped,
        "men_chrono": ACCEPTED / "men_chrono.csv",
        "ladies_chrono": ACCEPTED / "ladies_chrono.csv",
        "men_raw": RAW / "all_men_scrape.csv",
        "ladies_raw": RAW / "all_ladies_scrape.csv",
        "men_team_chrono": ACCEPTED / "men_team_chrono.csv",
        "ladies_team_chrono": ACCEPTED / "ladies_team_chrono.csv",
        "mixed_team_chrono": ACCEPTED / "mixed_team_chrono.csv",
    }
    run_replay(**inputs, output_dir=replay_dir, season=2026)
    return {"root": root, "replay_dir": replay_dir, **inputs}


def promotion_args(
    prepared: dict[str, object], output: Path, replay_dir: Path | None = None
) -> dict[str, object]:
    directory = replay_dir or prepared["replay_dir"]
    assert isinstance(directory, Path)
    report = directory / "replay_report.json"
    return {
        "replay_report": report,
        "expected_report_sha256": digest(report),
        "mapped_results": prepared["mapped_results"],
        "men_chrono": prepared["men_chrono"],
        "ladies_chrono": prepared["ladies_chrono"],
        "men_raw": prepared["men_raw"],
        "ladies_raw": prepared["ladies_raw"],
        "men_team_chrono": prepared["men_team_chrono"],
        "ladies_team_chrono": prepared["ladies_team_chrono"],
        "mixed_team_chrono": prepared["mixed_team_chrono"],
        "expected_season": 2026,
        "output_dir": output,
    }


def copy_replay(prepared: dict[str, object], name: str) -> Path:
    source = prepared["replay_dir"]
    root = prepared["root"]
    assert isinstance(source, Path) and isinstance(root, Path)
    destination = root / name
    shutil.copytree(source, destination)
    report_path = destination / "replay_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    for filename, receipt in report["outputs"].items():
        receipt["path"] = str(destination / filename)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return destination


def test_builds_atomic_candidate_without_modifying_accepted(
    prepared: dict[str, object],
) -> None:
    root = prepared["root"]
    assert isinstance(root, Path)
    accepted = [
        prepared[name]
        for name in (
            "men_chrono",
            "ladies_chrono",
            "men_raw",
            "ladies_raw",
            "men_team_chrono",
            "ladies_team_chrono",
            "mixed_team_chrono",
        )
    ]
    before = {path: digest(path) for path in accepted if isinstance(path, Path)}
    output = root / "promotion-happy"
    report = build_promotion_candidate(**promotion_args(prepared, output))
    assert report["status"] == "promotion_candidate"
    assert report["accepted_files_modified"] is False
    assert report["atomic_candidate_set"] is True
    assert set(report["outputs"]) == {
        "men_chrono.csv",
        "ladies_chrono.csv",
        "men_team_chrono.csv",
        "ladies_team_chrono.csv",
        "mixed_team_chrono.csv",
    }
    assert {path: digest(path) for path in before} == before
    assert (output / "report.json").is_file()


def test_promotes_missing_individual_event(tmp_path: Path) -> None:
    mapped = tmp_path / "individual-mapped.csv"
    write_individual_mapped(mapped)
    replay_dir = tmp_path / "individual-replay"
    inputs = {
        "mapped_results": mapped,
        "men_chrono": ACCEPTED / "men_chrono.csv",
        "ladies_chrono": ACCEPTED / "ladies_chrono.csv",
        "men_raw": RAW / "all_men_scrape.csv",
        "ladies_raw": RAW / "all_ladies_scrape.csv",
        "men_team_chrono": ACCEPTED / "men_team_chrono.csv",
        "ladies_team_chrono": ACCEPTED / "ladies_team_chrono.csv",
        "mixed_team_chrono": ACCEPTED / "mixed_team_chrono.csv",
    }
    replay = run_replay(**inputs, output_dir=replay_dir, season=2026)
    assert replay["eligible_individual_events"] == 1
    assert replay["eligible_mixed_team_events"] == 0

    report_path = replay_dir / "replay_report.json"
    promotion = build_promotion_candidate(
        replay_report=report_path,
        expected_report_sha256=digest(report_path),
        expected_season=2026,
        output_dir=tmp_path / "individual-promotion",
        **inputs,
    )
    assert promotion["status"] == "promotion_candidate"
    with (tmp_path / "individual-promotion" / "men_chrono.csv").open(
        newline="", encoding="utf-8-sig"
    ) as handle:
        rows = list(csv.DictReader(handle))
    inserted = [row for row in rows if row["Date"] == "2025-11-19"]
    assert len(inserted) == 2
    assert {row["TeamEvent"] for row in inserted} == {"0"}


@pytest.mark.parametrize(
    ("sex", "team_filename"),
    [("M", "men_team_chrono.csv"), ("L", "ladies_team_chrono.csv")],
)
def test_promotes_missing_same_sex_team_event(
    tmp_path: Path, sex: str, team_filename: str
) -> None:
    mapped = tmp_path / f"{sex}-team-mapped.csv"
    write_same_sex_team_mapped(mapped, sex)
    replay_dir = tmp_path / f"{sex}-team-replay"
    inputs = {
        "mapped_results": mapped,
        "men_chrono": ACCEPTED / "men_chrono.csv",
        "ladies_chrono": ACCEPTED / "ladies_chrono.csv",
        "men_raw": RAW / "all_men_scrape.csv",
        "ladies_raw": RAW / "all_ladies_scrape.csv",
        "men_team_chrono": ACCEPTED / "men_team_chrono.csv",
        "ladies_team_chrono": ACCEPTED / "ladies_team_chrono.csv",
        "mixed_team_chrono": ACCEPTED / "mixed_team_chrono.csv",
    }
    replay = run_replay(**inputs, output_dir=replay_dir, season=2026)
    assert replay["eligible_same_sex_team_events"] == {
        "M": int(sex == "M"),
        "L": int(sex == "L"),
    }

    report_path = replay_dir / "replay_report.json"
    output_dir = tmp_path / f"{sex}-team-promotion"
    promotion = build_promotion_candidate(
        replay_report=report_path,
        expected_report_sha256=digest(report_path),
        expected_season=2026,
        output_dir=output_dir,
        **inputs,
    )
    assert promotion["eligible_same_sex_team_dates"][sex] == ["2025-11-19"]
    with (output_dir / team_filename).open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    inserted = [row for row in rows if row["Date"] == "2025-11-19"]
    assert len(inserted) == 1
    assert inserted[0]["Sex"] == sex
    assert inserted[0]["TeamSize"] == "4"
    assert len(inserted[0]["MemberIDs"].split(",")) == 4


def test_rejects_wrong_report_hash(prepared: dict[str, object]) -> None:
    root = prepared["root"]
    assert isinstance(root, Path)
    args = promotion_args(prepared, root / "promotion-wrong-hash")
    args["expected_report_sha256"] = "0" * 64
    with pytest.raises(PromotionError, match="report SHA-256 mismatch"):
        build_promotion_candidate(**args)


def test_rejects_malformed_four_member_team(prepared: dict[str, object]) -> None:
    root = prepared["root"]
    assert isinstance(root, Path)
    replay = copy_replay(prepared, "replay-bad-team")
    team_path = replay / "candidate_mixed_team_chrono.csv"
    with team_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields, rows = list(reader.fieldnames or []), list(reader)
    rows[0]["TeamSize"] = "3"
    with team_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    report_path = replay / "replay_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["outputs"][team_path.name]["sha256"] = digest(team_path)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with pytest.raises(PromotionError, match="invalid four-member mixed team"):
        build_promotion_candidate(
            **promotion_args(prepared, root / "promotion-bad-team", replay)
        )


def test_fresh_replay_must_match_reviewed_candidate(
    prepared: dict[str, object],
) -> None:
    root = prepared["root"]
    assert isinstance(root, Path)
    replay = copy_replay(prepared, "replay-stale-candidate")
    candidate = replay / "candidate_men_chrono.csv"
    candidate.write_bytes(candidate.read_bytes() + b"\n")
    report_path = replay / "replay_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["outputs"][candidate.name]["sha256"] = digest(candidate)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with pytest.raises(PromotionError, match="fresh replay checksum differs"):
        build_promotion_candidate(
            **promotion_args(prepared, root / "promotion-stale", replay)
        )


def test_changed_mapped_input_is_rejected(prepared: dict[str, object]) -> None:
    root = prepared["root"]
    mapped = prepared["mapped_results"]
    assert isinstance(root, Path) and isinstance(mapped, Path)
    original = mapped.read_bytes()
    try:
        mapped.write_bytes(original + b"\n")
        with pytest.raises(PromotionError, match="accepted replay input changed"):
            build_promotion_candidate(
                **promotion_args(prepared, root / "promotion-drift")
            )
    finally:
        mapped.write_bytes(original)


def test_output_must_be_new_and_isolated(prepared: dict[str, object]) -> None:
    accepted = prepared["men_chrono"]
    assert isinstance(accepted, Path)
    with pytest.raises(PromotionError, match="cannot be inside source"):
        build_promotion_candidate(
            **promotion_args(prepared, accepted.parent / "candidate")
        )


def test_promoted_histories_replay_idempotently(prepared: dict[str, object]) -> None:
    root = prepared["root"]
    assert isinstance(root, Path)
    promoted = root / "promotion-idempotent"
    build_promotion_candidate(**promotion_args(prepared, promoted))
    for prefix in ("M", "L"):
        for suffix in ("", "_Small", "_Medium", "_Normal", "_Large", "_Flying"):
            source = ACCEPTED / f"{prefix}{suffix}.csv"
            shutil.copyfile(source, promoted / source.name)

    second = root / "post-promotion-replay"
    report = run_replay(
        mapped_results=prepared["mapped_results"],
        men_chrono=promoted / "men_chrono.csv",
        ladies_chrono=promoted / "ladies_chrono.csv",
        men_raw=prepared["men_raw"],
        ladies_raw=prepared["ladies_raw"],
        men_team_chrono=promoted / "men_team_chrono.csv",
        ladies_team_chrono=promoted / "ladies_team_chrono.csv",
        mixed_team_chrono=promoted / "mixed_team_chrono.csv",
        output_dir=second,
        season=2026,
    )
    assert report["eligible_mixed_team_events"] == 0
    assert report["inserted_member_rows"] == 0
    with (promoted / "mixed_team_chrono.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        promoted_team_rows = len(list(csv.DictReader(handle)))
    assert report["candidate_mixed_team_rows"] == promoted_team_rows
    for filename in (
        "men_chrono.csv",
        "ladies_chrono.csv",
        "men_team_chrono.csv",
        "ladies_team_chrono.csv",
        "mixed_team_chrono.csv",
    ):
        assert digest(promoted / filename) == digest(second / f"candidate_{filename}")
