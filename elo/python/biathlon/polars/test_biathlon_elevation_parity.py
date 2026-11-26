from __future__ import annotations

import csv
from pathlib import Path

import biathlon_elevation_parity as elevation
import polars as pl
import pytest
from biathlon_chrono_parity import _read
from ibu_source_contract import IBUContractError

HERE = Path(__file__).resolve().parent
INDIVIDUAL = HERE / "excel365"
RELAY = HERE / "relay" / "excel365"


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_add_elevation_preserves_legacy_normalization_and_default(
    tmp_path: Path,
) -> None:
    reference = tmp_path / "elevation.csv"
    write_csv(reference, [{"City": "ÖSTERSUND", "Elevation": 366}])
    frame = pl.DataFrame(
        {
            "City": ["  östersund ", "Unknown"],
            "Distance": ["20", "N/A"],
        }
    )

    result = elevation.add_elevation(frame, reference)

    assert result["City"].to_list() == ["Östersund", "Unknown"]
    assert result["Elevation"].to_list() == [366, 0]
    assert result["Distance"].to_list() == [20.0, None]


def test_full_accepted_elevation_chronos_rebuild_exactly(tmp_path: Path) -> None:
    report = elevation.run_parity(
        individual_dir=INDIVIDUAL,
        relay_dir=RELAY,
        output_dir=tmp_path / "candidate",
    )

    assert report["status"] == "passed"
    assert report["accepted_data_mutated"] is False
    comparisons = [
        report["policies"][policy][sex]
        for policy in ("individual", "relay")
        for sex in ("men", "ladies")
    ]
    assert all(item["generated_rows"] > 0 for item in comparisons)
    assert sum(item["generated_rows"] for item in comparisons) == sum(
        item["accepted_rows"] for item in comparisons
    )
    expected_zero_elevation_rows = sum(
        _read(root / f"{sex}_chrono_elevation.csv")
        .filter(pl.col("Elevation") == 0)
        .height
        for root in (INDIVIDUAL, RELAY)
        for sex in ("men", "ladies")
    )
    assert sum(
        item["zero_elevation_rows"] for item in comparisons
    ) == expected_zero_elevation_rows
    assert all(item["elevation_mismatches"] == 0 for item in comparisons)
    assert all(
        item["maximum_absolute_elevation_difference"] == 0 for item in comparisons
    )


def test_elevation_candidates_are_byte_deterministic(tmp_path: Path) -> None:
    first = elevation.run_parity(
        individual_dir=INDIVIDUAL,
        relay_dir=RELAY,
        output_dir=tmp_path / "first",
    )
    second = elevation.run_parity(
        individual_dir=INDIVIDUAL,
        relay_dir=RELAY,
        output_dir=tmp_path / "second",
    )

    first_hashes = sorted(
        first["policies"][policy][sex]["candidate"]["sha256"]
        for policy in ("individual", "relay")
        for sex in ("men", "ladies")
    )
    second_hashes = sorted(
        second["policies"][policy][sex]["candidate"]["sha256"]
        for policy in ("individual", "relay")
        for sex in ("men", "ladies")
    )
    assert first_hashes == second_hashes


def test_comparison_rejects_elevation_drift(tmp_path: Path) -> None:
    generated = _read(INDIVIDUAL / "men_chrono_elevation.csv").head(2)
    accepted = generated.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.col("Elevation") + 1)
        .otherwise(pl.col("Elevation"))
        .alias("Elevation")
    )
    accepted_path = tmp_path / "accepted.csv"
    accepted.write_csv(accepted_path)

    report = elevation.compare_elevation(generated, accepted_path, 1e-9)

    assert report["status"] == "failed"
    assert report["elevation_mismatches"] == 1


def test_duplicate_reference_city_fails_closed(tmp_path: Path) -> None:
    reference = tmp_path / "elevation.csv"
    write_csv(
        reference,
        [
            {"City": "Östersund", "Elevation": 366},
            {"City": " öSTERSUND ", "Elevation": 999},
        ],
    )
    with pytest.raises(IBUContractError, match="duplicate elevation city"):
        elevation.read_elevation_map(reference)


def test_output_cannot_enter_accepted_tree(tmp_path: Path) -> None:
    with pytest.raises(IBUContractError, match="outside accepted data"):
        elevation.run_parity(
            individual_dir=INDIVIDUAL,
            relay_dir=RELAY,
            output_dir=INDIVIDUAL / "candidate",
        )
