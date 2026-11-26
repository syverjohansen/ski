from __future__ import annotations

from pathlib import Path

import biathlon_chrono_parity as chrono
import polars as pl
import pytest
from ibu_source_contract import IBUContractError

HERE = Path(__file__).resolve().parent
INDIVIDUAL = HERE / "excel365"
RELAY = HERE / "relay" / "excel365"


def test_offseason_rules_preserve_current_and_discount_prior_seasons() -> None:
    frame = pl.DataFrame(
        {
            "RaceType": ["Offseason", "Offseason", "Sprint"],
            "Season": [2025, 2026, 2026],
            **{column: [1400.0, 1400.0, 1420.0] for column in chrono.RATING_COLUMNS},
        }
    )
    result = chrono._apply_offseason_rules(frame)

    assert result["Elo"].to_list() == [1385.0, 1400.0, 1420.0]
    assert result["Sprint_Elo"].to_list() == [1385.0, 1400.0, 1420.0]


def test_full_accepted_chronos_rebuild_with_exact_rating_parity(
    tmp_path: Path,
) -> None:
    report = chrono.run_parity(
        individual_dir=INDIVIDUAL,
        relay_dir=RELAY,
        output_dir=tmp_path / "candidate",
    )

    assert report["status"] == "passed"
    assert report["accepted_data_mutated"] is False
    assert report["promotion"] == {"status": "disabled"}
    assert report["publication"] == {"status": "disabled"}
    comparisons = [
        report["policies"][policy][sex]
        for policy in ("individual", "relay")
        for sex in ("M", "L")
    ]
    assert sum(item["generated_rows"] for item in comparisons) == 331_548
    assert all(item["maximum_absolute_rating_difference"] == 0 for item in comparisons)
    assert all(item["rating_mismatches"] == 0 for item in comparisons)
    assert all(
        item["missing_rows"] == item["unexpected_rows"] == 0 for item in comparisons
    )
    assert (tmp_path / "candidate" / "chrono_parity_report.json").is_file()


def test_generated_chronos_are_byte_deterministic(tmp_path: Path) -> None:
    first = chrono.run_parity(
        individual_dir=INDIVIDUAL,
        relay_dir=RELAY,
        output_dir=tmp_path / "first",
    )
    second = chrono.run_parity(
        individual_dir=INDIVIDUAL,
        relay_dir=RELAY,
        output_dir=tmp_path / "second",
    )

    first_hashes = {
        (policy, sex): first["policies"][policy][sex]["candidate"]["sha256"]
        for policy in ("individual", "relay")
        for sex in ("M", "L")
    }
    second_hashes = {
        (policy, sex): second["policies"][policy][sex]["candidate"]["sha256"]
        for policy in ("individual", "relay")
        for sex in ("M", "L")
    }
    assert first_hashes == second_hashes


def test_comparison_rejects_rating_drift(tmp_path: Path) -> None:
    generated = chrono.build_chrono(INDIVIDUAL, "M").head(2)
    accepted = generated.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.col("Elo") + 0.01)
        .otherwise(pl.col("Elo"))
        .alias("Elo")
    )
    accepted_path = tmp_path / "accepted.csv"
    accepted.write_csv(accepted_path)

    report = chrono.compare_chrono(generated, accepted_path, tolerance=1e-9)

    assert report["status"] == "failed"
    assert report["rating_mismatches"] == 1


def test_output_cannot_enter_accepted_data(tmp_path: Path) -> None:
    with pytest.raises(IBUContractError, match="outside accepted data"):
        chrono.run_parity(
            individual_dir=INDIVIDUAL,
            relay_dir=RELAY,
            output_dir=INDIVIDUAL / "candidate",
        )


def test_output_directory_must_be_new(tmp_path: Path) -> None:
    output = tmp_path / "candidate"
    output.mkdir()
    with pytest.raises(IBUContractError, match="already exists"):
        chrono.run_parity(
            individual_dir=INDIVIDUAL,
            relay_dir=RELAY,
            output_dir=output,
        )
