#!/usr/bin/env python3
"""Build the current private FIS prediction-Elo ledger end to end.

The pipeline composes the bounded lower-tier FIS acquisition, as-of identity
classification, and calibrated fixed-anchor replay. Accepted schedule and Elo
histories are immutable inputs. Every output is shadow-only and publication is
always disabled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterable
from datetime import date
from pathlib import Path

from cross_country_fis_prediction_acquire import (
    acquire_fis_prediction_evidence,
    fetch_html,
)
from cross_country_fis_prediction_identity import classify_fis_prediction_identities
from cross_country_fis_prediction_replay import (
    LEDGER_FILENAME,
    replay_fis_prediction_elo,
)
from cross_country_prediction_sources import PREDICTION_ONLY_FIS_CATEGORIES

REPORT_SCHEMA = "nordicnumbers.cross-country-prediction-elo-pipeline/v1"


class PredictionEloPipelineError(RuntimeError):
    """The current private prediction ledger cannot be built safely."""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _validate_output(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise PredictionEloPipelineError(
                "prediction pipeline output cannot overlap accepted inputs"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise PredictionEloPipelineError("prediction pipeline output must be empty")
    return resolved


def build_prediction_elo_ledger(
    *,
    season: int,
    as_of: date,
    accepted_schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    aliases_path: Path,
    policy_path: Path,
    output_dir: Path,
    categories: Iterable[str] | None = None,
    fetcher: Callable[[str], bytes] = fetch_html,
) -> dict[str, object]:
    sources = (
        accepted_schedule,
        men_chrono,
        ladies_chrono,
        aliases_path,
        policy_path,
    )
    output_dir = _validate_output(output_dir, sources)
    source_hashes = {path.name: _sha256(path) for path in sources}
    try:
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        selected_k = float(policy["prediction_elo"]["selected_k_factor"])
        allowed_categories = tuple(policy["fis_prediction_sources"]["categories"])
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise PredictionEloPipelineError("invalid prediction Elo policy") from exc
    if selected_k != 4.0:
        raise PredictionEloPipelineError("prediction policy has not selected K=4")
    if set(allowed_categories) != set(PREDICTION_ONLY_FIS_CATEGORIES):
        raise PredictionEloPipelineError("policy categories differ from implementation")
    selected_categories = tuple(sorted(set(categories or allowed_categories)))
    if not selected_categories or not set(selected_categories) <= set(
        allowed_categories
    ):
        raise PredictionEloPipelineError("pipeline contains a non-allowlisted category")

    acquisition_dir = output_dir / "acquisition"
    identity_dir = output_dir / "identity"
    replay_dir = output_dir / "replay"
    acquisition = acquire_fis_prediction_evidence(
        season=season,
        categories=selected_categories,
        accepted_schedule=accepted_schedule,
        as_of=as_of,
        output_dir=acquisition_dir,
        fetcher=fetcher,
    )
    identity = classify_fis_prediction_identities(
        results=acquisition_dir / "fis-prediction-results.csv",
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        aliases_path=aliases_path,
        output_dir=identity_dir,
    )
    replay = replay_fis_prediction_elo(
        participants=identity_dir / "fis-prediction-participants.csv",
        comparisons=identity_dir / "fis-anchor-comparisons.csv",
        output_dir=replay_dir,
        k_factor=selected_k,
    )
    ledger = replay_dir / LEDGER_FILENAME
    _atomic_bytes(output_dir / LEDGER_FILENAME, ledger.read_bytes())

    if {path.name: _sha256(path) for path in sources} != source_hashes:
        raise PredictionEloPipelineError("accepted inputs changed during the pipeline")
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "status": "private_ledger_ready",
        "season": season,
        "as_of": as_of.isoformat(),
        "categories": list(selected_categories),
        "selected_k_factor": selected_k,
        "publication": {"status": "disabled"},
        "source_sha256": source_hashes,
        "stage_reports": {
            "acquisition": {
                "schema_version": acquisition["schema_version"],
                "sha256": _sha256(acquisition_dir / "report.json"),
                "acquired_races": acquisition["acquired_races"],
            },
            "identity": {
                "schema_version": identity["schema_version"],
                "sha256": _sha256(identity_dir / "report.json"),
                "never_a_tier_appearances": identity["never_a_tier_appearances"],
            },
            "replay": {
                "schema_version": replay["schema_version"],
                "sha256": _sha256(replay_dir / "report.json"),
                "ledger_rows": replay["ledger_rows"],
            },
        },
        "ledger": {
            "path": LEDGER_FILENAME,
            "sha256": _sha256(output_dir / LEDGER_FILENAME),
            "rows": replay["ledger_rows"],
        },
        "accepted_history_modified": False,
        "unknown_vs_unknown_comparisons": replay["unknown_vs_unknown_comparisons"],
        "verified_anchor_updates": replay["verified_anchor_updates"],
    }
    _atomic_bytes(
        output_dir / "report.json",
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--accepted-schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--category",
        action="append",
        choices=sorted(PREDICTION_ONLY_FIS_CATEGORIES),
        dest="categories",
    )
    parser.add_argument("--allow-network", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.allow_network:
        raise PredictionEloPipelineError("network acquisition requires --allow-network")
    report = build_prediction_elo_ledger(
        season=args.season,
        as_of=args.as_of,
        accepted_schedule=args.accepted_schedule,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        aliases_path=args.aliases,
        policy_path=args.policy,
        output_dir=args.output_dir,
        categories=args.categories,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
