#!/usr/bin/env python3
"""Enrich a weekend startlist with private fixed-anchor prediction Elo.

This is a shadow-only adapter. Accepted A-tier athletes keep their accepted
ratings. Official FIS starters who have never raced at A tier receive the
latest calibrated fixed-anchor rating strictly before the target race, or the
1300 newcomer prior when no earlier evidence exists. Nothing is publishable.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from cross_country_results_elo_replay import BASE_ELO, STREAMS
from cross_country_weekend_inputs import STARTLIST_FIELDS

OUTPUT_FIELDS = (
    *STARTLIST_FIELDS,
    "rating_source",
    "rating_as_of_date",
    "rating_k_factor",
    *STREAMS,
)
REPORT_SCHEMA = "nordicnumbers.cross-country-prediction-elo-shadow/v1"


class PredictionEloShadowError(RuntimeError):
    """The private prediction-Elo overlay cannot be produced safely."""


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), [dict(row) for row in reader]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _csv_text(rows: list[dict[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=OUTPUT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _validate_output(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise PredictionEloShadowError(
                "shadow output cannot overlap an input source"
            )
    return resolved


def _official_index(path: Path, sex: str) -> dict[str, list[dict[str, str]]]:
    fields, rows = _read(path)
    required = {"ID", "Date", *STREAMS}
    if missing := sorted(required - set(fields)):
        raise PredictionEloShadowError(f"{path} is missing official fields: {missing}")
    result: dict[str, list[dict[str, str]]] = {}
    for line, row in enumerate(rows, start=2):
        athlete_id = row["ID"].strip()
        try:
            date.fromisoformat(row["Date"].strip())
        except ValueError as exc:
            raise PredictionEloShadowError(f"{path}:{line}: invalid date") from exc
        if not athlete_id:
            raise PredictionEloShadowError(f"{path}:{line}: missing athlete ID")
        result.setdefault(f"{sex}:{athlete_id}", []).append(row)
    for history in result.values():
        history.sort(key=lambda row: (row["Date"], int(row.get("Race") or 0)))
    return result


def _prediction_index(path: Path, selected_k: float) -> dict[str, list[dict[str, str]]]:
    fields, rows = _read(path)
    required = {
        "prediction_athlete_id",
        "fis_competitor_id",
        "fis_code",
        "date",
        "k_factor",
        *STREAMS,
    }
    if missing := sorted(required - set(fields)):
        raise PredictionEloShadowError(
            f"{path} is missing prediction fields: {missing}"
        )
    result: dict[str, list[dict[str, str]]] = {}
    for line, row in enumerate(rows, start=2):
        prediction_id = row["prediction_athlete_id"].strip()
        if prediction_id != f"fis:{row['fis_competitor_id'].strip()}":
            raise PredictionEloShadowError(
                f"{path}:{line}: unstable prediction identity"
            )
        if float(row["k_factor"]) != selected_k:
            raise PredictionEloShadowError(
                f"{path}:{line}: ledger K does not match policy"
            )
        try:
            date.fromisoformat(row["date"].strip())
        except ValueError as exc:
            raise PredictionEloShadowError(f"{path}:{line}: invalid date") from exc
        result.setdefault(prediction_id, []).append(row)
    for history in result.values():
        history.sort(key=lambda row: (row["date"], int(row.get("fis_race_id") or 0)))
    return result


def enrich_prediction_elo_shadow(
    *,
    startlist: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    prediction_ledger: Path,
    policy_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    sources = (startlist, men_chrono, ladies_chrono, prediction_ledger, policy_path)
    output_dir = _validate_output(output_dir, sources)
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    selected_k = float(policy["prediction_elo"]["selected_k_factor"])
    if selected_k != 4.0 or not policy["prediction_elo"]["prior_is_private"]:
        raise PredictionEloShadowError(
            "policy has not selected private K=4 prediction Elo"
        )

    fields, rows = _read(startlist)
    if tuple(fields) != STARTLIST_FIELDS:
        raise PredictionEloShadowError("unexpected startlist schema")
    official = {}
    official.update(_official_index(men_chrono, "M"))
    official.update(_official_index(ladies_chrono, "L"))
    predictions = _prediction_index(prediction_ledger, selected_k)
    enriched: list[dict[str, object]] = []
    counts = {
        "accepted_official_elo": 0,
        "fixed_anchor_prediction_elo": 0,
        "newcomer_prior": 0,
    }

    for line, row in enumerate(rows, start=2):
        race_date = row["race_date"].strip()
        try:
            date.fromisoformat(race_date)
        except ValueError as exc:
            raise PredictionEloShadowError(
                f"{startlist}:{line}: invalid race date"
            ) from exc
        athlete_id = row["canonical_athlete_id"].strip()
        if not athlete_id or row["selection_status"] != "confirmed":
            raise PredictionEloShadowError(
                f"{startlist}:{line}: shadow overlay requires confirmed identity"
            )

        if athlete_id.startswith("fis:"):
            expected_id = f"fis:{row['fis_competitor_id'].strip()}"
            if athlete_id != expected_id or not row["fis_code"].strip():
                raise PredictionEloShadowError(
                    f"{startlist}:{line}: FIS identity mismatch"
                )
            candidates = [
                item
                for item in predictions.get(athlete_id, [])
                if item["date"] < race_date
            ]
            if candidates:
                selected = candidates[-1]
                if selected["fis_code"].strip() != row["fis_code"].strip():
                    raise PredictionEloShadowError(
                        f"{startlist}:{line}: FIS code changed"
                    )
                ratings = {stream: float(selected[stream]) for stream in STREAMS}
                source = "fixed_anchor_prediction_elo"
                as_of = selected["date"]
                k_factor: object = selected_k
            else:
                ratings = {stream: float(BASE_ELO) for stream in STREAMS}
                source = "newcomer_prior"
                as_of = ""
                k_factor = selected_k
        else:
            key = f"{row['sex']}:{athlete_id}"
            candidates = [
                item for item in official.get(key, []) if item["Date"] < race_date
            ]
            if not candidates:
                raise PredictionEloShadowError(
                    f"{startlist}:{line}: no prior accepted official Elo"
                )
            selected = candidates[-1]
            ratings = {stream: float(selected[stream]) for stream in STREAMS}
            source = "accepted_official_elo"
            as_of = selected["Date"]
            k_factor = ""
        counts[source] += 1
        enriched.append(
            {
                **row,
                "rating_source": source,
                "rating_as_of_date": as_of,
                "rating_k_factor": k_factor,
                **ratings,
            }
        )

    text = _csv_text(enriched)
    _atomic_text(output_dir / "startlist_prediction_elo_shadow.csv", text)
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "status": "shadow_candidate",
        "publication": {"status": "disabled"},
        "selected_k_factor": selected_k,
        "rows": len(enriched),
        "rating_sources": counts,
        "input_sha256": {path.name: _sha256(path) for path in sources},
        "output_sha256": hashlib.sha256(text.encode()).hexdigest(),
    }
    _atomic_text(
        output_dir / "report.json", json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--startlist", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--prediction-ledger", required=True, type=Path)
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = enrich_prediction_elo_shadow(
        startlist=args.startlist,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        prediction_ledger=args.prediction_ledger,
        policy_path=args.policy,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
