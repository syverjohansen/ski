#!/usr/bin/env python3
"""Backtest fixed-anchor prediction Elo at each athlete's first A-tier race.

Candidate ledgers must contain identical lower-tier evidence and differ only in
their replay parameters. Reviewed identity mappings connect prediction-only FIS
identities to accepted history without using names as an automatic join.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import re
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from cross_country_results_elo_replay import (
    ELO_DIFFERENCE,
    PREVIOUS_COLUMNS,
    STREAMS,
    regress_rating,
    stream_includes,
)

REPORT_SCHEMA = "nordicnumbers.fis-prediction-backtest/v1"
CASE_FILENAME = "fis-prediction-backtest-cases.csv"
REPORT_FILENAME = "fis-prediction-backtest-report.json"


class FisPredictionBacktestError(ValueError):
    """Raised when prediction evidence cannot support a safe backtest."""


@dataclass(frozen=True)
class PredictionEndpoint:
    sex: str
    prediction_athlete_id: str
    fis_competitor_id: str
    fis_code: str
    fis_name: str
    event_id: str
    event_date: str
    season: int
    ratings: dict[str, float]


@dataclass(frozen=True)
class ReviewedIdentity:
    sex: str
    prediction_athlete_id: str
    fis_competitor_id: str
    fis_code: str
    fis_name: str
    canonical_athlete_id: str
    canonical_name: str


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), list(reader)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _number(value: str, field: str) -> float:
    try:
        result = float((value or "").strip())
    except ValueError as error:
        raise FisPredictionBacktestError(f"invalid {field}: {value!r}") from error
    if not math.isfinite(result):
        raise FisPredictionBacktestError(f"non-finite {field}: {value!r}")
    return result


def _integer(value: str, *, default: int = 0) -> int:
    raw = (value or "").strip()
    return int(float(raw)) if raw else default


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise FisPredictionBacktestError(
                "backtest output cannot overlap calibration sources"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise FisPredictionBacktestError("backtest output directory must be empty")
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]], fields: Iterable[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _parse_ledger(
    path: Path,
) -> tuple[
    dict[tuple[str, str], PredictionEndpoint],
    frozenset[tuple[str, ...]],
    float,
]:
    fields, rows = _read_csv(path)
    required = {
        "fis_race_id",
        "date",
        "season",
        "sex",
        "place",
        "prediction_athlete_id",
        "fis_competitor_id",
        "fis_code",
        "name",
        "anchor_comparisons",
        "k_factor",
        "update_status",
        *STREAMS,
    }
    missing = sorted(required - set(fields))
    if missing:
        raise FisPredictionBacktestError(f"{path} is missing columns: {missing}")
    endpoints: dict[tuple[str, str], PredictionEndpoint] = {}
    evidence_shape = set()
    observed_k_factors = set()
    for line, row in enumerate(rows, start=2):
        sex = row["sex"].strip()
        prediction_id = row["prediction_athlete_id"].strip()
        competitor_id = row["fis_competitor_id"].strip()
        fis_code = row["fis_code"].strip()
        fis_name = row["name"].strip()
        event_id = row["fis_race_id"].strip()
        event_date = row["date"].strip()
        try:
            raced = date.fromisoformat(event_date)
            season = int(row["season"])
            place = int(row["place"])
            anchor_count = int(row["anchor_comparisons"])
        except ValueError as error:
            raise FisPredictionBacktestError(
                f"{path}:{line}: invalid event metadata"
            ) from error
        expected_season = raced.year + 1 if raced.month >= 7 else raced.year
        if (
            sex not in {"M", "L"}
            or not prediction_id.startswith("fis:")
            or not competitor_id
            or prediction_id != f"fis:{competitor_id}"
            or not fis_code
            or not fis_name
            or not event_id
            or season != expected_season
            or place < 1
            or anchor_count < 0
            or row["update_status"]
            not in {"updated_from_fixed_anchors", "anchor_free_no_update"}
        ):
            raise FisPredictionBacktestError(f"{path}:{line}: invalid ledger row")
        ratings = {stream: _number(row[stream], stream) for stream in STREAMS}
        k_factor = _number(row["k_factor"], "k_factor")
        if not 0.0 < k_factor <= 10.0:
            raise FisPredictionBacktestError(f"{path}:{line}: invalid K factor")
        observed_k_factors.add(k_factor)
        key = (sex, prediction_id)
        endpoint = PredictionEndpoint(
            sex=sex,
            prediction_athlete_id=prediction_id,
            fis_competitor_id=competitor_id,
            fis_code=fis_code,
            fis_name=fis_name,
            event_id=event_id,
            event_date=event_date,
            season=season,
            ratings=ratings,
        )
        previous = endpoints.get(key)
        if previous is None or (event_date, event_id) > (
            previous.event_date,
            previous.event_id,
        ):
            endpoints[key] = endpoint
        evidence_shape.add(
            (
                sex,
                prediction_id,
                competitor_id,
                fis_code,
                fis_name,
                event_date,
                event_id,
                str(place),
                str(anchor_count),
                row["update_status"],
            )
        )
    if not endpoints:
        raise FisPredictionBacktestError(f"{path} has no prediction endpoints")
    if len(observed_k_factors) != 1:
        raise FisPredictionBacktestError(f"{path} contains multiple K factors")
    return endpoints, frozenset(evidence_shape), observed_k_factors.pop()


def _parse_crosswalk(path: Path) -> dict[tuple[str, str], ReviewedIdentity]:
    fields, rows = _read_csv(path)
    required = {
        "sex",
        "prediction_athlete_id",
        "fis_competitor_id",
        "fis_code",
        "fis_name",
        "canonical_athlete_id",
        "canonical_name",
        "status",
        "evidence",
    }
    missing = sorted(required - set(fields))
    if missing:
        raise FisPredictionBacktestError(f"{path} is missing columns: {missing}")
    identities = {}
    for line, row in enumerate(rows, start=2):
        if row["status"].strip() != "reviewed":
            continue
        identity = ReviewedIdentity(
            sex=row["sex"].strip(),
            prediction_athlete_id=row["prediction_athlete_id"].strip(),
            fis_competitor_id=row["fis_competitor_id"].strip(),
            fis_code=row["fis_code"].strip(),
            fis_name=row["fis_name"].strip(),
            canonical_athlete_id=row["canonical_athlete_id"].strip(),
            canonical_name=row["canonical_name"].strip(),
        )
        key = (identity.sex, identity.prediction_athlete_id)
        if (
            identity.sex not in {"M", "L"}
            or identity.prediction_athlete_id != f"fis:{identity.fis_competitor_id}"
            or not identity.fis_code
            or not identity.fis_name
            or not identity.canonical_athlete_id
            or not identity.canonical_name
            or not row["evidence"].strip()
        ):
            raise FisPredictionBacktestError(
                f"{path}:{line}: incomplete reviewed identity"
            )
        if key in identities:
            raise FisPredictionBacktestError(f"{path}:{line}: duplicate identity")
        identities[key] = identity
    if not identities:
        raise FisPredictionBacktestError("crosswalk has no reviewed identities")
    return identities


def _load_histories(
    men_chrono: Path, ladies_chrono: Path
) -> dict[str, list[dict[str, str]]]:
    required = {
        "Date",
        "Event",
        "Season",
        "Race",
        "ID",
        "Skier",
        "Place",
        "Distance",
        "Technique",
        *PREVIOUS_COLUMNS.values(),
    }
    histories = {}
    for sex, path in (("M", men_chrono), ("L", ladies_chrono)):
        fields, rows = _read_csv(path)
        missing = sorted(required - set(fields))
        if missing:
            raise FisPredictionBacktestError(f"{path} is missing columns: {missing}")
        for line, row in enumerate(rows, start=2):
            try:
                date.fromisoformat(row["Date"].strip())
                int(row["Season"])
                int(row["Race"])
            except ValueError as error:
                raise FisPredictionBacktestError(
                    f"{path}:{line}: invalid accepted history identity"
                ) from error
            if not row["ID"].strip() or not row["Skier"].strip():
                raise FisPredictionBacktestError(
                    f"{path}:{line}: incomplete accepted athlete identity"
                )
        histories[sex] = rows
    return histories


def _first_a_tier_race(
    identity: ReviewedIdentity,
    endpoint: PredictionEndpoint,
    history: list[dict[str, str]],
) -> tuple[dict[str, str], list[dict[str, str]]] | None:
    athlete_rows = [
        row
        for row in history
        if row["ID"].strip() == identity.canonical_athlete_id
        and row["Event"].strip() != "Offseason"
        and _integer(row["Race"], default=-1) > 0
        and _integer(row["Place"], default=-1) > 0
    ]
    if athlete_rows and identity.canonical_name not in {
        row["Skier"].strip() for row in athlete_rows
    }:
        raise FisPredictionBacktestError(
            f"reviewed canonical name does not match accepted history for "
            f"{identity.prediction_athlete_id}"
        )
    if any(row["Date"].strip() <= endpoint.event_date for row in athlete_rows):
        raise FisPredictionBacktestError(
            f"{identity.prediction_athlete_id} was already A-tier verified by "
            f"{endpoint.event_date}"
        )
    later = [row for row in athlete_rows if row["Date"].strip() > endpoint.event_date]
    if not later:
        return None
    target = min(
        later,
        key=lambda row: (
            row["Date"].strip(),
            _integer(row["Season"], default=-1),
            _integer(row["Race"], default=-1),
        ),
    )
    race_key = (target["Date"], target["Season"], target["Race"])
    opponents = [
        row
        for row in history
        if (row["Date"], row["Season"], row["Race"]) == race_key
        and row["ID"].strip() != identity.canonical_athlete_id
        and row["Event"].strip() != "Offseason"
        and _integer(row["Place"], default=-1) > 0
    ]
    if not opponents:
        raise FisPredictionBacktestError(
            f"first A-tier race has no opponents for {identity.prediction_athlete_id}"
        )
    return target, opponents


def _probability(target_rating: float, opponent_rating: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((opponent_rating - target_rating) / ELO_DIFFERENCE))


def _actual_score(target_place: int, opponent_place: int) -> float:
    if target_place < opponent_place:
        return 1.0
    if target_place > opponent_place:
        return 0.0
    return 0.5


def _correct_direction(predicted: float, actual: float) -> float:
    if actual == 0.5:
        return float(predicted == 0.5)
    return float((predicted > 0.5) == (actual > 0.5))


def _metrics(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "cases": len(rows),
        "athletes": len({(row["sex"], row["canonical_athlete_id"]) for row in rows}),
        "brier_score": round(
            sum(float(row["squared_error"]) for row in rows) / len(rows), 8
        ),
        "accuracy": round(
            sum(float(row["correct_direction"]) for row in rows) / len(rows), 8
        ),
    }


def backtest_fis_prediction_elo(
    *,
    candidate_ledgers: dict[str, Path],
    identity_crosswalk: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    output_dir: Path,
    minimum_athletes: int = 20,
) -> dict[str, object]:
    """Compare K-factor candidates at leakage-free first A-tier debuts."""
    if not candidate_ledgers:
        raise FisPredictionBacktestError("at least one candidate ledger is required")
    if any(
        not re.fullmatch(r"[A-Za-z0-9._-]+", label) or label == "official_prior"
        for label in candidate_ledgers
    ):
        raise FisPredictionBacktestError(
            "candidate labels must be safe and non-reserved"
        )
    if minimum_athletes < 1:
        raise FisPredictionBacktestError("minimum_athletes must be positive")

    sources = [
        *candidate_ledgers.values(),
        identity_crosswalk,
        men_chrono,
        ladies_chrono,
    ]
    output_dir = _validate_output_dir(output_dir, sources)
    source_hashes = {str(path): _sha256(path) for path in sources}
    parsed = {label: _parse_ledger(path) for label, path in candidate_ledgers.items()}
    endpoints = {label: value[0] for label, value in parsed.items()}
    shapes = {label: value[1] for label, value in parsed.items()}
    candidate_k_factors = {label: value[2] for label, value in parsed.items()}
    if len(set(candidate_k_factors.values())) != len(candidate_k_factors):
        raise FisPredictionBacktestError(
            "candidate ledgers must use distinct K factors"
        )
    reference_label = next(iter(candidate_ledgers))
    reference_endpoints = endpoints[reference_label]
    reference_shape = shapes[reference_label]
    for label in candidate_ledgers:
        if shapes[label] != reference_shape:
            raise FisPredictionBacktestError(
                f"candidate {label!r} does not use identical replay evidence"
            )
        if set(endpoints[label]) != set(reference_endpoints):
            raise FisPredictionBacktestError(
                f"candidate {label!r} does not cover identical endpoints"
            )

    identities = _parse_crosswalk(identity_crosswalk)
    histories = _load_histories(men_chrono, ladies_chrono)
    cases: list[dict[str, object]] = []
    athletes_without_debut = []
    reviewed_keys = sorted(set(reference_endpoints) & set(identities))
    if not reviewed_keys:
        raise FisPredictionBacktestError(
            "no prediction endpoint has a reviewed calibration identity"
        )
    for key in reviewed_keys:
        identity = identities[key]
        reference = reference_endpoints[key]
        if (
            identity.fis_competitor_id != reference.fis_competitor_id
            or identity.fis_code != reference.fis_code
            or identity.fis_name != reference.fis_name
        ):
            raise FisPredictionBacktestError(
                f"crosswalk identity does not match endpoint {identity.prediction_athlete_id}"
            )
        debut = _first_a_tier_race(identity, reference, histories[identity.sex])
        if debut is None:
            athletes_without_debut.append(
                {
                    "sex": identity.sex,
                    "prediction_athlete_id": identity.prediction_athlete_id,
                    "canonical_athlete_id": identity.canonical_athlete_id,
                    "canonical_name": identity.canonical_name,
                }
            )
            continue
        target, opponents = debut
        return_season = int(target["Season"])
        matching_streams = [
            stream
            for stream in STREAMS
            if stream_includes(stream, target["Distance"], target["Technique"])
        ]
        for stream in matching_streams:
            previous_column = PREVIOUS_COLUMNS[stream]
            official_prior = _number(target[previous_column], previous_column)
            candidate_ratings = {
                label: regress_rating(
                    endpoints[label][key].ratings[stream],
                    max(0, return_season - endpoints[label][key].season),
                )
                for label in candidate_ledgers
            }
            for opponent in opponents:
                opponent_raw = (opponent.get(previous_column) or "").strip()
                if not opponent_raw:
                    continue
                opponent_rating = _number(opponent_raw, previous_column)
                actual = _actual_score(int(target["Place"]), int(opponent["Place"]))
                methods = {"official_prior": official_prior, **candidate_ratings}
                for method, rating in methods.items():
                    predicted = _probability(rating, opponent_rating)
                    cases.append(
                        {
                            "method": method,
                            "stream": stream,
                            "sex": identity.sex,
                            "prediction_athlete_id": identity.prediction_athlete_id,
                            "canonical_athlete_id": identity.canonical_athlete_id,
                            "canonical_name": identity.canonical_name,
                            "prediction_last_event_id": reference.event_id,
                            "prediction_last_date": reference.event_date,
                            "debut_date": target["Date"],
                            "debut_season": return_season,
                            "debut_race": target["Race"],
                            "debut_place": target["Place"],
                            "opponent_id": opponent["ID"],
                            "opponent_place": opponent["Place"],
                            "target_rating": rating,
                            "opponent_rating": opponent_rating,
                            "actual_score": actual,
                            "predicted_probability": predicted,
                            "squared_error": (predicted - actual) ** 2,
                            "correct_direction": _correct_direction(predicted, actual),
                        }
                    )

    if not cases:
        raise FisPredictionBacktestError(
            "no reviewed prediction athlete has a later first A-tier race"
        )
    methods = sorted({str(row["method"]) for row in cases})
    metrics = {
        method: _metrics([row for row in cases if row["method"] == method])
        for method in methods
    }
    best_candidate = min(
        candidate_ledgers,
        key=lambda label: (
            float(metrics[label]["brier_score"]),
            -float(metrics[label]["accuracy"]),
            label,
        ),
    )
    evaluated_athletes = int(metrics["official_prior"]["athletes"])
    if evaluated_athletes < minimum_athletes:
        decision = "insufficient_evidence"
        recommended_candidate = None
    elif float(metrics[best_candidate]["brier_score"]) < float(
        metrics["official_prior"]["brier_score"]
    ):
        decision = "candidate_improves_official_prior"
        recommended_candidate = best_candidate
    else:
        decision = "candidate_not_better_than_official_prior"
        recommended_candidate = None

    _atomic_text(output_dir / CASE_FILENAME, _csv_text(cases, list(cases[0])))
    after_hashes = {str(path): _sha256(path) for path in sources}
    if after_hashes != source_hashes:
        raise FisPredictionBacktestError("calibration sources changed during backtest")
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "source_sha256": source_hashes,
        "reference_candidate": reference_label,
        "reviewed_identity_count": len(reviewed_keys),
        "evaluated_athletes": evaluated_athletes,
        "athletes_without_debut": athletes_without_debut,
        "minimum_athletes": minimum_athletes,
        "metrics": metrics,
        "candidate_k_factors": candidate_k_factors,
        "best_candidate": best_candidate,
        "best_k_factor": candidate_k_factors[best_candidate],
        "decision": decision,
        "recommended_candidate": recommended_candidate,
        "recommended_k_factor": candidate_k_factors[recommended_candidate]
        if recommended_candidate
        else None,
        "unknown_vs_unknown_comparisons": 0,
        "verified_anchor_updates": 0,
        "publication_eligible": False,
        "official_histories_modified": False,
        "status": "backtested_prediction_only",
    }
    _atomic_text(
        output_dir / REPORT_FILENAME,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def _candidate_argument(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("candidate ledger must use LABEL=PATH")
    label, raw_path = value.split("=", 1)
    if not label.strip() or not raw_path.strip():
        raise argparse.ArgumentTypeError("candidate ledger must use LABEL=PATH")
    return label.strip(), Path(raw_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-ledger",
        action="append",
        required=True,
        type=_candidate_argument,
        metavar="LABEL=PATH",
    )
    parser.add_argument("--identity-crosswalk", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--minimum-athletes", type=int, default=20)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    candidates = dict(args.candidate_ledger)
    if len(candidates) != len(args.candidate_ledger):
        raise FisPredictionBacktestError("candidate ledger labels must be unique")
    report = backtest_fis_prediction_elo(
        candidate_ledgers=candidates,
        identity_crosswalk=args.identity_crosswalk,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        minimum_athletes=args.minimum_athletes,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
