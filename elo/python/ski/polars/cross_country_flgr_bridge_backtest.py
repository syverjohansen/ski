#!/usr/bin/env python3
"""Backtest candidate FLGR bridge ratings against first returning FIS results.

For every reviewed athlete, the backtest finds the first accepted FIS race
strictly after the final supplied domestic event. It compares stale accepted
FIS Elo, one or more candidate bridge ledgers, and an optional quarantined
legacy ledger against every opponent in that return race. Outputs are isolated
research artifacts and can never promote a rating into production.
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
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from cross_country_results_elo_replay import ELO_DIFFERENCE, regress_rating

REPORT_SCHEMA = "nordicnumbers.flgr-bridge-backtest-report/v1"
CASE_FILENAME = "flgr-bridge-backtest-cases.csv"
REPORT_FILENAME = "flgr-bridge-backtest-report.json"


class FlgrBridgeBacktestError(ValueError):
    """Raised when bridge evidence cannot support a leakage-free backtest."""


@dataclass(frozen=True)
class BridgeEndpoint:
    sex: str
    canonical_athlete_id: str
    canonical_name: str
    event_id: str
    event_date: str
    season: int
    rating: float


@dataclass(frozen=True)
class ReturnRace:
    target: dict[str, str]
    opponents: tuple[dict[str, str], ...]


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), list(reader)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _integer(value: str, *, default: int = 0) -> int:
    raw = (value or "").strip()
    return int(float(raw)) if raw else default


def _number(value: str, field: str) -> float:
    try:
        result = float((value or "").strip())
    except ValueError as error:
        raise FlgrBridgeBacktestError(f"invalid {field}: {value!r}") from error
    if not math.isfinite(result):
        raise FlgrBridgeBacktestError(f"non-finite {field}: {value!r}")
    return result


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_path = source.expanduser().resolve()
        if resolved == source_path.parent or source_path.parent in resolved.parents:
            raise FlgrBridgeBacktestError(
                "backtest output cannot be inside a source directory"
            )
    if resolved.exists() and any(resolved.iterdir()):
        raise FlgrBridgeBacktestError("backtest output directory must be empty")
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]], fields: list[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _parse_bridge_ledger(
    path: Path,
) -> tuple[
    dict[tuple[str, str], BridgeEndpoint],
    frozenset[tuple[str, str, str, str, int, int, str]],
]:
    fields, rows = _read_csv(path)
    required = {
        "event_id",
        "date",
        "season",
        "sex",
        "canonical_athlete_id",
        "canonical_name",
        "identity_status",
        "place",
        "source_sha256",
        "Elo",
    }
    missing = sorted(required - set(fields))
    if missing:
        raise FlgrBridgeBacktestError(f"{path} is missing columns: {missing}")
    endpoints: dict[tuple[str, str], BridgeEndpoint] = {}
    evidence_shape: set[tuple[str, str, str, str, int, int, str]] = set()
    for line, row in enumerate(rows, start=2):
        if row["identity_status"] != "reviewed_crosswalk":
            continue
        sex = row["sex"].strip()
        athlete_id = row["canonical_athlete_id"].strip()
        canonical_name = row["canonical_name"].strip()
        event_id = row["event_id"].strip()
        event_date = row["date"].strip()
        if (
            sex not in {"M", "L"}
            or not athlete_id
            or not canonical_name
            or not event_id.startswith("flgr:")
        ):
            raise FlgrBridgeBacktestError(f"{path}:{line}: invalid reviewed endpoint")
        try:
            season = int(row["season"])
            date_key = date.fromisoformat(event_date)
        except ValueError as error:
            raise FlgrBridgeBacktestError(
                f"{path}:{line}: invalid event date or season"
            ) from error
        expected_season = date_key.year + 1 if date_key.month >= 7 else date_key.year
        if season != expected_season:
            raise FlgrBridgeBacktestError(
                f"{path}:{line}: date {event_date} belongs to season "
                f"{expected_season}, not {season}"
            )
        place = _integer(row["place"], default=-1)
        checksum = row["source_sha256"].strip().lower()
        if (
            place < 1
            or len(checksum) != 64
            or any(character not in "0123456789abcdef" for character in checksum)
        ):
            raise FlgrBridgeBacktestError(
                f"{path}:{line}: invalid place or source checksum"
            )
        evidence_shape.add(
            (sex, athlete_id, event_id, event_date, season, place, checksum)
        )
        rating = _number(row["Elo"], "bridge Elo")
        endpoint = BridgeEndpoint(
            sex=sex,
            canonical_athlete_id=athlete_id,
            canonical_name=canonical_name,
            event_id=event_id,
            event_date=event_date,
            season=season,
            rating=rating,
        )
        key = (sex, athlete_id)
        previous = endpoints.get(key)
        if previous is None or (date_key, event_id) > (
            date.fromisoformat(previous.event_date),
            previous.event_id,
        ):
            endpoints[key] = endpoint
    if not endpoints:
        raise FlgrBridgeBacktestError(f"{path} has no reviewed bridge athletes")
    return endpoints, frozenset(evidence_shape)


def _load_histories(
    men_chrono: Path, ladies_chrono: Path
) -> dict[str, list[dict[str, str]]]:
    histories = {}
    required = {
        "Date",
        "Event",
        "Season",
        "Race",
        "ID",
        "Skier",
        "Place",
        "Pelo",
    }
    for sex, path in (("M", men_chrono), ("L", ladies_chrono)):
        fields, rows = _read_csv(path)
        missing = sorted(required - set(fields))
        if missing:
            raise FlgrBridgeBacktestError(f"{path} is missing columns: {missing}")
        histories[sex] = rows
    return histories


def _first_return_race(
    endpoint: BridgeEndpoint, history: list[dict[str, str]]
) -> ReturnRace | None:
    returns = [
        row
        for row in history
        if row["ID"].strip() == endpoint.canonical_athlete_id
        and row["Event"].strip() != "Offseason"
        and row["Date"].strip() > endpoint.event_date
        and _integer(row.get("Place", ""), default=-1) >= 1
    ]
    if not returns:
        return None
    target = min(
        returns,
        key=lambda row: (
            row["Date"].strip(),
            _integer(row["Season"], default=-1),
            _integer(row["Race"], default=-1),
        ),
    )
    race_key = (
        target["Date"].strip(),
        target["Season"].strip(),
        target["Race"].strip(),
    )
    opponents = tuple(
        row
        for row in history
        if row["Event"].strip() != "Offseason"
        and (
            row["Date"].strip(),
            row["Season"].strip(),
            row["Race"].strip(),
        )
        == race_key
        and row["ID"].strip() != endpoint.canonical_athlete_id
        and _integer(row.get("Place", ""), default=-1) >= 1
        and (row.get("Pelo") or "").strip()
    )
    if not opponents:
        raise FlgrBridgeBacktestError(
            f"return race has no rated opponents for {endpoint.sex}/"
            f"{endpoint.canonical_athlete_id} on {target['Date']}"
        )
    return ReturnRace(target=target, opponents=opponents)


def _parse_legacy(path: Path | None) -> dict[tuple[str, str], list[dict[str, str]]]:
    if path is None:
        return {}
    fields, rows = _read_csv(path)
    required = {"Date", "Season", "ID", "Elo", "Sex"}
    missing = sorted(required - set(fields))
    if missing:
        raise FlgrBridgeBacktestError(f"{path} is missing columns: {missing}")
    indexed: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for line, row in enumerate(rows, start=2):
        sex = row["Sex"].strip()
        athlete_id = row["ID"].strip()
        if sex not in {"M", "L"} or not athlete_id:
            raise FlgrBridgeBacktestError(f"{path}:{line}: invalid legacy identity")
        try:
            date.fromisoformat(row["Date"].strip())
            int(row["Season"])
        except ValueError as error:
            raise FlgrBridgeBacktestError(
                f"{path}:{line}: invalid legacy date or season"
            ) from error
        _number(row["Elo"], "legacy Elo")
        indexed[(sex, athlete_id)].append(row)
    return indexed


def _latest_legacy_rating(
    rows: list[dict[str, str]], endpoint: BridgeEndpoint, return_row: dict[str, str]
) -> float | None:
    eligible = [
        row
        for row in rows
        if endpoint.event_date <= row["Date"].strip() < return_row["Date"].strip()
    ]
    if not eligible:
        return None
    latest = max(
        eligible,
        key=lambda row: (row["Date"].strip(), _integer(row["Season"], default=-1)),
    )
    boundaries = max(0, int(return_row["Season"]) - int(latest["Season"]))
    return regress_rating(_number(latest["Elo"], "legacy Elo"), boundaries)


def _probability(target_rating: float, opponent_rating: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((opponent_rating - target_rating) / ELO_DIFFERENCE))


def _actual_score(target_place: int, opponent_place: int) -> float:
    if target_place < opponent_place:
        return 1.0
    if target_place > opponent_place:
        return 0.0
    return 0.5


def _correct_direction(probability: float, actual: float) -> float:
    if actual == 0.5 or probability == 0.5:
        return 0.5
    return float((probability > 0.5) == (actual > 0.5))


def _method_metrics(rows: list[dict[str, object]]) -> dict[str, object]:
    if not rows:
        return {"comparisons": 0, "athletes": 0, "brier_score": None, "accuracy": None}
    return {
        "comparisons": len(rows),
        "athletes": len({(row["sex"], row["athlete_id"]) for row in rows}),
        "brier_score": round(
            sum(float(row["squared_error"]) for row in rows) / len(rows), 8
        ),
        "accuracy": round(
            sum(float(row["correct_direction"]) for row in rows) / len(rows), 8
        ),
    }


def backtest_flgr_bridge(
    *,
    bridge_ledgers: dict[str, Path],
    men_chrono: Path,
    ladies_chrono: Path,
    output_dir: Path,
    legacy_ledger: Path | None = None,
    minimum_athletes: int = 20,
) -> dict[str, object]:
    """Compare bridge candidates without mutating or promoting any input."""
    if not bridge_ledgers:
        raise FlgrBridgeBacktestError("at least one bridge ledger is required")
    if len(bridge_ledgers) != len(set(bridge_ledgers)) or any(
        not re.fullmatch(r"[A-Za-z0-9._-]+", label)
        or label.startswith(("stale_fis", "legacy_bridge"))
        for label in bridge_ledgers
    ):
        raise FlgrBridgeBacktestError("bridge labels must be unique and non-reserved")
    if minimum_athletes < 1:
        raise FlgrBridgeBacktestError("minimum_athletes must be positive")

    sources = [*bridge_ledgers.values(), men_chrono, ladies_chrono]
    if legacy_ledger:
        sources.append(legacy_ledger)
    output_dir = _validate_output_dir(output_dir, sources)
    source_hashes = {str(path): _sha256(path) for path in sources}
    parsed_candidates = {
        label: _parse_bridge_ledger(path) for label, path in bridge_ledgers.items()
    }
    candidates = {label: parsed[0] for label, parsed in parsed_candidates.items()}
    evidence_shapes = {label: parsed[1] for label, parsed in parsed_candidates.items()}
    reference_label = next(iter(candidates))
    reference = candidates[reference_label]
    reference_evidence = evidence_shapes[reference_label]
    reference_shapes = {
        key: (value.event_id, value.event_date, value.season, value.canonical_name)
        for key, value in reference.items()
    }
    for label, endpoints in candidates.items():
        if evidence_shapes[label] != reference_evidence:
            raise FlgrBridgeBacktestError(
                f"candidate {label!r} does not use identical bridge evidence"
            )
        shapes = {
            key: (value.event_id, value.event_date, value.season, value.canonical_name)
            for key, value in endpoints.items()
        }
        if shapes != reference_shapes:
            raise FlgrBridgeBacktestError(
                f"candidate {label!r} does not cover identical bridge endpoints"
            )

    histories = _load_histories(men_chrono, ladies_chrono)
    legacy = _parse_legacy(legacy_ledger)
    cases: list[dict[str, object]] = []
    athletes_without_return: list[dict[str, str]] = []
    for key, reference_endpoint in sorted(reference.items()):
        return_race = _first_return_race(reference_endpoint, histories[key[0]])
        if return_race is None:
            athletes_without_return.append(
                {
                    "sex": key[0],
                    "canonical_athlete_id": key[1],
                    "canonical_name": reference_endpoint.canonical_name,
                }
            )
            continue
        target = return_race.target
        return_season = int(target["Season"])
        ratings: dict[str, float] = {
            "stale_fis": _number(target["Pelo"], "return-race Pelo")
        }
        for label, endpoints in candidates.items():
            endpoint = endpoints[key]
            boundaries = max(0, return_season - endpoint.season)
            ratings[f"bridge:{label}"] = regress_rating(endpoint.rating, boundaries)
        legacy_rating = _latest_legacy_rating(
            legacy.get(key, []), reference_endpoint, target
        )
        if legacy_rating is not None:
            ratings["legacy_bridge"] = legacy_rating

        target_place = _integer(target["Place"])
        for opponent in return_race.opponents:
            opponent_place = _integer(opponent["Place"])
            opponent_rating = _number(opponent["Pelo"], "opponent Pelo")
            actual = _actual_score(target_place, opponent_place)
            for method, rating in ratings.items():
                predicted = _probability(rating, opponent_rating)
                cases.append(
                    {
                        "method": method,
                        "sex": key[0],
                        "athlete_id": key[1],
                        "athlete_name": reference_endpoint.canonical_name,
                        "bridge_last_event_id": reference_endpoint.event_id,
                        "bridge_last_date": reference_endpoint.event_date,
                        "return_date": target["Date"],
                        "return_season": return_season,
                        "return_race": target["Race"],
                        "return_place": target_place,
                        "opponent_id": opponent["ID"],
                        "opponent_place": opponent_place,
                        "target_rating": rating,
                        "opponent_pelo": opponent_rating,
                        "actual_score": actual,
                        "predicted_probability": predicted,
                        "squared_error": (predicted - actual) ** 2,
                        "correct_direction": _correct_direction(predicted, actual),
                    }
                )

    if not cases:
        raise FlgrBridgeBacktestError(
            "no reviewed athlete has a later accepted FIS result"
        )
    methods = sorted({str(row["method"]) for row in cases})
    metrics = {
        method: _method_metrics([row for row in cases if row["method"] == method])
        for method in methods
    }
    bridge_methods = [method for method in methods if method.startswith("bridge:")]
    best_bridge = min(
        bridge_methods,
        key=lambda method: (
            float(metrics[method]["brier_score"]),
            -float(metrics[method]["accuracy"]),
            method,
        ),
    )
    evaluated_athletes = int(metrics["stale_fis"]["athletes"])
    if evaluated_athletes < minimum_athletes:
        decision = "insufficient_evidence"
        recommended_candidate = None
    elif float(metrics[best_bridge]["brier_score"]) < float(
        metrics["stale_fis"]["brier_score"]
    ):
        decision = "candidate_improves_stale_fis"
        recommended_candidate = best_bridge.removeprefix("bridge:")
    else:
        decision = "bridge_not_better_than_stale_fis"
        recommended_candidate = None

    fields = list(cases[0])
    _atomic_text(output_dir / CASE_FILENAME, _csv_text(cases, fields))
    after_hashes = {str(path): _sha256(path) for path in sources}
    if after_hashes != source_hashes:
        raise FlgrBridgeBacktestError("source evidence changed during backtest")
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "source_sha256": source_hashes,
        "reference_candidate": reference_label,
        "evaluated_athletes": evaluated_athletes,
        "athletes_without_return": athletes_without_return,
        "minimum_athletes": minimum_athletes,
        "metrics": metrics,
        "best_bridge_candidate": best_bridge.removeprefix("bridge:"),
        "decision": decision,
        "recommended_candidate": recommended_candidate,
        "legacy_is_quarantined_benchmark_only": legacy_ledger is not None,
        "publication_eligible": False,
        "official_histories_modified": False,
        "status": "backtested_prediction_only",
    }
    _atomic_text(
        output_dir / REPORT_FILENAME,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def _bridge_argument(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("bridge ledger must use LABEL=PATH")
    label, raw_path = value.split("=", 1)
    if not label.strip() or not raw_path.strip():
        raise argparse.ArgumentTypeError("bridge ledger must use LABEL=PATH")
    return label.strip(), Path(raw_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bridge-ledger",
        action="append",
        required=True,
        type=_bridge_argument,
        metavar="LABEL=PATH",
    )
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--legacy-ledger", type=Path)
    parser.add_argument("--minimum-athletes", type=int, default=20)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ledgers = dict(args.bridge_ledger)
    if len(ledgers) != len(args.bridge_ledger):
        raise FlgrBridgeBacktestError("bridge ledger labels must be unique")
    report = backtest_flgr_bridge(
        bridge_ledgers=ledgers,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        legacy_ledger=args.legacy_ledger,
        minimum_athletes=args.minimum_athletes,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
