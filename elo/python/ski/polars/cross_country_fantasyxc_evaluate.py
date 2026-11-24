#!/usr/bin/env python3
"""Evaluate cumulative FantasyXC roster backtests against a guarded policy."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from cross_country_fantasyxc_backtest import REPORT_SCHEMA as BACKTEST_SCHEMA

POLICY_SCHEMA = "nordicnumbers.cross-country-announced-roster-automation-policy/v1"
EVALUATION_SCHEMA = "nordicnumbers.cross-country-announced-roster-evaluation/v1"
INTEGER_METRICS = (
    "decision_count",
    "correct_decisions",
    "confirmed_decisions",
    "confirmed_correct",
    "excluded_decisions",
    "excluded_correct",
    "official_starter_rows",
    "official_starter_rows_covered",
    "false_confirmations",
    "false_exclusions",
)


class FantasyXCEvaluationError(RuntimeError):
    """Cumulative backtest evidence or its policy is invalid."""


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _read_json(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        content = path.read_bytes()
        value = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FantasyXCEvaluationError(f"cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise FantasyXCEvaluationError(f"{label} must be a JSON object")
    return value, content


def _positive_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise FantasyXCEvaluationError(f"{label} must be a positive integer")
    return value


def _rate_threshold(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FantasyXCEvaluationError(f"{label} must be numeric")
    result = float(value)
    if not 0 <= result <= 1:
        raise FantasyXCEvaluationError(f"{label} must be between 0 and 1")
    return result


def _load_policy(path: Path) -> tuple[dict[str, Any], bytes]:
    policy, content = _read_json(path, label="automation policy")
    if policy.get("schema_version") != POLICY_SCHEMA:
        raise FantasyXCEvaluationError("unsupported automation policy schema")
    if policy.get("status") != "collecting_evidence":
        raise FantasyXCEvaluationError("automation policy must remain collecting_evidence")
    if not isinstance(policy.get("automatic_promotion_enabled"), bool):
        raise FantasyXCEvaluationError("automatic_promotion_enabled must be boolean")
    if not str(policy.get("resolver_version", "")):
        raise FantasyXCEvaluationError("automation policy requires a resolver version")
    thresholds = policy.get("thresholds")
    if not isinstance(thresholds, dict):
        raise FantasyXCEvaluationError("automation policy requires thresholds")
    for name in (
        "minimum_completed_weekends",
        "minimum_decisions",
        "minimum_confirmed_decisions",
        "minimum_excluded_decisions",
    ):
        _positive_integer(thresholds.get(name), label=name)
    for name in ("minimum_confirmed_precision", "minimum_excluded_accuracy"):
        _rate_threshold(thresholds.get(name), label=name)
    return policy, content


def _load_backtest(
    path: Path, *, resolver_version: str
) -> tuple[dict[str, Any], bytes]:
    report, content = _read_json(path, label=f"backtest report {path}")
    if report.get("schema_version") != BACKTEST_SCHEMA:
        raise FantasyXCEvaluationError(f"unsupported backtest schema: {path}")
    if report.get("status") != "research_backtest":
        raise FantasyXCEvaluationError(f"backtest must remain research-only: {path}")
    if report.get("publication") != {"status": "disabled"}:
        raise FantasyXCEvaluationError(f"backtest publication must be disabled: {path}")
    if report.get("resolver_version") != resolver_version:
        raise FantasyXCEvaluationError(f"mixed resolver versions are forbidden: {path}")
    logical_date = str(report.get("logical_date", ""))
    if not logical_date:
        raise FantasyXCEvaluationError(f"backtest logical date is missing: {path}")
    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        raise FantasyXCEvaluationError(f"backtest metrics are missing: {path}")
    for name in INTEGER_METRICS:
        value = metrics.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise FantasyXCEvaluationError(f"invalid {name} in {path}")
    if (
        metrics["confirmed_decisions"] + metrics["excluded_decisions"]
        != metrics["decision_count"]
    ):
        raise FantasyXCEvaluationError(f"decision counts do not balance: {path}")
    if metrics["correct_decisions"] != (
        metrics["confirmed_correct"] + metrics["excluded_correct"]
    ):
        raise FantasyXCEvaluationError(f"correct counts do not balance: {path}")
    if metrics["false_confirmations"] != (
        metrics["confirmed_decisions"] - metrics["confirmed_correct"]
    ):
        raise FantasyXCEvaluationError(f"false confirmation counts do not balance: {path}")
    if metrics["false_exclusions"] != (
        metrics["excluded_decisions"] - metrics["excluded_correct"]
    ):
        raise FantasyXCEvaluationError(f"false exclusion counts do not balance: {path}")
    if metrics["official_starter_rows_covered"] > metrics["official_starter_rows"]:
        raise FantasyXCEvaluationError(f"official coverage is invalid: {path}")
    return report, content


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def evaluate_backtests(
    *,
    report_paths: tuple[Path, ...],
    policy_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    """Aggregate distinct completed weekends and evaluate every policy threshold."""

    if not report_paths:
        raise FantasyXCEvaluationError("at least one backtest report is required")
    output_path = output_path.expanduser().resolve()
    protected = {
        policy_path.expanduser().resolve(),
        *(path.expanduser().resolve() for path in report_paths),
    }
    if output_path in protected:
        raise FantasyXCEvaluationError("evaluation output cannot overwrite an input")
    policy, policy_content = _load_policy(policy_path)
    resolver_version = policy["resolver_version"]
    reports: list[dict[str, Any]] = []
    input_reports: list[dict[str, str]] = []
    logical_dates: set[str] = set()
    for path in report_paths:
        report, content = _load_backtest(path, resolver_version=resolver_version)
        logical_date = report["logical_date"]
        if logical_date in logical_dates:
            raise FantasyXCEvaluationError(
                f"duplicate completed weekend: {logical_date}"
            )
        logical_dates.add(logical_date)
        reports.append(report)
        input_reports.append(
            {"path": str(path), "sha256": _sha256(content), "logical_date": logical_date}
        )

    totals = {
        name: sum(report["metrics"][name] for report in reports)
        for name in INTEGER_METRICS
    }
    totals.update(
        {
            "completed_weekends": len(reports),
            "confirmed_precision": _rate(
                totals["confirmed_correct"], totals["confirmed_decisions"]
            ),
            "excluded_accuracy": _rate(
                totals["excluded_correct"], totals["excluded_decisions"]
            ),
            "decision_accuracy": _rate(
                totals["correct_decisions"], totals["decision_count"]
            ),
            "official_field_coverage": _rate(
                totals["official_starter_rows_covered"],
                totals["official_starter_rows"],
            ),
            "unresolved_exception_count": sum(
                int(report.get("unresolved_exception_count", 0)) for report in reports
            ),
        }
    )
    thresholds = policy["thresholds"]
    checks = {
        "minimum_completed_weekends": totals["completed_weekends"]
        >= thresholds["minimum_completed_weekends"],
        "minimum_decisions": totals["decision_count"]
        >= thresholds["minimum_decisions"],
        "minimum_confirmed_decisions": totals["confirmed_decisions"]
        >= thresholds["minimum_confirmed_decisions"],
        "minimum_excluded_decisions": totals["excluded_decisions"]
        >= thresholds["minimum_excluded_decisions"],
        "minimum_confirmed_precision": (
            totals["confirmed_precision"] is not None
            and totals["confirmed_precision"]
            >= thresholds["minimum_confirmed_precision"]
        ),
        "minimum_excluded_accuracy": (
            totals["excluded_accuracy"] is not None
            and totals["excluded_accuracy"]
            >= thresholds["minimum_excluded_accuracy"]
        ),
    }
    gate_passed = all(checks.values())
    enabled = policy["automatic_promotion_enabled"]
    evaluation = {
        "schema_version": EVALUATION_SCHEMA,
        "status": "research_evaluation",
        "publication": {"status": "disabled"},
        "resolver_version": resolver_version,
        "evidence_gate_passed": gate_passed,
        "automatic_promotion_enabled": enabled,
        "automatic_promotion_active": gate_passed and enabled,
        "recommendation": (
            "eligible_for_operator_approval" if gate_passed else "insufficient_evidence"
        ),
        "threshold_checks": checks,
        "thresholds": thresholds,
        "metrics": totals,
        "inputs": {
            "policy": {"path": str(policy_path), "sha256": _sha256(policy_content)},
            "backtests": sorted(input_reports, key=lambda row: row["logical_date"]),
        },
    }
    _atomic_write(output_path, _json_bytes(evaluation))
    return evaluation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, action="append", type=Path)
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    evaluation = evaluate_backtests(
        report_paths=tuple(args.report),
        policy_path=args.policy,
        output_path=args.output,
    )
    print(json.dumps(evaluation, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
