#!/usr/bin/env python3
"""Acquire and atomically apply finalized missing IBU Biathlon results.

The command is the hosted result-update entrypoint.  It audits the official
season, exits cleanly when nothing final is missing, captures immutable source
evidence, resolves identities, rebuilds every affected derived artifact, creates
a checksummed promotion bundle, and optionally applies that bundle.  The source
API is only contacted with ``--allow-network`` and publication is never performed.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from biathlon_additive_promotion import apply_promotion, build_promotion
from biathlon_additive_result_adapter import adapt_additive_results
from biathlon_final_results_capture import capture_final_results
from biathlon_rating_chrono_candidate import build_candidates
from biathlon_results_gap import audit_gaps
from ibu_source_contract import IBUClient, IBUContractError, discover_public_api_key


REPORT_SCHEMA = "nordicnumbers.biathlon-results-update/v1"


def season_id_for(day: date) -> str:
    """Return the IBU two-year season ID for a calendar date."""
    start = day.year if day.month >= 7 else day.year - 1
    return f"{start % 100:02d}{(start + 1) % 100:02d}"


def _atomic_report(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_update(
    *,
    client: IBUClient,
    season_id: str,
    schedule_path: Path,
    individual_dir: Path,
    relay_dir: Path,
    aliases_path: Path,
    engine_path: Path,
    aggregator_path: Path,
    output_root: Path,
    apply: bool,
    as_of: date | None = None,
) -> dict[str, Any]:
    output_root = output_root.expanduser().resolve()
    if output_root.exists():
        raise IBUContractError(f"results-update output already exists: {output_root}")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_root.name}.", dir=output_root.parent)
    )
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "status": "running",
        "season_id": season_id,
        "as_of": as_of.isoformat() if as_of else None,
        "stages": {},
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    try:
        gap = audit_gaps(
            client=client,
            season_id=season_id,
            schedule_path=schedule_path,
            individual_dir=individual_dir,
            relay_dir=relay_dir,
            output_dir=staging / "gap",
            as_of=as_of,
        )
        report["stages"]["gap_audit"] = gap["status"]
        schedule_missing = gap["summary"]["schedule_missing"]
        missing = gap["summary"]["final_results_missing"]
        if schedule_missing:
            report.update(
                status="blocked",
                reason="official final results require schedule promotion first",
                schedule_missing=schedule_missing,
                final_results_missing=missing,
            )
            _atomic_report(staging / "report.json", report)
            staging.replace(output_root)
            return report
        if not missing:
            report.update(
                status="no_op",
                reason="no finalized missing IBU results",
                schedule_missing=0,
                final_results_missing=0,
            )
            _atomic_report(staging / "report.json", report)
            staging.replace(output_root)
            return report

        capture = capture_final_results(
            client=client,
            evidence_dir=staging / "gap",
            schedule_path=schedule_path,
            individual_dir=individual_dir,
            relay_dir=relay_dir,
            output_dir=staging / "capture",
            aliases_path=aliases_path,
        )
        report["stages"]["capture_final_results"] = capture["status"]
        adapter = adapt_additive_results(
            capture_dir=staging / "capture",
            gap_evidence_dir=staging / "gap",
            individual_dir=individual_dir,
            relay_dir=relay_dir,
            schedule_path=schedule_path,
            output_dir=staging / "adapter",
            aliases_path=aliases_path,
        )
        report["stages"]["adapt_additive_results"] = adapter["status"]
        if adapter["status"] != "passed":
            raise IBUContractError("additive result adaptation did not pass")
        derived = build_candidates(
            candidate_dir=staging / "adapter",
            individual_dir=individual_dir,
            relay_dir=relay_dir,
            engine_path=engine_path,
            aggregator_path=aggregator_path,
            output_dir=staging / "derived",
        )
        report["stages"]["rebuild_derived_artifacts"] = derived["status"]
        if derived["status"] != "passed":
            raise IBUContractError("full additive derived rehearsal did not pass")
        promotion = build_promotion(
            adapter_dir=staging / "adapter",
            derived_dir=staging / "derived",
            individual_dir=individual_dir,
            relay_dir=relay_dir,
            aliases_path=aliases_path,
            output_dir=staging / "promotion_bundle",
        )
        report["stages"]["build_promotion_bundle"] = promotion["status"]
        report["race_ids"] = promotion["race_ids"]
        report["target_count"] = promotion["target_count"]
        if apply:
            receipt = apply_promotion(
                bundle_dir=staging / "promotion_bundle",
                individual_dir=individual_dir,
                relay_dir=relay_dir,
                aliases_path=aliases_path,
            )
            report["stages"]["apply_promotion"] = receipt["status"]
            report["application"] = {
                "status": receipt["status"],
                "performed": receipt["performed"],
            }
            report["status"] = "applied"
        else:
            report["status"] = "candidate"
        _atomic_report(staging / "report.json", report)
        staging.replace(output_root)
        return report
    except Exception:
        # Retain all completed evidence for CI diagnosis, including a best-effort
        # failure summary, while leaving accepted data protected by the apply
        # transaction's rollback guarantee.
        report["status"] = "failed"
        _atomic_report(staging / "report.json", report)
        if not output_root.exists():
            staging.replace(output_root)
        else:
            shutil.rmtree(staging, ignore_errors=True)
        raise


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--season-id")
    command.add_argument("--as-of", default=date.today().isoformat())
    command.add_argument("--schedule", dest="schedule_path", type=Path, default=here / "excel365/races.csv")
    command.add_argument("--individual-dir", type=Path, default=here / "excel365")
    command.add_argument("--relay-dir", type=Path, default=here / "relay/excel365")
    command.add_argument("--aliases", dest="aliases_path", type=Path, default=here / "ibu_identity_aliases.csv")
    command.add_argument("--engine", dest="engine_path", type=Path, default=here / "elo.py")
    command.add_argument("--aggregator", dest="aggregator_path", type=Path, default=here / "relay/relay_chrono.py")
    command.add_argument("--output-root", type=Path, required=True)
    command.add_argument("--apply", action="store_true")
    command.add_argument("--allow-network", action="store_true")
    return command


def main() -> int:
    args = parser().parse_args()
    if not args.allow_network:
        print(json.dumps({"status": "failed", "error": "--allow-network is required"}, indent=2))
        return 1
    try:
        day = datetime.strptime(args.as_of, "%Y-%m-%d").date()
        season_id = args.season_id or season_id_for(day)
        report = run_update(
            client=IBUClient(api_key=discover_public_api_key()),
            season_id=season_id,
            schedule_path=args.schedule_path,
            individual_dir=args.individual_dir,
            relay_dir=args.relay_dir,
            aliases_path=args.aliases_path,
            engine_path=args.engine_path,
            aggregator_path=args.aggregator_path,
            output_root=args.output_root,
            apply=args.apply,
            as_of=day,
        )
    except (IBUContractError, ValueError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] in {"no_op", "candidate", "applied"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
