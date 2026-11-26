#!/usr/bin/env python3
"""Build and atomically apply a verified additive Biathlon promotion bundle.

The build command combines policy-specific additive result rows with accepted
histories and the fully rebuilt rating/chrono artifacts.  It writes an isolated,
checksummed bundle only.  The apply command revalidates every receipt and the
accepted-tree baseline, then replaces the complete target set with rollback on
any error.  Publication is outside this module and remains disabled.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from biathlon_additive_result_adapter import REPORT_SCHEMA as ADAPTER_SCHEMA
from biathlon_elo_parity import _read_csv, splice_candidates
from biathlon_rating_chrono_candidate import REPORT_SCHEMA as DERIVED_SCHEMA
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError


REPORT_SCHEMA = "nordicnumbers.biathlon-additive-promotion/v1"
HISTORY_NAMES = ("men_scrape_update.csv", "ladies_scrape_update.csv")
RATING_NAMES = tuple(
    f"{sex}{suffix}.csv"
    for sex in ("M", "L")
    for suffix in ("", "_Individual", "_Sprint", "_Pursuit", "_Mass_Start")
)
CHRONO_NAMES = ("men_chrono.csv", "ladies_chrono.csv")
ELEVATION_NAMES = (
    "men_chrono_elevation.csv",
    "ladies_chrono_elevation.csv",
)


class PromotionError(IBUContractError):
    """An additive candidate cannot safely become accepted data."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PromotionError(f"invalid {label}: {path}") from error
    if not isinstance(value, dict):
        raise PromotionError(f"{label} must be an object: {path}")
    return value


def _csv_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise PromotionError(f"promotion history unexpectedly has no rows: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(rows[0]), extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _trees(individual_dir: Path, relay_dir: Path) -> dict[str, dict[str, Any]]:
    return {
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }


def _content(receipt: dict[str, Any]) -> dict[str, Any]:
    return {key: receipt.get(key) for key in ("file_count", "total_bytes", "sha256")}


def _same_trees(observed: dict[str, Any], expected: dict[str, Any]) -> bool:
    return all(
        isinstance(expected.get(policy), dict)
        and _content(observed[policy]) == _content(expected[policy])
        for policy in ("individual", "relay")
    )


def _receipt(path: Path, root: Path) -> dict[str, Any]:
    return {
        "file": path.relative_to(root).as_posix(),
        "rows": _csv_rows(path),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _validate_adapter(adapter_dir: Path, current: dict[str, Any]) -> dict[str, Any]:
    report = _load_json(adapter_dir / "adapter_report.json", "adapter report")
    if (
        report.get("schema_version") != ADAPTER_SCHEMA
        or report.get("status") != "passed"
        or report.get("ready_for_derived_rehearsal") is not True
        or report.get("accepted_data_mutated") is not False
        or report.get("application") != {"status": "disabled", "performed": False}
        or report.get("publication") != {"status": "disabled"}
        or not report.get("race_ids")
    ):
        raise PromotionError("adapter report is not a passed additive candidate")
    fingerprints = report.get("accepted_fingerprints", {})
    if fingerprints.get("before") != fingerprints.get("after") or not _same_trees(
        current, fingerprints.get("after", {})
    ):
        raise PromotionError("accepted histories drifted after additive adaptation")
    for relative, receipt in report.get("candidate_files", {}).items():
        path = adapter_dir / relative
        if (
            not path.is_file()
            or receipt.get("rows") != _csv_rows(path)
            or receipt.get("sha256") != _sha256(path)
        ):
            raise PromotionError(f"adapter candidate drifted: {relative}")
    return report


def _validate_derived(
    derived_dir: Path, adapter_dir: Path, current: dict[str, Any]
) -> dict[str, Any]:
    report = _load_json(
        derived_dir / "rating_chrono_candidate_report.json", "derived report"
    )
    if (
        report.get("schema_version") != DERIVED_SCHEMA
        or report.get("status") != "passed"
        or report.get("ready_for_promotion_bundle") is not True
        or report.get("accepted_data_mutated") is not False
        or report.get("source_candidates_mutated") is not False
        or report.get("application") != {"status": "disabled", "performed": False}
        or report.get("publication") != {"status": "disabled"}
    ):
        raise PromotionError("derived report is not a passed immutable candidate")
    fingerprints = report.get("accepted_fingerprints", {})
    if fingerprints.get("before") != fingerprints.get("after") or not _same_trees(
        current, fingerprints.get("after", {})
    ):
        raise PromotionError("accepted histories drifted after derived rehearsal")
    source = report.get("source_candidate_fingerprints", {})
    actual_source = fingerprint_tree(adapter_dir)
    if source.get("before") != source.get("after") or _content(
        actual_source
    ) != _content(source.get("after", {})):
        raise PromotionError("derived rehearsal does not match the adapter bundle")
    return report


def build_promotion(
    *,
    adapter_dir: Path,
    derived_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
    aliases_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    adapter_dir = adapter_dir.expanduser().resolve()
    derived_dir = derived_dir.expanduser().resolve()
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    aliases_path = aliases_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise PromotionError(f"promotion output already exists: {output_dir}")
    for protected in (adapter_dir, derived_dir, individual_dir, relay_dir, aliases_path):
        if output_dir == protected or protected in output_dir.parents:
            raise PromotionError("promotion output must be outside source and accepted data")

    current = _trees(individual_dir, relay_dir)
    adapter = _validate_adapter(adapter_dir, current)
    aliases = adapter.get("aliases", {})
    if aliases.get("source_sha256") != _sha256(aliases_path):
        raise PromotionError("IBU identity aliases drifted after adaptation")
    derived = _validate_derived(derived_dir, adapter_dir, current)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        promotion = staging / "promotion"
        roots = {"individual": individual_dir, "relay": relay_dir}
        for policy, accepted_root in roots.items():
            for filename in HISTORY_NAMES:
                accepted = _read_csv(accepted_root / filename)
                candidate = _read_csv(adapter_dir / policy / filename.replace("scrape_update", "results_candidate"))
                if candidate:
                    merged, parity = splice_candidates(accepted, candidate)
                    if parity.get("status") != "passed" or parity.get("mode") != "additive":
                        raise PromotionError(
                            f"{policy}/{filename} is not a purely additive result set"
                        )
                else:
                    merged = accepted
                _write_rows(promotion / policy / filename, merged)

            for filename in RATING_NAMES:
                source = derived_dir / "full_ratings" / policy / filename
                if not source.is_file():
                    raise PromotionError(f"missing derived rating: {source}")
                destination = promotion / policy / filename
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
            for filename in CHRONO_NAMES:
                shutil.copy2(
                    derived_dir / "full_chronos" / policy / filename,
                    promotion / policy / filename,
                )
            for filename in ELEVATION_NAMES:
                shutil.copy2(
                    derived_dir / "full_elevation_chronos" / policy / filename,
                    promotion / policy / filename,
                )

        team_receipts = derived.get("relay_team_artifacts", {})
        for comparison in team_receipts.values():
            receipt = comparison.get("candidate") if isinstance(comparison, dict) else None
            if not isinstance(receipt, dict):
                raise PromotionError("derived relay-team receipt is malformed")
            relative = receipt.get("file")
            source = derived_dir / str(relative)
            if (
                not source.is_file()
                or receipt.get("rows") != _csv_rows(source)
                or receipt.get("sha256") != _sha256(source)
            ):
                raise PromotionError(f"derived relay-team artifact drifted: {relative}")
            shutil.copy2(source, promotion / "relay" / source.name)

        alias_candidate = adapter_dir / "identity_aliases_candidate.csv"
        if not alias_candidate.is_file():
            raise PromotionError("adapter identity-alias candidate is missing")
        alias_target = promotion / "identity" / aliases_path.name
        alias_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(alias_candidate, alias_target)

        artifacts = [
            _receipt(path, staging)
            for path in sorted(promotion.rglob("*.csv"))
        ]
        report: dict[str, Any] = {
            "schema_version": REPORT_SCHEMA,
            "status": "promotion_candidate",
            "season_id": adapter["season_id"],
            "race_ids": adapter["race_ids"],
            "accepted_before": current,
            "aliases_before": {
                "file": str(aliases_path),
                "sha256": _sha256(aliases_path),
            },
            "artifacts": artifacts,
            "target_count": len(artifacts),
            "apply_ready": True,
            "application": {"status": "disabled", "performed": False},
            "publication": {"status": "disabled"},
        }
        (staging / "promotion_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        staging.replace(output_dir)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def apply_promotion(
    *,
    bundle_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
    aliases_path: Path,
) -> dict[str, Any]:
    bundle_dir = bundle_dir.expanduser().resolve()
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    aliases_path = aliases_path.expanduser().resolve()
    report = _load_json(bundle_dir / "promotion_report.json", "promotion report")
    if (
        report.get("schema_version") != REPORT_SCHEMA
        or report.get("status") != "promotion_candidate"
        or report.get("apply_ready") is not True
        or report.get("application") != {"status": "disabled", "performed": False}
        or report.get("publication") != {"status": "disabled"}
    ):
        raise PromotionError("bundle is not an apply-ready additive promotion")
    current = _trees(individual_dir, relay_dir)
    if not _same_trees(current, report.get("accepted_before", {})):
        raise PromotionError("accepted Biathlon data drifted after promotion review")
    if report.get("aliases_before", {}).get("sha256") != _sha256(aliases_path):
        raise PromotionError("IBU identity aliases drifted after promotion review")

    expected = {item["file"]: item for item in report.get("artifacts", [])}
    actual = {
        path.relative_to(bundle_dir).as_posix(): path
        for path in (bundle_dir / "promotion").rglob("*.csv")
    }
    if set(actual) != set(expected):
        raise PromotionError("promotion inventory does not match its report")
    for relative, path in actual.items():
        receipt = expected[relative]
        if (
            receipt.get("rows") != _csv_rows(path)
            or receipt.get("bytes") != path.stat().st_size
            or receipt.get("sha256") != _sha256(path)
        ):
            raise PromotionError(f"promotion artifact drifted: {relative}")

    roots = {"individual": individual_dir, "relay": relay_dir}
    destinations: dict[str, Path] = {}
    for relative in actual:
        _, policy, filename = Path(relative).parts
        destination = aliases_path if policy == "identity" else roots[policy] / filename
        if not destination.is_file():
            raise PromotionError(f"accepted promotion target is missing: {policy}/{filename}")
        destinations[relative] = destination

    backup = Path(tempfile.mkdtemp(prefix="biathlon-additive-backup."))
    staged: dict[str, Path] = {}
    replaced: list[str] = []
    try:
        for relative, destination in destinations.items():
            saved = backup / relative
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(destination, saved)
            with tempfile.NamedTemporaryFile(
                dir=destination.parent,
                prefix=f".{destination.name}.additive.",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
            shutil.copyfile(actual[relative], temporary)
            staged[relative] = temporary
        for relative, destination in destinations.items():
            os.replace(staged.pop(relative), destination)
            replaced.append(relative)
        for relative, destination in destinations.items():
            if _sha256(destination) != expected[relative]["sha256"]:
                raise PromotionError(f"post-apply verification failed: {relative}")
    except Exception as error:
        for relative in replaced:
            shutil.copy2(backup / relative, destinations[relative])
        raise PromotionError(f"additive apply failed and was rolled back: {error}") from error
    finally:
        for path in staged.values():
            path.unlink(missing_ok=True)
        shutil.rmtree(backup, ignore_errors=True)
    return {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "performed": True,
        "race_ids": report["race_ids"],
        "targets": len(destinations),
        "publication": {"status": "disabled"},
    }


def parser() -> argparse.ArgumentParser:
    here = Path(__file__).resolve().parent
    command = argparse.ArgumentParser(description=__doc__)
    subcommands = command.add_subparsers(dest="command", required=True)
    build = subcommands.add_parser("build")
    build.add_argument("--adapter-dir", type=Path, required=True)
    build.add_argument("--derived-dir", type=Path, required=True)
    build.add_argument("--individual-dir", type=Path, default=here / "excel365")
    build.add_argument("--relay-dir", type=Path, default=here / "relay/excel365")
    build.add_argument("--aliases", dest="aliases_path", type=Path, default=here / "ibu_identity_aliases.csv")
    build.add_argument("--output-dir", type=Path, required=True)
    apply = subcommands.add_parser("apply")
    apply.add_argument("--bundle-dir", type=Path, required=True)
    apply.add_argument("--individual-dir", type=Path, default=here / "excel365")
    apply.add_argument("--relay-dir", type=Path, default=here / "relay/excel365")
    apply.add_argument("--aliases", dest="aliases_path", type=Path, default=here / "ibu_identity_aliases.csv")
    return command


def main() -> int:
    args = vars(parser().parse_args())
    command = args.pop("command")
    try:
        result = build_promotion(**args) if command == "build" else apply_promotion(**args)
    except IBUContractError as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
