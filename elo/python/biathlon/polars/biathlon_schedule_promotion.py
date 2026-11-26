#!/usr/bin/env python3
"""Review and explicitly apply a checksummed Biathlon season schedule."""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

CANDIDATE_SCHEMA = "nordicnumbers.biathlon-schedule-candidate/v1"
REVIEW_SCHEMA = "nordicnumbers.biathlon-schedule-review/v1"
RECEIPT_SCHEMA = "nordicnumbers.biathlon-schedule-application/v1"
RACE_FIELDS = [
    "Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
    "Race",
]
WEEKEND_FIELDS = [
    "Date",
    "Race_Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
]
COMMON_FIELDS = [
    "Startlist",
    "Sex",
    "City",
    "Country",
    "Distance",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
]
RACE_ID = re.compile(r"BT\d{4}SWRL(?:CP\d{2}|CH__|OG__)[A-Z0-9_]{4}")


class SchedulePromotionError(RuntimeError):
    """The candidate cannot be reviewed or applied safely."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SchedulePromotionError(f"invalid JSON input: {path}") from error
    if not isinstance(value, dict):
        raise SchedulePromotionError(f"JSON input is not an object: {path}")
    return value


def _read_csv(path: Path, fields: list[str]) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != fields:
                raise SchedulePromotionError(f"unexpected CSV schema: {path}")
            rows = list(reader)
    except (OSError, csv.Error) as error:
        raise SchedulePromotionError(f"invalid CSV artifact: {path}") from error
    if not rows:
        raise SchedulePromotionError(f"CSV artifact has no rows: {path}")
    return rows


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _parse_date(value: str, field: str) -> date:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as error:
        raise SchedulePromotionError(f"invalid {field}: {value!r}") from error


def _race_id(startlist: str) -> str:
    parsed = urlparse(startlist)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "www.biathlonworld.com"
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/startlist/")
    ):
        raise SchedulePromotionError(f"invalid official startlist URL: {startlist}")
    race_id = parsed.path.removeprefix("/startlist/")
    if not RACE_ID.fullmatch(race_id):
        raise SchedulePromotionError(f"invalid official RaceId: {race_id}")
    return race_id


def _validate_periods(rows: list[dict[str, str]]) -> None:
    parsed = [
        {
            "day": _parse_date(row["Date"], "race date"),
            "sex": row["Sex"],
            "period": int(row["Period"]),
            "championship": int(row["Championship"]),
            "race_id": _race_id(row["Startlist"]),
        }
        for row in rows
    ]
    championship_days = [row["day"] for row in parsed if row["championship"] == 1]
    championship_end = max(championship_days) if championship_days else None
    for row in parsed:
        if row["championship"] == 1 and row["period"] != 4:
            raise SchedulePromotionError("championship race is not in period 4")
        if (
            row["championship"] == 0
            and championship_end
            and row["day"] > championship_end
            and row["period"] != 5
        ):
            raise SchedulePromotionError("post-championship race is not in period 5")
        if (
            row["championship"] == 0
            and (not championship_end or row["day"] <= championship_end)
            and row["period"] not in {1, 2, 3}
        ):
            raise SchedulePromotionError("pre-championship race has invalid period")
    for sex in ("M", "L"):
        pre = [
            row
            for row in parsed
            if row["sex"] == sex
            and row["championship"] == 0
            and (not championship_end or row["day"] <= championship_end)
        ]
        periods = [row["period"] for row in pre]
        if periods != sorted(periods):
            raise SchedulePromotionError(f"{sex} pre-championship periods regress")
        counts = Counter(periods)
        expected_periods = set(range(1, min(3, len(pre)) + 1))
        if set(counts) != expected_periods or (
            counts and max(counts.values()) - min(counts.values()) > 1
        ):
            raise SchedulePromotionError(
                f"{sex} pre-championship periods are unbalanced"
            )


def _validate_candidates(
    races_path: Path, weekends_path: Path, season_id: str
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    if not re.fullmatch(r"\d{4}", season_id):
        raise SchedulePromotionError(f"invalid season ID: {season_id!r}")
    end_year = 2000 + int(season_id[2:])
    races = _read_csv(races_path, RACE_FIELDS)
    weekends = _read_csv(weekends_path, WEEKEND_FIELDS)
    if len(races) != len(weekends):
        raise SchedulePromotionError("races and weekends candidate counts differ")

    race_by_id: dict[str, dict[str, str]] = {}
    weekend_by_id: dict[str, dict[str, str]] = {}
    identities: set[tuple[str, str, str]] = set()
    for row in races:
        race_id = _race_id(row["Startlist"])
        day = _parse_date(row["Date"], "race date")
        identity = (row["Date"], row["RaceType"], row["Sex"])
        if race_id in race_by_id or identity in identities:
            raise SchedulePromotionError("duplicate race candidate identity")
        race_by_id[race_id] = row
        identities.add(identity)
        if day.year not in {end_year - 1, end_year}:
            raise SchedulePromotionError("race candidate is outside its season")
        if row["Sex"] not in {"M", "L", "Mixed"}:
            raise SchedulePromotionError(f"invalid race sex: {row['Sex']}")
        if row["Distance"] != "N/A" and not re.fullmatch(
            r"\d+(?:\.\d+)?", row["Distance"]
        ):
            raise SchedulePromotionError(f"invalid race distance: {row['Distance']}")
        if not row["City"] or not row["Country"]:
            raise SchedulePromotionError("candidate venue metadata is incomplete")
        if not re.fullmatch(r"\d+(?:\.\d+)?", row["Elevation"]):
            raise SchedulePromotionError("candidate elevation is invalid")
        if row["Championship"] not in {"0", "1"}:
            raise SchedulePromotionError("candidate championship flag is invalid")
        if not row["Race"].isdigit() or int(row["Race"]) < 1:
            raise SchedulePromotionError("candidate race number is invalid")

    for row in weekends:
        race_id = _race_id(row["Startlist"])
        event_day = _parse_date(row["Date"], "weekend date")
        race_day = _parse_date(row["Race_Date"], "weekend race date")
        if event_day > race_day or race_id in weekend_by_id:
            raise SchedulePromotionError("invalid or duplicate weekend candidate")
        weekend_by_id[race_id] = row
    if set(race_by_id) != set(weekend_by_id):
        raise SchedulePromotionError("races and weekends RaceIds differ")
    for race_id, race in race_by_id.items():
        weekend = weekend_by_id[race_id]
        if race["Date"] != weekend["Race_Date"] or any(
            race[field] != weekend[field] for field in COMMON_FIELDS
        ):
            raise SchedulePromotionError(f"candidate layouts disagree for {race_id}")

    by_event: dict[str, list[dict[str, str]]] = {}
    for race_id, row in weekend_by_id.items():
        by_event.setdefault(race_id[:-4], []).append(row)
    for event_id, event_rows in by_event.items():
        first = min(
            _parse_date(row["Race_Date"], "weekend race date") for row in event_rows
        )
        if any(_parse_date(row["Date"], "weekend date") != first for row in event_rows):
            raise SchedulePromotionError(f"weekend boundary differs within {event_id}")

    for sex in ("M", "L", "Mixed"):
        numbers = [int(row["Race"]) for row in races if row["Sex"] == sex]
        if numbers != list(range(1, len(numbers) + 1)):
            raise SchedulePromotionError(f"{sex} race numbering is not sequential")
    dates = [_parse_date(row["Date"], "race date") for row in races]
    if dates != sorted(dates):
        raise SchedulePromotionError("race candidates are not date-sorted")
    _validate_periods(races)
    return races, weekends


def _candidate_files(report: dict[str, Any], candidate_dir: Path) -> tuple[Path, Path]:
    season_id = str(report.get("season_id", ""))
    if not re.fullmatch(r"\d{4}", season_id):
        raise SchedulePromotionError("candidate season ID is invalid")
    year = 2000 + int(season_id[2:])
    names = (f"races{year}_candidate.csv", "weekends_candidate.csv")
    inventory = report.get("candidate_files")
    if not isinstance(inventory, dict) or set(inventory) != set(names):
        raise SchedulePromotionError("candidate file inventory is incomplete")
    actual = {
        path.name
        for path in candidate_dir.iterdir()
        if path.is_file() and path.name != "candidate_report.json"
    }
    if actual != set(names):
        raise SchedulePromotionError("candidate directory contains unexpected files")
    paths = tuple(candidate_dir / name for name in names)
    for name, path in zip(names, paths, strict=True):
        receipt = inventory[name]
        if (
            not isinstance(receipt, dict)
            or receipt.get("sha256") != _sha256(path)
            or not isinstance(receipt.get("rows"), int)
        ):
            raise SchedulePromotionError(f"candidate artifact drifted: {name}")
    races, weekends = _validate_candidates(paths[0], paths[1], season_id)
    if (
        len(races) != inventory[names[0]]["rows"]
        or len(weekends) != inventory[names[1]]["rows"]
    ):
        raise SchedulePromotionError("candidate row-count receipt drifted")
    return paths  # type: ignore[return-value]


def _accepted_baseline(accepted_dir: Path, year: int) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name in ("races.csv", f"races{year}.csv", "weekends.csv"):
        path = accepted_dir / name
        result[name] = (
            {"exists": True, "sha256": _sha256(path), "bytes": path.stat().st_size}
            if path.is_file()
            else {"exists": False}
        )
    if not result["races.csv"]["exists"] or not result["weekends.csv"]["exists"]:
        raise SchedulePromotionError("accepted current schedule is incomplete")
    return result


def _safe_new_directory(
    path: Path, accepted_dir: Path, label: str, *other_protected: Path
) -> Path:
    resolved = path.expanduser().resolve()
    accepted_dir = accepted_dir.expanduser().resolve()
    if resolved.exists():
        raise SchedulePromotionError(f"{label} already exists: {resolved}")
    for protected in (accepted_dir, *other_protected):
        protected = protected.expanduser().resolve()
        if resolved == protected or protected in resolved.parents:
            raise SchedulePromotionError(f"{label} cannot be inside protected input")
    return resolved


def build_review(
    *, candidate_report: Path, accepted_dir: Path, output_dir: Path
) -> dict[str, Any]:
    candidate_report = candidate_report.expanduser().resolve()
    candidate_dir = candidate_report.parent
    accepted_dir = accepted_dir.expanduser().resolve()
    output_dir = _safe_new_directory(
        output_dir, accepted_dir, "review output", candidate_dir
    )
    report = _load_json(candidate_report)
    if (
        report.get("schema_version") != CANDIDATE_SCHEMA
        or report.get("status") != "passed"
        or report.get("ready_for_review") is not True
        or report.get("accepted_schedule", {}).get("mutated") is not False
        or report.get("accepted_schedule", {}).get("sha256_before")
        != report.get("accepted_schedule", {}).get("sha256_after")
        or report.get("application") != {"status": "disabled", "performed": False}
        or report.get("publication") != {"status": "disabled"}
    ):
        raise SchedulePromotionError("schedule candidate is not review-ready")
    races_path, weekends_path = _candidate_files(report, candidate_dir)
    season_id = report["season_id"]
    year = 2000 + int(season_id[2:])
    baseline = _accepted_baseline(accepted_dir, year)
    if baseline["races.csv"]["sha256"] != report["accepted_schedule"]["sha256_after"]:
        raise SchedulePromotionError("accepted schedule drifted after candidate build")

    review: dict[str, Any] = {
        "schema_version": REVIEW_SCHEMA,
        "status": "schedule_review_candidate",
        "season_id": season_id,
        "candidate_report": {
            "file": str(candidate_report),
            "sha256": _sha256(candidate_report),
        },
        "accepted_before": baseline,
        "artifacts": {},
        "target_plan": {
            races_path.name: ["races.csv", f"races{year}.csv"],
            weekends_path.name: ["weekends.csv"],
        },
        "apply_ready": True,
        "application": {"status": "disabled", "performed": False},
        "publication": {"status": "disabled"},
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )
    try:
        for source in (races_path, weekends_path):
            destination = staging / source.name
            shutil.copyfile(source, destination)
            review["artifacts"][source.name] = {
                "sha256": _sha256(destination),
                "bytes": destination.stat().st_size,
                "rows": report["candidate_files"][source.name]["rows"],
            }
        (staging / "review_report.json").write_text(
            json.dumps(review, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        staging.replace(output_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return review


def _load_review(path: Path, expected_sha256: str) -> dict[str, Any]:
    if _sha256(path) != expected_sha256:
        raise SchedulePromotionError("review report SHA-256 mismatch")
    report = _load_json(path)
    if (
        report.get("schema_version") != REVIEW_SCHEMA
        or report.get("status") != "schedule_review_candidate"
        or report.get("apply_ready") is not True
        or report.get("application") != {"status": "disabled", "performed": False}
        or report.get("publication") != {"status": "disabled"}
    ):
        raise SchedulePromotionError("schedule review is not apply-ready")
    return report


def _verify_review_artifacts(
    report: dict[str, Any], review_dir: Path
) -> dict[str, Path]:
    receipts = report.get("artifacts")
    if not isinstance(receipts, dict) or len(receipts) != 2:
        raise SchedulePromotionError("review artifact inventory is incomplete")
    actual = {
        path.name: path
        for path in review_dir.iterdir()
        if path.is_file() and path.name != "review_report.json"
    }
    if set(actual) != set(receipts):
        raise SchedulePromotionError("review bundle contents drifted")
    for name, path in actual.items():
        receipt = receipts[name]
        if (
            receipt.get("sha256") != _sha256(path)
            or receipt.get("bytes") != path.stat().st_size
        ):
            raise SchedulePromotionError(f"review artifact drifted: {name}")
    return actual


def _same_baseline(observed: dict[str, dict[str, Any]], expected: Any) -> bool:
    return isinstance(expected, dict) and observed == expected


def _copy_and_sync(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def application_lock(accepted_dir: Path) -> Iterator[None]:
    lock_path = accepted_dir.parent / ".biathlon-schedule-promotion.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SchedulePromotionError(
                "another schedule promotion holds the lock"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _restore(
    accepted_dir: Path,
    baseline: dict[str, dict[str, Any]],
    backups: dict[str, Path],
) -> None:
    staged: dict[str, Path] = {}
    try:
        for name, state in baseline.items():
            target = accepted_dir / name
            if state["exists"]:
                with tempfile.NamedTemporaryFile(
                    dir=accepted_dir, prefix=f".{name}.restore.", delete=False
                ) as handle:
                    temporary = Path(handle.name)
                _copy_and_sync(backups[name], temporary)
                staged[name] = temporary
            elif target.exists():
                target.unlink()
        for name, temporary in staged.items():
            os.replace(temporary, accepted_dir / name)
        _fsync_directory(accepted_dir)
    finally:
        for temporary in staged.values():
            temporary.unlink(missing_ok=True)


def _safe_external(path: Path, accepted_dir: Path, label: str) -> Path:
    resolved = path.expanduser().resolve()
    accepted_dir = accepted_dir.expanduser().resolve()
    if resolved == accepted_dir or accepted_dir in resolved.parents:
        raise SchedulePromotionError(f"{label} cannot be inside accepted data")
    return resolved


def apply_review(
    *,
    review_report: Path,
    expected_report_sha256: str,
    accepted_dir: Path,
    approve_season_id: str,
    transaction_root: Path,
    backup_root: Path,
    receipt_path: Path,
) -> dict[str, Any]:
    review_report = review_report.expanduser().resolve()
    accepted_dir = accepted_dir.expanduser().resolve()
    transaction_root = _safe_external(
        transaction_root, accepted_dir, "transaction root"
    )
    backup_root = _safe_external(backup_root, accepted_dir, "backup root")
    receipt_path = _safe_external(receipt_path, accepted_dir, "receipt")
    if receipt_path.exists():
        raise SchedulePromotionError(f"receipt already exists: {receipt_path}")
    report = _load_review(review_report, expected_report_sha256)
    if report.get("season_id") != approve_season_id:
        raise SchedulePromotionError("approved season ID does not match review")
    year = 2000 + int(approve_season_id[2:])
    expected_plan = {
        f"races{year}_candidate.csv": ["races.csv", f"races{year}.csv"],
        "weekends_candidate.csv": ["weekends.csv"],
    }
    if report.get("target_plan") != expected_plan:
        raise SchedulePromotionError("review target plan is not exact")
    artifacts = _verify_review_artifacts(report, review_report.parent)
    _validate_candidates(
        artifacts[f"races{year}_candidate.csv"],
        artifacts["weekends_candidate.csv"],
        approve_season_id,
    )
    baseline = _accepted_baseline(accepted_dir, year)
    if not _same_baseline(baseline, report.get("accepted_before")):
        raise SchedulePromotionError("accepted schedule changed after review")

    sources = {
        "races.csv": artifacts[f"races{year}_candidate.csv"],
        f"races{year}.csv": artifacts[f"races{year}_candidate.csv"],
        "weekends.csv": artifacts["weekends_candidate.csv"],
    }
    transaction_root.mkdir(parents=True, exist_ok=True)
    workspace = Path(
        tempfile.mkdtemp(prefix="biathlon-schedule.", dir=transaction_root)
    )
    try:
        candidate_paths: dict[str, Path] = {}
        for name, source in sources.items():
            destination = workspace / name
            _copy_and_sync(source, destination)
            candidate_paths[name] = destination
        expected_after = {
            name: {
                "exists": True,
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for name, path in candidate_paths.items()
        }
        timestamp = datetime.now(UTC)
        application_id = (
            f"{approve_season_id}-{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}-"
            f"{expected_report_sha256[:12]}"
        )
        backup_dir = backup_root / application_id
        with application_lock(accepted_dir):
            locked = _accepted_baseline(accepted_dir, year)
            if locked != baseline:
                raise SchedulePromotionError(
                    "accepted schedule changed while acquiring lock"
                )
            backup_dir.mkdir(parents=True, exist_ok=False)
            backups: dict[str, Path] = {}
            for name, state in baseline.items():
                if state["exists"]:
                    backup = backup_dir / name
                    _copy_and_sync(accepted_dir / name, backup)
                    backups[name] = backup
            staged: dict[str, Path] = {}
            try:
                for name, candidate_path in candidate_paths.items():
                    with tempfile.NamedTemporaryFile(
                        dir=accepted_dir, prefix=f".{name}.promotion.", delete=False
                    ) as handle:
                        temporary = Path(handle.name)
                    _copy_and_sync(candidate_path, temporary)
                    staged[name] = temporary
                for name, temporary in staged.items():
                    os.replace(temporary, accepted_dir / name)
                _fsync_directory(accepted_dir)
            except Exception as error:
                _restore(accepted_dir, baseline, backups)
                raise SchedulePromotionError(
                    f"schedule application failed and was restored: {error}"
                ) from error
            finally:
                for temporary in staged.values():
                    temporary.unlink(missing_ok=True)

            observed_after = _accepted_baseline(accepted_dir, year)
            if observed_after != expected_after:
                _restore(accepted_dir, baseline, backups)
                raise SchedulePromotionError(
                    "post-application verification failed and was restored"
                )
            receipt: dict[str, Any] = {
                "schema_version": RECEIPT_SCHEMA,
                "status": "passed",
                "performed": True,
                "publication": {"status": "disabled"},
                "application_id": application_id,
                "applied_at": timestamp.isoformat(),
                "approved_season_id": approve_season_id,
                "review_report": {
                    "file": str(review_report),
                    "sha256": expected_report_sha256,
                },
                "before": baseline,
                "after": observed_after,
                "backup": {
                    "directory": str(backup_dir),
                    "file_count": len(backups),
                },
            }
            try:
                _atomic_json(receipt_path, receipt)
            except OSError as error:
                _restore(accepted_dir, baseline, backups)
                raise SchedulePromotionError(
                    f"receipt write failed and schedule was restored: {error}"
                ) from error
            return {**receipt, "receipt": str(receipt_path)}
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    subcommands = command.add_subparsers(dest="command", required=True)
    review = subcommands.add_parser("review")
    review.add_argument("--candidate-report", type=Path, required=True)
    review.add_argument("--accepted-dir", type=Path, required=True)
    review.add_argument("--output-dir", type=Path, required=True)
    apply = subcommands.add_parser("apply")
    apply.add_argument("--review-report", type=Path, required=True)
    apply.add_argument("--expected-report-sha256", required=True)
    apply.add_argument("--accepted-dir", type=Path, required=True)
    apply.add_argument("--approve-season-id", required=True)
    apply.add_argument("--transaction-root", type=Path, required=True)
    apply.add_argument("--backup-root", type=Path, required=True)
    apply.add_argument("--receipt", dest="receipt_path", type=Path, required=True)
    return command


def main() -> int:
    arguments = vars(parser().parse_args())
    command = arguments.pop("command")
    try:
        result = (
            build_review(**arguments)
            if command == "review"
            else apply_review(**arguments)
        )
    except (SchedulePromotionError, KeyError, TypeError, ValueError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
