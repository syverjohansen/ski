#!/usr/bin/env python3
"""Build all-or-nothing promotion candidates for XC team results and Elo.

Five accepted histories participate in a team event: the two athlete chrono
files, the two relay team files, and the combined team-sprint file.  This
command validates the replay receipt, proves every aggregate rating equals its
member rows, and creates byte-preserving candidate histories in a new output
directory.  Accepted histories are always read-only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import shutil
import tempfile
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from cross_country_results_elo_replay import PREVIOUS_COLUMNS
from cross_country_results_promote import (
    PromotionError,
    _atomic_bytes,
    _csv_line,
    _digest_bytes,
    _read_increment,
    _validate_output_dir,
    build_candidate_history,
)
from cross_country_team_results_elo_replay import TEAM_OUTPUT_FIELDS
from cross_country_team_results_identity import EXPECTED_MEMBERS


class TeamPromotionError(PromotionError):
    """Raised when a team replay cannot become a safe promotion candidate."""


REPLAY_FILES = (
    "men_athlete_elo_rows.csv",
    "ladies_athlete_elo_rows.csv",
    "men_relay_elo_rows.csv",
    "ladies_relay_elo_rows.csv",
    "team_sprint_elo_rows.csv",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _row_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def validate_replay_receipt(
    *, replay_dir: Path, men_chrono: Path, ladies_chrono: Path, expected_season: int
) -> dict[str, object]:
    report_path = replay_dir / "report.json"
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TeamPromotionError(f"invalid team replay report: {report_path}") from exc
    if report.get("schema_version") != "nordicnumbers.cross-country-team-elo-replay/v1":
        raise TeamPromotionError("unsupported team replay report schema")
    if report.get("status") != "replayed" or report.get("season") != expected_season:
        raise TeamPromotionError(
            f"team replay report is not replayed season {expected_season}"
        )
    if report.get("source_histories_modified") is not False:
        raise TeamPromotionError("team replay did not preserve source histories")
    expected_sources = {
        "men_chrono": _sha256(men_chrono),
        "ladies_chrono": _sha256(ladies_chrono),
    }
    if report.get("source_sha256") != expected_sources:
        raise TeamPromotionError("accepted athlete histories changed after team replay")
    receipts = report.get("outputs")
    if not isinstance(receipts, dict) or set(receipts) != set(REPLAY_FILES):
        raise TeamPromotionError("team replay report has incomplete output receipts")
    for filename in REPLAY_FILES:
        path = replay_dir / filename
        receipt = receipts[filename]
        if not isinstance(receipt, dict):
            raise TeamPromotionError(f"invalid replay receipt for {filename}")
        if not path.is_file() or receipt.get("sha256") != _sha256(path):
            raise TeamPromotionError(f"replay output checksum mismatch: {filename}")
        if receipt.get("rows") != _row_count(path):
            raise TeamPromotionError(f"replay output row-count mismatch: {filename}")
    return report


def _split_members(value: str) -> list[str]:
    return [member.strip() for member in value.split(",") if member.strip()]


def validate_team_aggregates(replay_dir: Path) -> None:
    athlete_rows: dict[tuple[str, int, int, int, str], dict[str, str]] = {}
    for filename in REPLAY_FILES[:2]:
        _, rows = _read_increment(replay_dir / filename)
        for row in rows:
            key = (
                row["Sex"],
                int(row["Season"]),
                int(row["Race"]),
                int(row["Place"]),
                row["ID"],
            )
            if key in athlete_rows:
                raise TeamPromotionError(f"duplicate athlete replay row: {key}")
            athlete_rows[key] = row

    for filename in REPLAY_FILES[2:]:
        header, teams = _read_increment(replay_dir / filename)
        if header != list(TEAM_OUTPUT_FIELDS):
            raise TeamPromotionError(f"team replay header mismatch: {filename}")
        for row_number, team in enumerate(teams, start=2):
            distance = team["Distance"]
            if distance not in EXPECTED_MEMBERS:
                raise TeamPromotionError(
                    f"{filename}:{row_number}: unsupported distance {distance!r}"
                )
            member_ids = _split_members(team["MemberIDs"])
            member_names = _split_members(team["TeamMembers"])
            expected_size = EXPECTED_MEMBERS[distance]
            if int(team["TeamSize"]) != expected_size or len(member_ids) != expected_size:
                raise TeamPromotionError(
                    f"{filename}:{row_number}: invalid team size or member IDs"
                )
            members = []
            for athlete_id in member_ids:
                key = (
                    team["Sex"],
                    int(team["Season"]),
                    int(team["Race"]),
                    int(team["Place"]),
                    athlete_id,
                )
                try:
                    members.append(athlete_rows[key])
                except KeyError as exc:
                    raise TeamPromotionError(
                        f"{filename}:{row_number}: missing member replay row {key}"
                    ) from exc
            if member_names != [member["Skier"] for member in members]:
                raise TeamPromotionError(
                    f"{filename}:{row_number}: member names do not match athlete rows"
                )
            if {member["Nation"] for member in members} != {team["Nation"]}:
                raise TeamPromotionError(
                    f"{filename}:{row_number}: member nations do not match team nation"
                )
            for stream, previous in PREVIOUS_COLUMNS.items():
                for column in (stream, previous):
                    expected_sum = sum(float(member[column]) for member in members)
                    expected_average = expected_sum / expected_size
                    team_value = float(team[f"Team_{column}"])
                    average_value = float(team[f"Avg_{column}"])
                    if not math.isfinite(team_value) or not math.isclose(
                        team_value, expected_sum, rel_tol=0.0, abs_tol=1e-8
                    ):
                        raise TeamPromotionError(
                            f"{filename}:{row_number}: Team_{column} does not equal member sum"
                        )
                    if not math.isfinite(average_value) or not math.isclose(
                        average_value, expected_average, rel_tol=0.0, abs_tol=1e-8
                    ):
                        raise TeamPromotionError(
                            f"{filename}:{row_number}: Avg_{column} does not equal member mean"
                        )


def _season_for_date(value: str) -> int:
    raced = date.fromisoformat(value)
    return raced.year + 1 if raced.month >= 7 else raced.year


def build_team_candidate_history(
    *,
    source: Path,
    increments: Path,
    output: Path,
    expected_season: int,
    expected_distance: str,
    expected_sex: str | None,
) -> dict[str, object]:
    source_bytes = source.read_bytes()
    if not source_bytes or not source_bytes.endswith(b"\n"):
        raise TeamPromotionError(f"accepted team history is empty or unterminated: {source}")
    reader = csv.DictReader(io.StringIO(source_bytes.decode("utf-8-sig"), newline=""))
    header = list(reader.fieldnames or [])
    source_rows = list(reader)
    if header != list(TEAM_OUTPUT_FIELDS):
        raise TeamPromotionError(f"accepted team history header mismatch: {source}")
    increment_header, increment_rows = _read_increment(increments)
    if increment_header != header:
        raise TeamPromotionError(f"increment header does not match {source}")

    allowed_sexes = {expected_sex} if expected_sex else {"M", "L"}
    max_races = {sex: 0 for sex in allowed_sexes}
    existing_keys: set[tuple[str, int, int, str]] = set()
    for row_number, row in enumerate(source_rows, start=2):
        if row["Sex"] not in allowed_sexes or row["Distance"] != expected_distance:
            raise TeamPromotionError(f"{source}:{row_number}: wrong sex or distance")
        key = (row["Sex"], int(row["Season"]), int(row["Race"]), row["MemberIDs"])
        if key in existing_keys:
            raise TeamPromotionError(f"duplicate accepted team result: {key}")
        existing_keys.add(key)
        if int(row["Season"]) == expected_season:
            max_races[row["Sex"]] = max(max_races[row["Sex"]], int(row["Race"]))

    new_keys: set[tuple[str, int, int, str]] = set()
    new_races: set[tuple[str, int]] = set()
    for row_number, row in enumerate(increment_rows, start=2):
        sex = row["Sex"]
        if (
            sex not in allowed_sexes
            or row["Distance"] != expected_distance
            or int(row["Season"]) != expected_season
        ):
            raise TeamPromotionError(
                f"{increments}:{row_number}: unexpected sex, distance, or season"
            )
        if _season_for_date(row["Date"]) != expected_season:
            raise TeamPromotionError(
                f"{increments}:{row_number}: date is outside season {expected_season}"
            )
        race = int(row["Race"])
        if race <= max_races[sex]:
            raise TeamPromotionError(
                f"{increments}:{row_number}: race {race} is not newer than accepted "
                f"race {max_races[sex]} for {sex}"
            )
        key = (sex, expected_season, race, row["MemberIDs"])
        if key in existing_keys or key in new_keys:
            raise TeamPromotionError(f"duplicate promoted team result: {key}")
        new_keys.add(key)
        new_races.add((sex, race))

    appended = "".join(
        _csv_line([row[field] for field in header]) for row in increment_rows
    ).encode("utf-8")
    data = source_bytes + appended
    _atomic_bytes(output, data)
    return {
        "accepted_rows": len(source_rows),
        "appended_rows": len(increment_rows),
        "appended_races": len(new_races),
        "historical_bytes_preserved": data.startswith(source_bytes),
        "source_sha256": _digest_bytes(source_bytes),
        "increment_sha256": _sha256(increments),
        "output_sha256": _digest_bytes(data),
        "output_file": output.name,
    }


def _source_hashes(paths: Iterable[Path]) -> dict[Path, str]:
    return {path: _sha256(path) for path in paths}


def promote_team_results(
    *,
    men_chrono: Path,
    ladies_chrono: Path,
    men_relay_chrono: Path,
    ladies_relay_chrono: Path,
    team_sprint_chrono: Path,
    replay_dir: Path,
    expected_season: int,
    output_dir: Path,
) -> dict[str, object]:
    sources = (
        men_chrono,
        ladies_chrono,
        men_relay_chrono,
        ladies_relay_chrono,
        team_sprint_chrono,
    )
    output_dir = _validate_output_dir(output_dir, (*sources, *(replay_dir / name for name in REPLAY_FILES)))
    if output_dir.exists():
        raise TeamPromotionError(f"promotion output already exists: {output_dir}")
    original_hashes = _source_hashes(sources)
    validate_replay_receipt(
        replay_dir=replay_dir,
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        expected_season=expected_season,
    )
    validate_team_aggregates(replay_dir)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        outputs = [
            build_candidate_history(
                source=men_chrono,
                increments=replay_dir / "men_athlete_elo_rows.csv",
                output=staging / "men_chrono.csv",
                sex="M",
                expected_season=expected_season,
            ),
            build_candidate_history(
                source=ladies_chrono,
                increments=replay_dir / "ladies_athlete_elo_rows.csv",
                output=staging / "ladies_chrono.csv",
                sex="L",
                expected_season=expected_season,
            ),
            build_team_candidate_history(
                source=men_relay_chrono,
                increments=replay_dir / "men_relay_elo_rows.csv",
                output=staging / "men_relay_chrono.csv",
                expected_season=expected_season,
                expected_distance="Rel",
                expected_sex="M",
            ),
            build_team_candidate_history(
                source=ladies_relay_chrono,
                increments=replay_dir / "ladies_relay_elo_rows.csv",
                output=staging / "ladies_relay_chrono.csv",
                expected_season=expected_season,
                expected_distance="Rel",
                expected_sex="L",
            ),
            build_team_candidate_history(
                source=team_sprint_chrono,
                increments=replay_dir / "team_sprint_elo_rows.csv",
                output=staging / "team_sprint_chrono.csv",
                expected_season=expected_season,
                expected_distance="Ts",
                expected_sex=None,
            ),
        ]
        if sum(int(item["appended_rows"]) for item in outputs) == 0:
            raise TeamPromotionError("promotion contains no new team result rows")
        if _source_hashes(sources) != original_hashes:
            raise TeamPromotionError("accepted histories changed during team promotion")
        report: dict[str, object] = {
            "schema_version": "nordicnumbers.cross-country-team-results-promotion/v1",
            "expected_season": expected_season,
            "status": "promotion_candidate",
            "accepted_files_modified": False,
            "atomic_candidate_set": True,
            "outputs": outputs,
        }
        _atomic_bytes(
            staging / "report.json",
            (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
        )
        staging.replace(output_dir)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-relay-chrono", required=True, type=Path)
    parser.add_argument("--ladies-relay-chrono", required=True, type=Path)
    parser.add_argument("--team-sprint-chrono", required=True, type=Path)
    parser.add_argument("--replay-dir", required=True, type=Path)
    parser.add_argument("--expected-season", required=True, type=int)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = promote_team_results(**vars(args))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
