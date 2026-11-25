#!/usr/bin/env python3
"""Build a guarded ski-jumping season-close candidate.

The May 1 Race 0 archive stores the season-ending rating in ``Pelo`` and the
legacy 85/15 regression toward 1300 in ``Elo``. Team histories are completeness
inputs and never receive synthetic Race 0 rows.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import tempfile
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from schedule_contract import RACES_COLUMNS, infer_fis_season, read_csv
from skijump_results_elo_replay import STREAMS, _stream_columns, offseason_rating
from skijump_results_gap import MissingRace, audit_races


class SeasonCloseError(RuntimeError):
    """Raised when a ski-jumping season cannot close safely."""


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def _atomic_json(path: Path, document: dict[str, object]) -> None:
    _atomic_bytes(
        path, (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    )


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _validate_output_root(output_root: Path, sources: Iterable[Path]) -> Path:
    resolved = output_root.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise SeasonCloseError(
                f"season-close output {resolved} cannot be inside source directory "
                f"{parent}"
            )
    return resolved


def _race_order(row: dict[str, str]) -> tuple[str, int]:
    return row["Date"], int(row["Race"])


def _rating_columns() -> list[tuple[str, str]]:
    return [_stream_columns(stream) for stream in STREAMS]


def _validate_offseason_rows(
    *,
    rows: list[dict[str, str]],
    athletes: dict[str, dict[str, str]],
    sex: str,
    season: int,
    close_date: date,
) -> int:
    corrections = 0
    by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        athlete_id = row["ID"].strip()
        if athlete_id in by_id:
            raise SeasonCloseError(
                f"duplicate offseason row for {sex}/{season}/{athlete_id}"
            )
        by_id[athlete_id] = row
        expected = {
            "Date": close_date.isoformat(),
            "City": "Summer",
            "Country": "Break",
            "Sex": sex,
            "HillSize": "0",
            "RaceType": "Offseason",
            "TeamEvent": "0",
            "Event": "Offseason",
            "Place": "0",
            "Season": str(season),
            "Race": "0",
            "Leg": "0",
        }
        for field, value in expected.items():
            if row[field] != value:
                raise SeasonCloseError(
                    f"invalid offseason {field} for {sex}/{season}/{athlete_id}: "
                    f"{row[field]!r} != {value!r}"
                )
        final = athletes.get(athlete_id)
        if final is None:
            continue
        for pelo_column, elo_column in _rating_columns():
            expected_rating = row[pelo_column].strip()
            observed_pelo = row[pelo_column].strip()
            observed_elo = row[elo_column].strip()
            if not expected_rating:
                if observed_pelo or observed_elo:
                    raise SeasonCloseError(
                        f"unexpected {elo_column} for {sex}/{season}/{athlete_id}"
                    )
                continue
            if not observed_pelo:
                raise SeasonCloseError(
                    f"missing {pelo_column} for {sex}/{season}/{athlete_id}"
                )
            discounted = offseason_rating(float(expected_rating))
            if not observed_elo or not math.isclose(
                float(observed_elo), discounted, abs_tol=1e-9
            ):
                row[elo_column] = str(discounted)
                corrections += 1
    expected_ids = set(athletes)
    observed_ids = set(by_id)
    if observed_ids != expected_ids:
        raise SeasonCloseError(
            f"partial offseason rows for {sex}/{season}: "
            f"missing={sorted(expected_ids - observed_ids)}, "
            f"unexpected={sorted(observed_ids - expected_ids)}"
        )
    return corrections


def _atomic_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    _atomic_bytes(path, output.getvalue().encode())


def build_closed_history(
    *, source: Path, output: Path, sex: str, season: int
) -> dict[str, object]:
    source_bytes = source.read_bytes()
    fields, rows = _read(source)
    required = {
        "Date",
        "City",
        "Country",
        "Sex",
        "HillSize",
        "RaceType",
        "TeamEvent",
        "Event",
        "Place",
        "Skier",
        "Nation",
        "ID",
        "Season",
        "Race",
        "Birthday",
        "Age",
        "Exp",
        "Leg",
        *(column for pair in _rating_columns() for column in pair),
    }
    missing = sorted(required - set(fields))
    if missing:
        raise SeasonCloseError(f"{source} is missing columns: {missing}")

    athletes: dict[str, dict[str, str]] = {}
    for row in rows:
        if row["Race"] == "0" or int(row["Season"]) != season:
            continue
        athlete_id = row["ID"].strip()
        if not athlete_id:
            raise SeasonCloseError(f"{source} contains a blank athlete ID")
        current = athletes.get(athlete_id)
        if current is None or _race_order(row) > _race_order(current):
            athletes[athlete_id] = row
    if not athletes:
        raise SeasonCloseError(f"{source} has no race rows for season {season}")

    close_date = date(season, 5, 1)
    existing = [
        row for row in rows if row["Race"] == "0" and int(row["Season"]) == season
    ]
    if existing:
        corrections = _validate_offseason_rows(
            rows=existing,
            athletes=athletes,
            sex=sex,
            season=season,
            close_date=close_date,
        )
        if corrections:
            _atomic_csv(output, fields, rows)
            output_bytes = output.read_bytes()
            return {
                "sex": sex,
                "status": "candidate",
                "athletes": len(athletes),
                "appended_rows": 0,
                "corrected_ratings": corrections,
                "source_sha256": _sha256(source_bytes),
                "output_sha256": _sha256(output_bytes),
                "output_file": output.name,
            }
        _atomic_bytes(output, source_bytes)
        return {
            "sex": sex,
            "status": "no_changes",
            "athletes": len(athletes),
            "appended_rows": 0,
            "corrected_ratings": 0,
            "source_sha256": _sha256(source_bytes),
            "output_sha256": _sha256(source_bytes),
            "output_file": output.name,
        }

    raise SeasonCloseError(
        f"{source} has no Race 0 rows for season {season}; run results replay first"
    )


def _gap_label(item: MissingRace) -> str:
    return f"{item.date}/{item.sex}/race-{item.race}/{item.event_kind}"


def _validate_team_history(path: Path, *, season: int, sex: str) -> int:
    _, rows = _read(path)
    current = [row for row in rows if int(row["Season"]) == season]
    if any(row.get("Race") == "0" for row in current):
        raise SeasonCloseError(f"team history contains Race 0 rows: {path}")
    if any(row.get("Sex") != sex for row in current):
        raise SeasonCloseError(f"team history contains unexpected sex rows: {path}")
    return len(current)


def build_season_close(
    *,
    schedule: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_team_chrono: Path,
    ladies_team_chrono: Path,
    mixed_team_chrono: Path,
    policy_exclusions: Path,
    season: int,
    as_of: date,
    output_root: Path,
) -> dict[str, object]:
    close_date = date(season, 5, 1)
    if as_of < close_date:
        raise SeasonCloseError(
            f"season {season} cannot close before {close_date.isoformat()}"
        )
    schedule_season = infer_fis_season(read_csv(schedule, RACES_COLUMNS))
    if schedule_season != season:
        raise SeasonCloseError(
            f"schedule season {schedule_season} does not match requested season {season}"
        )
    sources = (
        schedule,
        men_chrono,
        ladies_chrono,
        men_team_chrono,
        ladies_team_chrono,
        mixed_team_chrono,
        policy_exclusions,
    )
    output_root = _validate_output_root(output_root, sources)
    missing, policy_excluded, completed = audit_races(
        schedule=schedule,
        men_chrono=men_chrono,
        ladies_chrono=ladies_chrono,
        men_team_chrono=men_team_chrono,
        ladies_team_chrono=ladies_team_chrono,
        mixed_team_chrono=mixed_team_chrono,
        policy_exclusions=policy_exclusions,
        as_of=close_date,
    )
    if missing:
        raise SeasonCloseError(
            "season has unprocessed scheduled results: "
            + ", ".join(_gap_label(item) for item in missing)
        )
    team_rows = {
        "M": _validate_team_history(men_team_chrono, season=season, sex="M"),
        "L": _validate_team_history(ladies_team_chrono, season=season, sex="L"),
        "Mixed": _validate_team_history(mixed_team_chrono, season=season, sex="Mixed"),
    }
    outputs = [
        build_closed_history(
            source=men_chrono,
            output=output_root / "candidate/men_chrono.csv",
            sex="M",
            season=season,
        ),
        build_closed_history(
            source=ladies_chrono,
            output=output_root / "candidate/ladies_chrono.csv",
            sex="L",
            season=season,
        ),
    ]
    status = (
        "candidate"
        if any(output["status"] == "candidate" for output in outputs)
        else "no_changes"
    )
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.skijump-season-close/v1",
        "status": status,
        "season": season,
        "as_of": as_of.isoformat(),
        "close_date": close_date.isoformat(),
        "completed_schedule_races": completed,
        "policy_excluded_races": len(policy_excluded),
        "accepted_team_rows": team_rows,
        "team_histories_closed_by": "validated_event_completeness",
        "rollover": {
            "pelo": "undiscounted season-ending rating",
            "elo": "Pelo * 0.85 + 1300 * 0.15",
        },
        "outputs": outputs,
        "source_histories_modified": False,
    }
    _atomic_json(output_root / "report.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-team-chrono", required=True, type=Path)
    parser.add_argument("--ladies-team-chrono", required=True, type=Path)
    parser.add_argument("--mixed-team-chrono", required=True, type=Path)
    parser.add_argument("--policy-exclusions", required=True, type=Path)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output-root", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = build_season_close(**vars(args))
    except Exception as exc:
        try:
            _atomic_json(
                args.output_root / "report.json",
                {
                    "schema_version": "nordicnumbers.skijump-season-close/v1",
                    "status": "failed",
                    "season": args.season,
                    "as_of": args.as_of.isoformat(),
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
        except OSError:
            pass
        raise
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
