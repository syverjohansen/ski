#!/usr/bin/env python3
"""Resolve acquired FIS ski-jumping results to canonical athlete identities.

Automatic matching is limited to a unique normalized-name match in the
accepted full histories. Mixed-team members are searched across both sexes.
Known name changes require an explicit reviewed alias; unresolved or ambiguous
identities make the output ineligible for Elo replay.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import tempfile
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


MAPPED_FIELDS = (
    "canonical_athlete_id",
    "canonical_name",
    "canonical_nation",
    "canonical_sex",
    "identity_match_method",
)
IDENTITY_MAP_FIELDS = (
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "canonical_athlete_id",
    "canonical_name",
    "canonical_nation",
    "canonical_sex",
    "identity_match_method",
)
UNRESOLVED_FIELDS = (
    "event_sex",
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "fis_nation",
    "reason",
    "candidate_identities",
)


class IdentityError(RuntimeError):
    """Raised when an identity artifact violates its contract."""


@dataclass(frozen=True)
class CanonicalAthlete:
    sex: str
    athlete_id: str
    name: str
    nation: str


@dataclass(frozen=True)
class Alias:
    fis_competitor_id: str
    fis_code: str
    fis_name: str
    canonical_sex: str
    canonical_id: str
    canonical_name: str


def normalized_name(value: str) -> tuple[str, ...]:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_like = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    ).lower()
    return tuple(sorted(re.findall(r"[a-z0-9]+", ascii_like)))


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def load_histories(
    men_history: Path, ladies_history: Path
) -> tuple[
    dict[tuple[str, str], CanonicalAthlete],
    dict[tuple[str, tuple[str, ...]], set[str]],
    dict[tuple[str, str], set[str]],
]:
    athletes: dict[tuple[str, str], CanonicalAthlete] = {}
    latest_dates: dict[tuple[str, str], str] = {}
    name_index: dict[tuple[str, tuple[str, ...]], set[str]] = defaultdict(set)
    names_by_id: dict[tuple[str, str], set[str]] = defaultdict(set)
    for sex, path in (("M", men_history), ("L", ladies_history)):
        _, rows = _read_rows(path)
        for row_number, row in enumerate(rows, start=2):
            athlete_id = (row.get("ID") or "").strip()
            name = (row.get("Skier") or "").strip()
            nation = (row.get("Nation") or "").strip()
            observed_date = (row.get("Date") or "").strip()
            if not athlete_id or not name:
                raise IdentityError(f"{path}:{row_number}: missing ID or Skier")
            key = (sex, athlete_id)
            names_by_id[key].add(name)
            name_index[(sex, normalized_name(name))].add(athlete_id)
            if observed_date >= latest_dates.get(key, ""):
                athletes[key] = CanonicalAthlete(sex, athlete_id, name, nation)
                latest_dates[key] = observed_date
    return athletes, name_index, names_by_id


def load_aliases(
    path: Path,
    athletes: dict[tuple[str, str], CanonicalAthlete],
    names_by_id: dict[tuple[str, str], set[str]],
) -> dict[str, Alias]:
    aliases: dict[str, Alias] = {}
    _, rows = _read_rows(path)
    for row_number, row in enumerate(rows, start=2):
        alias = Alias(
            fis_competitor_id=(row.get("fis_competitor_id") or "").strip(),
            fis_code=(row.get("fis_code") or "").strip(),
            fis_name=(row.get("fis_name") or "").strip(),
            canonical_sex=(row.get("canonical_sex") or "").strip(),
            canonical_id=(row.get("canonical_id") or "").strip(),
            canonical_name=(row.get("canonical_name") or "").strip(),
        )
        if alias.canonical_sex not in {"M", "L"} or not all(
            (
                alias.fis_competitor_id,
                alias.fis_code,
                alias.fis_name,
                alias.canonical_id,
                alias.canonical_name,
            )
        ):
            raise IdentityError(f"{path}:{row_number}: incomplete alias")
        if alias.fis_competitor_id in aliases:
            raise IdentityError(
                f"{path}:{row_number}: duplicate FIS competitor "
                f"{alias.fis_competitor_id}"
            )
        canonical_key = (alias.canonical_sex, alias.canonical_id)
        if canonical_key not in athletes:
            raise IdentityError(
                f"{path}:{row_number}: unknown canonical athlete {canonical_key}"
            )
        if alias.canonical_name not in names_by_id[canonical_key]:
            raise IdentityError(
                f"{path}:{row_number}: canonical name {alias.canonical_name!r} "
                f"does not belong to {canonical_key}"
            )
        aliases[alias.fis_competitor_id] = alias
    return aliases


def _positive_integer(
    row: dict[str, str], field: str, *, path: Path, row_number: int
) -> int:
    raw = (row.get(field) or "").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise IdentityError(
            f"{path}:{row_number}: {field} is not an integer: {raw!r}"
        ) from exc
    if value <= 0:
        raise IdentityError(f"{path}:{row_number}: {field} must be positive")
    return value


def validate_result_memberships(
    path: Path, rows: list[dict[str, str]], *, mapped: bool = False
) -> None:
    athlete_field = "canonical_athlete_id" if mapped else "fis_competitor_id"
    event_athletes: dict[tuple[str, ...], set[str]] = defaultdict(set)
    teams: dict[tuple[str, ...], list[tuple[int, dict[str, str]]]] = defaultdict(list)
    event_teams: dict[tuple[str, ...], set[str]] = defaultdict(set)
    for row_number, row in enumerate(rows, start=2):
        event_values = tuple(
            (row.get(field) or "").strip()
            for field in ("date", "sex", "season", "race", "event_kind")
        )
        if not all(event_values):
            raise IdentityError(f"{path}:{row_number}: incomplete event identity")
        athlete_id = (row.get(athlete_field) or "").strip()
        if not athlete_id and mapped:
            fis_id = (row.get("fis_competitor_id") or "").strip()
            athlete_id = f"unresolved:{fis_id}" if fis_id else ""
        if not athlete_id:
            raise IdentityError(f"{path}:{row_number}: missing {athlete_field}")
        if athlete_id in event_athletes[event_values]:
            raise IdentityError(
                f"{path}:{row_number}: athlete {athlete_id} appears twice in "
                f"event {event_values}"
            )
        event_athletes[event_values].add(athlete_id)

        event_kind = event_values[-1]
        if event_kind == "individual":
            if (row.get("member_order") or "").strip():
                raise IdentityError(
                    f"{path}:{row_number}: individual row has member_order"
                )
            continue
        if event_kind != "team":
            raise IdentityError(
                f"{path}:{row_number}: unsupported event_kind {event_kind!r}"
            )
        team_name = (row.get("team_name") or "").strip()
        team_nation = (row.get("team_nation") or "").strip()
        place = _positive_integer(row, "place", path=path, row_number=row_number)
        if not team_name or not team_nation:
            raise IdentityError(f"{path}:{row_number}: incomplete team identity")
        team_key = (*event_values, str(place), team_nation, team_name)
        teams[team_key].append((row_number, row))
        event_teams[event_values].add(team_key[-2])

    for team_key, members in teams.items():
        if len(members) not in {2, 4}:
            raise IdentityError(
                f"{path}: team {team_key} has {len(members)} members; expected 2 or 4"
            )
        orders = sorted(
            _positive_integer(row, "member_order", path=path, row_number=row_number)
            for row_number, row in members
        )
        if orders != list(range(1, len(members) + 1)):
            raise IdentityError(
                f"{path}: team {team_key} has invalid member order {orders}"
            )
    for event, nations in event_teams.items():
        if len(nations) < 2:
            raise IdentityError(f"{path}: team event {event} has fewer than two teams")


def _validate_output_dir(output_dir: Path, source_paths: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in source_paths:
        source_parent = source.expanduser().resolve().parent
        if resolved == source_parent or source_parent in resolved.parents:
            raise IdentityError(
                f"identity output {resolved} cannot be inside source directory "
                f"{source_parent}"
            )
    return resolved


def _csv_text(rows: list[dict[str, object]], fields: Iterable[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def map_identities(
    *,
    results: Path,
    men_history: Path,
    ladies_history: Path,
    aliases_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    output_dir = _validate_output_dir(
        output_dir, (results, men_history, ladies_history, aliases_path)
    )
    raw_fields, raw_rows = _read_rows(results)
    required = {
        "date", "sex", "season", "race", "event_kind", "place",
        "fis_competitor_id", "fis_code", "name", "nation", "member_order",
        "team_name", "team_nation",
    }
    missing_fields = sorted(required - set(raw_fields))
    if missing_fields:
        raise IdentityError(f"results candidate is missing fields: {missing_fields}")
    if not raw_rows:
        raise IdentityError(f"results candidate is empty: {results}")
    validate_result_memberships(results, raw_rows)

    athletes, name_index, names_by_id = load_histories(
        men_history, ladies_history
    )
    aliases = load_aliases(aliases_path, athletes, names_by_id)
    mapped_rows: list[dict[str, object]] = []
    identity_rows: dict[str, dict[str, object]] = {}
    unresolved_rows: dict[str, dict[str, object]] = {}
    method_counts: dict[str, int] = defaultdict(int)

    for row_number, row in enumerate(raw_rows, start=2):
        event_sex = (row.get("sex") or "").strip()
        competitor_id = (row.get("fis_competitor_id") or "").strip()
        fis_code = (row.get("fis_code") or "").strip()
        fis_name = (row.get("name") or "").strip()
        fis_nation = (row.get("nation") or "").strip()
        if event_sex not in {"M", "L", "Mixed"} or not all(
            (competitor_id, fis_code, fis_name)
        ):
            raise IdentityError(f"{results}:{row_number}: incomplete FIS identity")

        alias = aliases.get(competitor_id)
        canonical: CanonicalAthlete | None = None
        method = ""
        candidates: list[CanonicalAthlete] = []
        reason = ""
        if alias:
            if alias.fis_code != fis_code or alias.fis_name != fis_name:
                raise IdentityError(
                    f"{results}:{row_number}: FIS identity changed for alias "
                    f"{competitor_id}"
                )
            if event_sex != "Mixed" and alias.canonical_sex != event_sex:
                raise IdentityError(
                    f"{results}:{row_number}: alias sex conflicts with event sex"
                )
            canonical = athletes[(alias.canonical_sex, alias.canonical_id)]
            method = "reviewed_alias"
        else:
            sexes = ("M", "L") if event_sex == "Mixed" else (event_sex,)
            for sex in sexes:
                ids = name_index.get((sex, normalized_name(fis_name)), set())
                candidates.extend(athletes[(sex, athlete_id)] for athlete_id in ids)
            if len(candidates) == 1:
                canonical = candidates[0]
                method = "unique_normalized_name"
            else:
                reason = "missing" if not candidates else "ambiguous"

        if canonical is None:
            unresolved_rows[competitor_id] = {
                "event_sex": event_sex,
                "fis_competitor_id": competitor_id,
                "fis_code": fis_code,
                "fis_name": fis_name,
                "fis_nation": fis_nation,
                "reason": reason,
                "candidate_identities": ";".join(
                    f"{item.sex}:{item.athlete_id}:{item.name}" for item in candidates
                ),
            }
            mapped_rows.append({**row, **{field: "" for field in MAPPED_FIELDS}})
            continue

        mapped = {
            **row,
            "canonical_athlete_id": canonical.athlete_id,
            "canonical_name": canonical.name,
            "canonical_nation": canonical.nation,
            "canonical_sex": canonical.sex,
            "identity_match_method": method,
        }
        mapped_rows.append(mapped)
        method_counts[method] += 1
        identity = {
            "fis_competitor_id": competitor_id,
            "fis_code": fis_code,
            "fis_name": fis_name,
            "canonical_athlete_id": canonical.athlete_id,
            "canonical_name": canonical.name,
            "canonical_nation": canonical.nation,
            "canonical_sex": canonical.sex,
            "identity_match_method": method,
        }
        previous = identity_rows.get(competitor_id)
        if previous and previous != identity:
            raise IdentityError(
                f"conflicting identity rows for FIS competitor {competitor_id}"
            )
        identity_rows[competitor_id] = identity

    validate_result_memberships(results, mapped_rows, mapped=True)
    mapped_path = output_dir / "mapped_results.csv"
    identity_map_path = output_dir / "identity_map.csv"
    unresolved_path = output_dir / "unresolved_identities.csv"
    _atomic_text(
        mapped_path, _csv_text(mapped_rows, raw_fields + list(MAPPED_FIELDS))
    )
    _atomic_text(
        identity_map_path,
        _csv_text(
            [identity_rows[key] for key in sorted(identity_rows)],
            IDENTITY_MAP_FIELDS,
        ),
    )
    _atomic_text(
        unresolved_path,
        _csv_text(
            [unresolved_rows[key] for key in sorted(unresolved_rows)],
            UNRESOLVED_FIELDS,
        ),
    )
    unresolved_count = len(unresolved_rows)
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.skijump-results-identity/v1",
        "input_sha256": hashlib.sha256(results.read_bytes()).hexdigest(),
        "result_rows": len(mapped_rows),
        "unique_fis_athletes": len(identity_rows) + unresolved_count,
        "resolved_fis_athletes": len(identity_rows),
        "unresolved_fis_athletes": unresolved_count,
        "match_methods": dict(sorted(method_counts.items())),
        "status": "mapped" if unresolved_count == 0 else "blocked",
        "replay_eligible": unresolved_count == 0,
        "accepted_histories_modified": False,
        "outputs": {
            "mapped_results": mapped_path.name,
            "identity_map": identity_map_path.name,
            "unresolved_identities": unresolved_path.name,
        },
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--men-history", required=True, type=Path)
    parser.add_argument("--ladies-history", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--fail-on-unresolved", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = map_identities(
        results=args.results,
        men_history=args.men_history,
        ladies_history=args.ladies_history,
        aliases_path=args.aliases,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if args.fail_on_unresolved and not report["replay_eligible"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
