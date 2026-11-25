#!/usr/bin/env python3
"""Map acquired Nordic-combined results to canonical athlete identities."""

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

MAPPED_FIELDS = (
    "canonical_athlete_id", "canonical_name", "canonical_nation",
    "canonical_sex", "identity_match_method",
)
ALIAS_FIELDS = (
    "fis_competitor_id", "fis_code", "fis_name", "canonical_sex",
    "canonical_id", "canonical_name",
)


class IdentityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Athlete:
    sex: str
    athlete_id: str
    name: str
    nation: str


def normalized_name(value: str) -> tuple[str, ...]:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(character for character in value if not unicodedata.combining(character))
    return tuple(sorted(re.findall(r"[a-z0-9]+", value.lower())))


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def load_athletes(
    men_history: Path, ladies_history: Path
) -> tuple[dict[tuple[str, str], Athlete], dict[tuple[str, tuple[str, ...]], set[str]], dict[tuple[str, str], set[str]]]:
    athletes: dict[tuple[str, str], Athlete] = {}
    latest: dict[tuple[str, str], str] = {}
    index: dict[tuple[str, tuple[str, ...]], set[str]] = defaultdict(set)
    names: dict[tuple[str, str], set[str]] = defaultdict(set)
    for sex, path in (("M", men_history), ("L", ladies_history)):
        _, rows = _read(path)
        for number, row in enumerate(rows, 2):
            athlete_id = (row.get("ID") or "").strip()
            name = (row.get("Skier") or "").strip()
            if not athlete_id or not name:
                raise IdentityError(f"{path}:{number}: missing ID or Skier")
            key = (sex, athlete_id)
            names[key].add(name)
            index[(sex, normalized_name(name))].add(athlete_id)
            observed = (row.get("Date") or "").strip()
            if observed >= latest.get(key, ""):
                athletes[key] = Athlete(sex, athlete_id, name, (row.get("Nation") or "").strip())
                latest[key] = observed
    return athletes, index, names


def load_aliases(
    path: Path,
    athletes: dict[tuple[str, str], Athlete],
    names: dict[tuple[str, str], set[str]],
) -> dict[str, dict[str, str]]:
    fields, rows = _read(path)
    if tuple(fields) != ALIAS_FIELDS:
        raise IdentityError(f"{path}: unexpected alias columns")
    result: dict[str, dict[str, str]] = {}
    for number, row in enumerate(rows, 2):
        if not all((row.get(field) or "").strip() for field in ALIAS_FIELDS):
            raise IdentityError(f"{path}:{number}: incomplete alias")
        fis_id = row["fis_competitor_id"]
        key = (row["canonical_sex"], row["canonical_id"])
        if fis_id in result:
            raise IdentityError(f"{path}:{number}: duplicate FIS competitor {fis_id}")
        if key not in athletes or row["canonical_name"] not in names[key]:
            raise IdentityError(f"{path}:{number}: unknown canonical identity {key}")
        result[fis_id] = row
    return result


def validate_memberships(rows: list[dict[str, str]], *, mapped: bool = False) -> None:
    athlete_field = "canonical_athlete_id" if mapped else "fis_competitor_id"
    event_athletes: dict[tuple[str, ...], set[str]] = defaultdict(set)
    teams: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for number, row in enumerate(rows, 2):
        event = tuple(row[field].strip() for field in ("date", "sex", "season", "race", "event_kind"))
        if not all(event):
            raise IdentityError(f"row {number}: incomplete event identity")
        athlete = row.get(athlete_field, "").strip()
        if not athlete:
            if mapped:
                fis_id = row.get("fis_competitor_id", "").strip()
                athlete = f"unresolved:{fis_id}" if fis_id else ""
            if not athlete:
                raise IdentityError(f"row {number}: missing {athlete_field}")
        if athlete in event_athletes[event]:
            raise IdentityError(f"row {number}: duplicate athlete in event {event}")
        event_athletes[event].add(athlete)
        if event[-1] == "individual":
            if row.get("member_order", "").strip():
                raise IdentityError(f"row {number}: individual row has member_order")
            continue
        if event[-1] not in {"team", "team_sprint"}:
            raise IdentityError(f"row {number}: unsupported event kind {event[-1]}")
        team = (row.get("team_name", "").strip(), row.get("team_nation", "").strip())
        if not all(team):
            raise IdentityError(f"row {number}: incomplete team identity")
        teams[(*event, *team)].append(row)
    for key, members in teams.items():
        expected = {"team_sprint": {2}, "team": {3, 4}}[key[4]]
        if len(members) not in expected:
            raise IdentityError(f"team {key} has {len(members)} members; expected {sorted(expected)}")
        orders = sorted(int(row["member_order"]) for row in members)
        if orders != list(range(1, len(members) + 1)):
            raise IdentityError(f"team {key} has invalid member order {orders}")


def _atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv(rows: list[dict[str, object]], fields: list[str] | tuple[str, ...]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def map_identities(
    *, results: Path, men_history: Path, ladies_history: Path,
    aliases_path: Path, output_dir: Path,
) -> dict[str, object]:
    sources = [results, men_history, ladies_history, aliases_path]
    resolved = output_dir.resolve()
    if any(source.resolve().parent in resolved.parents or source.resolve().parent == resolved for source in sources):
        raise IdentityError("identity output cannot be inside a source directory")
    fields, rows = _read(results)
    required = {"date", "sex", "season", "race", "event_kind", "place", "team_name", "team_nation", "member_order", "fis_competitor_id", "fis_code", "name", "nation"}
    if missing := sorted(required - set(fields)):
        raise IdentityError(f"results candidate is missing fields: {missing}")
    if not rows:
        raise IdentityError("results candidate is empty")
    validate_memberships(rows)
    athletes, index, names = load_athletes(men_history, ladies_history)
    aliases = load_aliases(aliases_path, athletes, names)
    mapped: list[dict[str, object]] = []
    identities: dict[str, dict[str, object]] = {}
    unresolved: dict[str, dict[str, object]] = {}
    methods: dict[str, int] = defaultdict(int)
    for row in rows:
        event_sex = row["sex"]
        fis_id = row["fis_competitor_id"]
        alias = aliases.get(fis_id)
        candidates: list[Athlete] = []
        athlete: Athlete | None = None
        method = ""
        if alias:
            if alias["fis_code"] != row["fis_code"] or alias["fis_name"] != row["name"]:
                raise IdentityError(f"FIS identity changed for reviewed alias {fis_id}")
            athlete = athletes[(alias["canonical_sex"], alias["canonical_id"])]
            method = "reviewed_alias"
        else:
            sexes = ("M", "L") if event_sex == "Mixed" else (event_sex,)
            for sex in sexes:
                candidates.extend(athletes[(sex, athlete_id)] for athlete_id in index.get((sex, normalized_name(row["name"])), set()))
            if len(candidates) == 1:
                athlete = candidates[0]
                method = "unique_normalized_name"
        if athlete and event_sex != "Mixed" and athlete.sex != event_sex:
            raise IdentityError(f"identity sex conflicts with event sex for {fis_id}")
        if athlete is None:
            unresolved[fis_id] = {
                "event_sex": event_sex, "fis_competitor_id": fis_id,
                "fis_code": row["fis_code"], "fis_name": row["name"],
                "fis_nation": row["nation"],
                "reason": "missing" if not candidates else "ambiguous",
                "candidate_identities": ";".join(f"{item.sex}:{item.athlete_id}:{item.name}" for item in candidates),
            }
            mapped.append({**row, **{field: "" for field in MAPPED_FIELDS}})
            continue
        values = {
            "canonical_athlete_id": athlete.athlete_id,
            "canonical_name": athlete.name,
            "canonical_nation": athlete.nation,
            "canonical_sex": athlete.sex,
            "identity_match_method": method,
        }
        mapped.append({**row, **values})
        methods[method] += 1
        identities[fis_id] = {
            "fis_competitor_id": fis_id, "fis_code": row["fis_code"],
            "fis_name": row["name"], **values,
        }
    validate_memberships(mapped, mapped=True)
    identity_fields = ["fis_competitor_id", "fis_code", "fis_name", *MAPPED_FIELDS]
    unresolved_fields = ["event_sex", "fis_competitor_id", "fis_code", "fis_name", "fis_nation", "reason", "candidate_identities"]
    _atomic(output_dir / "mapped_results.csv", _csv(mapped, [*fields, *MAPPED_FIELDS]))
    _atomic(output_dir / "identity_map.csv", _csv([identities[key] for key in sorted(identities)], identity_fields))
    _atomic(output_dir / "unresolved_identities.csv", _csv([unresolved[key] for key in sorted(unresolved)], unresolved_fields))
    report = {
        "schema_version": "nordicnumbers.nordic-combined-results-identity/v1",
        "input_sha256": hashlib.sha256(results.read_bytes()).hexdigest(),
        "result_rows": len(mapped), "unique_fis_athletes": len(identities) + len(unresolved),
        "resolved_fis_athletes": len(identities), "unresolved_fis_athletes": len(unresolved),
        "match_methods": dict(sorted(methods.items())),
        "status": "mapped" if not unresolved else "blocked",
        "replay_eligible": not unresolved, "accepted_histories_modified": False,
    }
    _atomic(output_dir / "identity_report.json", json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--men-history", required=True, type=Path)
    parser.add_argument("--ladies-history", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    report = map_identities(results=args.results, men_history=args.men_history, ladies_history=args.ladies_history, aliases_path=args.aliases, output_dir=args.output_dir)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if not report["replay_eligible"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
