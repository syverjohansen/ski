#!/usr/bin/env python3
"""Normalize official IBU startlists into isolated Biathlon model candidates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from biathlon_official_adapter import IdentityResolver, load_aliases


SCHEMA_VERSION = "nordicnumbers.biathlon-startlist-normalization/v1"
ACQUISITION_SCHEMA = "nordicnumbers.biathlon-startlist-acquisition/v1"
RATING_FIELDS = (
    "Elo", "Individual_Elo", "Sprint_Elo", "Pursuit_Elo", "MassStart_Elo",
)
FILES = {
    "men": "startlist_races_men.csv",
    "ladies": "startlist_races_ladies.csv",
    "men_relay": "startlist_relay_races_teams_men.csv",
    "ladies_relay": "startlist_relay_races_teams_ladies.csv",
    "mixed_relay": "startlist_mixed_relay_races_teams.csv",
    "single_mixed_relay": "startlist_single_mixed_relay_races_teams.csv",
}


class BiathlonNormalizationError(RuntimeError):
    """Official startlist rows cannot be normalized without guessing."""


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(payload)
        temporary = Path(handle.name)
    temporary.replace(path)


def _json(path: Path, document: Any) -> None:
    _atomic(path, (json.dumps(document, indent=2, sort_keys=True) + "\n").encode())


def _csv(rows: list[dict[str, Any]], fields: Iterable[str]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _day(value: str) -> date:
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            pass
    raise BiathlonNormalizationError(f"unsupported source date: {value!r}")


def _latest(path: Path, logical_date: date) -> tuple[list[dict[str, str]], dict[str, dict[str, str]]]:
    rows = _read_csv(path)
    latest: dict[str, tuple[date, int, dict[str, str]]] = {}
    for row in rows:
        athlete_id = row.get("ID", "").strip()
        observed = _day(row.get("Date", ""))
        if not athlete_id or observed >= logical_date:
            continue
        race = int(float(row.get("Race", "0") or 0))
        if athlete_id not in latest or (observed, race) >= latest[athlete_id][:2]:
            latest[athlete_id] = (observed, race, row)
    return rows, {key: value[2] for key, value in latest.items()}


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fallback(latest: dict[str, dict[str, str]]) -> dict[str, float]:
    result: dict[str, float] = {}
    for field in RATING_FIELDS:
        values = sorted(
            value for row in latest.values()
            if (value := _number(row.get(field))) is not None
        )
        result[field] = values[(len(values) - 1) // 4] if values else 1300.0
    return result


def _ratings(row: dict[str, str] | None, fallback: dict[str, float]) -> dict[str, float]:
    if row is None:
        return fallback.copy()
    return {
        field: value if (value := _number(row.get(field))) is not None else fallback[field]
        for field in RATING_FIELDS
    }


def _transient_id(ibu_id: str) -> str:
    """Stable positive numeric ID for an official debut, outside accepted ID space."""
    return str(900_000_000 + int(hashlib.sha256(ibu_id.encode()).hexdigest()[:8], 16) % 99_000_000)


def _resolve(
    *, row: dict[str, Any], sex: str, resolver: IdentityResolver
) -> dict[str, str]:
    ibu_id = str(row.get("IBUId", ""))
    official_name = str(row.get("Name", "")).strip()
    identity, method = resolver.resolve(ibu_id=ibu_id, official_name=official_name, sex=sex)
    if identity:
        return {
            "id": identity.local_id, "name": identity.skier,
            "nation": str(row.get("Nat", "")), "sex": sex, "method": method,
        }
    # A real athlete on a complete official IBU startlist is eligible even on debut.
    # The transient ID is isolated to prediction artifacts and never promoted to history.
    return {
        "id": _transient_id(ibu_id), "name": official_name,
        "nation": str(row.get("Nat", "")), "sex": sex,
        "method": "official_debut",
    }


def _member_sex(race: dict[str, Any], leg: int) -> str:
    if race["sex"] in {"M", "L"}:
        return str(race["sex"])
    order = str(race.get("gender_order", "")).upper().split("+")
    if len(order) != 2 or set(order) != {"M", "W"}:
        raise BiathlonNormalizationError(
            f"mixed race {race['race_id']} has no authoritative gender order"
        )
    first, second = ("L" if value == "W" else "M" for value in order)
    if race["event_kind"] == "single_mixed_relay":
        return first if leg % 2 else second
    return first if leg <= 2 else second


def _category(event_kind: str, sex: str) -> str:
    if event_kind == "relay":
        return "men_relay" if sex == "M" else "ladies_relay"
    return event_kind


def _individual_candidates(
    races: list[dict[str, Any]], resolver: IdentityResolver, logical_date: date
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    by_sex: dict[str, list[dict[str, Any]]] = {"M": [], "L": []}
    identities: list[dict[str, Any]] = []
    race_numbers: dict[str, dict[str, int]] = defaultdict(dict)
    for race in races:
        sex = race["sex"]
        if race["event_kind"] in {"relay", "mixed_relay", "single_mixed_relay"}:
            continue
        race_numbers[sex][race["race_id"]] = len(race_numbers[sex]) + 1
        payload = json.loads(Path(race["results_file"]).read_text(encoding="utf-8"))
        for row in payload["Results"]:
            if row.get("IsTeam"):
                continue
            identity = _resolve(row=row, sex=sex, resolver=resolver)
            identities.append({"race_id": race["race_id"], "IBUId": row["IBUId"], **identity})
            by_sex[sex].append({
                "race_id": race["race_id"], "source_order": row.get("StartOrder") or row.get("Bib"),
                "bib": row.get("Bib"), **identity,
            })

    output: dict[str, list[dict[str, Any]]] = {}
    for sex, rows in by_sex.items():
        if not rows:
            continue
        probabilities = [f"Race{index}_Prob" for index in range(1, len(race_numbers[sex]) + 1)]
        merged: dict[str, dict[str, Any]] = {}
        for row in rows:
            candidate = merged.setdefault(row["id"], {
                "ID": row["id"], "Skier": row["name"], "Nation": row["nation"],
                "Sex": sex, "StartOrder": row["source_order"], "Bib": row["bib"],
                "In_Startlist": True, **{column: 0.0 for column in probabilities},
                "Race_Date": logical_date.strftime("%m/%d/%Y"),
            })
            candidate[f"Race{race_numbers[sex][row['race_id']]}_Prob"] = 1.0
        output["men" if sex == "M" else "ladies"] = list(merged.values())
    return output, identities


def _relay_candidates(
    races: list[dict[str, Any]], resolver: IdentityResolver,
    latest: dict[str, dict[str, dict[str, str]]], logical_date: date,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    identities: list[dict[str, Any]] = []
    fallbacks = {sex: _fallback(latest[sex]) for sex in ("M", "L")}
    for race in races:
        kind, event_sex = race["event_kind"], race["sex"]
        if kind not in {"relay", "mixed_relay", "single_mixed_relay"}:
            continue
        payload = json.loads(Path(race["results_file"]).read_text(encoding="utf-8"))
        teams = {
            (str(row.get("Bib", "")), str(row.get("Nat", ""))): row
            for row in payload["Results"] if row.get("IsTeam")
        }
        members: dict[tuple[str, str], list[tuple[int, dict[str, Any], dict[str, str]]]] = defaultdict(list)
        seen: set[tuple[tuple[str, str], str]] = set()
        for row in payload["Results"]:
            if row.get("IsTeam"):
                continue
            key = (str(row.get("Bib", "")), str(row.get("Nat", "")))
            unique = (key, str(row.get("IBUId", "")))
            if unique in seen:
                continue
            seen.add(unique)
            sex = _member_sex(race, int(row.get("Leg") or 0))
            identity = _resolve(row=row, sex=sex, resolver=resolver)
            identities.append({"race_id": race["race_id"], "IBUId": row["IBUId"], **identity})
            members[key].append((int(row.get("Leg") or 0), row, identity))
        expected_size = 2 if kind == "single_mixed_relay" else 4
        if set(teams) != set(members):
            raise BiathlonNormalizationError(f"relay team/member mismatch for {race['race_id']}")
        candidates: list[dict[str, Any]] = []
        for key, team in sorted(teams.items()):
            roster = sorted(members[key], key=lambda item: item[0])
            if len(roster) != expected_size:
                raise BiathlonNormalizationError(
                    f"{race['race_id']} {key} has {len(roster)} unique members; expected {expected_size}"
                )
            candidate: dict[str, Any] = {
                "Team_Name": str(team.get("Name", "")), "Nation": key[1],
                "Team_Rank": team.get("StartOrder") or team.get("Bib"),
                "Race_Date": logical_date.strftime("%m/%d/%Y"),
                "City": race["schedule"]["City"], "Country": race["schedule"]["Country"],
                "Race_Type": race["schedule"]["RaceType"], "Is_Present": True,
                "Race1_Prob": 1.0,
            }
            totals = {field: 0.0 for field in RATING_FIELDS}
            for position, (_, source, identity) in enumerate(roster, 1):
                values = _ratings(latest[identity["sex"]].get(identity["id"]), fallbacks[identity["sex"]])
                prefix = f"Member_{position}"
                candidate.update({
                    prefix: identity["name"], f"{prefix}_ID": identity["id"],
                    f"{prefix}_Sex": identity["sex"], f"{prefix}_IBUId": source["IBUId"],
                    **{f"{prefix}_{field}": value for field, value in values.items()},
                })
                for field, value in values.items():
                    totals[field] += value
            for field, total in totals.items():
                candidate[f"Total_{field}"] = total
                candidate[f"Avg_{field}"] = total / expected_size
            candidates.append(candidate)
        output[_category(kind, event_sex)] = candidates
    return output, identities


def normalize_startlists(
    *, acquisition_dir: Path, men_history: Path, ladies_history: Path,
    aliases: Path | None, output_dir: Path,
) -> dict[str, Any]:
    report_path = acquisition_dir / "report.json"
    acquisition = json.loads(report_path.read_text(encoding="utf-8"))
    if acquisition.get("schema_version") != ACQUISITION_SCHEMA or acquisition.get("status") != "ready":
        raise BiathlonNormalizationError("only a complete ready acquisition can be normalized")
    logical_date = date.fromisoformat(acquisition["logical_date"])
    output_dir = output_dir.expanduser().resolve()
    if "excel365" in output_dir.parts:
        raise BiathlonNormalizationError("candidate output cannot be written inside accepted data")
    men_rows, men_latest = _latest(men_history, logical_date)
    ladies_rows, ladies_latest = _latest(ladies_history, logical_date)
    resolver = IdentityResolver([*men_rows, *ladies_rows], load_aliases(aliases))
    races = []
    for race in acquisition["races"]:
        if race.get("status") != "ready":
            raise BiathlonNormalizationError("acquisition contains a non-ready race")
        races.append({
            **race,
            "results_file": str(acquisition_dir / "raw" / f"{race['race_id']}-results.json"),
        })
    individual, identities_a = _individual_candidates(races, resolver, logical_date)
    relay, identities_b = _relay_candidates(
        races, resolver, {"M": men_latest, "L": ladies_latest}, logical_date
    )
    candidates = {**individual, **relay}
    expected = {
        _category(race["event_kind"], race["sex"])
        if race["event_kind"] in {"relay", "mixed_relay", "single_mixed_relay"}
        else ("men" if race["sex"] == "M" else "ladies")
        for race in races
    }
    if set(candidates) != expected:
        raise BiathlonNormalizationError(
            f"candidate categories do not match schedule: {sorted(candidates)} != {sorted(expected)}"
        )
    candidate_root = output_dir / "candidates"
    outputs: dict[str, Any] = {}
    for category, rows in sorted(candidates.items()):
        if not rows:
            raise BiathlonNormalizationError(f"empty normalized category: {category}")
        fields = list(dict.fromkeys(key for row in rows for key in row))
        payload = _csv(rows, fields)
        target = candidate_root / FILES[category]
        _atomic(target, payload)
        outputs[category] = {
            "file": str(target.relative_to(output_dir)), "rows": len(rows),
            "sha256": _sha256(payload),
        }
    identity_rows = identities_a + identities_b
    identity_payload = _csv(
        identity_rows, ("race_id", "IBUId", "id", "name", "nation", "sex", "method")
    )
    _atomic(output_dir / "identity_map.csv", identity_payload)
    final = {
        "schema_version": SCHEMA_VERSION,
        "status": "ready", "prediction_eligible": True,
        "logical_date": logical_date.isoformat(), "outputs": outputs,
        "identity": {
            "resolved": sum(row["method"] != "official_debut" for row in identity_rows),
            "official_debuts": sum(row["method"] == "official_debut" for row in identity_rows),
        },
        "accepted_startlists_modified": False,
        "publication": {"status": "disabled"},
    }
    _json(output_dir / "report.json", final)
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquisition-dir", type=Path, required=True)
    parser.add_argument("--men-history", type=Path, required=True)
    parser.add_argument("--ladies-history", type=Path, required=True)
    parser.add_argument("--aliases", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = normalize_startlists(
        acquisition_dir=args.acquisition_dir, men_history=args.men_history,
        ladies_history=args.ladies_history, aliases=args.aliases,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
