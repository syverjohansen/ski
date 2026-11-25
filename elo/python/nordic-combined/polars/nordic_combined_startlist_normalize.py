#!/usr/bin/env python3
"""Normalize acquired Nordic Combined startlists into isolated model candidates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from nordic_combined_results_identity import load_aliases, load_athletes, normalized_name


ELO_FIELDS = (
    "Elo", "Individual_Elo", "Sprint_Elo", "MassStart_Elo",
    "IndividualCompact_Elo",
)
INDIVIDUAL_BASE_FIELDS = (
    "FISName", "Skier", "ID", "Nation", "StartOrder", "Bib",
    "Is_Host_Nation", "In_Startlist", "Rank", "Year", "Jump", "Points",
    "JumpRank", "Time", "TimeDiff", *ELO_FIELDS, "Race_Elo",
)
INDIVIDUAL_TRAILING_FIELDS = ("City", "Country", "Race_Type", "Race_Date")
TEAM_BASE_FIELDS = (
    "Team_Name", "Nation", "Team_Rank", "Race_Date", "City", "Country",
    "Race_Type", "Is_Present", "Race1_Prob", "Team_Time", "Team_TimeDiff",
    "Team_Points",
)
UNRESOLVED_FIELDS = (
    "event_sex", "fis_competitor_id", "fis_code", "fis_name", "fis_nation",
    "reason", "candidate_identities",
)
FILENAMES = {
    "men": "startlist_races_men.csv",
    "ladies": "startlist_races_ladies.csv",
    "men_team": "startlist_team_races_men.csv",
    "ladies_team": "startlist_team_races_ladies.csv",
    "men_team_sprint": "startlist_team_sprint_races_men.csv",
    "ladies_team_sprint": "startlist_team_sprint_races_ladies.csv",
    "mixed_team": "startlist_mixed_team_races_teams.csv",
    "mixed_team_sprint": "startlist_mixed_team_sprint_races_teams.csv",
}


class StartlistNormalizationError(RuntimeError):
    """Raised when model-ready candidates cannot be trusted."""


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or ()), list(reader)


def _atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv(rows: list[dict[str, object]], fields: Iterable[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _validate_output(output: Path, sources: Iterable[Path]) -> Path:
    resolved = output.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise StartlistNormalizationError(
                f"candidate output {resolved} cannot be inside source directory {parent}"
            )
    return resolved


def _parse_date(value: str, path: Path, number: int) -> date:
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            pass
    raise StartlistNormalizationError(f"{path}:{number}: unsupported date {value!r}")


def _latest_ratings(path: Path, logical_date: date) -> dict[str, dict[str, str]]:
    fields, rows = _read(path)
    required = {"Date", "Race", "ID", "Skier", "Nation", *ELO_FIELDS}
    if missing := sorted(required - set(fields)):
        raise StartlistNormalizationError(f"{path} is missing rating fields: {missing}")
    latest: dict[str, tuple[date, int, dict[str, str]]] = {}
    for number, row in enumerate(rows, 2):
        athlete_id = row["ID"].strip()
        observed = _parse_date(row["Date"], path, number)
        if not athlete_id or observed >= logical_date:
            continue
        race = int(row["Race"] or 0)
        if athlete_id not in latest or (observed, race) >= latest[athlete_id][:2]:
            latest[athlete_id] = (observed, race, row)
    return {key: value[2] for key, value in latest.items()}


def _number(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _quartiles(ratings: dict[str, dict[str, str]]) -> dict[str, float]:
    result: dict[str, float] = {}
    for field in ELO_FIELDS:
        values = sorted(
            value for row in ratings.values()
            if (value := _number(row.get(field, ""))) is not None
        )
        if not values:
            result[field] = 1300.0
            continue
        position = (len(values) - 1) * 0.25
        lower = int(position)
        upper = min(lower + 1, len(values) - 1)
        fraction = position - lower
        result[field] = values[lower] * (1 - fraction) + values[upper] * fraction
    return result


def _rating_values(row: dict[str, str] | None, fallback: dict[str, float]) -> dict[str, float]:
    if row is None:
        return fallback.copy()
    return {
        field: value if (value := _number(row.get(field, ""))) is not None else fallback[field]
        for field in ELO_FIELDS
    }


def _race_elo(values: dict[str, float], race_type: str) -> float:
    normalized = "".join(race_type.casefold().split())
    field = {
        "individualcompact": "IndividualCompact_Elo",
        "massstart": "MassStart_Elo",
        "sprint": "Sprint_Elo",
    }.get(normalized, "Individual_Elo")
    return values.get(field) or values["Elo"]


def _resolve(
    rows: list[dict[str, str]], men_history: Path, ladies_history: Path, aliases_path: Path
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, int]]:
    athletes, index, names = load_athletes(men_history, ladies_history)
    aliases = load_aliases(aliases_path, athletes, names)
    mapped: list[dict[str, object]] = []
    unresolved: dict[str, dict[str, object]] = {}
    methods: dict[str, int] = defaultdict(int)
    identities: dict[str, tuple[str, str]] = {}
    for row in rows:
        event_sex = row["sex"].strip()
        fis_id = row["fis_competitor_id"].strip()
        alias = aliases.get(fis_id)
        candidates = []
        athlete = None
        method = ""
        if alias:
            if alias["fis_code"] != row["fis_code"] or alias["fis_name"] != row["name"]:
                raise StartlistNormalizationError(f"FIS identity changed for reviewed alias {fis_id}")
            athlete = athletes[(alias["canonical_sex"], alias["canonical_id"])]
            method = "reviewed_alias"
        else:
            sexes = ("M", "L") if event_sex == "Mixed" else (event_sex,)
            for sex in sexes:
                candidates.extend(
                    athletes[(sex, athlete_id)]
                    for athlete_id in index.get((sex, normalized_name(row["name"])), set())
                )
            if len(candidates) == 1:
                athlete = candidates[0]
                method = "unique_normalized_name"
        if athlete is None:
            unresolved[fis_id] = {
                "event_sex": event_sex, "fis_competitor_id": fis_id,
                "fis_code": row["fis_code"], "fis_name": row["name"],
                "fis_nation": row["nation"],
                "reason": "missing" if not candidates else "ambiguous",
                "candidate_identities": ";".join(
                    f"{item.sex}:{item.athlete_id}:{item.name}" for item in candidates
                ),
            }
            continue
        if event_sex != "Mixed" and athlete.sex != event_sex:
            raise StartlistNormalizationError(f"identity sex conflicts with event sex for {fis_id}")
        identity = (athlete.sex, athlete.athlete_id)
        if fis_id in identities and identities[fis_id] != identity:
            raise StartlistNormalizationError(f"conflicting identity for FIS competitor {fis_id}")
        identities[fis_id] = identity
        methods[method] += 1
        mapped.append({
            **row, "canonical_athlete_id": athlete.athlete_id,
            "canonical_name": athlete.name, "canonical_nation": athlete.nation,
            "canonical_sex": athlete.sex, "identity_match_method": method,
        })
    return mapped, [unresolved[key] for key in sorted(unresolved)], dict(methods)


def _category(row: dict[str, object]) -> str:
    sex, kind = str(row["sex"]), str(row["event_kind"])
    if sex == "Mixed":
        return "mixed_team_sprint" if kind == "team_sprint" else "mixed_team"
    label = "men" if sex == "M" else "ladies"
    return label if kind == "individual" else f"{label}_{kind}"


def _individual(
    rows: list[dict[str, object]], ratings: dict[str, dict[str, str]], logical_date: date
) -> tuple[list[str], list[dict[str, object]]]:
    races = sorted({int(str(row["race"])) for row in rows})
    probabilities = [f"Race{index}_Prob" for index in range(1, len(races) + 1)]
    race_index = {race: index for index, race in enumerate(races, 1)}
    fallback = _quartiles(ratings)
    candidates: dict[str, dict[str, object]] = {}
    for row in rows:
        athlete_id = str(row["canonical_athlete_id"])
        values = _rating_values(ratings.get(athlete_id), fallback)
        candidate = candidates.setdefault(athlete_id, {
            "FISName": row["name"], "Skier": row["canonical_name"], "ID": athlete_id,
            "Nation": row["canonical_nation"], "StartOrder": row["source_order"],
            "Bib": row["source_order"],
            "Is_Host_Nation": row["canonical_nation"] == row["country"],
            "In_Startlist": True, "Rank": "", "Year": row["birth_year"],
            "Jump": "", "Points": "", "JumpRank": "", "Time": "", "TimeDiff": "",
            **values, "Race_Elo": _race_elo(values, str(row["race_type"])),
            **{field: 0.0 for field in probabilities},
            "City": row["city"], "Country": row["country"],
            "Race_Type": row["race_type"], "Race_Date": logical_date.strftime("%m/%d/%Y"),
        })
        candidate[f"Race{race_index[int(str(row['race']))]}_Prob"] = 1.0
    ordered = sorted(candidates.values(), key=lambda item: (-float(item["Race_Elo"]), str(item["ID"])))
    return [*INDIVIDUAL_BASE_FIELDS, *probabilities, *INDIVIDUAL_TRAILING_FIELDS], ordered


def _team_fields(count: int, mixed: bool) -> list[str]:
    aggregate = [item for field in ELO_FIELDS for item in (f"Total_{field}", f"Avg_{field}")]
    members: list[str] = []
    for position in range(1, count + 1):
        prefix = f"Member_{position}"
        members.extend([prefix, f"{prefix}_ID", f"{prefix}_FisCode"])
        if mixed:
            members.append(f"{prefix}_Sex")
        members.extend([f"{prefix}_Year", f"{prefix}_Leg", f"{prefix}_Jump", f"{prefix}_Points"])
        members.extend(f"{prefix}_{field}" for field in ELO_FIELDS)
    return [*TEAM_BASE_FIELDS, *aggregate, *members]


def _teams(
    rows: list[dict[str, object]], men: dict[str, dict[str, str]],
    ladies: dict[str, dict[str, str]], logical_date: date,
) -> tuple[list[str], list[dict[str, object]]]:
    events = {(row["sex"], row["event_kind"], row["race"]) for row in rows}
    if len(events) != 1:
        raise StartlistNormalizationError("one candidate cannot represent multiple same-category team events")
    mixed = str(rows[0]["sex"]) == "Mixed"
    groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        groups[(str(row["team_nation"]), str(row["team_name"]))].append(row)
    sizes = {len(members) for members in groups.values()}
    kind = str(rows[0]["event_kind"])
    allowed = {2} if kind == "team_sprint" else {3, 4}
    if len(sizes) != 1 or not sizes <= allowed:
        raise StartlistNormalizationError(f"invalid {kind} team sizes: {sorted(sizes)}")
    count = next(iter(sizes))
    fallback = {"M": _quartiles(men), "L": _quartiles(ladies)}
    candidates = []
    for (nation, team_name), members in sorted(groups.items()):
        members.sort(key=lambda item: int(str(item["member_order"])))
        if [int(str(item["member_order"])) for item in members] != list(range(1, count + 1)):
            raise StartlistNormalizationError(f"invalid member order for {team_name}")
        candidate: dict[str, object] = {
            "Team_Name": team_name, "Nation": nation, "Team_Rank": members[0]["source_order"],
            "Race_Date": logical_date.strftime("%m/%d/%Y"), "City": members[0]["city"],
            "Country": members[0]["country"], "Race_Type": members[0]["race_type"],
            "Is_Present": True, "Race1_Prob": 1.0, "Team_Time": "",
            "Team_TimeDiff": "", "Team_Points": "",
        }
        totals = {field: 0.0 for field in ELO_FIELDS}
        for position, member in enumerate(members, 1):
            sex = str(member["canonical_sex"])
            history = men if sex == "M" else ladies
            values = _rating_values(history.get(str(member["canonical_athlete_id"])), fallback[sex])
            prefix = f"Member_{position}"
            candidate.update({
                prefix: member["canonical_name"], f"{prefix}_ID": member["canonical_athlete_id"],
                f"{prefix}_FisCode": member["fis_code"], f"{prefix}_Year": member["birth_year"],
                f"{prefix}_Leg": position, f"{prefix}_Jump": "", f"{prefix}_Points": "",
                **{f"{prefix}_{field}": value for field, value in values.items()},
            })
            if mixed:
                candidate[f"{prefix}_Sex"] = sex
            for field, value in values.items():
                totals[field] += value
        for field, total in totals.items():
            candidate[f"Total_{field}"] = total
            candidate[f"Avg_{field}"] = total / count
        candidates.append(candidate)
    return _team_fields(count, mixed), candidates


def normalize_startlists(
    *, acquired_startlists: Path, acquisition_report: Path, men_history: Path,
    ladies_history: Path, aliases_path: Path, logical_date: date, output_dir: Path,
) -> dict[str, object]:
    sources = (acquired_startlists, acquisition_report, men_history, ladies_history, aliases_path)
    output_dir = _validate_output(output_dir, sources)
    acquisition = json.loads(acquisition_report.read_text(encoding="utf-8"))
    if acquisition.get("schema_version") != "nordicnumbers.nordic-combined-startlist-acquisition/v1":
        raise StartlistNormalizationError("unsupported acquisition report schema")
    if acquisition.get("logical_date") != logical_date.isoformat():
        raise StartlistNormalizationError("acquisition logical date does not match")
    if acquisition.get("status") != "ready" or not acquisition.get("prediction_eligible"):
        raise StartlistNormalizationError("acquisition is not prediction eligible")
    digest = hashlib.sha256(acquired_startlists.read_bytes()).hexdigest()
    if acquisition.get("startlists_sha256") != digest:
        raise StartlistNormalizationError("acquired startlist checksum does not match report")
    fields, rows = _read(acquired_startlists)
    required = {
        "logical_date", "sex", "event_kind", "race", "city", "country", "distance",
        "race_type", "source_order", "team_name", "team_nation", "member_order",
        "fis_competitor_id", "fis_code", "name", "birth_year", "nation",
        "source_url", "source_sha256",
    }
    if missing := sorted(required - set(fields)):
        raise StartlistNormalizationError(f"acquired startlists missing fields: {missing}")
    if not rows or any(row["logical_date"] != logical_date.isoformat() for row in rows):
        raise StartlistNormalizationError("acquired rows do not match logical date")
    mapped, unresolved, methods = _resolve(rows, men_history, ladies_history, aliases_path)
    mapped_fields = [*fields, "canonical_athlete_id", "canonical_name", "canonical_nation", "canonical_sex", "identity_match_method"]
    _atomic(output_dir / "mapped_startlists.csv", _csv(mapped, mapped_fields))
    _atomic(output_dir / "unresolved_identities.csv", _csv(unresolved, UNRESOLVED_FIELDS))
    men = _latest_ratings(men_history, logical_date)
    ladies = _latest_ratings(ladies_history, logical_date)
    outputs: dict[str, dict[str, object]] = {}
    if not unresolved:
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in mapped:
            grouped[_category(row)].append(row)
        for category, category_rows in sorted(grouped.items()):
            if category in {"men", "ladies"}:
                output_fields, output_rows = _individual(
                    category_rows, men if category == "men" else ladies, logical_date
                )
            else:
                output_fields, output_rows = _teams(category_rows, men, ladies, logical_date)
            path = output_dir / "candidates" / FILENAMES[category]
            _atomic(path, _csv(output_rows, output_fields))
            outputs[category] = {
                "file": str(path.relative_to(output_dir)), "rows": len(output_rows),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.nordic-combined-startlist-normalization/v1",
        "logical_date": logical_date.isoformat(), "input_sha256": digest,
        "entrant_rows": len(rows), "mapped_rows": len(mapped),
        "unresolved_identities": len(unresolved), "match_methods": dict(sorted(methods.items())),
        "status": "ready" if not unresolved else "blocked",
        "prediction_eligible": not unresolved and bool(outputs),
        "accepted_startlists_modified": False, "outputs": outputs,
    }
    _atomic(output_dir / "report.json", json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquired-startlists", required=True, type=Path)
    parser.add_argument("--acquisition-report", required=True, type=Path)
    parser.add_argument("--men-history", required=True, type=Path)
    parser.add_argument("--ladies-history", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--logical-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--fail-on-unresolved", action="store_true")
    args = parser.parse_args()
    report = normalize_startlists(
        acquired_startlists=args.acquired_startlists, acquisition_report=args.acquisition_report,
        men_history=args.men_history, ladies_history=args.ladies_history,
        aliases_path=args.aliases, logical_date=args.logical_date, output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if args.fail_on_unresolved and not report["prediction_eligible"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
