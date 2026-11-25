#!/usr/bin/env python3
"""Normalize acquired ski-jumping startlists into prediction candidates.

The normalizer resolves immutable FIS identities against accepted histories,
selects the latest rating strictly before the logical race date, and writes
prediction-compatible individual and team CSVs beneath an isolated directory.
It cannot mutate the accepted startlist files.
"""

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

from skijump_results_identity import Alias, load_histories, normalized_name

ELO_FIELDS = (
    "Elo",
    "Small_Elo",
    "Medium_Elo",
    "Normal_Elo",
    "Large_Elo",
    "Flying_Elo",
)
INDIVIDUAL_BASE_FIELDS = (
    "FISName",
    "Skier",
    "ID",
    "Nation",
    "StartOrder",
    "Bib",
    "Is_Host_Nation",
    "In_Startlist",
    "Rank",
    "Year",
    "Length1",
    "Length2",
    "Points",
    "Hill_Size",
    *ELO_FIELDS,
    "Race_Elo",
)
INDIVIDUAL_TRAILING_FIELDS = ("City", "Country", "Race_Type", "Race_Date")
TEAM_BASE_FIELDS = (
    "Team_Name",
    "Nation",
    "Team_Rank",
    "Race_Date",
    "City",
    "Country",
    "Race_Type",
    "Hill_Size",
    "Is_Present",
    "Race1_Prob",
    "Team_Points",
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


class StartlistNormalizationError(RuntimeError):
    """Raised when prediction-compatible candidates cannot be trusted."""


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _parse_date(value: str, *, path: Path, row_number: int) -> date:
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            pass
    raise StartlistNormalizationError(
        f"{path}:{row_number}: unsupported date {value!r}"
    )


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]], fields: Iterable[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        parent = source.expanduser().resolve().parent
        if resolved == parent or parent in resolved.parents:
            raise StartlistNormalizationError(
                f"candidate output {resolved} cannot be inside source directory {parent}"
            )
    return resolved


def _latest_ratings(path: Path, logical_date: date) -> dict[str, dict[str, str]]:
    fields, rows = _read_csv(path)
    required = {"Date", "ID", "Skier", "Nation", *ELO_FIELDS}
    missing = sorted(required - set(fields))
    if missing:
        raise StartlistNormalizationError(f"{path} is missing rating fields: {missing}")
    latest: dict[str, tuple[date, int, dict[str, str]]] = {}
    for row_number, row in enumerate(rows, start=2):
        athlete_id = (row.get("ID") or "").strip()
        if not athlete_id:
            raise StartlistNormalizationError(f"{path}:{row_number}: missing ID")
        observed = _parse_date(row.get("Date", ""), path=path, row_number=row_number)
        if observed >= logical_date:
            continue
        race = int((row.get("Race") or "0").strip() or 0)
        previous = latest.get(athlete_id)
        if previous is None or (observed, race) >= previous[:2]:
            latest[athlete_id] = (observed, race, row)
    return {athlete_id: value[2] for athlete_id, value in latest.items()}


def _number(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _quartiles(ratings: dict[str, dict[str, str]]) -> dict[str, float]:
    result: dict[str, float] = {}
    for field in ELO_FIELDS:
        values = sorted(
            number
            for row in ratings.values()
            if (number := _number(row.get(field, ""))) is not None
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


def _rating_values(
    row: dict[str, str], quartiles: dict[str, float]
) -> dict[str, float]:
    return {
        field: value
        if (value := _number(row.get(field, ""))) is not None
        else quartiles[field]
        for field in ELO_FIELDS
    }


def _hill_category(hill_size: str, race_type: str) -> str:
    lowered = race_type.casefold()
    if "flying" in lowered:
        return "Flying"
    if "large" in lowered:
        return "Large"
    if "normal" in lowered:
        return "Normal"
    try:
        size = int(float(hill_size))
    except ValueError:
        return "Elo"
    if size >= 185:
        return "Flying"
    if size >= 110:
        return "Large"
    if size >= 85:
        return "Normal"
    if size >= 50:
        return "Medium"
    return "Small"


def _race_elo(values: dict[str, float], hill_size: str, race_type: str) -> float:
    category = _hill_category(hill_size, race_type)
    priority = {
        "Small": ("Small_Elo", "Elo"),
        "Medium": ("Medium_Elo", "Elo"),
        "Normal": ("Normal_Elo", "Elo"),
        "Large": ("Large_Elo", "Elo"),
        "Flying": ("Flying_Elo", "Large_Elo", "Elo"),
        "Elo": ("Elo",),
    }
    return next(values[field] for field in priority[category])


def _resolve_identities(
    *,
    rows: list[dict[str, str]],
    men_history: Path,
    ladies_history: Path,
    aliases_path: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, int]]:
    athletes, name_index, names_by_id = load_histories(men_history, ladies_history)
    relevant_fis_ids = {row["fis_competitor_id"].strip() for row in rows}
    _, alias_rows = _read_csv(aliases_path)
    aliases: dict[str, Alias] = {}
    for row_number, row in enumerate(alias_rows, start=2):
        competitor_id = (row.get("fis_competitor_id") or "").strip()
        if competitor_id not in relevant_fis_ids:
            continue
        alias = Alias(
            fis_competitor_id=competitor_id,
            fis_code=(row.get("fis_code") or "").strip(),
            fis_name=(row.get("fis_name") or "").strip(),
            canonical_sex=(row.get("canonical_sex") or "").strip(),
            canonical_id=(row.get("canonical_id") or "").strip(),
            canonical_name=(row.get("canonical_name") or "").strip(),
        )
        if alias.fis_competitor_id in aliases:
            raise StartlistNormalizationError(
                f"{aliases_path}:{row_number}: duplicate relevant FIS alias {competitor_id}"
            )
        canonical_key = (alias.canonical_sex, alias.canonical_id)
        if alias.canonical_sex not in {"M", "L"} or canonical_key not in athletes:
            raise StartlistNormalizationError(
                f"{aliases_path}:{row_number}: unknown canonical athlete {canonical_key}"
            )
        if alias.canonical_name not in names_by_id[canonical_key]:
            raise StartlistNormalizationError(
                f"{aliases_path}:{row_number}: canonical name does not belong to {canonical_key}"
            )
        aliases[competitor_id] = alias
    mapped: list[dict[str, object]] = []
    unresolved: dict[str, dict[str, object]] = {}
    methods: dict[str, int] = defaultdict(int)
    identity_by_fis_id: dict[str, tuple[str, str]] = {}
    pending_mixed: list[tuple[dict[str, str], list[object]]] = []

    def append_identity(
        row: dict[str, str],
        *,
        sex: str,
        athlete_id: str,
        name: str,
        nation: str,
        method: str,
    ) -> None:
        competitor_id = row["fis_competitor_id"].strip()
        identity = (sex, athlete_id)
        previous = identity_by_fis_id.get(competitor_id)
        if previous and previous != identity:
            raise StartlistNormalizationError(
                f"conflicting canonical identity for FIS competitor {competitor_id}"
            )
        identity_by_fis_id[competitor_id] = identity
        methods[method] += 1
        mapped.append(
            {
                **row,
                "canonical_athlete_id": athlete_id,
                "canonical_name": name,
                "canonical_nation": nation,
                "canonical_sex": sex,
                "identity_match_method": method,
            }
        )

    for row in rows:
        event_sex = row["sex"].strip()
        competitor_id = row["fis_competitor_id"].strip()
        fis_code = row["fis_code"].strip()
        fis_name = row["name"].strip()
        alias = aliases.get(competitor_id)
        candidates = []
        canonical = None
        method = ""
        if alias:
            if alias.fis_code != fis_code or alias.fis_name != fis_name:
                raise StartlistNormalizationError(
                    f"FIS identity changed for reviewed alias {competitor_id}"
                )
            if event_sex != "Mixed" and alias.canonical_sex != event_sex:
                raise StartlistNormalizationError(
                    f"reviewed alias {competitor_id} conflicts with event sex"
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
        if canonical is None:
            if candidates:
                unresolved[competitor_id] = {
                    "event_sex": event_sex,
                    "fis_competitor_id": competitor_id,
                    "fis_code": fis_code,
                    "fis_name": fis_name,
                    "fis_nation": row["nation"].strip(),
                    "reason": "ambiguous",
                    "candidate_identities": ";".join(
                        f"{item.sex}:{item.athlete_id}:{item.name}"
                        for item in candidates
                    ),
                }
            elif event_sex in {"M", "L"}:
                append_identity(
                    row,
                    sex=event_sex,
                    athlete_id=f"-{competitor_id}",
                    name=fis_name,
                    nation=row["nation"].strip(),
                    method="provisional_fis_identity",
                )
            else:
                pending_mixed.append((row, candidates))
            continue
        append_identity(
            row,
            sex=canonical.sex,
            athlete_id=canonical.athlete_id,
            name=canonical.name,
            nation=canonical.nation,
            method=method,
        )

    for row, candidates in pending_mixed:
        competitor_id = row["fis_competitor_id"].strip()
        team_key = (
            row["logical_date"],
            row["race"],
            row["team_nation"],
            row["team_name"],
        )
        member_order = int(row["member_order"])
        same_parity = {
            str(item["canonical_sex"])
            for item in mapped
            if (
                item["logical_date"],
                item["race"],
                item["team_nation"],
                item["team_name"],
            )
            == team_key
            and int(str(item["member_order"])) % 2 == member_order % 2
        }
        opposite_parity = {
            str(item["canonical_sex"])
            for item in mapped
            if (
                item["logical_date"],
                item["race"],
                item["team_nation"],
                item["team_name"],
            )
            == team_key
            and int(str(item["member_order"])) % 2 != member_order % 2
        }
        inferred_sex = ""
        if len(same_parity) == 1:
            inferred_sex = next(iter(same_parity))
        elif len(opposite_parity) == 1:
            inferred_sex = "L" if next(iter(opposite_parity)) == "M" else "M"
        if inferred_sex:
            append_identity(
                row,
                sex=inferred_sex,
                athlete_id=f"-{competitor_id}",
                name=row["name"].strip(),
                nation=row["nation"].strip(),
                method="provisional_fis_identity",
            )
        else:
            unresolved[competitor_id] = {
                "event_sex": "Mixed",
                "fis_competitor_id": competitor_id,
                "fis_code": row["fis_code"].strip(),
                "fis_name": row["name"].strip(),
                "fis_nation": row["nation"].strip(),
                "reason": "mixed_team_sex_unresolved",
                "candidate_identities": ";".join(
                    f"{item.sex}:{item.athlete_id}:{item.name}" for item in candidates
                ),
            }
    return mapped, [unresolved[key] for key in sorted(unresolved)], dict(methods)


def _individual_candidates(
    rows: list[dict[str, object]],
    ratings: dict[str, dict[str, str]],
    logical_date: date,
) -> tuple[list[str], list[dict[str, object]]]:
    events = sorted({int(str(row["race"])) for row in rows})
    probabilities = [f"Race{index}_Prob" for index in range(1, len(events) + 1)]
    by_athlete: dict[str, dict[str, object]] = {}
    event_index = {race: index for index, race in enumerate(events, start=1)}
    quartiles = _quartiles(ratings)
    for row in rows:
        athlete_id = str(row["canonical_athlete_id"])
        rating = ratings.get(athlete_id)
        values = (
            quartiles.copy() if rating is None else _rating_values(rating, quartiles)
        )
        candidate = by_athlete.setdefault(
            athlete_id,
            {
                "FISName": row["name"],
                "Skier": row["canonical_name"],
                "ID": athlete_id,
                "Nation": row["canonical_nation"],
                "StartOrder": row["source_order"],
                "Bib": row["source_order"],
                "Is_Host_Nation": row["canonical_nation"] == row["country"],
                "In_Startlist": True,
                "Rank": "",
                "Year": row["birth_year"],
                "Length1": "",
                "Length2": "",
                "Points": "",
                "Hill_Size": row["hill_size"],
                **values,
                "Race_Elo": _race_elo(
                    values, str(row["hill_size"]), str(row["race_type"])
                ),
                **{field: 0.0 for field in probabilities},
                "City": row["city"],
                "Country": row["country"],
                "Race_Type": row["race_type"],
                "Race_Date": logical_date.strftime("%m/%d/%Y"),
            },
        )
        candidate[f"Race{event_index[int(str(row['race']))]}_Prob"] = 1.0
    ordered = sorted(
        by_athlete.values(), key=lambda row: (-float(row["Race_Elo"]), str(row["ID"]))
    )
    return [
        *INDIVIDUAL_BASE_FIELDS,
        *probabilities,
        *INDIVIDUAL_TRAILING_FIELDS,
    ], ordered


def _team_fields(member_count: int, mixed: bool) -> list[str]:
    aggregate = [
        item for field in ELO_FIELDS for item in (f"Total_{field}", f"Avg_{field}")
    ]
    members: list[str] = []
    for position in range(1, member_count + 1):
        prefix = f"Member_{position}"
        members.extend([prefix, f"{prefix}_ID", f"{prefix}_FisCode"])
        if mixed:
            members.append(f"{prefix}_Sex")
        members.extend(
            [
                f"{prefix}_Year",
                f"{prefix}_Length1",
                f"{prefix}_Length2",
                f"{prefix}_Points",
            ]
        )
        members.extend(f"{prefix}_{field}" for field in ELO_FIELDS)
    return [*TEAM_BASE_FIELDS, *aggregate, *members]


def _team_candidates(
    rows: list[dict[str, object]],
    men_ratings: dict[str, dict[str, str]],
    ladies_ratings: dict[str, dict[str, str]],
    logical_date: date,
) -> tuple[list[str], list[dict[str, object]]]:
    event_keys = {(row["sex"], row["event_kind"], row["race"]) for row in rows}
    if len(event_keys) != 1:
        raise StartlistNormalizationError(
            "one prediction file cannot represent multiple same-category team events"
        )
    mixed = str(rows[0]["event_kind"]) == "mixed_team"
    by_team: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_team[(str(row["team_nation"]), str(row["team_name"]))].append(row)
    counts = {len(members) for members in by_team.values()}
    if len(counts) != 1 or next(iter(counts), 0) not in {2, 4}:
        raise StartlistNormalizationError(f"inconsistent team sizes: {sorted(counts)}")
    member_count = next(iter(counts))
    men_quartiles = _quartiles(men_ratings)
    ladies_quartiles = _quartiles(ladies_ratings)
    candidates: list[dict[str, object]] = []
    for (nation, team_name), members in sorted(by_team.items()):
        members.sort(key=lambda row: int(str(row["member_order"])))
        candidate: dict[str, object] = {
            "Team_Name": team_name,
            "Nation": nation,
            "Team_Rank": members[0]["source_order"],
            "Race_Date": logical_date.strftime("%m/%d/%Y"),
            "City": members[0]["city"],
            "Country": members[0]["country"],
            "Race_Type": members[0]["race_type"],
            "Hill_Size": members[0]["hill_size"],
            "Is_Present": True,
            "Race1_Prob": 1.0,
            "Team_Points": "",
        }
        totals = {field: 0.0 for field in ELO_FIELDS}
        for position, member in enumerate(members, start=1):
            sex = str(member["canonical_sex"])
            ratings = men_ratings if sex == "M" else ladies_ratings
            rating = ratings.get(str(member["canonical_athlete_id"]))
            quartiles = men_quartiles if sex == "M" else ladies_quartiles
            values = (
                quartiles.copy()
                if rating is None
                else _rating_values(rating, quartiles)
            )
            for field, value in values.items():
                totals[field] += value
            prefix = f"Member_{position}"
            candidate.update(
                {
                    prefix: member["canonical_name"],
                    f"{prefix}_ID": member["canonical_athlete_id"],
                    f"{prefix}_FisCode": member["fis_code"],
                    f"{prefix}_Year": member["birth_year"],
                    f"{prefix}_Length1": "",
                    f"{prefix}_Length2": "",
                    f"{prefix}_Points": "",
                    **{f"{prefix}_{field}": value for field, value in values.items()},
                }
            )
            if mixed:
                candidate[f"{prefix}_Sex"] = sex
        for field, total in totals.items():
            candidate[f"Total_{field}"] = total
            candidate[f"Avg_{field}"] = total / member_count
        candidates.append(candidate)
    return _team_fields(member_count, mixed), candidates


def normalize_startlists(
    *,
    acquired_startlists: Path,
    acquisition_report: Path,
    men_history: Path,
    ladies_history: Path,
    aliases_path: Path,
    logical_date: date,
    output_dir: Path,
) -> dict[str, object]:
    sources = (
        acquired_startlists,
        acquisition_report,
        men_history,
        ladies_history,
        aliases_path,
    )
    output_dir = _validate_output_dir(output_dir, sources)
    acquisition = json.loads(acquisition_report.read_text(encoding="utf-8"))
    if (
        acquisition.get("schema_version")
        != "nordicnumbers.skijump-startlist-acquisition/v1"
    ):
        raise StartlistNormalizationError("unsupported acquisition report schema")
    if acquisition.get("logical_date") != logical_date.isoformat():
        raise StartlistNormalizationError("acquisition logical date does not match")
    if acquisition.get("status") != "ready" or not acquisition.get(
        "prediction_eligible"
    ):
        raise StartlistNormalizationError("acquisition is not prediction eligible")
    acquired_sha256 = hashlib.sha256(acquired_startlists.read_bytes()).hexdigest()
    if acquisition.get("startlists_sha256") != acquired_sha256:
        raise StartlistNormalizationError(
            "acquired startlist checksum does not match report"
        )
    fields, rows = _read_csv(acquired_startlists)
    required = {
        "logical_date",
        "sex",
        "event_kind",
        "race",
        "city",
        "country",
        "hill_size",
        "race_type",
        "source_order",
        "team_name",
        "team_nation",
        "member_order",
        "fis_competitor_id",
        "fis_code",
        "name",
        "birth_year",
        "nation",
        "source_url",
        "source_sha256",
    }
    missing = sorted(required - set(fields))
    if missing:
        raise StartlistNormalizationError(
            f"acquired startlists missing fields: {missing}"
        )
    if not rows or any(row["logical_date"] != logical_date.isoformat() for row in rows):
        raise StartlistNormalizationError("acquired rows do not match logical date")
    mapped, unresolved, methods = _resolve_identities(
        rows=rows,
        men_history=men_history,
        ladies_history=ladies_history,
        aliases_path=aliases_path,
    )
    mapped_fields = [
        *fields,
        "canonical_athlete_id",
        "canonical_name",
        "canonical_nation",
        "canonical_sex",
        "identity_match_method",
    ]
    _atomic_text(output_dir / "mapped_startlists.csv", _csv_text(mapped, mapped_fields))
    _atomic_text(
        output_dir / "unresolved_identities.csv",
        _csv_text(unresolved, UNRESOLVED_FIELDS),
    )
    men_ratings = _latest_ratings(men_history, logical_date)
    ladies_ratings = _latest_ratings(ladies_history, logical_date)
    outputs: dict[str, dict[str, object]] = {}
    if not unresolved:
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in mapped:
            kind = str(row["event_kind"])
            sex = str(row["sex"])
            category = (
                "mixed_team"
                if kind == "mixed_team"
                else (
                    f"{'men' if sex == 'M' else 'ladies'}_team"
                    if kind == "team"
                    else ("men" if sex == "M" else "ladies")
                )
            )
            grouped[category].append(row)
        filenames = {
            "men": "startlist_races_men.csv",
            "ladies": "startlist_races_ladies.csv",
            "men_team": "startlist_team_races_men.csv",
            "ladies_team": "startlist_team_races_ladies.csv",
            "mixed_team": "startlist_mixed_team_races_teams.csv",
        }
        for category, category_rows in sorted(grouped.items()):
            if category in {"men", "ladies"}:
                ratings = men_ratings if category == "men" else ladies_ratings
                output_fields, output_rows = _individual_candidates(
                    category_rows, ratings, logical_date
                )
            else:
                output_fields, output_rows = _team_candidates(
                    category_rows, men_ratings, ladies_ratings, logical_date
                )
            path = output_dir / "candidates" / filenames[category]
            _atomic_text(path, _csv_text(output_rows, output_fields))
            outputs[category] = {
                "file": str(path.relative_to(output_dir)),
                "rows": len(output_rows),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.skijump-startlist-normalization/v1",
        "logical_date": logical_date.isoformat(),
        "input_sha256": acquired_sha256,
        "entrant_rows": len(rows),
        "mapped_rows": len(mapped),
        "unresolved_identities": len(unresolved),
        "match_methods": dict(sorted(methods.items())),
        "provisional_fis_identities": len(
            {
                str(row["canonical_athlete_id"])
                for row in mapped
                if row["identity_match_method"] == "provisional_fis_identity"
            }
        ),
        "baseline_rating_athletes": len(
            {
                (str(row["canonical_sex"]), str(row["canonical_athlete_id"]))
                for row in mapped
                if str(row["canonical_athlete_id"])
                not in (men_ratings if row["canonical_sex"] == "M" else ladies_ratings)
            }
        ),
        "status": "ready" if not unresolved else "blocked",
        "prediction_eligible": not unresolved and bool(outputs),
        "accepted_startlists_modified": False,
        "outputs": outputs,
    }
    _atomic_text(
        output_dir / "report.json", json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquired-startlists", required=True, type=Path)
    parser.add_argument("--acquisition-report", required=True, type=Path)
    parser.add_argument("--men-history", required=True, type=Path)
    parser.add_argument("--ladies-history", required=True, type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--logical-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--fail-on-unresolved", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = normalize_startlists(
        acquired_startlists=args.acquired_startlists,
        acquisition_report=args.acquisition_report,
        men_history=args.men_history,
        ladies_history=args.ladies_history,
        aliases_path=args.aliases,
        logical_date=args.logical_date,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if args.fail_on_unresolved and not report["prediction_eligible"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
