#!/usr/bin/env python3
"""Adapt finalized missing IBU evidence into policy-specific history candidates.

Unlike the historical parity adapter, this command expects the selected races
to be absent from accepted history. It emits separate candidates for the
individual and relay-inclusive trees because their legacy race-number policies
are intentionally different. Unknown identities fail closed for review.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from biathlon_official_adapter import (
    RESULT_FIELDS,
    Identity,
    IdentityResolver,
    _age,
    _distance,
    _numeric_rank,
    _race_type,
    _season_year,
    _sex_for_member,
    load_aliases,
)
from biathlon_runtime_shadow import fingerprint_tree
from ibu_source_contract import IBUContractError, competition_kind


REPORT_SCHEMA = "nordicnumbers.biathlon-additive-result-adapter/v1"
CAPTURE_SCHEMA = "nordicnumbers.biathlon-final-results-capture/v1"
NATION_NAMES = {
    "AND": "Andorra", "ARG": "Argentina", "AUS": "Australia",
    "AUT": "Austria", "BEL": "Belgium", "BIH": "Bosnia&Herzegovina",
    "BLR": "Belarus", "BUL": "Bulgaria", "CAN": "Canada",
    "CHI": "Chile", "CHN": "China", "CRC": "Costa Rica",
    "CRO": "Croatia", "CZE": "Czechia", "DEN": "Denmark",
    "ESP": "Spain", "EST": "Estonia", "FIN": "Finland",
    "FRA": "France", "GBR": "Great Britain", "GER": "Germany",
    "GRE": "Greece", "GRL": "Greenland", "GUM": "Guam",
    "HUN": "Hungary", "ITA": "Italy", "JPN": "Japan",
    "KAZ": "Kazakhstan", "KGZ": "Kyrgyzstan", "KOR": "South Korea",
    "LAT": "Latvia", "LTU": "Lithuania", "MDA": "Moldova",
    "MKD": "North Macedonia", "MGL": "Mongolia", "NED": "Netherlands",
    "NOR": "Norway", "NZL": "New Zealand", "POL": "Poland",
    "PUR": "Puerto Rico", "ROU": "Romania", "RUS": "Russia",
    "SRB": "Serbia", "SLO": "Slovenia", "SVK": "Slovakia",
    "SWE": "Sweden", "SUI": "Switzerland", "TPE": "Taiwan",
    "TUR": "Turkey", "UKR": "Ukraine", "USA": "USA",
    "UZB": "Uzbekistan",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise IBUContractError(f"invalid additive-adapter input: {path}") from error


def _json_sha256(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            return list(csv.DictReader(handle))
    except (OSError, csv.Error) as error:
        raise IBUContractError(f"invalid accepted Biathlon CSV: {path}") from error


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _safe_output(
    output_dir: Path,
    capture_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
) -> Path:
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise IBUContractError(f"additive-adapter output already exists: {output_dir}")
    for protected in (
        capture_dir.resolve(),
        individual_dir.resolve(),
        relay_dir.resolve(),
    ):
        if output_dir == protected or protected in output_dir.parents:
            raise IBUContractError(
                "additive-adapter output must be outside evidence and accepted data"
            )
    return output_dir


def _accepted_fingerprints(individual_dir: Path, relay_dir: Path) -> dict[str, Any]:
    return {
        "individual": fingerprint_tree(individual_dir),
        "relay": fingerprint_tree(relay_dir),
    }


def _race_numbers(
    competitions: list[dict[str, Any]], policy: str
) -> dict[tuple[str, str], int]:
    counters = Counter()
    numbers: dict[tuple[str, str], int] = {}
    for item in competitions:
        sex = item["sex"]
        if sex != "Mixed":
            counters[sex] += 1
            numbers[(item["race_id"], sex)] = counters[sex]
        elif policy == "individual":
            # Legacy individual history uses the men's all-event ordinal for
            # mixed rows without advancing the women's regular-race ordinal.
            counters["M"] += 1
            numbers[(item["race_id"], "M")] = counters["M"]
            numbers[(item["race_id"], "L")] = counters["M"]
        else:
            counters["M"] += 1
            counters["L"] += 1
            numbers[(item["race_id"], "M")] = counters["M"]
            numbers[(item["race_id"], "L")] = counters["L"]
    return numbers


def _schedule_index(path: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    result: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in _read_csv(path):
        day = datetime.strptime(row["Date"], "%m/%d/%Y").date().isoformat()
        key = day, row["RaceType"], row["Sex"]
        if key in result:
            raise IBUContractError(f"duplicate accepted schedule identity: {key}")
        result[key] = row
    return result


def _new_identity(
    bio: dict[str, Any], *, ibu_id: str, sex: str, local_id: str
) -> Identity:
    if (
        bio.get("IBUId") != ibu_id
        or {"M": "M", "W": "L", "L": "L"}.get(str(bio.get("GenderId"))) != sex
        or not bio.get("GivenName")
        or not bio.get("FamilyName")
        or not bio.get("Birthdate")
        or bio.get("NAT") not in NATION_NAMES
    ):
        raise IBUContractError(f"official biography is incomplete: {ibu_id}")
    try:
        birthday = datetime.fromisoformat(str(bio["Birthdate"])).isoformat()
    except ValueError as error:
        raise IBUContractError(f"official biography has invalid birthdate: {ibu_id}") from error
    return Identity(
        local_id=local_id,
        skier=f"{bio['GivenName']} {str(bio['FamilyName']).title()}",
        nation=NATION_NAMES[str(bio["NAT"])],
        birthday=birthday,
        sex=sex,
    )


def adapt_additive_results(
    *,
    capture_dir: Path,
    individual_dir: Path,
    relay_dir: Path,
    schedule_path: Path,
    output_dir: Path,
    aliases_path: Path | None = None,
    gap_evidence_dir: Path | None = None,
) -> dict[str, Any]:
    capture_dir = capture_dir.expanduser().resolve()
    individual_dir = individual_dir.expanduser().resolve()
    relay_dir = relay_dir.expanduser().resolve()
    schedule_path = schedule_path.expanduser().resolve()
    output_dir = _safe_output(output_dir, capture_dir, individual_dir, relay_dir)
    capture_path = capture_dir / "capture_report.json"
    capture = _load_json(capture_path)
    if (
        capture.get("schema_version") != CAPTURE_SCHEMA
        or capture.get("status") != "passed"
        or capture.get("accepted_data_mutated") is not False
        or capture.get("application") != {"status": "disabled", "performed": False}
        or capture.get("publication") != {"status": "disabled"}
        or not capture.get("races")
    ):
        raise IBUContractError("capture report is not a non-empty immutable passed bundle")

    before = _accepted_fingerprints(individual_dir, relay_dir)
    captured_expected = capture["accepted_fingerprints"]
    if (
        captured_expected.get("before") != captured_expected.get("after")
        or any(
            captured_expected["after"].get(policy) != before[policy]
            for policy in ("individual", "relay")
        )
    ):
        raise IBUContractError("accepted Biathlon histories drifted after capture")
    captured_schedule = captured_expected["after"].get("schedule", {})
    if captured_schedule.get("sha256") != _sha256(schedule_path):
        raise IBUContractError("accepted Biathlon schedule drifted after capture")

    gap_path = (
        gap_evidence_dir.expanduser().resolve() / "gap_report.json"
        if gap_evidence_dir is not None
        else Path(capture["source"]["gap_report"])
    )
    if not gap_path.is_file() or _sha256(gap_path) != capture["source"]["gap_report_sha256"]:
        raise IBUContractError("captured gap report is missing or drifted")
    gap = _load_json(gap_path)
    if (
        gap.get("schema_version") != "nordicnumbers.biathlon-results-gap/v1"
        or gap.get("status") != "passed"
        or gap.get("accepted_data_mutated") is not False
    ):
        raise IBUContractError("captured gap report is not a passed immutable audit")
    gap_raw = gap_path.parent / "raw"
    raw_events = _load_json(gap_raw / "events.json")
    if _json_sha256(raw_events) != gap["source"]["event_payload_sha256"]:
        raise IBUContractError("official event evidence hash mismatch")
    events = {row["EventId"]: row for row in raw_events}
    competitions: list[dict[str, Any]] = []
    raw_competitions: dict[str, dict[str, Any]] = {}
    for event_id in sorted(events):
        competition_payload = _load_json(
            gap_raw / f"{event_id}-competitions.json"
        )
        if (
            _json_sha256(competition_payload)
            != gap["source"]["competition_payload_sha256"].get(event_id)
        ):
            raise IBUContractError(
                f"official competition evidence hash mismatch: {event_id}"
            )
        for source in competition_payload:
            kind, sex = competition_kind(source)
            item = {
                "event_id": event_id,
                "race_id": source["RaceId"],
                "kind": kind,
                "gender_order": str(source.get("GenderOrder") or "").strip(),
                "sex": sex,
                "start": datetime.fromisoformat(source["StartTime"].replace("Z", "+00:00")),
            }
            competitions.append(item)
            raw_competitions[item["race_id"]] = source
    competitions.sort(key=lambda row: (row["start"], row["race_id"]))
    selected = {row["race_id"] for row in capture["races"]}
    selected_items = [row for row in competitions if row["race_id"] in selected]
    if len(selected_items) != len(selected):
        raise IBUContractError("captured races do not match gap competition evidence")

    histories: dict[str, dict[str, list[dict[str, str]]]] = {}
    all_identity_rows: list[dict[str, str]] = []
    for policy, root in (("individual", individual_dir), ("relay", relay_dir)):
        histories[policy] = {
            "M": _read_csv(root / "men_scrape_update.csv"),
            "L": _read_csv(root / "ladies_scrape_update.csv"),
        }
    aliases = load_aliases(aliases_path)
    resolver = IdentityResolver(
        [*histories["individual"]["M"], *histories["individual"]["L"]],
        aliases,
    )
    existing_alias_rows = _read_csv(aliases_path) if aliases_path is not None else []
    next_local_id = 1 + max(
        int(float(row["ID"]))
        for policy in ("individual", "relay")
        for sex in ("M", "L")
        for row in histories[policy][sex]
        if row.get("ID")
    )
    new_identities: dict[str, Identity] = {}
    new_alias_rows: list[dict[str, str]] = []
    schedule = _schedule_index(schedule_path)
    season = _season_year(capture["season_id"])
    existing_events = {
        policy: {
            (row["Date"], row["RaceType"], row["Sex"])
            for sex in ("M", "L")
            for row in histories[policy][sex]
            if row.get("Season") == str(season)
        }
        for policy in ("individual", "relay")
    }
    result_rows: dict[str, dict[str, list[dict[str, Any]]]] = {
        policy: {"M": [], "L": []} for policy in ("individual", "relay")
    }
    experience: dict[str, Counter] = {}
    for policy in ("individual", "relay"):
        experience[policy] = Counter()
        for sex in ("M", "L"):
            for row in histories[policy][sex]:
                if row.get("Season") == str(season):
                    key = sex, row["ID"]
                    experience[policy][key] = max(
                        experience[policy][key], int(float(row.get("Exp") or 0))
                    )
    numbers = {
        policy: _race_numbers(competitions, policy)
        for policy in ("individual", "relay")
    }
    unresolved: list[dict[str, str]] = []
    seen_identity: set[tuple[str, str]] = set()

    for item in selected_items:
        race_id, kind = item["race_id"], item["kind"]
        day = item["start"].date()
        race_type = _race_type(kind)
        identity = (day.isoformat(), race_type, item["sex"])
        compatibility = schedule.get(identity)
        if not compatibility:
            raise IBUContractError(f"accepted schedule lacks captured race: {race_id}")
        for policy in existing_events:
            result_sexes = ("M", "L") if item["sex"] == "Mixed" else (item["sex"],)
            if any((day.isoformat(), race_type, sex) in existing_events[policy] for sex in result_sexes):
                raise IBUContractError(f"captured race already exists in {policy} history: {race_id}")

        result_path = capture_dir / "raw" / f"{race_id}-results.json"
        captured = next(row for row in capture["races"] if row["race_id"] == race_id)
        payload = _load_json(result_path)
        if _json_sha256(payload) != captured["results_sha256"]:
            raise IBUContractError(f"captured result hash mismatch: {race_id}")
        source = raw_competitions[race_id]
        distance = compatibility.get("Distance") or _distance(source.get("ShortDescription", ""), kind)
        team_places = {
            (str(row.get("Bib", "")), str(row.get("Nat", ""))): _numeric_rank(row.get("Rank"))
            for row in payload.get("Results", []) if row.get("IsTeam")
        }
        single_seen: set[tuple[str, str]] = set()
        for source_row in payload.get("Results", []):
            if source_row.get("IsTeam"):
                continue
            leg = int(source_row.get("Leg") or 0)
            place = (
                team_places.get((str(source_row.get("Bib", "")), str(source_row.get("Nat", ""))))
                if item["sex"] == "Mixed" or kind == "relay"
                else _numeric_rank(source_row.get("Rank"))
            )
            if place is None:
                continue
            ibu_id = str(source_row.get("IBUId", ""))
            if kind == "single_mixed_relay":
                key = ibu_id, str(source_row.get("Bib", ""))
                if key in single_seen:
                    continue
                single_seen.add(key)
            sex = _sex_for_member(item["sex"], kind, leg, gender_order=item.get("gender_order", ""))
            profile, method = resolver.resolve(
                ibu_id=ibu_id,
                official_name=str(source_row.get("Name", "")),
                sex=sex,
            )
            if profile is None and ibu_id in new_identities:
                profile, method = new_identities[ibu_id], "new_official_ibu_identity"
            if profile is None:
                biography_receipts = capture.get("biographies", {}).get("sha256", {})
                bio_path = capture_dir / "raw" / f"{ibu_id}-bio.json"
                if bio_path.is_file() and ibu_id in biography_receipts:
                    bio = _load_json(bio_path)
                    if _json_sha256(bio) != biography_receipts[ibu_id]:
                        raise IBUContractError(
                            f"captured biography hash mismatch: {ibu_id}"
                        )
                    try:
                        profile = _new_identity(
                            bio, ibu_id=ibu_id, sex=sex, local_id=str(next_local_id)
                        )
                    except IBUContractError as error:
                        # Retain every unresolved athlete in identity_map.csv so
                        # failed runs can email and queue the actual identities.
                        method = f"invalid_official_biography: {error}"
                    if profile is not None:
                        next_local_id += 1
                        new_identities[ibu_id] = profile
                        new_alias_rows.append(
                            {
                                "IBUId": ibu_id,
                                "ID": profile.local_id,
                                "reason": "automatic official IBU debut identity",
                            }
                        )
                        method = "new_official_ibu_identity"

            if (race_id, ibu_id) not in seen_identity:
                mapped = {
                    "race_id": race_id,
                    "IBUId": ibu_id,
                    "official_name": str(source_row.get("Name", "")),
                    "sex": sex,
                    "local_id": profile.local_id if profile else "",
                    "accepted_name": profile.skier if profile else "",
                    "method": method,
                    "status": "resolved" if profile else "unresolved",
                }
                all_identity_rows.append(mapped)
                seen_identity.add((race_id, ibu_id))
                if not profile:
                    unresolved.append(mapped)
            if not profile:
                continue
            for policy in ("individual", "relay"):
                experience[policy][(sex, profile.local_id)] += 1
                result_rows[policy][sex].append(
                    {
                        "Date": day.isoformat(),
                        "City": compatibility["City"],
                        "Country": compatibility["Country"],
                        "Event": "World Cup" if events[item["event_id"]].get("EventClassificationId") == "BTSWRLCP" else events[item["event_id"]].get("Description", ""),
                        "Distance": distance,
                        "RaceType": race_type,
                        "MassStart": 1 if kind == "mass_start" else 0,
                        "Season": season,
                        "Race": numbers[policy][(race_id, sex)],
                        "Place": place,
                        "Skier": profile.skier,
                        "Nation": profile.nation,
                        "ID": profile.local_id,
                        "Sex": sex,
                        "Birthday": profile.birthday,
                        "Leg": leg,
                        "Age": _age(day, profile.birthday),
                        "Exp": experience[policy][(sex, profile.local_id)],
                    }
                )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        files: dict[str, Any] = {}
        for policy in ("individual", "relay"):
            for sex, label in (("M", "men"), ("L", "ladies")):
                path = staging / policy / f"{label}_results_candidate.csv"
                _write_csv(path, result_rows[policy][sex], RESULT_FIELDS)
                files[path.relative_to(staging).as_posix()] = {
                    "rows": len(result_rows[policy][sex]), "sha256": _sha256(path)
                }
        identity_path = staging / "identity_map.csv"
        _write_csv(identity_path, all_identity_rows, [
            "race_id", "IBUId", "official_name", "sex", "local_id",
            "accepted_name", "method", "status",
        ])
        files["identity_map.csv"] = {"rows": len(all_identity_rows), "sha256": _sha256(identity_path)}
        aliases_candidate = staging / "identity_aliases_candidate.csv"
        alias_rows = [*existing_alias_rows, *new_alias_rows]
        _write_csv(aliases_candidate, alias_rows, ["IBUId", "ID", "reason"])
        files["identity_aliases_candidate.csv"] = {
            "rows": len(alias_rows),
            "sha256": _sha256(aliases_candidate),
        }
        after = _accepted_fingerprints(individual_dir, relay_dir)
        mutated = before != after
        report = {
            "schema_version": REPORT_SCHEMA,
            "status": "passed" if not unresolved and not mutated else "failed",
            "season_id": capture["season_id"],
            "race_ids": sorted(selected),
            "identity": {
                "resolved": len(all_identity_rows) - len(unresolved),
                "unresolved": len(unresolved),
                "methods": dict(sorted(Counter(row["method"] for row in all_identity_rows).items())),
                "unresolved_examples": unresolved[:10],
                "new_official_identities": len(new_identities),
            },
            "aliases": {
                "source_file": str(aliases_path.resolve()) if aliases_path else None,
                "source_sha256": _sha256(aliases_path) if aliases_path else None,
                "candidate_rows": len(alias_rows),
                "new_rows": len(new_alias_rows),
            },
            "candidate_files": files,
            "race_number_policy": {
                "individual": "mixed advances men only; mixed rows use men's ordinal",
                "relay": "mixed advances both sexes",
            },
            "accepted_fingerprints": {"before": before, "after": after},
            "accepted_data_mutated": mutated,
            "ready_for_derived_rehearsal": not unresolved and not mutated,
            "application": {"status": "disabled", "performed": False},
            "publication": {"status": "disabled"},
        }
        (staging / "adapter_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        staging.replace(output_dir)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-dir", type=Path, required=True)
    parser.add_argument("--individual-dir", type=Path, default=here / "excel365")
    parser.add_argument("--relay-dir", type=Path, default=here / "relay/excel365")
    parser.add_argument("--schedule", dest="schedule_path", type=Path, default=here / "excel365/races.csv")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--aliases", dest="aliases_path", type=Path)
    parser.add_argument("--gap-evidence-dir", type=Path)
    args = parser.parse_args()
    try:
        report = adapt_additive_results(**vars(args))
    except IBUContractError as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
