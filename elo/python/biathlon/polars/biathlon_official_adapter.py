#!/usr/bin/env python3
"""Adapt isolated official IBU evidence to Nordic Numbers compatibility rows.

The adapter is intentionally a shadow tool. It writes candidates, an identity
map, and parity evidence outside ``excel365``; it never promotes accepted data.
Unknown or ambiguous athletes remain unresolved and make parity fail closed.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import tempfile
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from ibu_source_contract import IBUContractError, competition_kind


REPORT_SCHEMA = "nordicnumbers.biathlon-official-adapter/v1"
RESULT_FIELDS = [
    "Date", "City", "Country", "Event", "Distance", "RaceType", "MassStart",
    "Season", "Race", "Place", "Skier", "Nation", "ID", "Sex", "Birthday",
    "Leg", "Age", "Exp",
]
SCHEDULE_FIELDS = [
    "Date", "Startlist", "Sex", "City", "Country", "Distance", "RaceType",
    "Period", "Elevation", "Championship", "Race",
]
CORE_SCHEDULE_FIELDS = [field for field in SCHEDULE_FIELDS if field != "Startlist"]
CORE_RESULT_FIELDS = [
    "Date", "City", "Country", "Event", "Distance", "RaceType", "MassStart",
    "Season", "Race", "Place", "Skier", "Nation", "ID", "Sex", "Birthday", "Leg",
]
RACE_TYPES = {
    "individual": "Individual",
    "short_individual": "Short Individual",
    "sprint": "Sprint",
    "pursuit": "Pursuit",
    "mass_start": "Mass Start",
    "relay": "Relay",
    "mixed_relay": "Mixed Relay",
    "single_mixed_relay": "Single Mixed Relay",
}
SPECIAL_CHARACTERS = str.maketrans({
    "æ": "ae", "ø": "o", "ł": "l", "ð": "d", "þ": "th", "ß": "ss",
})


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        temporary = Path(handle.name)
    temporary.replace(path)


def normalized_name(value: str, *, loose: bool = False) -> str:
    value = value.casefold().translate(SPECIAL_CHARACTERS)
    value = "".join(
        character for character in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(character)
    )
    tokens = re.findall(r"[a-z0-9]+", value)
    if loose:
        tokens = [
            re.sub(r"([aeiou])\1+", r"\1", token)
            .replace("ae", "a").replace("oe", "o").replace("ue", "u")
            for token in tokens
        ]
    return " ".join(sorted(tokens))


@dataclass(frozen=True)
class Identity:
    local_id: str
    skier: str
    nation: str
    birthday: str
    sex: str


class IdentityResolver:
    def __init__(
        self,
        histories: Iterable[dict[str, str]],
        aliases: dict[str, str] | None = None,
    ) -> None:
        profiles: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        for row in histories:
            profiles[(row["Sex"], row["ID"])].append(row)
        self.profiles: dict[tuple[str, str], Identity] = {}
        self.exact: dict[tuple[str, str], set[str]] = defaultdict(set)
        self.loose: dict[tuple[str, str], set[str]] = defaultdict(set)
        for (sex, local_id), rows in profiles.items():
            latest = max(rows, key=lambda row: (row["Date"], int(row["Exp"] or 0)))
            identity = Identity(
                local_id=local_id,
                skier=latest["Skier"],
                nation=latest["Nation"],
                birthday=latest["Birthday"],
                sex=sex,
            )
            self.profiles[(sex, local_id)] = identity
            for name in {row["Skier"] for row in rows}:
                self.exact[(sex, normalized_name(name))].add(local_id)
                self.loose[(sex, normalized_name(name, loose=True))].add(local_id)
        self.aliases = aliases or {}

    def resolve(self, *, ibu_id: str, official_name: str, sex: str) -> tuple[Identity | None, str]:
        alias = self.aliases.get(ibu_id)
        if alias:
            identity = self.profiles.get((sex, alias))
            return (identity, "reviewed_alias") if identity else (None, "invalid_alias")
        for method, index, key in (
            ("exact_name", self.exact, normalized_name(official_name)),
            ("loose_name", self.loose, normalized_name(official_name, loose=True)),
        ):
            candidates = index.get((sex, key), set())
            if len(candidates) == 1:
                return self.profiles[(sex, next(iter(candidates)))], method
            if len(candidates) > 1:
                return None, f"ambiguous_{method}"
        return None, "unresolved"


def load_aliases(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    aliases: dict[str, str] = {}
    for row in _read_csv(path):
        ibu_id, local_id = row.get("IBUId", "").strip(), row.get("ID", "").strip()
        if not re.fullmatch(r"[A-Z0-9_]+", ibu_id) or not local_id:
            raise IBUContractError(f"invalid reviewed identity alias: {row}")
        if ibu_id in aliases:
            raise IBUContractError(f"duplicate reviewed identity alias: {ibu_id}")
        aliases[ibu_id] = local_id
    return aliases


def _race_type(kind: str) -> str:
    try:
        return RACE_TYPES[kind]
    except KeyError as error:
        raise IBUContractError(f"unsupported compatibility race kind: {kind}") from error


def _distance(description: str, kind: str) -> str:
    if kind in {"relay", "mixed_relay", "single_mixed_relay"}:
        return "N/A"
    match = re.search(r"(?<!x)(\d+(?:\.\d+)?)\s*km", description, re.IGNORECASE)
    if not match:
        raise IBUContractError(f"cannot derive distance from {description!r}")
    return match.group(1)


def _sex_for_member(
    competition_sex: str, kind: str, leg: int, *, gender_order: str = ""
) -> str:
    if competition_sex in {"M", "L"}:
        return competition_sex
    # IBU publishes the starting gender per race; it changes between events.
    orders = {"M+W": ("M", "L"), "W+M": ("L", "M")}
    if gender_order not in orders or leg not in {1, 2, 3, 4}:
        raise IBUContractError(
            f"cannot derive member sex for {kind} leg {leg}, GenderOrder={gender_order!r}"
        )
    first, second = orders[gender_order]
    if kind == "single_mixed_relay":
        return first if leg % 2 else second
    if kind == "mixed_relay":
        return first if leg <= 2 else second
    raise IBUContractError(f"cannot derive member sex for {kind} leg {leg}")


def _numeric_rank(value: Any) -> int | None:
    text = str(value or "").strip()
    return int(text) if text.isdigit() else None


def _season_year(season_id: str) -> int:
    if not re.fullmatch(r"\d{4}", season_id):
        raise IBUContractError(f"invalid season id: {season_id}")
    return 2000 + int(season_id[2:])


def _period(day: date) -> int:
    if day.month in {11, 12}:
        return 1
    if day.month in {1, 2}:
        return 2
    return 3


def _age(day: date, birthday: str) -> float:
    born = datetime.fromisoformat(birthday).date()
    return (day - born).days / 365.25


def _accepted_schedule_by_event(
    rows: Iterable[dict[str, str]],
) -> dict[tuple[str, str, str], dict[str, str]]:
    result: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (row["Date"], row["RaceType"], row["Sex"])
        if key in result:
            raise IBUContractError(f"duplicate accepted schedule event: {key}")
        result[key] = row
    return result


def adapt_evidence(
    *,
    evidence_dir: Path,
    accepted_dir: Path,
    output_dir: Path,
    aliases_path: Path | None = None,
) -> dict[str, Any]:
    accepted_dir = accepted_dir.resolve()
    output_dir = output_dir.resolve()
    if accepted_dir == output_dir or accepted_dir in output_dir.parents:
        raise IBUContractError("adapter output must be outside accepted data directory")
    raw = evidence_dir.resolve() / "raw"
    events_payload = json.loads((raw / "events.json").read_text(encoding="utf-8"))
    competitions_payload = json.loads((raw / "competitions.json").read_text(encoding="utf-8"))
    source_report = json.loads((evidence_dir / "report.json").read_text(encoding="utf-8"))
    event_id = source_report["event_id"]
    season_id = source_report["season_id"]
    event = next((row for row in events_payload if row.get("EventId") == event_id), None)
    if not event:
        raise IBUContractError(f"evidence lacks selected event {event_id}")

    men_history = _read_csv(accepted_dir / "men_scrape_update.csv")
    ladies_history = _read_csv(accepted_dir / "ladies_scrape_update.csv")
    histories = [*men_history, *ladies_history]
    resolver = IdentityResolver(histories, load_aliases(aliases_path))
    accepted_races = _read_csv(accepted_dir / f"races{_season_year(season_id)}.csv")
    accepted_by_event = _accepted_schedule_by_event(accepted_races)

    selected_races = {item["race_id"] for item in source_report["races"]}
    all_competitions = []
    for source in competitions_payload:
        kind, sex = competition_kind(source)
        all_competitions.append({
            "source": source,
            "race_id": source["RaceId"],
            "kind": kind,
            "sex": sex,
            "start": datetime.fromisoformat(source["StartTime"].replace("Z", "+00:00")),
        })
    all_competitions.sort(key=lambda item: (item["start"], item["race_id"]))
    competitions = [
        item for item in all_competitions if item["race_id"] in selected_races
    ]

    result_race_numbers: dict[tuple[str, str], int] = {}
    schedule_race_numbers: dict[str, int] = {}
    counters = Counter()
    schedule_number_counters = Counter()
    for item in all_competitions:
        schedule_number_counters[item["sex"]] += 1
        schedule_race_numbers[item["race_id"]] = schedule_number_counters[item["sex"]]
        if item["sex"] == "Mixed":
            counters["M"] += 1
            result_race_numbers[(item["race_id"], "M")] = counters["M"]
            result_race_numbers[(item["race_id"], "L")] = counters["M"]
        else:
            counters[item["sex"]] += 1
            result_race_numbers[(item["race_id"], item["sex"])] = counters[item["sex"]]

    schedule_rows: list[dict[str, Any]] = []
    candidate_rows: dict[str, list[dict[str, Any]]] = {"M": [], "L": []}
    identity_rows: list[dict[str, Any]] = []
    experience = Counter()
    unresolved: list[dict[str, Any]] = []
    seen_identities: set[tuple[str, str]] = set()

    for item in competitions:
        source, race_id, kind = item["source"], item["race_id"], item["kind"]
        day = item["start"].date()
        race_type = _race_type(kind)
        distance = _distance(source.get("ShortDescription", ""), kind)
        compatibility = accepted_by_event.get(
            (day.strftime("%m/%d/%Y"), race_type, item["sex"]), {}
        )
        schedule_sex = item["sex"]
        schedule_rows.append({
            "Date": day.strftime("%m/%d/%Y"),
            "Startlist": "",
            "Sex": schedule_sex,
            "City": compatibility.get("City") or event.get("Organizer", ""),
            "Country": compatibility.get("Country") or event.get("NatLong", ""),
            "Distance": distance,
            "RaceType": race_type,
            "Period": compatibility.get("Period") or _period(day),
            "Elevation": compatibility.get("Elevation") or event.get("Altitude", ""),
            "Championship": compatibility.get("Championship") or 0,
            "Race": schedule_race_numbers[race_id],
        })

        payload_path = raw / f"{race_id}-results.json"
        if not payload_path.is_file():
            raise IBUContractError(f"missing captured results: {payload_path.name}")
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        rows = payload.get("Results", [])
        team_places = {
            (str(row.get("Bib", "")), str(row.get("Nat", ""))): _numeric_rank(row.get("Rank"))
            for row in rows if row.get("IsTeam")
        }
        single_mixed_seen: set[tuple[str, str]] = set()
        for row in rows:
            if row.get("IsTeam"):
                continue
            leg = int(row.get("Leg") or 0)
            place = (
                team_places.get((str(row.get("Bib", "")), str(row.get("Nat", ""))))
                if item["sex"] == "Mixed" or kind == "relay"
                else _numeric_rank(row.get("Rank"))
            )
            if place is None:
                continue
            ibu_id = str(row.get("IBUId", ""))
            if kind == "single_mixed_relay":
                key = (ibu_id, str(row.get("Bib", "")))
                if key in single_mixed_seen:
                    continue
                single_mixed_seen.add(key)
            sex = _sex_for_member(item["sex"], kind, leg, gender_order=item.get("gender_order", ""))
            identity, method = resolver.resolve(
                ibu_id=ibu_id, official_name=str(row.get("Name", "")), sex=sex
            )
            identity_key = (race_id, ibu_id)
            if identity_key not in seen_identities:
                identity_rows.append({
                    "race_id": race_id,
                    "IBUId": ibu_id,
                    "official_name": row.get("Name", ""),
                    "sex": sex,
                    "local_id": identity.local_id if identity else "",
                    "accepted_name": identity.skier if identity else "",
                    "method": method,
                    "status": "resolved" if identity else "unresolved",
                })
                seen_identities.add(identity_key)
            if not identity:
                unresolved.append(identity_rows[-1])
                continue
            experience[(sex, identity.local_id)] += 1
            candidate_rows[sex].append({
                "Date": day.isoformat(),
                "City": compatibility.get("City") or event.get("Organizer", ""),
                "Country": compatibility.get("Country") or event.get("NatLong", ""),
                "Event": "World Cup" if event.get("EventClassificationId") == "BTSWRLCP" else event.get("Description", ""),
                "Distance": distance,
                "RaceType": race_type,
                "MassStart": 1 if kind == "mass_start" else 0,
                "Season": _season_year(season_id),
                "Race": result_race_numbers[(race_id, sex)],
                "Place": place,
                "Skier": identity.skier,
                "Nation": identity.nation,
                "ID": identity.local_id,
                "Sex": sex,
                "Birthday": identity.birthday,
                "Leg": leg,
                "Age": _age(day, identity.birthday),
                "Exp": experience[(sex, identity.local_id)],
            })

    expected_events = {
        (item["start"].date().isoformat(), _race_type(item["kind"]), sex)
        for item in competitions
        for sex in (("M", "L") if item["sex"] == "Mixed" else (item["sex"],))
    }
    expected = {
        "M": [row for row in men_history if (row["Date"], row["RaceType"], "M") in expected_events],
        "L": [row for row in ladies_history if (row["Date"], row["RaceType"], "L") in expected_events],
    }
    schedule_rows.sort(key=lambda row: (
        datetime.strptime(str(row["Date"]), "%m/%d/%Y").date(),
        {"M": 0, "L": 1, "Mixed": 2}[str(row["Sex"])],
        int(row["Race"]),
    ))
    selected_schedule_events = {
        (item["start"].date(), _race_type(item["kind"]), item["sex"])
        for item in competitions
    }
    expected_schedule = [
        row for row in accepted_races
        if (
            datetime.strptime(row["Date"], "%m/%d/%Y").date(),
            row["RaceType"],
            row["Sex"],
        ) in selected_schedule_events
    ]
    candidate_schedule_keys = {
        tuple(str(row[field]) for field in CORE_SCHEDULE_FIELDS) for row in schedule_rows
    }
    expected_schedule_keys = {
        tuple(str(row[field]) for field in CORE_SCHEDULE_FIELDS) for row in expected_schedule
    }
    schedule_missing = expected_schedule_keys - candidate_schedule_keys
    schedule_unexpected = candidate_schedule_keys - expected_schedule_keys
    schedule_equal = (
        len(expected_schedule) == len(competitions)
        and len(schedule_rows) == len(expected_schedule)
        and not schedule_missing
        and not schedule_unexpected
    )
    parity: dict[str, Any] = {}
    all_equal = not unresolved and schedule_equal
    for sex in ("M", "L"):
        candidate_keys = {tuple(str(row[field]) for field in CORE_RESULT_FIELDS) for row in candidate_rows[sex]}
        expected_keys = {tuple(str(row[field]) for field in CORE_RESULT_FIELDS) for row in expected[sex]}
        missing = expected_keys - candidate_keys
        unexpected = candidate_keys - expected_keys
        equal = not missing and not unexpected and len(candidate_rows[sex]) == len(expected[sex])
        all_equal = all_equal and equal
        parity[sex] = {
            "candidate_rows": len(candidate_rows[sex]),
            "accepted_rows": len(expected[sex]),
            "missing_core_rows": len(missing),
            "unexpected_core_rows": len(unexpected),
            "core_parity": "passed" if equal else "failed",
            "missing_examples": [dict(zip(CORE_RESULT_FIELDS, row)) for row in sorted(missing)[:5]],
            "unexpected_examples": [dict(zip(CORE_RESULT_FIELDS, row)) for row in sorted(unexpected)[:5]],
        }

    _write_csv(output_dir / "schedule_candidate.csv", schedule_rows, SCHEDULE_FIELDS)
    _write_csv(output_dir / "men_results_candidate.csv", candidate_rows["M"], RESULT_FIELDS)
    _write_csv(output_dir / "ladies_results_candidate.csv", candidate_rows["L"], RESULT_FIELDS)
    _write_csv(
        output_dir / "identity_map.csv",
        identity_rows,
        ["race_id", "IBUId", "official_name", "sex", "local_id", "accepted_name", "method", "status"],
    )
    report = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed" if all_equal else "failed",
        "season_id": season_id,
        "event_id": event_id,
        "competition_count": len(competitions),
        "schedule_candidate_rows": len(schedule_rows),
        "schedule_parity": {
            "accepted_rows": len(expected_schedule),
            "candidate_rows": len(schedule_rows),
            "missing_core_rows": len(schedule_missing),
            "unexpected_core_rows": len(schedule_unexpected),
            "core_parity": "passed" if schedule_equal else "failed",
        },
        "identity": {
            "resolved": sum(row["status"] == "resolved" for row in identity_rows),
            "unresolved": len(unresolved),
            "methods": dict(sorted(Counter(row["method"] for row in identity_rows).items())),
            "unresolved_examples": unresolved[:10],
        },
        "result_parity": parity,
        "accepted_data_mutated": False,
        "publication": {"status": "disabled"},
    }
    _atomic_json(output_dir / "parity_report.json", report)
    return report


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--evidence-dir", type=Path, required=True)
    command.add_argument("--accepted-dir", type=Path, required=True)
    command.add_argument("--output-dir", type=Path, required=True)
    command.add_argument("--aliases", type=Path)
    return command


def main() -> int:
    args = parser().parse_args()
    report = adapt_evidence(
        evidence_dir=args.evidence_dir,
        accepted_dir=args.accepted_dir,
        output_dir=args.output_dir,
        aliases_path=args.aliases,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
