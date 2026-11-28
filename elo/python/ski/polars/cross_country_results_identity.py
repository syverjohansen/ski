#!/usr/bin/env python3
"""Map raw FIS cross-country identities to Nordic Numbers athlete IDs.

Automatic mapping requires one unique normalized-name match within the same
sex. Reviewed aliases bind the stable FIS code to an existing athlete; the FIS
page ID and display name are retained only as audit evidence. Unknown or
ambiguous identities fail the whole candidate.
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
from difflib import SequenceMatcher
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

MAPPED_FIELDS = (
    "canonical_athlete_id",
    "canonical_name",
    "canonical_nation",
    "identity_match_method",
)
IDENTITY_MAP_FIELDS = (
    "sex",
    "fis_competitor_id",
    "fis_code",
    "fis_name",
    "canonical_athlete_id",
    "canonical_name",
    "canonical_nation",
    "identity_match_method",
)
NATION_CONTRACT_PATH = Path(__file__).with_name(
    "cross_country_world_cup_quotas_2027.json"
)


class IdentityError(RuntimeError):
    """Raised when an FIS identity cannot be mapped without ambiguity."""


@dataclass(frozen=True)
class CanonicalAthlete:
    athlete_id: str
    name: str
    nation: str


@dataclass(frozen=True)
class Alias:
    sex: str
    fis_competitor_id: str
    fis_code: str
    fis_name: str
    canonical_id: str
    canonical_name: str


def load_nation_names(path: Path = NATION_CONTRACT_PATH) -> dict[str, str]:
    """Return current FIS nation codes mapped to accepted history names."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        nations = document["nations"]
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError) as error:
        raise IdentityError(f"cannot load FIS nation names from {path}") from error
    if not isinstance(nations, list):
        raise IdentityError(f"invalid FIS nation names in {path}")
    output: dict[str, str] = {}
    for item in nations:
        if not isinstance(item, dict):
            raise IdentityError(f"invalid FIS nation entry in {path}")
        code = str(item.get("code", "")).strip().upper()
        name = str(item.get("name", "")).strip()
        if len(code) != 3 or not name or code in output:
            raise IdentityError(f"invalid FIS nation entry in {path}")
        output[code] = name
    return output


def canonical_nation(value: str, nation_names: dict[str, str]) -> str:
    """Normalize a FIS code while preserving already-canonical history names."""
    nation = value.strip()
    return nation_names.get(nation.upper(), nation)


def normalized_name(value: str) -> tuple[str, ...]:
    """Normalize accents, punctuation, case, and given/family-name order."""
    cross_country_transliteration = str.maketrans(
        {
            "æ": "ae",
            "ø": "oe",
            "œ": "oe",
            "ł": "l",
            "ð": "d",
            "þ": "th",
            "ß": "ss",
        }
    )
    decomposed = unicodedata.normalize(
        "NFKD", value.lower().translate(cross_country_transliteration)
    )
    ascii_like = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return tuple(sorted(re.findall(r"[a-z0-9]+", ascii_like)))


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_prediction_identities(
    path: Path | None,
) -> dict[tuple[str, str], tuple[str, str]]:
    """Load stable FIS identities from the private lower-tier rating ledger."""
    if path is None:
        return {}
    rows = _read_rows(path)
    identities: dict[tuple[str, str], tuple[str, str]] = {}
    for row_number, row in enumerate(rows, start=2):
        sex = (row.get("sex") or "").strip()
        competitor_id = (row.get("fis_competitor_id") or "").strip()
        fis_code = (row.get("fis_code") or "").strip()
        prediction_id = (row.get("prediction_athlete_id") or "").strip()
        if (
            sex not in {"M", "L"}
            or not competitor_id
            or not fis_code.isdigit()
            or prediction_id != f"fis:{competitor_id}"
        ):
            raise IdentityError(f"{path}:{row_number}: invalid prediction identity")
        key = (sex, competitor_id)
        identity = (fis_code, prediction_id)
        previous = identities.setdefault(key, identity)
        if previous != identity:
            raise IdentityError(f"{path}:{row_number}: prediction identity changed")
    return identities


def load_history(
    men_chrono: Path,
    ladies_chrono: Path,
    men_reference_history: Path | None = None,
    ladies_reference_history: Path | None = None,
) -> tuple[
    dict[tuple[str, str], CanonicalAthlete],
    dict[tuple[str, tuple[str, ...]], set[str]],
    dict[tuple[str, str], set[str]],
]:
    athletes: dict[tuple[str, str], CanonicalAthlete] = {}
    latest_order: dict[tuple[str, str], tuple[int, str]] = {}
    name_index: dict[tuple[str, tuple[str, ...]], set[str]] = defaultdict(set)
    names_by_id: dict[tuple[str, str], set[str]] = defaultdict(set)
    sources = [
        ("M", men_reference_history, 0),
        ("L", ladies_reference_history, 0),
        ("M", men_chrono, 1),
        ("L", ladies_chrono, 1),
    ]
    for sex, path, priority in sources:
        if path is None:
            continue
        for row_number, row in enumerate(_read_rows(path), start=2):
            athlete_id = (row.get("ID") or "").strip()
            name = (row.get("Skier") or "").strip()
            nation = (row.get("Nation") or "").strip()
            observed_date = (row.get("Date") or "").strip()
            if not athlete_id or not name:
                raise IdentityError(f"{path}:{row_number}: missing ID or Skier")
            key = (sex, athlete_id)
            names_by_id[key].add(name)
            name_index[(sex, normalized_name(name))].add(athlete_id)
            order = (priority, observed_date)
            if order >= latest_order.get(key, (-1, "")):
                athletes[key] = CanonicalAthlete(athlete_id, name, nation)
                latest_order[key] = order
    return athletes, name_index, names_by_id


def load_aliases(
    path: Path,
    athletes: dict[tuple[str, str], CanonicalAthlete],
    names_by_id: dict[tuple[str, str], set[str]],
) -> dict[tuple[str, str], Alias]:
    aliases: dict[tuple[str, str], Alias] = {}
    for row_number, row in enumerate(_read_rows(path), start=2):
        alias = Alias(
            sex=(row.get("sex") or "").strip(),
            fis_competitor_id=(row.get("fis_competitor_id") or "").strip(),
            fis_code=(row.get("fis_code") or "").strip(),
            fis_name=(row.get("fis_name") or "").strip(),
            canonical_id=(row.get("canonical_id") or "").strip(),
            canonical_name=(row.get("canonical_name") or "").strip(),
        )
        key = (alias.sex, alias.fis_code)
        if alias.sex not in {"M", "L"} or not all(
            (
                alias.fis_competitor_id,
                alias.fis_code,
                alias.fis_name,
                alias.canonical_id,
                alias.canonical_name,
            )
        ):
            raise IdentityError(f"{path}:{row_number}: incomplete alias")
        if key in aliases:
            raise IdentityError(f"{path}:{row_number}: duplicate alias {key}")
        canonical_key = (alias.sex, alias.canonical_id)
        if canonical_key not in athletes:
            raise IdentityError(
                f"{path}:{row_number}: unknown canonical athlete {canonical_key}"
            )
        if alias.canonical_name not in names_by_id[canonical_key]:
            raise IdentityError(
                f"{path}:{row_number}: canonical name {alias.canonical_name!r} "
                f"does not belong to ID {alias.canonical_id}"
            )
        aliases[key] = alias
    return aliases


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


def _csv_text(rows: list[dict[str, str]], fields: Iterable[str]) -> str:
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
    men_chrono: Path,
    ladies_chrono: Path,
    aliases_path: Path,
    output_dir: Path,
    men_reference_history: Path | None = None,
    ladies_reference_history: Path | None = None,
    prediction_ledger: Path | None = None,
) -> dict[str, object]:
    references = tuple(
        path
        for path in (men_reference_history, ladies_reference_history, prediction_ledger)
        if path is not None
    )
    output_dir = _validate_output_dir(
        output_dir, (results, men_chrono, ladies_chrono, aliases_path, *references)
    )
    raw_rows = _read_rows(results)
    if not raw_rows:
        raise IdentityError(f"results candidate is empty: {results}")
    raw_fields = list(raw_rows[0])
    required = {"sex", "fis_competitor_id", "fis_code", "name", "nation"}
    missing_fields = sorted(required - set(raw_fields))
    if missing_fields:
        raise IdentityError(f"results candidate is missing fields: {missing_fields}")

    athletes, name_index, names_by_id = load_history(
        men_chrono,
        ladies_chrono,
        men_reference_history,
        ladies_reference_history,
    )
    aliases = load_aliases(aliases_path, athletes, names_by_id)
    prediction_identities = load_prediction_identities(prediction_ledger)
    nation_names = load_nation_names()
    mapped_rows: list[dict[str, str]] = []
    identity_rows: dict[tuple[str, str], dict[str, str]] = {}
    method_counts: dict[str, int] = defaultdict(int)
    review_required: dict[tuple[str, str], dict[str, object]] = {}
    unresolved = 0

    for row_number, row in enumerate(raw_rows, start=2):
        try:
            sex = (row.get("sex") or "").strip()
            competitor_id = (row.get("fis_competitor_id") or "").strip()
            fis_code = (row.get("fis_code") or "").strip()
            fis_name = (row.get("name") or "").strip()
            if sex not in {"M", "L"} or not competitor_id or not fis_code or not fis_name:
                raise IdentityError(f"{results}:{row_number}: incomplete FIS identity")

            alias = aliases.get((sex, fis_code))
            if alias:
                canonical_id = alias.canonical_id
                method = "reviewed_alias"
            elif (sex, fis_code) in athletes:
                # Newcomers are assigned their stable numeric FIS code when they first
                # enter accepted A-tier history. This makes later name changes harmless.
                canonical_id = fis_code
                method = "canonical_fis_code"
            else:
                candidate_ids = name_index.get((sex, normalized_name(fis_name)), set())
                if len(candidate_ids) > 1:
                    raise IdentityError(
                        f"{results}:{row_number}: ambiguous identity for "
                        f"{sex}/{competitor_id} {fis_name!r}; candidates="
                        f"{sorted(candidate_ids)}"
                    )
                if candidate_ids:
                    canonical_id = next(iter(candidate_ids))
                    reviewed_codes = {
                        reviewed.fis_code
                        for reviewed in aliases.values()
                        if reviewed.sex == sex and reviewed.canonical_id == canonical_id
                    }
                    if reviewed_codes:
                        raise IdentityError(
                            f"{results}:{row_number}: FIS code changed for reviewed athlete "
                            f"{sex}/{canonical_id}; observed={fis_code}, "
                            f"reviewed={sorted(reviewed_codes)}"
                        )
                    method = "unique_normalized_name"
                else:
                    if not fis_code.isdigit():
                        raise IdentityError(
                            f"{results}:{row_number}: newcomer has nonnumeric FIS code"
                        )
                    collision = next(
                        (
                            existing
                            for (existing_sex, existing_id), existing in athletes.items()
                            if existing_sex == sex and existing_id == fis_code
                        ),
                        None,
                    )
                    if collision is not None:
                        raise IdentityError(
                            f"{results}:{row_number}: FIS-code identity collides with "
                            f"accepted athlete {sex}/{fis_code}"
                        )
                    ledger_identity = prediction_identities.get((sex, competitor_id))
                    if ledger_identity is not None and ledger_identity[0] != fis_code:
                        raise IdentityError(
                            f"{results}:{row_number}: prediction FIS code changed"
                        )
                    canonical_id = fis_code
                    method = (
                        "prediction_ledger_fis_identity"
                        if ledger_identity is not None
                        else "newcomer_fis_identity"
                    )

            canonical = athletes.get((sex, canonical_id)) or CanonicalAthlete(
                canonical_id, fis_name, canonical_nation(row["nation"], nation_names)
            )
            mapped = {
                **row,
                "canonical_athlete_id": canonical.athlete_id,
                "canonical_name": canonical.name,
                "canonical_nation": canonical.nation,
                "identity_match_method": method,
            }
            mapped_rows.append(mapped)
            method_counts[method] += 1
            identity_key = (sex, competitor_id)
            identity = {
                "sex": sex,
                "fis_competitor_id": competitor_id,
                "fis_code": fis_code,
                "fis_name": fis_name,
                "canonical_athlete_id": canonical.athlete_id,
                "canonical_name": canonical.name,
                "canonical_nation": canonical.nation,
                "identity_match_method": method,
            }
            previous = identity_rows.get(identity_key)
            if previous and previous != identity:
                raise IdentityError(f"conflicting identity rows for {identity_key}")
            identity_rows[identity_key] = identity
        except IdentityError as error:
            unresolved += 1
            review_required[(row.get("sex", ""), row.get("fis_code", ""))] = {
                "sex": row.get("sex", ""), "fis_code": row.get("fis_code", ""),
                "fis_competitor_id": row.get("fis_competitor_id", ""),
                "fis_name": row.get("name", ""), "reason": str(error),
                "status": "unresolved", "suggestions": [],
            }
            continue
        if method in {"newcomer_fis_identity", "prediction_ledger_fis_identity"}:
            observed = " ".join(normalized_name(fis_name))
            suggestions = sorted(
                ({"canonical_id": athlete.athlete_id, "name": athlete.name,
                  "score": round(SequenceMatcher(None, observed, " ".join(normalized_name(athlete.name))).ratio(), 3)}
                 for (history_sex, _), athlete in athletes.items()
                 if history_sex == sex and canonical_nation(athlete.nation, nation_names) == canonical_nation(row["nation"], nation_names)),
                key=lambda item: (-item["score"], item["canonical_id"]),
            )[:3]
            review_required[(sex, fis_code)] = {
                **identity, "reason": "No accepted historical identity matched; automatic newcomer needs review",
                "status": "provisional_newcomer", "suggestions": suggestions,
            }

    mapped_path = output_dir / "mapped_results.csv"
    identity_map_path = output_dir / "identity_map.csv"
    if not unresolved:
        _atomic_text(mapped_path, _csv_text(mapped_rows, raw_fields + list(MAPPED_FIELDS)))
    _atomic_text(
        identity_map_path,
        _csv_text(
            [identity_rows[key] for key in sorted(identity_rows)], IDENTITY_MAP_FIELDS
        ),
    )
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.cross-country-results-identity/v1",
        "input_sha256": hashlib.sha256(results.read_bytes()).hexdigest(),
        "result_rows": len(mapped_rows),
        "unique_fis_athletes": len(identity_rows),
        "match_methods": dict(sorted(method_counts.items())),
        "unresolved_rows": unresolved,
        "review_required": list(review_required.values()),
        "status": "blocked" if unresolved else "mapped",
        "outputs": {
            "mapped_results": mapped_path.name,
            "identity_map": identity_map_path.name,
        },
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    if unresolved:
        raise IdentityError(f"{unresolved} unresolved result identities; see {output_dir / 'report.json'}: " + "; ".join(str(item["reason"]) for item in review_required.values() if item["status"] == "unresolved"))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
    parser.add_argument("--men-reference-history", type=Path)
    parser.add_argument("--ladies-reference-history", type=Path)
    parser.add_argument("--prediction-ledger", type=Path)
    parser.add_argument("--aliases", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = map_identities(
        results=args.results,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        aliases_path=args.aliases,
        output_dir=args.output_dir,
        men_reference_history=args.men_reference_history,
        ladies_reference_history=args.ladies_reference_history,
        prediction_ledger=args.prediction_ledger,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
