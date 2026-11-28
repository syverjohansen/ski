#!/usr/bin/env python3
"""Map raw FIS result identities to existing Nordic Numbers athlete IDs.

Automatic mapping is deliberately limited to a unique normalized-name match
within the same sex.  Known spelling differences require a reviewed alias.
Unknown or ambiguous athletes fail the entire candidate before Elo replay.
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


def normalized_name(value: str) -> tuple[str, ...]:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_like = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    ).lower()
    return tuple(sorted(re.findall(r"[a-z0-9]+", ascii_like)))


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_history(
    men_chrono: Path, ladies_chrono: Path
) -> tuple[
    dict[tuple[str, str], CanonicalAthlete],
    dict[tuple[str, tuple[str, ...]], set[str]],
    dict[tuple[str, str], set[str]],
]:
    athletes: dict[tuple[str, str], CanonicalAthlete] = {}
    latest_dates: dict[tuple[str, str], str] = {}
    name_index: dict[tuple[str, tuple[str, ...]], set[str]] = defaultdict(set)
    names_by_id: dict[tuple[str, str], set[str]] = defaultdict(set)
    for sex, path in (("M", men_chrono), ("L", ladies_chrono)):
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
            if observed_date >= latest_dates.get(key, ""):
                athletes[key] = CanonicalAthlete(athlete_id, name, nation)
                latest_dates[key] = observed_date
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
        key = (alias.sex, alias.fis_competitor_id)
        if alias.sex not in {"M", "L"} or not all(
            (alias.fis_competitor_id, alias.fis_code, alias.fis_name,
             alias.canonical_id, alias.canonical_name)
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
) -> dict[str, object]:
    output_dir = _validate_output_dir(
        output_dir, (results, men_chrono, ladies_chrono, aliases_path)
    )
    raw_rows = _read_rows(results)
    if not raw_rows:
        raise IdentityError(f"results candidate is empty: {results}")
    raw_fields = list(raw_rows[0])
    required = {"sex", "fis_competitor_id", "fis_code", "name", "nation"}
    missing_fields = sorted(required - set(raw_fields))
    if missing_fields:
        raise IdentityError(f"results candidate is missing fields: {missing_fields}")

    athletes, name_index, names_by_id = load_history(men_chrono, ladies_chrono)
    aliases = load_aliases(aliases_path, athletes, names_by_id)
    mapped_rows: list[dict[str, str]] = []
    identity_rows: dict[tuple[str, str], dict[str, str]] = {}
    method_counts: dict[str, int] = defaultdict(int)
    review_required = []

    for row_number, row in enumerate(raw_rows, start=2):
        try:
            sex = (row.get("sex") or "").strip()
            competitor_id = (row.get("fis_competitor_id") or "").strip()
            fis_code = (row.get("fis_code") or "").strip()
            fis_name = (row.get("name") or "").strip()
            if sex not in {"M", "L"} or not competitor_id or not fis_name:
                raise IdentityError(f"{results}:{row_number}: incomplete FIS identity")

            alias = aliases.get((sex, competitor_id))
            if alias:
                if alias.fis_code != fis_code or alias.fis_name != fis_name:
                    raise IdentityError(
                        f"{results}:{row_number}: FIS identity changed for alias "
                        f"{sex}/{competitor_id}"
                    )
                canonical_id = alias.canonical_id
                method = "reviewed_alias"
            else:
                candidate_ids = name_index.get((sex, normalized_name(fis_name)), set())
                if len(candidate_ids) != 1:
                    state = "missing" if not candidate_ids else "ambiguous"
                    raise IdentityError(
                        f"{results}:{row_number}: {state} identity for "
                        f"{sex}/{competitor_id} {fis_name!r}; candidates="
                        f"{sorted(candidate_ids)}"
                    )
                canonical_id = next(iter(candidate_ids))
                method = "unique_normalized_name"

            canonical = athletes[(sex, canonical_id)]
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
            review_required.append({
                "sex": row.get("sex", ""), "fis_code": row.get("fis_code", ""),
                "fis_competitor_id": row.get("fis_competitor_id", ""),
                "fis_name": row.get("name", ""), "reason": str(error), "status": "unresolved",
            })

    mapped_path = output_dir / "mapped_results.csv"
    identity_map_path = output_dir / "identity_map.csv"
    if not review_required:
        _atomic_text(mapped_path, _csv_text(mapped_rows, raw_fields + list(MAPPED_FIELDS)))
    _atomic_text(
        identity_map_path,
        _csv_text(
            [identity_rows[key] for key in sorted(identity_rows)], IDENTITY_MAP_FIELDS
        ),
    )
    report: dict[str, object] = {
        "schema_version": "nordicnumbers.alpine-results-identity/v1",
        "input_sha256": hashlib.sha256(results.read_bytes()).hexdigest(),
        "result_rows": len(mapped_rows),
        "unique_fis_athletes": len(identity_rows),
        "match_methods": dict(sorted(method_counts.items())),
        "unresolved_rows": len(review_required),
        "review_required": review_required,
        "status": "blocked" if review_required else "mapped",
        "outputs": {
            "mapped_results": mapped_path.name,
            "identity_map": identity_map_path.name,
        },
    }
    _atomic_text(
        output_dir / "report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    if review_required:
        raise IdentityError("; ".join(item["reason"] for item in review_required))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--men-chrono", required=True, type=Path)
    parser.add_argument("--ladies-chrono", required=True, type=Path)
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
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
