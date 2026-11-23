"""Resolve reviewed FLGR identities without automatically applying suggestions."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import tempfile
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

CANDIDATE_SCHEMA = "nordicnumbers.flgr-results-candidate/v1"
REPORT_SCHEMA = "nordicnumbers.flgr-identity-report/v1"
CROSSWALK_FIELDS = (
    "sex",
    "flgr_competitor_id",
    "flgr_name",
    "birth_year",
    "canonical_athlete_id",
    "canonical_name",
    "status",
    "evidence",
)

CYRILLIC_TO_LATIN = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "yo",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "kh",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "shch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}


class FlgrIdentityError(ValueError):
    """Raised when reviewed FLGR identity evidence is inconsistent."""


@dataclass(frozen=True)
class CanonicalAthlete:
    sex: str
    athlete_id: str
    names: frozenset[str]
    normalized_names: frozenset[tuple[str, ...]]
    birth_years: frozenset[int]
    nations: frozenset[str]

    @property
    def display_name(self) -> str:
        return min(self.names)


@dataclass(frozen=True)
class ReviewedCrosswalk:
    sex: str
    flgr_competitor_id: str
    flgr_name: str
    birth_year: int | None
    canonical_athlete_id: str
    canonical_name: str
    evidence: str


def normalized_name(value: str) -> tuple[str, ...]:
    transliterated = "".join(
        CYRILLIC_TO_LATIN.get(character.lower(), character.lower())
        for character in value
    )
    decomposed = unicodedata.normalize("NFKD", transliterated)
    ascii_like = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return tuple(sorted(re.findall(r"[a-z0-9]+", ascii_like)))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _birth_year(value: str | None) -> int | None:
    match = re.search(r"(?:19|20)\d{2}", value or "")
    return int(match.group()) if match else None


def load_canonical_history(
    men_chrono: Path, ladies_chrono: Path
) -> dict[tuple[str, str], CanonicalAthlete]:
    aggregate: dict[tuple[str, str], dict[str, set]] = {}
    for sex, path in (("M", men_chrono), ("L", ladies_chrono)):
        for row_number, row in enumerate(_read_csv(path), start=2):
            athlete_id = (row.get("ID") or "").strip()
            name = (row.get("Skier") or "").strip()
            if not athlete_id or not name:
                raise FlgrIdentityError(f"{path}:{row_number}: missing ID or Skier")
            state = aggregate.setdefault(
                (sex, athlete_id),
                {"names": set(), "birth_years": set(), "nations": set()},
            )
            state["names"].add(name)
            if birth_year := _birth_year(row.get("Birthday")):
                state["birth_years"].add(birth_year)
            if nation := (row.get("Nation") or "").strip():
                state["nations"].add(nation)

    return {
        key: CanonicalAthlete(
            sex=key[0],
            athlete_id=key[1],
            names=frozenset(state["names"]),
            normalized_names=frozenset(normalized_name(name) for name in state["names"]),
            birth_years=frozenset(state["birth_years"]),
            nations=frozenset(state["nations"]),
        )
        for key, state in aggregate.items()
    }


def load_reviewed_crosswalk(
    path: Path, canonical: dict[tuple[str, str], CanonicalAthlete]
) -> dict[tuple[str, str], ReviewedCrosswalk]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(set(CROSSWALK_FIELDS) - set(reader.fieldnames or ()))
        if missing:
            raise FlgrIdentityError(f"crosswalk is missing fields: {missing}")
        rows = list(reader)

    crosswalk: dict[tuple[str, str], ReviewedCrosswalk] = {}
    claimed_canonical: set[tuple[str, str]] = set()
    for row_number, row in enumerate(rows, start=2):
        sex = (row.get("sex") or "").strip()
        flgr_id = (row.get("flgr_competitor_id") or "").strip()
        flgr_name = (row.get("flgr_name") or "").strip()
        canonical_id = (row.get("canonical_athlete_id") or "").strip()
        canonical_name = (row.get("canonical_name") or "").strip()
        status = (row.get("status") or "").strip()
        evidence = (row.get("evidence") or "").strip()
        birth_year = _birth_year(row.get("birth_year"))
        if (
            sex not in {"M", "L"}
            or not flgr_id.isdigit()
            or not flgr_name
            or not canonical_id
            or not canonical_name
            or status != "reviewed"
            or not evidence
        ):
            raise FlgrIdentityError(f"{path}:{row_number}: incomplete reviewed mapping")
        key = (sex, flgr_id)
        canonical_key = (sex, canonical_id)
        if key in crosswalk:
            raise FlgrIdentityError(f"{path}:{row_number}: duplicate FLGR mapping {key}")
        if canonical_key in claimed_canonical:
            raise FlgrIdentityError(
                f"{path}:{row_number}: canonical athlete is mapped more than once: "
                f"{canonical_key}"
            )
        athlete = canonical.get(canonical_key)
        if athlete is None:
            raise FlgrIdentityError(
                f"{path}:{row_number}: unknown canonical athlete {canonical_key}"
            )
        if normalized_name(canonical_name) not in athlete.normalized_names:
            raise FlgrIdentityError(
                f"{path}:{row_number}: canonical name does not belong to {canonical_id}"
            )
        if birth_year and athlete.birth_years and birth_year not in athlete.birth_years:
            raise FlgrIdentityError(
                f"{path}:{row_number}: birth year conflicts with canonical history"
            )
        crosswalk[key] = ReviewedCrosswalk(
            sex=sex,
            flgr_competitor_id=flgr_id,
            flgr_name=flgr_name,
            birth_year=birth_year,
            canonical_athlete_id=canonical_id,
            canonical_name=canonical_name,
            evidence=evidence,
        )
        claimed_canonical.add(canonical_key)
    return crosswalk


def _russian_candidates(
    canonical: dict[tuple[str, str], CanonicalAthlete], sex: str
) -> list[CanonicalAthlete]:
    return [
        athlete
        for (athlete_sex, _), athlete in canonical.items()
        if athlete_sex == sex
        and any(nation.lower() in {"russia", "rus"} for nation in athlete.nations)
    ]


def suggestion_report(
    *,
    source_name: str,
    birth_year: int | None,
    candidates: list[CanonicalAthlete],
    limit: int = 3,
) -> list[dict[str, object]]:
    source_key = " ".join(normalized_name(source_name))
    suggestions = []
    for athlete in candidates:
        name_score = max(
            SequenceMatcher(None, source_key, " ".join(candidate_name)).ratio()
            for candidate_name in athlete.normalized_names
        )
        birth_year_match = bool(birth_year and birth_year in athlete.birth_years)
        score = min(1.0, name_score + (0.1 if birth_year_match else 0.0))
        suggestions.append(
            {
                "canonical_athlete_id": athlete.athlete_id,
                "canonical_name": athlete.display_name,
                "score": round(score, 4),
                "birth_year_match": birth_year_match,
                "applied": False,
            }
        )
    return sorted(
        suggestions,
        key=lambda suggestion: (-suggestion["score"], suggestion["canonical_athlete_id"]),
    )[:limit]


def _validate_output_dir(output_dir: Path, sources: Iterable[Path]) -> Path:
    resolved = output_dir.expanduser().resolve()
    for source in sources:
        source_path = source.expanduser().resolve()
        if resolved == source_path.parent or source_path.parent in resolved.parents:
            raise FlgrIdentityError("identity output cannot be inside a source directory")
    if resolved.exists() and any(resolved.iterdir()):
        raise FlgrIdentityError("identity output directory must be empty")
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def _csv_text(rows: list[dict[str, object]], fields: list[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def resolve_flgr_identities(
    *,
    candidate_path: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    crosswalk_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    output_dir = _validate_output_dir(
        output_dir, (candidate_path, men_chrono, ladies_chrono, crosswalk_path)
    )
    candidate_bytes = candidate_path.read_bytes()
    candidate = json.loads(candidate_bytes)
    if candidate.get("schema_version") != CANDIDATE_SCHEMA:
        raise FlgrIdentityError("unsupported FLGR candidate schema")
    sex = candidate.get("sex")
    if sex not in {"M", "L"} or not candidate.get("results"):
        raise FlgrIdentityError("candidate has no valid sex or result rows")

    canonical = load_canonical_history(men_chrono, ladies_chrono)
    crosswalk = load_reviewed_crosswalk(crosswalk_path, canonical)
    russian_candidates = _russian_candidates(canonical, sex)
    mapped_rows: list[dict[str, object]] = []
    suggestions: list[dict[str, object]] = []
    reviewed_count = 0
    for result in candidate["results"]:
        source_id = str(result.get("source_competitor_id") or "")
        athlete_id = str(result.get("athlete_id") or "")
        source_name = str(result.get("source_name") or "").strip()
        birth_year = result.get("birth_year")
        if athlete_id != f"flgr:{source_id}" or not source_id.isdigit() or not source_name:
            raise FlgrIdentityError("candidate contains an invalid FLGR identity")
        reviewed = crosswalk.get((sex, source_id))
        if reviewed:
            if normalized_name(reviewed.flgr_name) != normalized_name(source_name):
                raise FlgrIdentityError(
                    f"reviewed FLGR name changed for {athlete_id}: {source_name!r}"
                )
            if reviewed.birth_year and birth_year and reviewed.birth_year != birth_year:
                raise FlgrIdentityError(
                    f"reviewed FLGR birth year changed for {athlete_id}"
                )
            canonical_id = reviewed.canonical_athlete_id
            canonical_name = reviewed.canonical_name
            identity_status = "reviewed_crosswalk"
            reviewed_count += 1
        else:
            canonical_id = ""
            canonical_name = ""
            identity_status = "flgr_only"
            suggestions.append(
                {
                    "athlete_id": athlete_id,
                    "source_name": source_name,
                    "birth_year": birth_year,
                    "suggestions": suggestion_report(
                        source_name=source_name,
                        birth_year=birth_year,
                        candidates=russian_candidates,
                    ),
                }
            )
        mapped_rows.append(
            {
                "event_id": candidate["event_id"],
                "date": candidate["date"],
                "season": candidate["season"],
                "sex": sex,
                "distance": candidate["distance"],
                "technique": candidate["technique"],
                "mass_start": candidate["mass_start"],
                **result,
                "canonical_athlete_id": canonical_id,
                "canonical_name": canonical_name,
                "identity_status": identity_status,
                "source_sha256": candidate["source_sha256"],
            }
        )

    report = {
        "schema_version": REPORT_SCHEMA,
        "source_candidate_sha256": hashlib.sha256(candidate_bytes).hexdigest(),
        "crosswalk_sha256": hashlib.sha256(crosswalk_path.read_bytes()).hexdigest(),
        "event_id": candidate["event_id"],
        "result_rows": len(mapped_rows),
        "reviewed_crosswalk_rows": reviewed_count,
        "flgr_only_rows": len(mapped_rows) - reviewed_count,
        "suggestions_are_non_publishing": True,
        "suggestions": suggestions,
    }
    fields = list(mapped_rows[0])
    _atomic_text(output_dir / "flgr-identity-results.csv", _csv_text(mapped_rows, fields))
    _atomic_text(
        output_dir / "flgr-identity-report.json",
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--men-chrono", type=Path, required=True)
    parser.add_argument("--ladies-chrono", type=Path, required=True)
    parser.add_argument("--crosswalk", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = resolve_flgr_identities(
        candidate_path=args.candidate,
        men_chrono=args.men_chrono,
        ladies_chrono=args.ladies_chrono,
        crosswalk_path=args.crosswalk,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
