#!/usr/bin/env python3
"""Review FantasyXC roster candidates before creating an accepted overlay."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from cross_country_announced_rosters import (
    AUTOMATED_SCHEMA_VERSION,
    SCHEMA_VERSION as ANNOUNCED_SCHEMA,
    load_announced_rosters,
)
from cross_country_fantasyxc_resolve import CANDIDATES_SCHEMA, RESOLVER_VERSION
from cross_country_results_identity import load_history, normalized_name

REVIEW_SCHEMA = "nordicnumbers.cross-country-announced-roster-review/v1"
TEMPLATE_STATUS = "draft_review"
REVIEWED_STATUS = "reviewed"
AUTOMATION_POLICY_VERSION = "fantasyxc-auto-roster-v3"
AUTOMATIC_IDENTITY_METHODS = frozenset(
    {
        "exact_canonical_name_tokens",
        "exact_full_history_name_tokens",
        "reviewed_source_alias",
    }
)


def _prediction_identity_index(
    path: Path | None,
) -> dict[tuple[str, str], dict[str, str]]:
    """Index only unambiguous ledger identities by sex and normalized name."""

    if path is None:
        return {}
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fields = set(reader.fieldnames or ())
            required = {
                "sex",
                "name",
                "prediction_athlete_id",
                "fis_competitor_id",
                "fis_code",
            }
            if missing := sorted(required - fields):
                raise FantasyXCReviewError(
                    f"prediction ledger is missing fields: {missing}"
                )
            identities: dict[tuple[str, str], dict[str, str] | None] = {}
            for line, row in enumerate(reader, start=2):
                sex = str(row["sex"]).strip().upper()
                name = normalized_name(str(row["name"]))
                competitor_id = str(row["fis_competitor_id"]).strip()
                fis_code = str(row["fis_code"]).strip()
                prediction_id = str(row["prediction_athlete_id"]).strip()
                if sex not in {"M", "L"} or not name or not fis_code:
                    raise FantasyXCReviewError(
                        f"prediction ledger line {line} has incomplete identity"
                    )
                if not competitor_id or prediction_id != f"fis:{competitor_id}":
                    raise FantasyXCReviewError(
                        f"prediction ledger line {line} has unstable identity"
                    )
                identity = {
                    "prediction_athlete_id": prediction_id,
                    "fis_competitor_id": competitor_id,
                    "fis_code": fis_code,
                }
                key = (sex, name)
                existing = identities.get(key)
                if existing is None and key in identities:
                    continue
                if existing is not None and existing != identity:
                    identities[key] = None
                else:
                    identities[key] = identity
    except OSError as exc:
        raise FantasyXCReviewError(f"cannot read prediction ledger: {exc}") from exc
    return {key: value for key, value in identities.items() if value is not None}


class FantasyXCReviewError(RuntimeError):
    """Candidate review is incomplete, stale, or internally inconsistent."""


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _validate_output(path: Path, sources: tuple[Path, ...]) -> Path:
    resolved = path.expanduser().resolve()
    if resolved in {source.expanduser().resolve() for source in sources}:
        raise FantasyXCReviewError("review output cannot overwrite an input")
    return resolved


def _read_object(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        content = path.read_bytes()
        value = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FantasyXCReviewError(f"cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise FantasyXCReviewError(f"{label} must be a JSON object")
    return value, content


def _candidate_document(
    path: Path, *, logical_date: date
) -> tuple[dict[str, Any], bytes]:
    document, content = _read_object(path, label="candidate artifact")
    if document.get("schema_version") != CANDIDATES_SCHEMA:
        raise FantasyXCReviewError("unsupported candidate schema")
    if document.get("resolver_version") != RESOLVER_VERSION:
        raise FantasyXCReviewError("unsupported candidate resolver version")
    if document.get("logical_date") != logical_date.isoformat():
        raise FantasyXCReviewError("candidate logical date does not match")
    if document.get("status") != "candidate_review":
        raise FantasyXCReviewError("candidate artifact must remain candidate_review")
    if document.get("publication") != {"status": "disabled"}:
        raise FantasyXCReviewError("candidate publication must remain disabled")
    for field, prefix in (("candidates", "candidate"), ("review_exceptions", "exception")):
        rows = document.get(field)
        if not isinstance(rows, list):
            raise FantasyXCReviewError(f"candidate artifact {field} must be a list")
        ids = [str(row.get(f"{prefix}_id", "")) for row in rows if isinstance(row, dict)]
        if len(ids) != len(rows) or any(not value.startswith(f"{prefix}:") for value in ids):
            raise FantasyXCReviewError(f"candidate artifact {field} has invalid IDs")
        if len(ids) != len(set(ids)):
            raise FantasyXCReviewError(f"candidate artifact {field} has duplicate IDs")
    return document, content


def create_review_template(
    *, candidates_path: Path, logical_date: date, output_path: Path
) -> dict[str, Any]:
    """Create a complete pending checklist bound to one candidate checksum."""

    output_path = _validate_output(output_path, (candidates_path,))
    candidates, content = _candidate_document(candidates_path, logical_date=logical_date)
    review = {
        "schema_version": REVIEW_SCHEMA,
        "status": TEMPLATE_STATUS,
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "candidate_source": {
            "path": str(candidates_path),
            "sha256": _sha256(content),
        },
        "reviewer": "",
        "reviewed_at": "",
        "candidate_decisions": [
            {
                "candidate_id": row["candidate_id"],
                "decision": "pending",
                "note": "",
            }
            for row in candidates["candidates"]
        ],
        "exception_decisions": [
            {
                "exception_id": row["exception_id"],
                "decision": "pending",
                "note": "",
            }
            for row in candidates["review_exceptions"]
        ],
    }
    _atomic_write(output_path, _json_bytes(review))
    return review


def _timezone_timestamp(value: object, *, label: str) -> str:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FantasyXCReviewError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise FantasyXCReviewError(f"{label} must include a timezone")
    return text


def _decisions(
    rows: object,
    *,
    id_field: str,
    expected_ids: set[str],
    allowed: set[str],
) -> dict[str, dict[str, str]]:
    if not isinstance(rows, list):
        raise FantasyXCReviewError(f"{id_field} decisions must be a list")
    decisions: dict[str, dict[str, str]] = {}
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise FantasyXCReviewError(f"{id_field} decision {index} must be an object")
        item_id = str(row.get(id_field, "")).strip()
        decision = str(row.get("decision", "")).strip()
        note = str(row.get("note", "")).strip()
        if item_id not in expected_ids:
            raise FantasyXCReviewError(f"unknown {id_field}: {item_id}")
        if item_id in decisions:
            raise FantasyXCReviewError(f"duplicate {id_field}: {item_id}")
        if decision not in allowed:
            raise FantasyXCReviewError(f"invalid decision for {item_id}: {decision}")
        decisions[item_id] = {"decision": decision, "note": note}
    missing = expected_ids - set(decisions)
    if missing:
        raise FantasyXCReviewError(f"missing decisions for {sorted(missing)}")
    return decisions


def promote_review(
    *,
    candidates_path: Path,
    review_path: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    logical_date: date,
    output_path: Path,
) -> dict[str, Any]:
    """Promote only explicitly accepted candidates to the reviewed contract."""

    output_path = _validate_output(
        output_path,
        (candidates_path, review_path, men_chrono, ladies_chrono),
    )
    candidates, candidate_content = _candidate_document(
        candidates_path, logical_date=logical_date
    )
    review, _review_content = _read_object(review_path, label="review artifact")
    if review.get("schema_version") != REVIEW_SCHEMA:
        raise FantasyXCReviewError("unsupported review schema")
    if review.get("status") != REVIEWED_STATUS:
        raise FantasyXCReviewError("review artifact status must be reviewed")
    if review.get("logical_date") != logical_date.isoformat():
        raise FantasyXCReviewError("review logical date does not match")
    if review.get("publication") != {"status": "disabled"}:
        raise FantasyXCReviewError("review publication must remain disabled")
    source = review.get("candidate_source")
    if not isinstance(source, dict) or source.get("sha256") != _sha256(candidate_content):
        raise FantasyXCReviewError("review is not bound to this candidate artifact")
    reviewer = str(review.get("reviewer", "")).strip()
    if not reviewer:
        raise FantasyXCReviewError("reviewer is required")
    reviewed_at = _timezone_timestamp(review.get("reviewed_at"), label="reviewed_at")

    candidates_by_id = {row["candidate_id"]: row for row in candidates["candidates"]}
    exceptions_by_id = {
        row["exception_id"]: row for row in candidates["review_exceptions"]
    }
    candidate_decisions = _decisions(
        review.get("candidate_decisions"),
        id_field="candidate_id",
        expected_ids=set(candidates_by_id),
        allowed={"accepted", "rejected"},
    )
    exception_decisions = _decisions(
        review.get("exception_decisions"),
        id_field="exception_id",
        expected_ids=set(exceptions_by_id),
        allowed={"acknowledged", "deferred"},
    )
    for exception_id, decision in exception_decisions.items():
        if not decision["note"]:
            raise FantasyXCReviewError(
                f"review exception {exception_id} requires a note"
            )
    for candidate_id, decision in candidate_decisions.items():
        if decision["decision"] == "rejected" and not decision["note"]:
            raise FantasyXCReviewError(
                f"rejected candidate {candidate_id} requires a note"
            )

    entries = []
    for candidate_id, candidate in candidates_by_id.items():
        if candidate_decisions[candidate_id]["decision"] != "accepted":
            continue
        entries.append(
            {
                "sex": candidate["sex"],
                "canonical_athlete_id": candidate["canonical_athlete_id"],
                "status": candidate["suggested_status"],
                "race_ids": candidate["race_ids"],
                "source_url": candidate["source_url"],
                "observed_at": candidate["observed_at"],
            }
        )
    if not entries:
        raise FantasyXCReviewError("review accepted no roster candidates")
    output = {
        "schema_version": ANNOUNCED_SCHEMA,
        "status": "reviewed",
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "review": {
            "reviewer": reviewer,
            "reviewed_at": reviewed_at,
            "candidate_sha256": _sha256(candidate_content),
            "accepted": len(entries),
            "rejected": sum(
                decision["decision"] == "rejected"
                for decision in candidate_decisions.values()
            ),
            "exceptions_acknowledged": len(exception_decisions),
        },
        "entries": sorted(
            entries,
            key=lambda row: (
                row["sex"],
                row["canonical_athlete_id"],
                row["race_ids"],
            ),
        ),
    }
    athletes, _name_index, _names_by_id = load_history(men_chrono, ladies_chrono)
    output_bytes = _json_bytes(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=output_path.parent,
        delete=False,
    ) as handle:
        handle.write(output_bytes)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        load_announced_rosters(
            temporary,
            logical_date=logical_date,
            athletes=athletes,
        )
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def auto_promote_candidates(
    *,
    candidates_path: Path,
    men_chrono: Path,
    ladies_chrono: Path,
    men_identity_history: Path,
    ladies_identity_history: Path,
    prediction_ledger: Path | None = None,
    logical_date: date,
    output_path: Path,
) -> dict[str, Any]:
    """Promote deterministic candidates and retain every uncertainty.

    This is not a publication action. It creates the versioned roster overlay
    consumed before official FIS startlists exist. Unknown or unsafe candidates
    are recorded under ``unresolved`` and therefore remain governed by the
    historical/quota fallback.
    """

    sources = (
        candidates_path,
        men_chrono,
        ladies_chrono,
        men_identity_history,
        ladies_identity_history,
    ) + ((prediction_ledger,) if prediction_ledger is not None else ())
    output_path = _validate_output(output_path, sources)
    candidates, candidate_content = _candidate_document(
        candidates_path, logical_date=logical_date
    )
    chronology, _name_index, _names_by_id = load_history(
        men_chrono, ladies_chrono
    )
    full_history, _full_name_index, _full_names_by_id = load_history(
        men_identity_history, ladies_identity_history
    )
    athletes = {**full_history, **chronology}
    prediction_identities = _prediction_identity_index(prediction_ledger)

    entries: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = [
        {"kind": "review_exception", **row}
        for row in candidates["review_exceptions"]
    ]
    for candidate in candidates["candidates"]:
        key = (candidate.get("sex"), candidate.get("canonical_athlete_id"))
        known_athlete = athletes.get(key)
        prediction_ready_athlete = chronology.get(key)
        identity_method = candidate.get("identity_match_method")
        ledger_identity = None
        if known_athlete is not None:
            ledger_identity = prediction_identities.get(
                (str(key[0]), normalized_name(known_athlete.name))
            )
        if (
            candidate.get("confidence") != "high"
            or identity_method not in AUTOMATIC_IDENTITY_METHODS
            or known_athlete is None
            or (prediction_ready_athlete is None and ledger_identity is None)
        ):
            unresolved.append(
                {
                    "kind": "candidate_not_automatically_eligible",
                    "candidate_id": candidate.get("candidate_id", ""),
                    "sex": candidate.get("sex", ""),
                    "canonical_athlete_id": candidate.get(
                        "canonical_athlete_id", ""
                    ),
                    "canonical_name": candidate.get("canonical_name", ""),
                    "canonical_nation": (
                        known_athlete.nation if known_athlete is not None else ""
                    ),
                    "suggested_status": candidate.get("suggested_status", ""),
                    "race_ids": candidate.get("race_ids", []),
                    "source_url": candidate.get("source_url", ""),
                    "observed_at": candidate.get("observed_at", ""),
                    "evidence_text": candidate.get("evidence_text", ""),
                    "identity_match_method": identity_method or "",
                    "confidence": candidate.get("confidence", ""),
                    "reason": (
                        "candidate is not present in accepted prediction history"
                        if known_athlete is not None
                        and prediction_ready_athlete is None
                        else "candidate failed automatic confidence or identity policy"
                    ),
                }
            )
            continue
        athlete = prediction_ready_athlete or known_athlete
        entry = {
                "sex": key[0],
                "canonical_athlete_id": athlete.athlete_id,
                "canonical_name": athlete.name,
                "canonical_nation": athlete.nation,
                "status": candidate["suggested_status"],
                "race_ids": candidate["race_ids"],
                "source_url": candidate["source_url"],
                "observed_at": candidate["observed_at"],
                "identity_match_method": identity_method,
            }
        if prediction_ready_athlete is None:
            entry.update(ledger_identity or {})
            entry["rating_eligibility"] = "private_prediction_elo"
        entries.append(entry)
    output = {
        "schema_version": AUTOMATED_SCHEMA_VERSION,
        "status": "automated",
        "logical_date": logical_date.isoformat(),
        "publication": {"status": "disabled"},
        "automation": {
            "policy_version": AUTOMATION_POLICY_VERSION,
            "resolver_version": RESOLVER_VERSION,
            "candidate_sha256": _sha256(candidate_content),
            "accepted": len(entries),
            "unresolved": len(unresolved),
            "unresolved_behavior": "historical_quota_fallback",
            "official_fis_startlist_precedence": True,
        },
        "entries": sorted(
            entries,
            key=lambda row: (
                row["canonical_nation"],
                row["sex"],
                row["canonical_name"],
                row["race_ids"],
            ),
        ),
        "unresolved": unresolved,
    }
    output_bytes = _json_bytes(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=output_path.parent, delete=False) as handle:
        handle.write(output_bytes)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        load_announced_rosters(
            temporary,
            logical_date=logical_date,
            athletes=athletes,
        )
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    template = subparsers.add_parser("template")
    template.add_argument("--candidates", required=True, type=Path)
    template.add_argument("--logical-date", required=True, type=date.fromisoformat)
    template.add_argument("--output", required=True, type=Path)
    promote = subparsers.add_parser("promote")
    promote.add_argument("--candidates", required=True, type=Path)
    promote.add_argument("--review", required=True, type=Path)
    promote.add_argument("--men-chrono", required=True, type=Path)
    promote.add_argument("--ladies-chrono", required=True, type=Path)
    promote.add_argument("--logical-date", required=True, type=date.fromisoformat)
    promote.add_argument("--output", required=True, type=Path)
    automatic = subparsers.add_parser("auto-promote")
    automatic.add_argument("--candidates", required=True, type=Path)
    automatic.add_argument("--men-chrono", required=True, type=Path)
    automatic.add_argument("--ladies-chrono", required=True, type=Path)
    automatic.add_argument("--men-identity-history", required=True, type=Path)
    automatic.add_argument("--ladies-identity-history", required=True, type=Path)
    automatic.add_argument("--prediction-ledger", type=Path)
    automatic.add_argument("--logical-date", required=True, type=date.fromisoformat)
    automatic.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "template":
        result = create_review_template(
            candidates_path=args.candidates,
            logical_date=args.logical_date,
            output_path=args.output,
        )
    elif args.command == "promote":
        result = promote_review(
            candidates_path=args.candidates,
            review_path=args.review,
            men_chrono=args.men_chrono,
            ladies_chrono=args.ladies_chrono,
            logical_date=args.logical_date,
            output_path=args.output,
        )
    else:
        result = auto_promote_candidates(
            candidates_path=args.candidates,
            men_chrono=args.men_chrono,
            ladies_chrono=args.ladies_chrono,
            men_identity_history=args.men_identity_history,
            ladies_identity_history=args.ladies_identity_history,
            prediction_ledger=args.prediction_ledger,
            logical_date=args.logical_date,
            output_path=args.output,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
