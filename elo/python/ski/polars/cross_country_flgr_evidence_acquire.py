#!/usr/bin/env python3
"""Acquire bounded, hashed FLGR race evidence from an approved manifest.

Only explicitly listed HTTPS race URLs are fetched. Every page must pass the
FLGR result contract and contain the expected source athlete. Raw HTML and
normalized candidates are written outside Git for later identity review; no
proposed canonical mapping is applied by this command.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import tempfile
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from cross_country_flgr_results import build_race_candidate, parse_source_url

MANIFEST_FIELDS = (
    "cohort",
    "season",
    "sex",
    "source_url",
    "target_flgr_competitor_id",
    "proposed_canonical_athlete_id",
    "proposed_canonical_name",
    "evidence_note",
)
ALLOWED_COHORTS = {"post_ban_holdout", "historical_calibration"}
REPORT_SCHEMA = "nordicnumbers.flgr-evidence-acquisition-report/v1"
USER_AGENT = "NordicNumbersFLGREvidence/1.0"
MAX_PAGE_BYTES = 2_000_000
REPOSITORY_ROOT = Path(__file__).resolve().parents[4]


class FlgrEvidenceAcquisitionError(ValueError):
    """Raised when bounded FLGR evidence cannot be acquired safely."""


@dataclass(frozen=True)
class EvidenceRequest:
    cohort: str
    season: int
    sex: str
    source_url: str
    target_flgr_competitor_id: str
    proposed_canonical_athlete_id: str
    proposed_canonical_name: str
    evidence_note: str


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_manifest(path: Path) -> list[EvidenceRequest]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(set(MANIFEST_FIELDS) - set(reader.fieldnames or ()))
        if missing:
            raise FlgrEvidenceAcquisitionError(
                f"evidence manifest is missing fields: {missing}"
            )
        rows = list(reader)
    if not rows:
        raise FlgrEvidenceAcquisitionError("evidence manifest is empty")

    requests: list[EvidenceRequest] = []
    approved_sources: dict[str, tuple[str, int, str]] = {}
    seen_targets: set[tuple[str, str]] = set()
    for line, row in enumerate(rows, start=2):
        cohort = (row.get("cohort") or "").strip()
        sex = (row.get("sex") or "").strip()
        source_url = (row.get("source_url") or "").strip()
        source_id = (row.get("target_flgr_competitor_id") or "").strip()
        canonical_id = (row.get("proposed_canonical_athlete_id") or "").strip()
        canonical_name = (row.get("proposed_canonical_name") or "").strip()
        evidence_note = (row.get("evidence_note") or "").strip()
        try:
            season = int((row.get("season") or "").strip())
        except ValueError as error:
            raise FlgrEvidenceAcquisitionError(
                f"{path}:{line}: invalid season"
            ) from error
        if (
            cohort not in ALLOWED_COHORTS
            or sex not in {"M", "L"}
            or season < 2000
            or not source_id.isdigit()
            or not canonical_id.isdigit()
            or not canonical_name
            or not evidence_note
        ):
            raise FlgrEvidenceAcquisitionError(
                f"{path}:{line}: incomplete evidence request"
            )
        parse_source_url(source_url)
        source_shape = (cohort, season, sex)
        previous_source_shape = approved_sources.setdefault(source_url, source_shape)
        if previous_source_shape != source_shape:
            raise FlgrEvidenceAcquisitionError(
                f"{path}:{line}: conflicting source metadata for {source_url}"
            )
        target_key = (source_url, source_id)
        if target_key in seen_targets:
            raise FlgrEvidenceAcquisitionError(
                f"{path}:{line}: duplicate target {source_id} for {source_url}"
            )
        seen_targets.add(target_key)
        requests.append(
            EvidenceRequest(
                cohort=cohort,
                season=season,
                sex=sex,
                source_url=source_url,
                target_flgr_competitor_id=source_id,
                proposed_canonical_athlete_id=canonical_id,
                proposed_canonical_name=canonical_name,
                evidence_note=evidence_note,
            )
        )
    return requests


def _validate_output_dir(output_dir: Path) -> Path:
    resolved = output_dir.expanduser().resolve()
    if resolved == REPOSITORY_ROOT or REPOSITORY_ROOT in resolved.parents:
        raise FlgrEvidenceAcquisitionError(
            "raw FLGR evidence must be stored outside the Git repository"
        )
    if resolved.exists() and any(resolved.iterdir()):
        raise FlgrEvidenceAcquisitionError("evidence output directory must be empty")
    return resolved


def _default_fetch(source_url: str) -> bytes:
    request = Request(source_url, headers={"User-Agent": USER_AGENT})
    delays = (0, 1, 4)
    for attempt, delay in enumerate(delays, start=1):
        if delay:
            time.sleep(delay)
        try:
            with urlopen(request, timeout=30) as response:
                content_type = response.headers.get_content_type()
                if content_type != "text/html":
                    raise FlgrEvidenceAcquisitionError(
                        f"unexpected content type for {source_url}: {content_type}"
                    )
                html = response.read(MAX_PAGE_BYTES + 1)
                if not html or len(html) > MAX_PAGE_BYTES:
                    raise FlgrEvidenceAcquisitionError(
                        f"invalid page size for {source_url}: {len(html)} bytes"
                    )
                return html
        except HTTPError as error:
            if attempt == len(delays) or error.code not in {429, 500, 502, 503, 504}:
                raise FlgrEvidenceAcquisitionError(
                    f"FLGR request failed for {source_url}: HTTP {error.code}"
                ) from error
        except URLError as error:
            if attempt == len(delays):
                raise FlgrEvidenceAcquisitionError(
                    f"FLGR request failed for {source_url}: {error.reason}"
                ) from error
    raise AssertionError("unreachable FLGR retry state")


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(data)
        temporary = Path(handle.name)
    temporary.replace(path)


def _atomic_text(path: Path, text: str) -> None:
    _atomic_bytes(path, text.encode("utf-8"))


def _csv_text(rows: list[dict[str, object]], fields: list[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def acquire_flgr_evidence(
    *,
    manifest_path: Path,
    output_dir: Path,
    as_of: date,
    fetch: Callable[[str], bytes] = _default_fetch,
    retrieved_at: datetime | None = None,
) -> dict[str, object]:
    """Download and validate every explicitly approved evidence page."""
    output_dir = _validate_output_dir(output_dir)
    requests = _read_manifest(manifest_path)
    retrieved_at = retrieved_at or datetime.now(UTC)
    if retrieved_at.tzinfo is None:
        raise FlgrEvidenceAcquisitionError("retrieved_at must be timezone-aware")

    index_rows: list[dict[str, object]] = []
    observed_identities: dict[tuple[str, str], tuple[str, int | None]] = {}
    requests_by_url: dict[str, list[EvidenceRequest]] = {}
    for evidence in requests:
        requests_by_url.setdefault(evidence.source_url, []).append(evidence)

    cohort_pages: Counter[str] = Counter()
    for source_url, page_requests in requests_by_url.items():
        first_request = page_requests[0]
        html = fetch(source_url)
        if not html or len(html) > MAX_PAGE_BYTES:
            raise FlgrEvidenceAcquisitionError(
                f"invalid page size for {source_url}: {len(html)} bytes"
            )
        candidate = build_race_candidate(
            html,
            source_url=source_url,
            season=first_request.season,
            sex=first_request.sex,
            as_of=as_of,
            retrieved_at=retrieved_at,
        )
        safe_name = candidate["event_id"].replace(":", "_")
        raw_relative = Path("raw") / f"{safe_name}.html"
        candidate_relative = Path("candidates") / f"{safe_name}.json"
        _atomic_bytes(output_dir / raw_relative, html)
        _atomic_text(
            output_dir / candidate_relative,
            json.dumps(candidate, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )
        cohort_pages[first_request.cohort] += 1
        for evidence in page_requests:
            target = next(
                (
                    row
                    for row in candidate["results"]
                    if row["source_competitor_id"] == evidence.target_flgr_competitor_id
                ),
                None,
            )
            if target is None:
                raise FlgrEvidenceAcquisitionError(
                    f"target flgr:{evidence.target_flgr_competitor_id} is absent "
                    f"from {candidate['event_id']}"
                )
            identity_key = (evidence.sex, evidence.target_flgr_competitor_id)
            identity_shape = (target["source_name"], target["birth_year"])
            previous_identity = observed_identities.setdefault(
                identity_key, identity_shape
            )
            if previous_identity != identity_shape:
                raise FlgrEvidenceAcquisitionError(
                    f"source identity changed across evidence for {identity_key}"
                )
            index_rows.append(
                {
                    "cohort": evidence.cohort,
                    "event_id": candidate["event_id"],
                    "date": candidate["date"],
                    "season": candidate["season"],
                    "sex": candidate["sex"],
                    "distance": candidate["distance"],
                    "technique": candidate["technique"],
                    "source_url": evidence.source_url,
                    "source_sha256": candidate["source_sha256"],
                    "classified_results": len(candidate["results"]),
                    "target_athlete_id": target["athlete_id"],
                    "target_source_name": target["source_name"],
                    "target_birth_year": target["birth_year"],
                    "target_place": target["place"],
                    "proposed_canonical_athlete_id": (
                        evidence.proposed_canonical_athlete_id
                    ),
                    "proposed_canonical_name": evidence.proposed_canonical_name,
                    "mapping_status": "proposed_requires_review",
                    "evidence_note": evidence.evidence_note,
                    "raw_file": raw_relative.as_posix(),
                    "candidate_file": candidate_relative.as_posix(),
                }
            )

    fields = list(index_rows[0])
    _atomic_text(output_dir / "evidence-index.csv", _csv_text(index_rows, fields))
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "as_of": as_of.isoformat(),
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "race_pages": len(requests_by_url),
        "evidence_requests": len(index_rows),
        "cohorts": dict(sorted(cohort_pages.items())),
        "target_athletes": len(observed_identities),
        "mapping_applied": False,
        "publication_eligible": False,
        "status": "acquired_for_identity_review",
    }
    _atomic_text(
        output_dir / "evidence-report.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = acquire_flgr_evidence(
        manifest_path=args.manifest,
        output_dir=args.output_dir,
        as_of=args.as_of,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
