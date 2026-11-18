"""Shared validation and atomic publication for standings scrapers."""

from __future__ import annotations

import os
import re
import tempfile
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = ("Skier", "ID", "Nation", "Ranking", "Points")


class StandingsContractError(RuntimeError):
    """Raised when scraped standings are incomplete or unsafe to publish."""


def season_for_date(as_of: date) -> int:
    """Return the FIS season-ending year for a calendar date."""
    return as_of.year + 1 if as_of.month >= 7 else as_of.year


def normalized_name(value: str) -> tuple[str, ...]:
    transliteration = str.maketrans(
        {
            "ä": "ae",
            "ö": "oe",
            "ü": "ue",
            "æ": "ae",
            "ø": "oe",
            "œ": "oe",
            "ł": "l",
            "ð": "d",
            "þ": "th",
            "ß": "ss",
        }
    )
    decomposed = unicodedata.normalize("NFKD", value.lower().translate(transliteration))
    ascii_like = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    tokens = re.findall(r"[a-z0-9]+", ascii_like)
    collapsed = []
    initials = []
    for token in tokens:
        if len(token) == 1:
            initials.append(token)
        else:
            if initials:
                collapsed.append("".join(initials))
                initials = []
            collapsed.append(token)
    if initials:
        collapsed.append("".join(initials))
    return tuple(sorted(collapsed))


def canonicalize_standings(
    records: list[dict], history: Path, gender: str, aliases: Path | None = None
) -> list[dict]:
    try:
        frame = pd.read_csv(history, dtype={"ID": str})
    except OSError as exc:
        raise StandingsContractError(f"cannot read {gender} history: {exc}") from exc
    required = {"ID", "Skier", "Nation", "Date"}
    missing = required - set(frame.columns)
    if missing:
        raise StandingsContractError(
            f"{gender} history is missing columns: {sorted(missing)}"
        )
    latest = frame.sort_values("Date").drop_duplicates("ID", keep="last")
    athletes_by_id = {str(row["ID"]): row for row in latest.to_dict("records")}
    index: dict[tuple[str, ...], list[dict]] = {}
    for row in latest.to_dict("records"):
        index.setdefault(normalized_name(str(row["Skier"])), []).append(row)

    reviewed_aliases = {}
    if aliases is not None:
        try:
            alias_frame = pd.read_csv(aliases, dtype=str).fillna("")
        except OSError as exc:
            raise StandingsContractError(
                f"cannot read {gender} identity aliases: {exc}"
            ) from exc
        required_alias_columns = {
            "sex",
            "fis_competitor_id",
            "fis_name",
            "canonical_id",
            "canonical_name",
        }
        missing_alias_columns = required_alias_columns - set(alias_frame.columns)
        if missing_alias_columns:
            raise StandingsContractError(
                f"identity aliases are missing columns: {sorted(missing_alias_columns)}"
            )
        sex = "L" if gender == "ladies" else "M"
        for alias in alias_frame[alias_frame["sex"].str.strip() == sex].to_dict(
            "records"
        ):
            reviewed_aliases[str(alias["fis_competitor_id"]).strip()] = alias

    canonical = []
    for record in records:
        fis_id = str(record.get("ID", "")).strip()
        alias = reviewed_aliases.get(fis_id)
        if alias is not None:
            if normalized_name(str(record.get("Skier", ""))) != normalized_name(
                str(alias["fis_name"])
            ):
                raise StandingsContractError(
                    f"{gender} FIS identity changed for reviewed alias {fis_id}: "
                    f"{record.get('Skier')!r}"
                )
            athlete = athletes_by_id.get(str(alias["canonical_id"]).strip())
            if athlete is None:
                raise StandingsContractError(
                    f"{gender} alias {fis_id} references missing canonical athlete "
                    f"{alias['canonical_id']!r}"
                )
            matches = [athlete]
        else:
            matches = index.get(normalized_name(str(record.get("Skier", ""))), [])
        if len(matches) != 1:
            state = "missing" if not matches else "ambiguous"
            raise StandingsContractError(
                f"{gender} standings contain {state} athlete identity: "
                f"{record.get('Skier')!r}"
            )
        athlete = matches[0]
        mapped = dict(record)
        mapped["ID"] = str(athlete["ID"])
        mapped["Skier"] = str(athlete["Skier"])
        mapped["Nation"] = str(athlete["Nation"])
        canonical.append(mapped)
    return canonical


def validate_standings(records: list[dict], gender: str) -> pd.DataFrame:
    if not records:
        raise StandingsContractError(f"no {gender} standings rows were scraped")
    frame = pd.DataFrame(records)
    missing = set(REQUIRED_COLUMNS) - set(frame.columns)
    if missing:
        raise StandingsContractError(
            f"{gender} standings are missing columns: {sorted(missing)}"
        )
    for column in ("Skier", "ID", "Nation"):
        if (
            frame[column].isna().any()
            or frame[column].astype(str).str.strip().eq("").any()
        ):
            raise StandingsContractError(
                f"{gender} standings contain blank {column} values"
            )
    if frame["ID"].astype(str).duplicated().any():
        raise StandingsContractError(
            f"{gender} standings contain duplicate athlete IDs"
        )
    rankings = pd.to_numeric(frame["Ranking"], errors="coerce")
    points = pd.to_numeric(frame["Points"], errors="coerce")
    if rankings.isna().any() or (rankings < 1).any():
        raise StandingsContractError(f"{gender} standings contain invalid rankings")
    if points.isna().any() or (points < 0).any():
        raise StandingsContractError(f"{gender} standings contain invalid points")
    frame["Ranking"] = rankings.astype(int)
    frame["Points"] = points.astype(int)
    return frame


def atomic_write_standings(records: list[dict], output: Path, gender: str) -> int:
    frame = validate_standings(records, gender)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=output.parent, delete=False
    ) as handle:
        frame.to_csv(handle, index=False)
        temporary = Path(handle.name)
    os.replace(temporary, output)
    return len(frame)
