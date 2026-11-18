"""Configurable paths for cross-country team-event artifacts."""

from __future__ import annotations

import os
from pathlib import Path

EVENT_PREFIXES = {"relay": "relay", "team_sprint": "team_sprint"}
GENDERS = {"men", "ladies"}


def ski_data_root() -> Path:
    configured = os.environ.get("NORDIC_SKI_DATA_ROOT")
    return (
        Path(configured).expanduser().resolve()
        if configured
        else Path(__file__).resolve().parent.parent / "excel365"
    )


def team_data_root() -> Path:
    configured = os.environ.get("NORDIC_TEAM_DATA_ROOT")
    return (
        Path(configured).expanduser().resolve()
        if configured
        else Path(__file__).resolve().parent / "excel365"
    )


def schedule_path(*, weekend: bool = False) -> Path:
    return ski_data_root() / ("weekends.csv" if weekend else "races.csv")


def chrono_prediction_path(gender: str) -> Path:
    if gender not in GENDERS:
        raise ValueError(f"unsupported gender: {gender}")
    return ski_data_root() / f"{gender}_chrono_pred.csv"


def startlist_paths(event_type: str, gender: str) -> tuple[Path, Path]:
    try:
        prefix = EVENT_PREFIXES[event_type]
    except KeyError as exc:
        raise ValueError(f"unsupported team event type: {event_type}") from exc
    if gender not in GENDERS:
        raise ValueError(f"unsupported gender: {gender}")
    root = team_data_root()
    return (
        root / f"startlist_{prefix}_races_teams_{gender}.csv",
        root / f"startlist_{prefix}_races_individuals_{gender}.csv",
    )
