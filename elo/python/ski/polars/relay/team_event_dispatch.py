"""Invoke packaged team-event startlist builders with strict failure handling."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

TEAM_EVENT_SCRIPTS = {
    "relay": "startlist_scrape_races_relay.py",
    "team_sprint": "startlist_scrape_races_team_sprint.py",
    "mixed_relay": "startlist_scrape_races_mixed_relay.py",
}


def run_startlist_builder(
    race_type: str,
    races_csv: Path,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> subprocess.CompletedProcess[str]:
    try:
        filename = TEAM_EVENT_SCRIPTS[race_type]
    except KeyError as exc:
        raise ValueError(f"unsupported team event type: {race_type}") from exc
    script = Path(__file__).resolve().parent / filename
    if not script.is_file():
        raise FileNotFoundError(f"team-event startlist builder is missing: {script}")
    if not races_csv.is_file():
        raise FileNotFoundError(f"team-event race input is missing: {races_csv}")
    return runner(
        [sys.executable, str(script), str(races_csv.resolve())],
        check=True,
        capture_output=True,
        text=True,
    )
