"""Strict runner for cross-country team-event R predictions."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Callable
from pathlib import Path

PREDICTION_SCRIPTS = {
    ("races", "relay"): "race-picks-relay.R",
    ("races", "team_sprint"): "race-picks-team-sprint.R",
    ("races", "mixed_relay"): "race-picks-mixed-relay.R",
    ("weekend", "relay"): "weekly-picks-relay.R",
    ("weekend", "team_sprint"): "weekly-picks-team-sprint.R",
    ("weekend", "mixed_relay"): "weekly-picks-mixed-relay.R",
}


class PredictionRunnerError(RuntimeError):
    """Raised when a team prediction command cannot be constructed safely."""


def resolve_prediction_script(
    script_type: str, race_type: str, *, blog_root: Path | None = None
) -> Path:
    try:
        filename = PREDICTION_SCRIPTS[(script_type, race_type)]
    except KeyError as exc:
        raise PredictionRunnerError(
            f"unsupported team prediction route: {script_type}/{race_type}"
        ) from exc
    configured_root = blog_root or (
        Path(value) if (value := os.environ.get("NORDIC_BLOG_ROOT")) else None
    )
    if configured_root is None:
        raise PredictionRunnerError(
            "NORDIC_BLOG_ROOT must point to the checked-out blog repository"
        )
    script = (
        configured_root.expanduser().resolve()
        / "content"
        / "post"
        / "cross-country"
        / "drafts"
        / filename
    )
    if not script.is_file():
        raise PredictionRunnerError(f"team prediction script is missing: {script}")
    return script


def run_prediction(
    script_type: str,
    race_type: str,
    *,
    gender: str | None = None,
    blog_root: Path | None = None,
    rscript_bin: str | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> subprocess.CompletedProcess[str]:
    """Run one checked team prediction script and propagate any failure."""
    script = resolve_prediction_script(
        script_type, race_type, blog_root=blog_root
    )
    command = [rscript_bin or os.environ.get("NORDIC_RSCRIPT_BIN", "Rscript"), str(script)]
    if gender:
        command.append(gender)
    return runner(command, check=True, capture_output=True, text=True)
