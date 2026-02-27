"""
Shared configuration module for Winter Sports Prediction Pipeline.

This module reads from ~/.env (or ~/ski/elo/.env) and provides
configuration values to all Python scripts in the pipeline.

Usage:
    from config import TEST_MODE, get_races_file, get_weekends_file

    # Check if in test mode
    if TEST_MODE:
        print("Running in TEST mode")

    # Get the appropriate file path
    races_file = get_races_file("ski")  # Returns test_races.csv or races.csv
"""

import os
from pathlib import Path

# Find and load .env file
def _load_env():
    """Load environment variables from .env file."""
    env_paths = [
        Path.home() / "ski" / "elo" / ".env",
        Path.home() / ".env",
        Path(__file__).parent.parent / ".env",  # ~/ski/elo/.env relative to this file
    ]

    for env_path in env_paths:
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        os.environ.setdefault(key, value)
            return str(env_path)
    return None

# Load .env on module import
_env_file = _load_env()

# Configuration values
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'

# Base paths
SKI_ELO_BASE = Path.home() / "ski" / "elo"
PYTHON_BASE = SKI_ELO_BASE / "python"
BLOG_BASE = Path.home() / "blog" / "daehl-e"

# Sport directory names
SPORTS = {
    'alpine': 'alpine',
    'biathlon': 'biathlon',
    'cross-country': 'ski',
    'ski': 'ski',
    'nordic-combined': 'nordic-combined',
    'skijump': 'skijump',
}

def get_sport_dir(sport: str) -> str:
    """Get the directory name for a sport."""
    return SPORTS.get(sport.lower(), sport)

def get_excel_dir(sport: str) -> Path:
    """Get the excel365 directory for a sport."""
    sport_dir = get_sport_dir(sport)
    return PYTHON_BASE / sport_dir / "polars" / "excel365"

def get_relay_excel_dir(sport: str) -> Path:
    """Get the relay excel365 directory for a sport."""
    sport_dir = get_sport_dir(sport)
    return PYTHON_BASE / sport_dir / "polars" / "relay" / "excel365"

def get_races_file(sport: str, relay: bool = False) -> Path:
    """
    Get the races.csv file path for a sport.

    Returns test_races.csv if TEST_MODE is True, otherwise races.csv.
    """
    base_dir = get_relay_excel_dir(sport) if relay else get_excel_dir(sport)
    filename = "test_races.csv" if TEST_MODE else "races.csv"
    return base_dir / filename

def get_weekends_file(sport: str, relay: bool = False) -> Path:
    """
    Get the weekends.csv file path for a sport.

    Returns test_weekends.csv if TEST_MODE is True, otherwise weekends.csv.
    """
    base_dir = get_relay_excel_dir(sport) if relay else get_excel_dir(sport)
    filename = "test_weekends.csv" if TEST_MODE else "weekends.csv"
    return base_dir / filename

def get_startlist_file(sport: str, gender: str, race_type: str = "races") -> Path:
    """
    Get the startlist file path for a sport.

    Args:
        sport: Sport name
        gender: 'men' or 'ladies'
        race_type: 'races', 'champs', 'ts', 'relay', 'mixed_relay'
    """
    base_dir = get_excel_dir(sport)

    if race_type in ['relay', 'ts', 'mixed_relay']:
        base_dir = get_relay_excel_dir(sport)

    if race_type == 'champs':
        filename = f"startlist_champs_{gender}.csv"
    elif race_type == 'ts':
        filename = f"startlist_ts_{gender}.csv"
    elif race_type == 'relay':
        filename = f"startlist_races_{gender}.csv"
    elif race_type == 'mixed_relay':
        filename = "startlist_mixed_relay.csv"
    else:
        filename = f"startlist_races_{gender}.csv"

    return base_dir / filename

# Print config on import if running directly
if __name__ == "__main__":
    print(f"Configuration loaded from: {_env_file}")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"SKI_ELO_BASE: {SKI_ELO_BASE}")
    print(f"PYTHON_BASE: {PYTHON_BASE}")
    print()
    print("Example paths:")
    for sport in ['alpine', 'ski', 'biathlon']:
        print(f"  {sport} races: {get_races_file(sport)}")
        print(f"  {sport} weekends: {get_weekends_file(sport)}")
