#!/usr/bin/env python3
"""
Test Harness for Winter Sports Prediction Pipeline

This script runs the full pipeline in TEST_MODE for all sports,
validating that startlist scraping and R simulation scripts work correctly.

Usage:
    python test_harness.py                    # Test all sports
    python test_harness.py alpine             # Test specific sport
    python test_harness.py alpine biathlon    # Test multiple sports
    python test_harness.py --scrape-only      # Only run scraping phase
    python test_harness.py --simulate-only    # Only run simulation phase

The harness tests:
1. Startlist scraping (Python) - reads test_races.csv, scrapes startlists
2. Race picks simulation (R) - runs Monte Carlo predictions
3. Output validation - checks that expected files are generated
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
import traceback

# Configuration
SPORTS = {
    'alpine': {
        'scraper': '~/ski/elo/python/alpine/polars/startlist-scrape-races.py',
        'r_script': '~/blog/daehl-e/content/post/alpine/drafts/race-picks-simulation.R',
        'output_dir': '~/ski/elo/python/alpine/polars/excel365',
        'expected_outputs': ['startlist_races_men.csv', 'startlist_races_ladies.csv'],
    },
    'biathlon': {
        'scraper': '~/ski/elo/python/biathlon/polars/startlist-scrape-races.py',
        'r_script': '~/blog/daehl-e/content/post/biathlon/drafts/race-picks-simulation.R',
        'output_dir': '~/ski/elo/python/biathlon/polars/excel365',
        'expected_outputs': ['startlist_races_men.csv', 'startlist_races_ladies.csv'],
    },
    'cross-country': {
        'scraper': '~/ski/elo/python/ski/polars/startlist-scrape-races.py',
        'r_script': '~/blog/daehl-e/content/post/cross-country/drafts/race-picks-simulation.R',
        'output_dir': '~/ski/elo/python/ski/polars/excel365',
        'expected_outputs': ['startlist_races_men.csv', 'startlist_races_ladies.csv'],
    },
    'nordic-combined': {
        'scraper': '~/ski/elo/python/nordic-combined/polars/startlist-scrape-races.py',
        'r_script': '~/blog/daehl-e/content/post/nordic-combined/drafts/race-picks-simulation.R',
        'output_dir': '~/ski/elo/python/nordic-combined/polars/excel365',
        'expected_outputs': ['startlist_races_men.csv', 'startlist_races_ladies.csv'],
    },
    'skijump': {
        'scraper': '~/ski/elo/python/skijump/polars/startlist-scrape-races.py',
        'r_script': '~/blog/daehl-e/content/post/skijump/drafts/race-picks-simulation.R',
        'output_dir': '~/ski/elo/python/skijump/polars/excel365',
        'expected_outputs': ['startlist_races_men.csv', 'startlist_races_ladies.csv'],
    },
}

# Colors for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(msg):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{msg}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")

def print_success(msg):
    print(f"{Colors.GREEN}✓ {msg}{Colors.ENDC}")

def print_error(msg):
    print(f"{Colors.FAIL}✗ {msg}{Colors.ENDC}")

def print_warning(msg):
    print(f"{Colors.WARNING}⚠ {msg}{Colors.ENDC}")

def print_info(msg):
    print(f"{Colors.CYAN}→ {msg}{Colors.ENDC}")

def set_test_mode(enabled: bool) -> None:
    """Set TEST_MODE in the .env file"""
    env_file = Path.home() / "ski" / "elo" / ".env"

    # Read existing content
    content = ""
    if env_file.exists():
        with open(env_file, 'r') as f:
            lines = f.readlines()
            # Filter out existing TEST_MODE line
            lines = [line for line in lines if not line.strip().startswith('TEST_MODE')]
            content = ''.join(lines)

    # Add TEST_MODE line
    test_mode_value = "true" if enabled else "false"
    content = content.rstrip() + f"\nTEST_MODE={test_mode_value}\n"

    with open(env_file, 'w') as f:
        f.write(content)

    # Also set environment variable for current process
    os.environ['TEST_MODE'] = test_mode_value

    print_info(f"TEST_MODE set to {test_mode_value}")

def run_scraper(sport: str, config: dict, verbose: bool = False) -> tuple[bool, str]:
    """Run the startlist scraper for a sport"""
    scraper_path = Path(config['scraper']).expanduser()

    if not scraper_path.exists():
        return False, f"Scraper not found: {scraper_path}"

    print_info(f"Running scraper: {scraper_path.name}")

    try:
        # Change to the scraper's directory for relative imports
        scraper_dir = scraper_path.parent

        result = subprocess.run(
            [sys.executable, str(scraper_path)],
            cwd=str(scraper_dir),
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            env={**os.environ, 'TEST_MODE': 'true'}
        )

        if verbose:
            if result.stdout:
                print(f"STDOUT:\n{result.stdout}")
            if result.stderr:
                print(f"STDERR:\n{result.stderr}")

        if result.returncode != 0:
            return False, f"Scraper failed with return code {result.returncode}\n{result.stderr}"

        return True, "Scraper completed successfully"

    except subprocess.TimeoutExpired:
        return False, "Scraper timed out after 5 minutes"
    except Exception as e:
        return False, f"Error running scraper: {e}"

def run_r_simulation(sport: str, config: dict, verbose: bool = False) -> tuple[bool, str]:
    """Run the R simulation script for a sport"""
    r_script_path = Path(config['r_script']).expanduser()

    if not r_script_path.exists():
        return False, f"R script not found: {r_script_path}"

    print_info(f"Running R simulation: {r_script_path.name}")

    try:
        # Change to the R script's directory
        r_script_dir = r_script_path.parent

        result = subprocess.run(
            ['Rscript', str(r_script_path)],
            cwd=str(r_script_dir),
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout for R simulations
            env={**os.environ, 'TEST_MODE': 'true'}
        )

        if verbose:
            if result.stdout:
                print(f"STDOUT:\n{result.stdout}")
            if result.stderr:
                print(f"STDERR:\n{result.stderr}")

        if result.returncode != 0:
            return False, f"R script failed with return code {result.returncode}\n{result.stderr}"

        return True, "R simulation completed successfully"

    except subprocess.TimeoutExpired:
        return False, "R simulation timed out after 10 minutes"
    except Exception as e:
        return False, f"Error running R script: {e}"

def validate_outputs(sport: str, config: dict) -> tuple[bool, list]:
    """Validate that expected output files exist"""
    output_dir = Path(config['output_dir']).expanduser()
    expected = config['expected_outputs']

    missing = []
    found = []

    for filename in expected:
        filepath = output_dir / filename
        if filepath.exists():
            # Check if file is not empty
            size = filepath.stat().st_size
            if size > 0:
                found.append(f"{filename} ({size} bytes)")
            else:
                missing.append(f"{filename} (empty)")
        else:
            missing.append(filename)

    return len(missing) == 0, found, missing

def test_sport(sport: str, config: dict, scrape_only: bool = False,
               simulate_only: bool = False, verbose: bool = False) -> dict:
    """Test a single sport through the pipeline"""
    results = {
        'sport': sport,
        'scraper': {'success': None, 'message': ''},
        'simulation': {'success': None, 'message': ''},
        'validation': {'success': None, 'found': [], 'missing': []},
    }

    print_header(f"Testing: {sport.upper()}")

    # Phase 1: Scraping
    if not simulate_only:
        print(f"\n{Colors.BOLD}Phase 1: Startlist Scraping{Colors.ENDC}")
        success, message = run_scraper(sport, config, verbose)
        results['scraper'] = {'success': success, 'message': message}

        if success:
            print_success(message)
        else:
            print_error(message)
            if not scrape_only:
                print_warning("Continuing to simulation phase despite scraper error...")
    else:
        print_info("Skipping scraper phase (--simulate-only)")

    # Phase 2: R Simulation
    if not scrape_only:
        print(f"\n{Colors.BOLD}Phase 2: R Simulation{Colors.ENDC}")
        success, message = run_r_simulation(sport, config, verbose)
        results['simulation'] = {'success': success, 'message': message}

        if success:
            print_success(message)
        else:
            print_error(message)
    else:
        print_info("Skipping simulation phase (--scrape-only)")

    # Phase 3: Output Validation
    print(f"\n{Colors.BOLD}Phase 3: Output Validation{Colors.ENDC}")
    success, found, missing = validate_outputs(sport, config)
    results['validation'] = {'success': success, 'found': found, 'missing': missing}

    if success:
        print_success("All expected outputs found:")
        for f in found:
            print(f"  - {f}")
    else:
        if found:
            print_warning("Some outputs found:")
            for f in found:
                print(f"  - {f}")
        if missing:
            print_error("Missing outputs:")
            for f in missing:
                print(f"  - {f}")

    return results

def print_summary(all_results: list) -> int:
    """Print summary of all test results and return exit code"""
    print_header("TEST SUMMARY")

    total_pass = 0
    total_fail = 0

    for result in all_results:
        sport = result['sport']

        # Determine overall status
        scraper_ok = result['scraper']['success'] is None or result['scraper']['success']
        simulation_ok = result['simulation']['success'] is None or result['simulation']['success']
        validation_ok = result['validation']['success']

        all_ok = scraper_ok and simulation_ok and validation_ok

        if all_ok:
            print_success(f"{sport}: PASSED")
            total_pass += 1
        else:
            print_error(f"{sport}: FAILED")
            if not scraper_ok:
                print(f"    - Scraper: {result['scraper']['message'][:50]}...")
            if not simulation_ok:
                print(f"    - Simulation: {result['simulation']['message'][:50]}...")
            if not validation_ok:
                print(f"    - Missing: {result['validation']['missing']}")
            total_fail += 1

    print(f"\n{Colors.BOLD}Results: {total_pass} passed, {total_fail} failed{Colors.ENDC}")

    return 0 if total_fail == 0 else 1

def main():
    parser = argparse.ArgumentParser(
        description='Test harness for winter sports prediction pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        'sports',
        nargs='*',
        default=list(SPORTS.keys()),
        help='Sports to test (default: all)'
    )
    parser.add_argument(
        '--scrape-only',
        action='store_true',
        help='Only run the scraping phase'
    )
    parser.add_argument(
        '--simulate-only',
        action='store_true',
        help='Only run the simulation phase'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed output from scrapers and R scripts'
    )
    parser.add_argument(
        '--no-reset',
        action='store_true',
        help='Do not reset TEST_MODE to false after testing'
    )

    args = parser.parse_args()

    # Validate sport names
    invalid_sports = [s for s in args.sports if s not in SPORTS]
    if invalid_sports:
        print_error(f"Invalid sports: {invalid_sports}")
        print_info(f"Available sports: {list(SPORTS.keys())}")
        return 1

    print_header("WINTER SPORTS PREDICTION PIPELINE TEST HARNESS")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Sports to test: {args.sports}")

    # Enable TEST_MODE
    print(f"\n{Colors.BOLD}Enabling TEST_MODE...{Colors.ENDC}")
    set_test_mode(True)

    all_results = []

    try:
        for sport in args.sports:
            config = SPORTS[sport]
            result = test_sport(
                sport,
                config,
                scrape_only=args.scrape_only,
                simulate_only=args.simulate_only,
                verbose=args.verbose
            )
            all_results.append(result)
    finally:
        # Reset TEST_MODE unless --no-reset is specified
        if not args.no_reset:
            print(f"\n{Colors.BOLD}Resetting TEST_MODE to false...{Colors.ENDC}")
            set_test_mode(False)

    # Print summary and get exit code
    exit_code = print_summary(all_results)

    print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return exit_code

if __name__ == "__main__":
    sys.exit(main())
