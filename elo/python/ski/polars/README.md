# Ski Polars - Cross-Country Skiing ELO Rating and Fantasy Sports System

A comprehensive Python-based system for processing cross-country skiing race data, maintaining ELO ratings, and generating startlist predictions for fantasy sports applications.

## System Overview

This system handles the complete pipeline from race data collection to fantasy sports predictions:

1. **Race Data Collection** - Scrape FIS race calendars and results
2. **ELO Rating System** - Maintain historical ELO ratings for all athletes
3. **Startlist Generation** - Process weekend and individual race startlists
4. **Fantasy Integration** - Incorporate pricing and prediction data
5. **Data Updates** - Incrementally update datasets with new results

## Directory Structure

```
ski/polars/
├── config.py                           # Nation quotas and configuration
├── elo.py                              # Main ELO calculation engine
├── chrono.py                           # ELO data consolidation
├── elevation_chrono_merge.py           # Altitude data integration
├── scrape.py                           # Core race data scraping
├── update_scrape.py                    # Incremental data updates
├── startlist_common.py                 # Shared startlist utilities
├── startlist-scrape-weekend.py        # Weekend race processing
├── startlist-scrape-races.py          # Individual race processing
├── race_scrape.py                      # Comprehensive race scraper
├── scrape_fis_dates.py                 # Basic race date extraction
├── scrape_fis_races.py                 # Minimal race data scraper
├── standings_scrape.py                 # Current season rankings
├── excel365/                           # Data storage (CSV/Feather files)
├── relay/                              # Relay-specific processing
└── archive/                            # Legacy scripts
```

## Core Components

### Configuration (`config.py`)
**Purpose:** Central configuration for nation quotas and World Cup rules

**Key Data:**
- `NATION_QUOTAS`: FIS World Cup nation quotas by country and gender
- `WC_LEADERS`: Current World Cup leaders (get extra starts)
- `DEFAULT_QUOTA`: Fallback quota for unlisted nations

**Functions:**
- `get_nation_quota(nation, gender, is_host)` - Calculate total quota including bonuses
- `get_additional_skiers(gender)` - Get additional skiers by country

**Additional Skiers Format:**
The `ADDITIONAL_SKIERS_MEN` and `ADDITIONAL_SKIERS_LADIES` dictionaries should be formatted as country names (keys) mapping to lists of skier names (values):

```python
ADDITIONAL_SKIERS_MEN = {
    'Norway': ['Johannes Hoesflot Klaebo', 'Paal Golberg', 'Erik Valnes'],
    'Sweden': ['Calle Halfvarsson', 'William Poromaa', 'Jens Burman'],
    'USA': ['Gus Schumacher', 'Ben Ogden', 'Scott Patterson'],
    'Canada': ['Graham Ritchie', 'Antoine Cyr', 'Russell Kennedy'],
    'Finland': ['Iivo Niskanen', 'Ristomatti Hakola', 'Lauri Vuorinen']
}

ADDITIONAL_SKIERS_LADIES = {
    'Norway': ['Therese Johaug', 'Tiril Udnes Weng', 'Helene Marie Fossesholm'],
    'Sweden': ['Frida Karlsson', 'Ebba Andersson', 'Jonna Sundling'],
    'USA': ['Jessie Diggins', 'Sophia Laukli', 'Julia Kern'],
    'Finland': ['Kerttu Niskanen', 'Jasmi Joensuu', 'Krista Parmakoski'],
    'Germany': ['Katharina Hennig', 'Victoria Carl', 'Laura Gimmler']
}
```

These additional skiers are used as fallback athletes when FIS startlists are unavailable or incomplete.

### ELO Engine (`elo.py`)
**Purpose:** Main ELO rating calculation engine for athlete performance tracking

**Key Inputs:**
- Historical race result files (`*_scrape_update.feather`)
- Configuration parameters

**Key Outputs:**
- ELO rating files by category (`L.feather`, `M.feather`, `L_Distance.feather`, etc.)
- 9 different ELO variants per gender (general, distance, sprint × classic/freestyle/null)

**Functionality:**
- K-factor based ELO updates (K=32 default)
- Technique-specific ratings (classic, freestyle)
- Distance-specific ratings (sprint vs distance)
- Season progression tracking

### Data Consolidation (`chrono.py`)
**Purpose:** Merge multiple ELO rating files into consolidated chronological datasets

**Key Inputs:**
- 18 ELO rating files from `excel365/` directory
- 9 files each for men (M_*.feather) and ladies (L_*.feather)

**Key Outputs:**
- `ladies_chrono.feather` and `ladies_chrono.csv`
- `men_chrono.feather` and `men_chrono.csv`

**Processing Logic:**
- Forward-fill missing ELO values within athlete groups
- Offseason adjustments: `Elo = Pelo * 0.85 + 1300 * 0.15`
- Handles null techniques and merges all variants

### Elevation Integration (`elevation_chrono_merge.py`)
**Purpose:** Add altitude data to race results for performance analysis

**Key Inputs:**
- `men_chrono.feather` and `ladies_chrono.feather`
- `elevation.csv` (city→elevation mappings)

**Key Outputs:**
- `men_chrono_elevation.csv` and `ladies_chrono_elevation.csv`

**Processing:**
- Fuzzy matching for city names
- Default 0m elevation for missing cities
- Logging for missing elevation data

## Data Collection Scripts

### Schedule contract (`schedule_contract.py`)

`excel365/races.csv` is the canonical one-row-per-race schedule.
`excel365/weekends.csv` is a deterministic compatibility view derived from it;
it is not a second source of truth. The contract preserves the existing Tour de
Ski, relay, team-sprint, and championship grouping behavior while validating
schema, FIS cross-country URLs, race identities, season coverage, and rollover
safety.

Run a local, non-promoting shadow validation with:

```bash
python schedule_contract.py shadow \
  --candidate-races excel365/races.csv \
  --accepted-races excel365/races.csv \
  --output-root /tmp/nordicnumbers-xc-schedule-shadow
```

The command writes only beneath `--output-root`. A same-season candidate may be
reported as eligible; a complete next-season candidate is reported as requiring
approval. The command never modifies `excel365` itself.

The FIS acquisition path is also shadow-only:

```bash
python scrape_schedule_shadow.py \
  --season 2027 \
  --output-root /tmp/nordicnumbers-xc-schedule-acquisition
```

It reads the parameterized World Cup plus applicable World Championship or
Olympic calendars, uses stable FIS race IDs, and then runs the candidate through
the same contract. Missing calendars, unknown venues, ambiguous event metadata,
or incomplete schedules fail closed. Promotion remains a separate operation.

An approved next-season shadow can be promoted with its exact report digest:

```bash
python promote_schedule.py \
  --shadow-report /path/to/shadow/reports/schedule-shadow.json \
  --expected-report-sha256 <sha256> \
  --accepted-dir excel365 \
  --approve-season 2027 \
  --receipt /path/to/receipts/cross-country-2027.json
```

Promotion revalidates the candidate and unchanged accepted pair, creates a
timestamped backup, replaces both schedule files atomically, verifies their
hashes, and restores both originals if replacement or receipt writing fails.

### Results gap audit (`cross_country_results_gap.py`)

Before acquiring results, run the read-only audit against the accepted schedule
and individual chrono histories:

```bash
python cross_country_results_gap.py \
  --schedule excel365/races.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --as-of 2026-11-17 \
  --output /tmp/cross-country-results-gap.json \
  --fail-on-gaps
```

Future races are counted but never reported as missing. Relay and team-sprint
rows are excluded from the individual-results expectation and reported
separately. Completed legacy rows with incomplete schedule metadata are counted
without being mistaken for absent result rows. The command does not fetch or
modify data.

### Isolated results acquisition (`cross_country_results_backfill.py`)

When the audit reports completed individual gaps, acquire only those FIS result
pages into a candidate directory outside `excel365`:

```bash
python cross_country_results_backfill.py \
  --schedule excel365/races.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --as-of 2026-11-27 \
  --output-dir /tmp/cross-country-results-candidate \
  --allow-network
```

The command accepts only `fis-ski.com` cross-country result URLs, checks the
page city and season, records SHA-256 source evidence, and preserves the
schedule's technique and race-format metadata. It never modifies accepted
schedule, scrape, or chrono files. If no completed gaps exist—such as before
the 2027 opener—it exits without fetching anything.

### Athlete identity mapping (`cross_country_results_identity.py`)

Before Elo replay, map acquired FIS identities to existing Nordic Numbers IDs:

```bash
python cross_country_results_identity.py \
  --results /tmp/cross-country-results-candidate/results.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --aliases excel365/fis_identity_aliases.csv \
  --output-dir /tmp/cross-country-identity-candidate
```

An automatic match is allowed only when the normalized FIS name resolves to
exactly one historical athlete of the same sex. Nordic characters and FIS
transliterations are normalized deterministically. New spellings require an
exact reviewed alias binding the FIS competitor ID, FIS code, and FIS name to an
existing Nordic Numbers ID. Unknown, changed, or ambiguous identities fail the
whole candidate; accepted histories are never modified.

### Isolated Elo replay (`cross_country_results_elo_replay.py`)

After every candidate identity is resolved, generate incremental Elo rows in a
separate directory:

```bash
python cross_country_results_elo_replay.py \
  --mapped-results /tmp/cross-country-identity-candidate/mapped_results.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --output-dir /tmp/cross-country-elo-candidate
```

The replay preserves the nine existing rating streams, cross-country technique
and distance rules, mass-start metadata, championship event names, and the
postseason `0.85` regression toward the `1300` base rating. Pursuit events do
not change classic/freestyle-specific ratings. The command rejects team events,
duplicate races, invalid metadata, unknown canonical athletes, and output paths
inside accepted source directories. It hashes accepted histories before and
after replay and writes only incremental candidate rows.

### Team-event coverage audit (`cross_country_team_event_audit.py`)

Relay and team-sprint results intentionally live outside the individual chrono
tables. Audit their dedicated accepted histories separately:

```bash
python cross_country_team_event_audit.py \
  --schedule excel365/races2026.csv \
  --men-relay-chrono relay/excel365/men_relay_chrono.csv \
  --ladies-relay-chrono relay/excel365/ladies_relay_chrono.csv \
  --team-sprint-chrono relay/excel365/team_sprint_chrono.csv \
  --as-of 2026-03-22 \
  --fail-on-gaps
```

The audit matches canonical schedule identity `(sex, season, race, date,
distance)` to the accepted team tables. Future events never count as gaps. The
2026 regression fixture proves coverage for both Olympic relays and all six
scheduled team-sprint events.

### Promotion candidate (`cross_country_results_promote.py`)

Build complete candidate histories only after isolated replay succeeds:

```bash
python cross_country_results_promote.py \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --men-rows /tmp/cross-country-elo-candidate/men_elo_rows.csv \
  --ladies-rows /tmp/cross-country-elo-candidate/ladies_elo_rows.csv \
  --expected-season 2027 \
  --output-dir /tmp/cross-country-promotion-candidate
```

The candidate begins with the exact accepted history bytes and appends only
newer validated race rows. Race=0 archive rows remain unchanged and
undiscounted; the first new race row carries the discounted starting `Pelo`.
Returning athletes receive one offseason regression for every season boundary
they missed.
The command rejects duplicate or out-of-order results, mismatched headers,
incorrect season dates, and first-race ratings that do not match the replayed
offseason state. A header-only increment is allowed when only one sex races,
but a complete promotion cannot contain zero new rows. Accepted files are never
modified by this step.

### Comprehensive Race Scraper (`race_scrape.py`)
**Purpose:** Extract detailed race information from FIS calendars

**Data Sources:**
- FIS World Cup calendar (2026 season)
- FIS Olympic calendar (2026 season)

**Key Outputs:**
- `races2026.csv` with 17 race attributes

**Data Fields Extracted:**
- Date, Startlist URL, Sex, City, Country
- Distance, Technique, Mass Start, Period
- Pursuit, Stage, Final Climb, Elevation
- Race Number, Championship Status

**Processing Features:**
- Sophisticated distance/technique parsing
- Period-based season categorization
- Fuzzy elevation matching

### Basic Race Scrapers
**`scrape_fis_dates.py`** - Extracts just dates and startlist URLs  
**`scrape_fis_races.py`** - Minimal race data extraction  
Both output to `races2026.csv` with varying levels of detail.

### Current Rankings (`standings_scrape.py`)
**Purpose:** Scrape current World Cup standings

**Data Sources:**
- FirstSkiSport men's ranking URL
- FirstSkiSport women's ranking URL

**Key Outputs:**
- `men_standings.csv` and `ladies_standings.csv`

**Data Fields:**
- Athlete name, ranking position, points, nation
- Athlete ID and formatted names

## Startlist Processing

### Shared Utilities (`startlist_common.py`)
**Purpose:** Core utility functions used across startlist scripts

**Key Functions:**
```python
get_fis_startlist(url)           # Scrape FIS startlist URLs
get_fantasy_prices(gender)       # Fetch Fantasy XC pricing
get_latest_elo_scores(df, gender) # Process ELO with quartile imputation
fuzzy_match_name(name1, name2)   # Athlete name matching
convert_to_first_last(name)      # Name format conversion
```

**Manual Name Mappings:**
- Handles problematic athlete names
- Ensures consistent name matching across data sources

### Weekend Processing (`startlist-scrape-weekend.py`)
**Purpose:** Main script for processing weekend race batches

**Key Inputs:**
- `weekends.csv` (race schedule)
- `*_chrono_elevation.csv` (ELO data)
- FIS startlist URLs
- Fantasy XC pricing API

**Key Outputs:**
- `startlist_weekend_men.csv`
- `startlist_weekend_ladies.csv`
- Calls R scripts for final predictions

**Processing Logic:**
- UTC date-based filtering for current day
- Relay race detection → delegation to specialized scripts
- Comprehensive data integration: FIS + ELO + Fantasy + Quotas
- Fallback logic for unavailable startlists
- Duplicate race detection and removal

### Individual Race Processing (`startlist-scrape-races.py`)
**Purpose:** Process individual race days (non-weekend batches)

**Key Inputs:**
- `races.csv` (race schedule)
- ELO data files
- FIS startlist URLs

**Key Outputs:**
- `startlist_races_men.csv`
- `startlist_races_ladies.csv`
- Calls R scripts for race-day predictions

**Processing Features:**
- Next-available race detection with look-ahead
- Race-specific ELO prioritization (sprint vs distance, classic vs freestyle)
- Recent competitor analysis from chronological data
- Mixed-gender and relay race handling

## Data Update System

### Incremental Updates (`update_scrape.py`)
**Purpose:** Efficiently update existing datasets with new race results

**Key Inputs:**
- Existing `*_scrape.feather` files
- Current season FIS race data

**Key Outputs:**
- `*_scrape_update.feather` and `*_scrape_update.csv`

**Processing Features:**
- Parallel race processing with ThreadPoolExecutor
- Duplicate detection and removal based on race metadata
- Experience tracking - accumulates athlete experience across seasons
- Memory-efficient streaming for large datasets

### Core Scraping (`scrape.py`)
**Purpose:** Fundamental race data fetching and processing functions

**Key Features:**
- SSL context configuration
- Robust error handling and logging
- Birthday caching for athlete data
- Multi-threaded race processing
- Environmental compatibility checks

## Relay Processing

The `relay/` subdirectory contains specialized scripts for team events:

- **`elo.py`** - Relay-specific ELO calculations
- **`chrono.py`** - Relay chronological data processing
- **`scrape.py`** - Relay race data collection
- **`startlist_*.py`** - Various relay startlist processors:
  - Mixed relay (4×5km mixed teams)
  - Team sprint (6×1.25km)
  - Traditional relay (4×10km men, 4×5km women)
- **`update_scrape.py`** - Relay data updates

Team-event race predictions use `relay/prediction_runner.py`. The runner
requires `NORDIC_BLOG_ROOT` to identify the checked-out blog repository,
resolves the requested R script beneath that root, and propagates missing
scripts and non-zero R exits. `relay/team_event_dispatch.py` similarly invokes
packaged startlist builders with absolute paths and the active Python
interpreter. This replaces machine-specific `~/blog/...` paths and prevents a
failed relay or team-sprint prediction from being reported as successful.

## Data Flow

```
Race Calendars (FIS) → race_scrape.py → races.csv/weekends.csv
                                              ↓
Historical Results → scrape.py → *_scrape.feather → elo.py → ELO ratings
                                              ↓
ELO ratings → chrono.py → consolidated chronological data
                                              ↓
Chronological + elevation.csv → elevation_chrono_merge.py → altitude-enhanced data
                                              ↓
Weekend/Race Processing → startlist-scrape-*.py → startlists + R script calls
                                              ↓
New Results → update_scrape.py → updated datasets
```

## External Dependencies

### Data Sources
- **FIS Website**: Race calendars and startlists
- **FirstSkiSport**: Current World Cup standings
- **Fantasy XC API**: Athlete pricing data

### R Script Integration
- Weekly picks scripts: `weekly-picks*.R`
- Race day scripts: `race-picks*.R`
- Located in: `~/blog/daehl-e/content/post/cross-country/drafts`

### Python Libraries
- **polars**: Fast DataFrame operations
- **pandas**: Data manipulation
- **BeautifulSoup**: HTML parsing
- **numpy**: Numerical operations
- **aiohttp**: Async HTTP requests
- **concurrent.futures**: Parallel processing

## File Formats

### Primary Data Storage
- **Feather files**: Fast binary format for DataFrames
- **CSV files**: Human-readable exports and R script inputs

### Key Data Files
- `*_scrape_update.feather`: Latest race results
- `*_chrono_elevation.csv`: ELO histories with altitude
- `startlist_*.csv`: Processed startlists for predictions
- `races.csv/weekends.csv`: Race schedules
- `*_standings.csv`: Current World Cup rankings

## Usage Examples

### Generate Weekend Startlists
```bash
python startlist-scrape-weekend.py
```

### Update Race Data
```bash
python update_scrape.py
```

### Scrape Current Standings
```bash
python standings_scrape.py
```

### Process ELO Ratings
```bash
python elo.py
```

## Configuration

The system uses hard-coded configuration in Python files rather than external config files:

- **Nation quotas**: Defined in `config.py`
- **API endpoints**: Hard-coded in individual scripts
- **File paths**: Relative to `~/ski/elo/python/ski/polars/excel365/`
- **Rate limiting**: 5 requests/second for relay processing

## Archive Directory

The `archive/` directory contains legacy scripts that are no longer actively used but preserved for reference:

- **`chrono_pct.py`** - Chronological percentage calculations
- **`chrono_regress.py`** - Regression analysis on chronological data
- **`course_*.py`** - Course-specific data processing
- **`multithread_*.py`** - Experimental multi-threading implementations
- **`pdf_parser.py`** - PDF parsing utilities
- **`startlist_*.py`** - Legacy startlist processing scripts

These scripts represent earlier approaches or experimental features that have been superseded by the current implementation.
