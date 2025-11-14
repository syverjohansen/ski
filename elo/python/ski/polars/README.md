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