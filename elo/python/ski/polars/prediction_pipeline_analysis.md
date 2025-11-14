# Prediction Pipeline Analysis: Cross-Country vs Other Winter Sports

This document analyzes the differences between cross-country skiing and other winter sports (alpine, biathlon, nordic-combined, skijump) in their prediction pipelines, based on analysis of the master_automation.sh and predict_script.sh workflows.

## Executive Summary

**Cross-country skiing has the most complex prediction pipeline** with multiple points systems, optimization requirements, and race format variations, while **alpine skiing has the most streamlined approach**. Other sports fall between these extremes with moderate complexity.

## Data Flow Overview

### Master Automation Pipeline
```bash
master_automation.sh (daily at midnight UTC)
├── Determines season dates across all sports
├── Checks for races today using races.csv files
├── If races found: runs predict_script.sh
├── Always runs: score_scrape.sh (yesterday's results)
├── Mondays only: recap_script.sh (weekly analysis)
└── Git automation (commit/push changes)
```

### Prediction Script Pipeline
```bash
predict_script.sh (when races detected)
├── For each sport (alpine, biathlon, nordic-combined, ski, skijump):
│   ├── Check weekends.csv for today's date → run startlist-scrape-weekend.py
│   ├── Check races.csv for today's date → run startlist-scrape-races.py
│   ├── Process generated Excel files → JSON conversion
│   └── Return prediction data by type (weekly-picks/race-picks)
├── Create Hugo blog posts:
│   ├── Weekend predictions → weekly-picks/{YYYYMMDD}.md
│   └── Race predictions → race-picks/{YYYYMMDD}.md
└── Generate final markdown content
```

## Key Sport Differences

### 1. Directory Structure and Naming
| Sport | Directory Name | Content Sport Name | R Script Pattern |
|-------|---------------|-------------------|------------------|
| Cross-Country | `ski` | `cross-country` | `weekly-picks2.R` + relay variants |
| Alpine | `alpine` | `alpine` | `weekly-picks2.R` |
| Biathlon | `biathlon` | `biathlon` | `weekly-picks2.R` |
| Nordic Combined | `nordic-combined` | `nordic-combined` | `weekly-picks2.R` |
| Ski Jumping | `skijump` | `skijump` | `weekly-picks2.R` |

### 2. Data Sources and Paths

**Standard Pattern:**
```
~/ski/elo/python/{sport}/polars/excel365/
├── weekends.csv
├── races.csv
├── {gender}_chrono{_suffix}.csv
└── startlist_weekend_{gender}.csv
```

**Key Differences:**
- **Cross-Country & Nordic Combined & Ski Jumping**: Use `*_chrono_elevation.csv`
- **Alpine**: Uses `*_chrono.csv` (no elevation suffix)
- **Biathlon**: Some chronological data in `biathlon/polars/relay/excel365/` subdirectory

### 3. Race Detection Logic

#### Cross-Country (Future-Looking)
```r
# Filter races after the current date and get the next date
next_races <- weekends %>%
  filter(Date >= current_date) %>%
  arrange(Date)
next_weekend_date <- min(next_races$Date, na.rm = TRUE)
```

#### All Other Sports (Current Day Only)
```r
# Filter races for the current date
next_races <- weekends %>%
  filter(Date == current_date) %>%
  arrange(Date)
```

**Impact**: Cross-country can predict future weekends, other sports only process same-day races.

### 4. Points Systems Complexity

#### Cross-Country (Most Complex - 3 Systems)
```r
wc_points <- c(100,95,90,85,80,75,72,69,...,3,2,1)      # 50 places
stage_points <- c(50,47,44,41,38,35,32,30,...,3,2,1)    # 30 places  
tds_points <- c(300,285,270,255,240,216,...,6,3)        # 49 places
```

#### Alpine (Simplest - 1 System)
```r
wc_points <- c(100,80,60,50,45,40,36,32,...,3,2,1)      # 30 places only
```

#### Other Sports (Moderate - 1-2 Systems)
- **Biathlon**: Regular (40 places) + Mass Start (30 places)
- **Nordic Combined**: Individual (40 places)
- **Ski Jumping**: Individual (30 places)

### 5. Race Categories and Filtering

#### Cross-Country (Most Complex)
```r
# Individual races
filter(!Distance %in% c("Rel", "Ts"))  # Exclude relay and team sprint

# Separate R scripts for:
# - weekly-picks2.R (individual)
# - weekly-picks-relay.R 
# - weekly-picks-mixed-relay.R
# - weekly-picks-team-sprint.R
```

#### Alpine (Simplest)
```r
# Individual races only - no relay exclusions needed
# Single R script handles all race types
```

#### Other Sports (Moderate)
```r
# Biathlon
filter(!RaceType %in% c("Relay", "Mixed Relay", "Single Mixed Relay"))

# Nordic Combined  
filter(!RaceType %in% c("Team", "Mixed Team", "Team Sprint"))

# Ski Jumping
filter(!grepl("Team", RaceType))
```

### 6. Configuration and Quota Systems

#### Cross-Country (Most Complex)
```r
# Complex national quotas with multiple tiers
base_quota <- 4  # Standard nations
major_nations <- c("Norway", "Sweden", "Finland", "Germany", "Switzerland", "Russia", "Italy", "France")
major_quota <- 6-8  # Major skiing nations
host_bonus <- +2    # Host country bonus

# Uses optimization libraries
library(ompr)          # For optimization model
library(ompr.roi)      # For optimization solver interface  
library(ROI.plugin.glpk) # For GLPK solver
```

#### Alpine (Simplest)
```r
# No special quota configurations
# Standard participation logic only
```

#### Other Sports (Moderate)
```r
# Standard quota systems without optimization
# Some elevation/hill size considerations
```

### 7. Race Field Processing

| Sport | Key Fields | Special Considerations |
|-------|------------|----------------------|
| Cross-Country | `Distance, Technique, MS, Elevation, Period, Pursuit` | Technique-specific ELO, elevation effects |
| Alpine | `Distance, Period, Country` | Discipline-specific ELO (DH, SG, GS, SL, AC) |
| Biathlon | `RaceType, Period, Elevation` | Shooting accuracy not in ELO model |
| Nordic Combined | `RaceType, Period, Elevation` | Combined ski jump + cross-country |
| Ski Jumping | `RaceType, Period, HillSize` | Hill size classifications (Normal, Large, Flying) |

### 8. Output Generation Patterns

#### Cross-Country (Most Complex)
```
content/post/cross-country/drafts/weekly-picks/{YYYYMMDD}/
├── men.xlsx
├── ladies.xlsx
├── men_relay.xlsx
├── ladies_relay.xlsx
├── mixed_relay.xlsx
├── men_team_sprint.xlsx
├── ladies_team_sprint.xlsx
└── fantasy_*.xlsx (multiple files)
```

#### Alpine (Simplest)
```
content/post/alpine/drafts/weekly-picks/{YYYYMMDD}/
├── men.xlsx
└── ladies.xlsx
```

#### Other Sports (Moderate)
```
content/post/{sport}/drafts/weekly-picks/{YYYYMMDD}/
├── men.xlsx
├── ladies.xlsx
├── {men/ladies}_{team_type}.xlsx (if applicable)
└── mixed_{team_type}.xlsx (if applicable)
```

## Critical Differences for Olympics Pipeline

### 1. Data Processing Complexity
- **Cross-Country**: Requires 3 points systems, optimization algorithms, multiple race formats
- **Alpine**: Single points system, straightforward individual races
- **Others**: Moderate complexity with 1-2 points systems and team events

### 2. Configuration Requirements
- **Cross-Country**: Complex quota systems requiring optimization libraries
- **Alpine**: Minimal configuration needs
- **Others**: Standard quota configurations

### 3. Race Format Handling
- **Cross-Country**: 4+ different race types requiring separate processing pipelines
- **Alpine**: Single race type (individual disciplines)
- **Others**: Individual + 1-2 team formats

### 4. Timing and Scheduling
- **Cross-Country**: Future-looking (can predict next weekend)
- **All Others**: Current-day only processing

### 5. Output Structure
- **Cross-Country**: Most complex with relay/team sprint variations + fantasy elements
- **Alpine**: Simplest with just men/ladies individual
- **Others**: Moderate with individual + team variations

## Implications for Olympics Prediction Pipeline

Based on this analysis, the Olympics prediction pipeline should:

1. **Use Alpine as the Base Template**: Simplest, most reliable pattern
2. **Add Cross-Country Complexity Gradually**: Most complex sport with optimization needs
3. **Handle Championship-Specific Logic**: Different from daily race processing
4. **Standardize Configuration Management**: Each sport needs consistent config.py usage
5. **Create Sport-Specific Implementations**: One size does not fit all

### Recommended Implementation Order
1. **Alpine** (baseline implementation)
2. **Ski Jumping** (alpine + hill size)
3. **Biathlon** (alpine + shooting/mass start)
4. **Nordic Combined** (biathlon + team events)
5. **Cross-Country** (full complexity with optimization)

This graduated approach allows testing and refinement of the Olympics pipeline architecture before tackling the most complex sport (cross-country).