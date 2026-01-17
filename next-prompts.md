# Ski Prediction Systems Development Status

## Recently Completed Work (Current Session)

### Ski Jumping Race Scraper Fixes (COMPLETED)

**Objective**: Fix ski jumping race scraper that was only returning 8 events instead of ~100 events  
**Status**: COMPLETED

**Issues Identified and Fixed**:

1. **Race Grouping Algorithm Failure**
   - **Problem**: Qualification races getting separate rows instead of being properly paired with competitions
   - **Root Cause**: Overly complex matching algorithm that lost most races during grouping
   - **Solution**: Added standalone race handling and improved debug logging to track race processing

2. **"Unhashable Type" Error**
   - **Problem**: `TypeError: unhashable type: 'dict'` when trying to use race dictionaries in sets
   - **Root Cause**: Attempting to track used qualifications with dictionary objects in sets
   - **Solution**: Changed to track indices instead of dictionary objects (`used_qualification_indices`)

**Files Modified**:
- `/Users/syverjohansen/ski/elo/python/skijump/polars/race_scrape.py`
  - Added comprehensive debug logging
  - Fixed race grouping to handle standalone races
  - Changed used_qualifications tracking from dict objects to indices

**Results**: 
- Race scraper now successfully processes all ~100+ ski jumping events
- Proper qualification/competition pairing while preserving all races
- Found Ljubno January 9th/10th events that were previously missing

### Polars DataFrame Type Consistency Fix (COMPLETED)

**Objective**: Fix "could not append value: 141.0 of type: f64 to the builder" error occurring on VPS  
**Status**: COMPLETED

**Root Cause Analysis**:
- Polars DataFrame constructor failing when receiving mixed `None` and `float` values in same column
- `extract_jump_distance()` function returning either `None` or `float` values
- Length1, Length2, Points columns containing inconsistent data types
- Issue manifests differently between local (works) and VPS (fails) due to different Polars versions

**Solution Applied**:
```python
# Ensure consistent data types before creating DataFrame
# Convert None values to 0.0 for float columns to avoid type conflicts
for row in all_results:
    for col in ['Length1', 'Length2', 'Points']:
        if row.get(col) is None:
            row[col] = 0.0

df = pl.DataFrame(all_results)
```

**Files Fixed**:
1. **`/Users/syverjohansen/ski/elo/python/skijump/polars/scrape.py`** ✅ FIXED
   - Added type consistency check before DataFrame creation at line 813

2. **`/Users/syverjohansen/ski/elo/python/skijump/polars/relay/scrape.py`** ✅ FIXED  
   - Applied same fix to relay version

**Files Already Safe** (defensive programming already in place):
3. **`/Users/syverjohansen/ski/elo/python/skijump/polars/elo.py`** ✅ ALREADY SAFE
4. **`/Users/syverjohansen/ski/elo/python/skijump/polars/relay/elo.py`** ✅ ALREADY SAFE
5. **`/Users/syverjohansen/ski/elo/python/skijump/polars/chrono.py`** ✅ ALREADY SAFE
6. **`/Users/syverjohansen/ski/elo/python/skijump/polars/relay/relay_chrono.py`** ✅ ALREADY SAFE

### Nordic Combined Remaining Race Count Fix (COMPLETED)

**Objective**: Fix `calculate_remaining_races_nc` function returning 5 races instead of correct 8 men's races  
**Status**: COMPLETED

**Issues Fixed**:
1. **Race Type Mapping Mismatch**
   - **Problem**: Function looked for `"Individual Compact"` (with space) but data contains `"IndividualCompact"` (no space)
   - **Solution**: Fixed mapping: `RaceType == "IndividualCompact" ~ "Individual_Compact"`

2. **Added Debug Output**
   - Enhanced function with comprehensive logging to track date filtering and race categorization
   - Shows today's date, remaining races after filtering, and final counts by gender

**Files Modified**:
- `/Users/syverjohansen/blog/daehl-e/content/post/nordic-combined/drafts/race-recap2.R`
  - Fixed race type categorization (line 1060)
  - Added debug output for troubleshooting

**Results**: Function now correctly identifies 8 remaining men's non-championship races from 2026-01-12 onwards

## Current System Architecture

### Ski Jumping Prediction Pipeline
```
race_scrape.py → races.csv → startlist scrapers → predictions → race-picks.R
     ↓                ↓              ↓                ↓            ↓
- FIS website    - Race calendar  - Startlists    - GAM models  - Position probs
- Event details  - Date/venue     - Participant   - Linear      - Race probs  
- Qual/Comp      - Hill sizes     - Nation/ID     - fallback    - Normalization
```

### Data Flow Structure
1. **Web Scraping**: `race_scrape.py` processes FIS calendar → `races.csv` 
2. **Startlist Collection**: Various startlist scrapers get participant data
3. **Model Training**: `scrape.py` builds historical DataFrames → ELO processing
4. **Predictions**: `race-picks.R` generates position and race probabilities
5. **Output**: Normalized predictions for race selections

### File Organization
```
/ski/elo/python/
├── skijump/polars/           # Main ski jumping system
│   ├── scrape.py             # Historical data scraping
│   ├── race_scrape.py        # Race calendar scraping  
│   ├── elo.py               # ELO processing
│   ├── chrono.py            # Chronological data
│   ├── update_scrape.py     # Update pipeline
│   └── relay/               # Relay-specific versions
│       ├── scrape.py        # Relay historical data
│       ├── elo.py          # Relay ELO processing
│       ├── relay_chrono.py # Relay chronological
│       └── update_scrape.py # Relay updates
└── nordic-combined/polars/   # Nordic Combined system
    └── races.csv            # Race calendar
```

## Key Technical Insights

### Race Scraping Strategy
- **Dual-date system**: `Date` (qualification) and `Final_Date` (competition) for proper startlist/results matching
- **Startlist URLs**: Point to qualification races for complete participant field
- **Robust HTML parsing**: Multiple CSS selectors with fallbacks for category detection
- **Proper race grouping**: Qualification-competition pairing while preserving standalone races

### Data Type Management
- **Polars sensitivity**: Mixed None/float values cause DataFrame creation failures
- **Defensive programming**: Convert None → 0.0 before DataFrame creation
- **Version differences**: Local vs VPS environments handle type inference differently
- **Schema-based loading**: CSV reading with explicit schemas safer than dictionary construction

### ELO Processing Robustness
- **Experience tracking**: Max experience per ID maintained across seasons
- **Type safety**: Float64 casting with try-catch and fallback values
- **Chronological integrity**: Proper date sequencing for ELO progression

## Known Issues and Monitoring

### Current Operational Status
1. **Ski Jumping Scraper**: ✅ Working properly (~100+ events scraped)
2. **Polars DataFrames**: ✅ Type consistency issues resolved
3. **Nordic Combined Counter**: ✅ Race counting fixed
4. **Race Grouping**: ✅ Qual/competition pairing working

### Areas to Watch
1. **VPS vs Local Differences**: Monitor for environment-specific issues
2. **FIS Website Changes**: HTML structure changes could break scrapers
3. **New Race Formats**: Ensure race_scrape.py handles new event types
4. **Season Transitions**: Verify date filtering works across season boundaries

## Development Best Practices Established

### Error Handling Strategy
1. **Type Consistency**: Always ensure consistent data types before Polars DataFrame creation
2. **Defensive Programming**: Use try-catch blocks with sensible fallbacks
3. **Comprehensive Logging**: Add debug output for complex algorithms
4. **Environment Testing**: Test on both local and VPS environments

### Code Organization
1. **Modular Functions**: Separate concerns (scraping, processing, type handling)
2. **Consistent Patterns**: Apply same fixes across main/relay versions
3. **Schema Definitions**: Use explicit type schemas when possible
4. **Index-based Tracking**: Avoid using complex objects as dictionary keys

### Debugging Workflow
1. **Start with logging**: Add comprehensive debug output first
2. **Identify bottlenecks**: Find where data/races are being lost
3. **Type checking**: Verify data consistency at DataFrame creation points
4. **Cross-reference data**: Compare expected vs actual counts/types

## Future Work Considerations

### Short-term Priorities
1. **Monitor VPS stability**: Ensure fixes work consistently in production
2. **Validation checks**: Add data quality assertions in critical paths  
3. **Performance optimization**: Consider async processing for large scraping jobs
4. **Error notifications**: Implement alerts for scraping failures

### Medium-term Enhancements
1. **Incremental updates**: Only scrape new/changed races instead of full rebuilds
2. **Data validation**: Comprehensive checks for scraped data quality
3. **Backup strategies**: Multiple data sources/fallbacks for critical races
4. **Automated testing**: Unit tests for critical scraping and processing functions

### Technical Debt
1. **Code duplication**: Main vs relay versions could be unified with parameters
2. **Configuration management**: Hardcoded paths and settings should be configurable
3. **Error recovery**: More sophisticated retry and recovery mechanisms
4. **Documentation**: Inline documentation for complex algorithms

This represents a mature ski prediction system with robust data collection, processing, and prediction capabilities across multiple winter sports disciplines.

---

## Cross-Country Skiing System (Current Focus)

### Overview

The cross-country skiing prediction system consists of:
1. **Data scraping** (Python): FIS results, Russia results, startlists
2. **ELO calculations** (Python): elo_predict.py processes ratings
3. **Chrono files** (Python): chrono_dynamic.py and chrono_predict.py merge all ELO types
4. **Predictions** (R): weekly-picks2.R and race-picks.R generate probabilities

---

### Data Pipeline Flow

```
all_scrape.py → all_{gender}_scrape.csv
     ↓
russia_scrape.py → russia_{gender}_scrape.csv
     ↓
merge_scrape.py → combined_{gender}_scrape.csv (FIS + Russia data)
     ↓
elo_predict.py → pred_{gender}.csv, pred_{gender}_{type}.csv (9 ELO types each)
     ↓
chrono_predict.py → {gender}_chrono_pred.csv (all ELOs merged)
     ↓
startlist-scrape-weekend.py → startlist_weekend_{gender}.csv
     ↓
weekly-picks2.R → race probabilities and predictions
```

---

### Key Files - Python (~/ski/elo/python/ski/polars/)

| File | Purpose |
|------|---------|
| `all_scrape.py` | Scrapes all FIS race results |
| `all_update_scrape.py` | Updates scrape with new races |
| `russia_scrape.py` | Scrapes Russian ski federation results |
| `russia_update_scrape.py` | Updates Russia scrape |
| `merge_scrape.py` | Merges FIS and Russia data into combined files |
| `elo_predict.py` | Calculates ELO ratings (9 types: overall, distance, sprint, by technique) |
| `chrono_dynamic.py` | Merges all dyn_* ELO files into {gender}_chrono_dyn.csv |
| `chrono_predict.py` | Merges all pred_* ELO files into {gender}_chrono_pred.csv |
| `startlist-scrape-weekend.py` | Scrapes FIS startlists, creates weekend startlist CSVs |
| `startlist_common.py` | Shared utilities for startlist processing |
| `config.py` | Nation quotas and additional skiers configuration |

### Key Files - R (~/blog/daehl-e/content/post/cross-country/drafts/)

| File | Purpose |
|------|---------|
| `weekly-picks2.R` | Weekend race probability calculations |
| `race-picks.R` | Individual race predictions |

---

### ELO Types (9 per gender)

1. **Overall**: `pred_{L/M}.csv` → Elo, Pelo
2. **Distance**: `pred_{L/M}_Distance.csv` → Distance_Elo, Distance_Pelo
3. **Distance Classic**: `pred_{L/M}_Distance_C.csv` → Distance_C_Elo, Distance_C_Pelo
4. **Distance Freestyle**: `pred_{L/M}_Distance_F.csv` → Distance_F_Elo, Distance_F_Pelo
5. **Sprint**: `pred_{L/M}_Sprint.csv` → Sprint_Elo, Sprint_Pelo
6. **Sprint Classic**: `pred_{L/M}_Sprint_C.csv` → Sprint_C_Elo, Sprint_C_Pelo
7. **Sprint Freestyle**: `pred_{L/M}_Sprint_F.csv` → Sprint_F_Elo, Sprint_F_Pelo
8. **Classic**: `pred_{L/M}_C.csv` → Classic_Elo, Classic_Pelo
9. **Freestyle**: `pred_{L/M}_F.csv` → Freestyle_Elo, Freestyle_Pelo

---

### Recent Fixes Applied

#### 1. Race Probability Fixes in weekly-picks2.R (Lines ~337-420)

**Problem**:
- FIS list athletes (like Pellegrino) getting probability 0 instead of 1
- Race2_Prob was 0 for everyone instead of being calculated

**Root Cause**:
The `config_non_included` block set probabilities to 0 for `Config_Nation && !In_Config` without checking `In_FIS_List`. The preservation check `existing_prob %in% c(0, 1)` preserved ALL 0s including ones that should be calculated.

**Solution Applied**:

1. **config_non_included block** (~line 345): Sets prob=0 ONLY for:
   ```r
   config_non_included <- which(startlist$Config_Nation & !startlist$In_Config & !in_fis_list)
   ```

2. **Preservation logic** (~lines 384-420): Only preserve 0 if:
   - `prob == 1`: Always keep (confirmed racing)
   - `prob == 0` AND `Config_Nation && !In_Config && !In_FIS_List`: Keep
   - `prob == 0` AND `In_Config == TRUE`: Keep (explicit config no list)
   - Otherwise: Calculate using `get_race_probability()`

**Probability Rules**:
| Condition | Probability |
|-----------|-------------|
| In_FIS_List = TRUE | 1.0 (confirmed racing) |
| Config athlete with yes list | 1.0 |
| Config athlete with no list | 0.0 |
| Config_Nation && !In_Config && !In_FIS_List | 0.0 |
| Non-config nation athletes | CALCULATE from history |
| Config athletes without yes/no | CALCULATE from history |

---

#### 2. Elo Score Source Fix in race-picks.R (~line 423-440)

**Problem**:
`prepare_startlist_data` got Elo scores from `race_df` filtered by distance/technique. If last Sprint Freestyle was 6 months ago, Elo values were outdated.

**Solution**:
Read Elo scores from chrono_pred files (most recent values):

```r
chrono_pred_path <- if(gender == "men") {
  "~/ski/elo/python/ski/polars/excel365/men_chrono_pred.csv"
} else {
  "~/ski/elo/python/ski/polars/excel365/ladies_chrono_pred.csv"
}

most_recent_elos <- chrono_pred %>%
  filter(ID %in% base_df$ID) %>%
  group_by(ID) %>%
  arrange(Date, Season, Race) %>%
  slice_tail(n = 1) %>%
  ungroup() %>%
  dplyr::select(ID, any_of(elo_cols))
```

**Key Changes**:
- Uses **ID** instead of Skier for matching (handles duplicate names)
- Weighted points still come from filtered `race_df` (race-type specific)
- Join: Elos by ID, points by Skier

---

#### 3. Git Pre-commit Hook for Large Files

**File**: `~/ski/.git/hooks/pre-commit`

Automatically unstages files over 100MB:

```bash
#!/bin/bash
MAX_SIZE=104857600  # 100MB in bytes
staged_files=$(git diff --cached --name-only --diff-filter=ACM)

for file in $staged_files; do
    if [ -f "$file" ]; then
        file_size=$(wc -c < "$file" | tr -d ' ')
        if [ "$file_size" -gt "$MAX_SIZE" ]; then
            size_mb=$((file_size / 1048576))
            echo "Removing from commit (>${MAX_SIZE} bytes): $file (${size_mb}MB)"
            git reset HEAD "$file" >/dev/null 2>&1
        fi
    fi
done
exit 0
```

**Also added to .gitignore**:
```
# Large chrono files (too big for GitHub)
elo/python/ski/polars/excel365/ladies_chrono_pred.csv
elo/python/ski/polars/excel365/men_chrono_pred.csv
```

---

### Config.py Structure

**Location**: `~/ski/elo/python/ski/polars/config.py`

Contains:
1. **Nation quotas**: How many athletes each nation can enter
2. **Additional skiers**: Config for athletes with yes/no race lists

```python
def get_nation_quota(nation: str, gender: str, is_host: bool = False) -> int:
    # Returns quota for nation (host nations get +1)

def get_additional_skiers(gender: str) -> Dict[str, List]:
    # Returns dict of nation -> list of skier entries
    # Each entry can be string (name) or dict with yes/no lists:
    # {"name": "Athlete Name", "yes": [1, 2], "no": [3]}
    # yes = confirmed racing races 1,2
    # no = confirmed NOT racing race 3
```

---

### Startlist Processing Flow (startlist-scrape-weekend.py)

1. **Read weekends.csv** for today's races
2. **For each race**:
   - Scrape FIS startlist → FIS athletes get prob = 1.0
   - Add config athletes → prob based on yes/no or None
   - Add non-config nation athletes from chronos → prob = 0.0 (Python default, R calculates)
3. **Merge race dataframes** by Skier/ID
4. **Output**: `startlist_weekend_{gender}.csv`

---

### Relay System (~/ski/elo/python/ski/polars/relay/)

Parallel structure for relay races:
- `all_scrape.py`, `russia_scrape.py`, `merge_scrape.py`
- `elo_predict.py`
- `chrono_dynamic.py`, `chrono_predict.py`
- `startlist_scrape_weekend_relay.py`, `startlist_scrape_weekend_team_sprint.py`

Output files go to `relay/excel365/`

---

### Running the Pipeline

**Daily update sequence**:
```bash
cd ~/ski/elo/python/ski/polars

# 1. Update scrapes
python all_update_scrape.py
python russia_update_scrape.py
python merge_scrape.py

# 2. Calculate ELOs
python elo_predict.py

# 3. Merge chrono files
python chrono_predict.py

# 4. Weekend startlists (run on race day)
python startlist-scrape-weekend.py

# 5. R predictions (called automatically by startlist script)
# Or manually: Rscript ~/blog/daehl-e/content/post/cross-country/drafts/weekly-picks2.R
```

---

### Debugging Tips

1. **Check probability columns**: Look for Race1_Prob, Race2_Prob in startlist CSVs
2. **Verify FIS list status**: In_FIS_List should be TRUE for athletes on FIS startlist
3. **Config nation check**: Config_Nation = TRUE for major nations in config.py
4. **In_Config check**: TRUE only if athlete explicitly listed in config
5. **ELO freshness**: chrono_pred files should have recent dates for active athletes

### Common Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| FIS athlete has prob=0 | config_non_included overwrote | Check In_FIS_List flag |
| Race2_Prob all zeros | Same overwrite issue | Fixed in weekly-picks2.R |
| Outdated Elo values | Reading from filtered race_df | Fixed in race-picks.R to use chrono_pred |
| Large file push fails | Files >100MB | Pre-commit hook + .gitignore |
| Duplicate skier names | Matching by name | Use ID for joins |