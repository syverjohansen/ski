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