# Championships Prediction Pipeline Implementation Strategy

Based on the analysis in `prediction_pipeline_analysis.md`, this document outlines the implementation strategy for creating a Championships prediction pipeline that works across all winter sports.

## Overview

The Championships pipeline will create a unified system that:
1. Reads `weekends.csv` and filters for `Championship == 1` 
2. Processes all championship races simultaneously (ignoring Date column)
3. Uses sport-specific configurations for athlete quotas
4. Generates predictions using existing R scripts with modifications
5. Outputs results as a single comprehensive championship prediction

## Core Requirements from champs.md

1. **Championship Detection**: Filter `weekends.csv` for `Championship == 1`
2. **Date Agnostic Processing**: Process all championship races together regardless of Date
3. **Quota-Based Athlete Selection**: Only athletes from configuration lists, no "all season competitors"
4. **Unified Output**: Single shell script that handles config → startlist → R script → markdown
5. **Sport Consistency**: Works across alpine, biathlon, nordic-combined, ski, skijump

## Implementation Architecture

### 1. Directory Structure
```
~/ski/elo/python/{sport}/polars/
├── config.py                              # Enhanced with Championships config
├── startlist-scrape-champs.py             # New championship-specific script
├── champs-predictions.R                   # Modified R script for championships
└── run_champs_predictions.sh              # Master execution script
```

### 2. Enhanced Configuration System

Modify each sport's `config.py` to include Olympics-specific settings:

```python
# Nation quotas based on FIS World Cup rules (existing)
NATION_QUOTAS = { ... }

# Championships-specific quotas (new)
CHAMPS_QUOTAS = {
    'Norway': {'men': 4, 'ladies': 4},
    'Sweden': {'men': 4, 'ladies': 4},
    'USA': {'men': 4, 'ladies': 4},
    # ... other nations
}

# Championships athlete lists (new) 
CHAMPS_ATHLETES_MEN = {
    'Norway': ['Johannes Hoesflot Klaebo', 'Paal Golberg', 'Erik Valnes', 'Simen Hegstad Krueger'],
    'Sweden': ['Calle Halfvarsson', 'William Poromaa', 'Jens Burman', 'Marcus Grate'],
    'USA': ['Gus Schumacher', 'Ben Ogden', 'Scott Patterson', 'JC Schoonmaker'],
    # ... other nations
}

CHAMPS_ATHLETES_LADIES = {
    'Norway': ['Therese Johaug', 'Tiril Udnes Weng', 'Helene Marie Fossesholm', 'Kristine Stavaas Skistad'],
    'Sweden': ['Frida Karlsson', 'Ebba Andersson', 'Jonna Sundling', 'Maja Dahlqvist'],
    'USA': ['Jessie Diggins', 'Sophia Laukli', 'Julia Kern', 'Novie McCabe'],
    # ... other nations
}

def get_champs_quota(nation: str, gender: str) -> int:
    """Get Championships quota for a nation"""
    return CHAMPS_QUOTAS.get(nation, {}).get(gender, 2)  # Default 2 per nation

def get_champs_athletes(nation: str, gender: str) -> list:
    """Get Championships athlete list for a nation"""
    if gender == 'men':
        return CHAMPS_ATHLETES_MEN.get(nation, [])
    elif gender == 'ladies':
        return CHAMPS_ATHLETES_LADIES.get(nation, [])
    else:
        return []
```

### 3. Championships Startlist Script

Create `startlist-scrape-champs.py` based on existing weekend scripts:

```python
#!/usr/bin/env python3
"""
Championships Prediction Startlist Generator

Key differences from weekend scripts:
1. Filters weekends.csv for Championship == 1
2. Processes all championship races together (ignores Date)
3. Uses CHAMPS_ATHLETES lists instead of FIS startlists
4. Applies CHAMPS_QUOTAS instead of regular quotas
"""

import pandas as pd
from config import get_champs_quota, get_champs_athletes

def filter_champs_races():
    """Filter weekends.csv for championship races"""
    weekends_df = pd.read_csv('excel365/weekends.csv')
    champs_races = weekends_df[weekends_df['Championship'] == 1]
    return champs_races

def create_champs_startlist(champs_races, gender):
    """Create startlist using only configured Championships athletes"""
    data = []
    
    # Get all nations with Championships athletes
    all_nations = set()
    for nation in [list of nations with athletes]:
        athletes = get_champs_athletes(nation, gender)
        quota = get_champs_quota(nation, gender)
        
        for athlete in athletes[:quota]:  # Respect quota limits
            # Get ELO scores for athlete
            # Add fantasy pricing if available
            # Calculate race probabilities (100% for all Championships races)
            row_data = {
                'Skier': athlete,
                'Nation': nation,
                'Price': price,
                'Elo': elo_score,
                # Add Race1_Prob, Race2_Prob etc. = 1.0 for all races
            }
            data.append(row_data)
    
    return pd.DataFrame(data)
```

### 4. Championships R Script Modifications

Modify each sport's R script to create `champs-predictions.R`:

```r
# Key changes from weekly-picks2.R:
# 1. Don't look for "next weekend" - process all Championship races
# 2. All probabilities = 1.0 (everyone competes in all Championships races)
# 3. Read champs startlist instead of weekend startlist
# 4. Output to champs-predictions/ directory

# Read Championships race schedule
champs_races <- weekends %>%
  filter(Championship == 1)  # Get all championship races

# Read Championships startlists (generated by startlist-scrape-champs.py)
men_startlist <- read.csv("excel365/startlist_champs_men.csv")
ladies_startlist <- read.csv("excel365/startlist_champs_ladies.csv")

# Process all Championships races (no date filtering)
men_races <- champs_races %>%
  filter(Sex == "M") %>%
  filter(!Distance %in% c("Rel", "Ts"))  # Sport-specific filtering

# All athletes compete in all races (Championships assumption)
# Skip race probability calculations - set all to 1.0
```

### 5. Master Execution Script

Create `run_champs_predictions.sh` for each sport:

```bash
#!/bin/bash
# Championships Prediction Pipeline for {Sport}

SPORT_DIR="~/ski/elo/python/{sport}/polars"
CHAMPS_DATE=$(date '+%Y%m%d')
OUTPUT_DIR="~/blog/daehl-e/content/post/{content-sport}/champs-predictions"

cd "$SPORT_DIR"

echo "Starting Championships predictions for {sport}..."

# Step 1: Generate Championships startlists
echo "Generating Championships startlists..."
python startlist-scrape-champs.py

# Step 2: Run Championships R predictions
echo "Running Championships predictions..."
cd "$SPORT_DIR"
Rscript champs-predictions.R

# Step 3: Create output directory
mkdir -p "$OUTPUT_DIR/$CHAMPS_DATE"

# Step 4: Move generated files
mv excel365/champs-predictions/* "$OUTPUT_DIR/$CHAMPS_DATE/"

# Step 5: Generate Hugo markdown (optional)
echo "Generating Championships prediction markdown..."
python "$BLOG_DIR/static/python/create_champs_post.py" \
    "$OUTPUT_DIR/$CHAMPS_DATE" \
    "{sport}" \
    "$CHAMPS_DATE"

echo "Championships predictions complete for {sport}"
```

## Implementation Plan

### Phase 1: Alpine (Baseline)
1. **Rationale**: Simplest sport with individual races only
2. **Tasks**:
   - Enhance `alpine/polars/config.py` with Championships config
   - Create `startlist-scrape-champs.py`
   - Modify `weekly-picks2.R` → `champs-predictions.R`
   - Create `run_champs_predictions.sh`
   - Test end-to-end pipeline

### Phase 2: Ski Jumping
1. **Rationale**: Similar to alpine but adds hill size considerations
2. **Tasks**:
   - Adapt Alpine implementation
   - Add hill size handling
   - Test individual + team events

### Phase 3: Biathlon  
1. **Rationale**: Adds complexity with different race types
2. **Tasks**:
   - Handle Individual, Sprint, Pursuit, Mass Start
   - Add relay/mixed relay events
   - Test shooting sport specifics

### Phase 4: Nordic Combined
1. **Rationale**: Moderate complexity with team events
2. **Tasks**:
   - Handle Individual + Team formats
   - Add jumping + cross-country combination logic
   - Test team sprint variations

### Phase 5: Cross-Country (Full Complexity)
1. **Rationale**: Most complex with optimization and multiple race types
2. **Tasks**:
   - Handle all race formats (distance, sprint, relay, team sprint, mixed)
   - Implement optimization constraints for team events
   - Add fantasy team selections
   - Test full pipeline with all complexities

## Key Modifications Required

### 1. Weekend Script Changes
- Add championship detection: `weekends_df[weekends_df['Championship'] == 1]`
- Remove date filtering: process all championship races together
- Replace FIS startlists with configured athlete lists
- Set all race probabilities to 1.0 (Championships assumption)

### 2. R Script Changes
- Remove "next weekend" logic
- Read from `startlist_champs_{gender}.csv`
- Process all championship races simultaneously
- Output to `champs-predictions/` directory
- Maintain sport-specific points systems and logic

### 3. Configuration Integration
- Add Championships quotas per nation
- Add Championships athlete lists per nation per gender
- Create helper functions for Championships-specific data access
- Maintain backward compatibility with existing weekend scripts

## Testing Strategy

### 1. Unit Testing
- Test each sport's config.py modifications
- Verify Championships athlete list access
- Test quota calculations

### 2. Integration Testing  
- Test startlist-scrape-champs.py for each sport
- Verify R script modifications produce expected outputs
- Test end-to-end shell script execution

### 3. Cross-Sport Testing
- Run all sports' Championships pipelines
- Verify consistent output formats
- Test Hugo blog post generation

## Risk Mitigation

### 1. Backward Compatibility
- Keep existing scripts unchanged
- Championships scripts are additions, not modifications
- Separate output directories prevent conflicts

### 2. Configuration Management
- Use separate Championships config sections
- Maintain clear separation from daily prediction configs
- Document all new configuration options

### 3. Error Handling
- Add robust error checking to all Championships scripts
- Graceful degradation if athlete lists are incomplete
- Comprehensive logging for troubleshooting

## Success Criteria

1. **Functional**: All 5 sports generate Championships predictions successfully
2. **Consistent**: Output formats are compatible across sports
3. **Configurable**: Easy to update athlete lists and quotas
4. **Maintainable**: Clear separation from existing daily prediction system
5. **Automated**: Single shell script execution per sport

This implementation strategy provides a clear path from the current daily prediction system to a specialized Championships prediction pipeline while maintaining the flexibility and sport-specific optimizations that make each sport's predictions accurate.