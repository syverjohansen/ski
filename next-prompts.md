# About This Document

This file serves as a **handoff document between Claude conversation sessions**. Since each conversation starts fresh without memory of previous sessions, this file provides:

1. **Persistent context** - Background on the current problem being solved and why
2. **Implementation reference** - Exact code patterns to follow for consistency
3. **Progress tracker** - Checklist showing what's done and what remains
4. **Quick onboarding** - User can say "read next-prompts.md" to bring Claude up to speed instantly

## IMPORTANT: Continuous Updates Required

**Claude must update this document continuously and proactively throughout each session without being prompted by the user.** This includes:

- Updating progress checklists as tasks are completed
- Adding new tasks or sub-tasks as they are discovered
- Documenting decisions made during implementation
- Recording any blockers or issues encountered
- Clearing completed projects and adding new ones as work shifts

The goal is for this document to always reflect the current state of the project, so the next session can pick up seamlessly.

---

# Current Task: Update K Calculation in elo_predict.py Files

**Status: COMPLETE**

## Background

In the ELO system, K is a factor that determines how much a single race result affects ELO ratings. Higher K = more volatile changes, lower K = more stable ratings.

### How K is Currently Calculated in elo.py (World Cup only)
```python
def k_finder(season_df, max_racers):
    racers = season_df.height  # race entries in current season
    k = float(max_racers / racers)
    k = min(k, 5)  # cap at 5
    k = max(k, 1)  # floor at 1
    return k
```
Where `max_racers` = maximum race entries across ALL seasons in the dataset.

### The Problem with elo_predict.py
`elo_predict.py` processes ALL professional races (not just World Cup), using the `all_*_scrape.csv` files. Previously, it calculated K based on all-races data, which caused inconsistency with the base WC ELO system because:
1. Non-WC events have different participation patterns
2. Seasons with many Continental Cup races would artificially lower K
3. K should match the volatility assumptions of the base WC ELO

### The Solution
Calculate K from the World Cup ELO file (`wc_elo_df`) that's already being read in, not from the all-races data. This ensures K values are consistent with the base ELO system.

## Implementation Details

### What to Change in elo_predict.py Files

**BEFORE** (old code around line 560-580):
```python
# Initialize pred_id_dict: predicted Elo for all skiers (starts at 1300)
pred_id_dict = {k: 1300.0 for k in id_dict_list}

# Calculate max_racers using Polars aggregation (no Python loop)
max_racers = df.group_by('Season').agg(pl.len().alias('count'))['count'].max()

# Get list of seasons
seasons = df['Season'].unique().sort().to_list()
```

And in the season loop:
```python
# Get the K value based on number of racers
K = k_finder(season_df, max_racers)
print(f"({season_year}, {K})")
```

**AFTER** (new code):
```python
# Initialize pred_id_dict: predicted Elo for all skiers (starts at 1300)
pred_id_dict = {k: 1300.0 for k in id_dict_list}

# Calculate K values from WC data for each season (for consistency with base ELO)
k_values = {}
if wc_elo_df is not None and not wc_elo_df.is_empty():
    # Filter out end-of-season markers (Race == 0) - only count actual races
    wc_races_only = wc_elo_df.filter(pl.col('Race') != 0)
    wc_season_counts = wc_races_only.group_by('Season').agg(pl.len().alias('count'))
    wc_max_racers = wc_season_counts['count'].max()
    for row in wc_season_counts.iter_rows():
        season, count = row
        k = float(wc_max_racers / count)
        k = min(k, 5)
        k = max(k, 1)
        k_values[season] = k

# Get list of seasons
seasons = df['Season'].unique().sort().to_list()
```

And in the season loop:
```python
# Get K from WC-based values; default to 5 if no WC races in this season
K = k_values.get(season_year, 5)
print(f"({season_year}, {K})")
```

### Key Points

1. **Filter Race == 0**: The WC ELO files have end-of-season rows (Race == 0, City == "Summer") for annual ELO resets. These must be excluded when counting race entries.

2. **Default K = 5**: If a season has no WC races but has other professional races, K defaults to 5 (maximum volatility) since we have no WC baseline.

3. **wc_elo_df**: This is the World Cup ELO file already being loaded (e.g., `M_Sprint.csv`, `L_Distance.csv`). The variable name may differ by sport.

## Files Completed
- [x] Cross-country: `/ski/polars/elo_predict.py`
- [x] Cross-country relay: `/ski/polars/relay/elo_predict.py`
- [x] Biathlon: `/biathlon/polars/elo_predict.py`
- [x] Biathlon relay: `/biathlon/polars/relay/elo_predict.py`
- [x] Nordic Combined: `/nordic-combined/polars/elo_predict.py`
- [x] Nordic Combined relay: `/nordic-combined/polars/relay/elo_predict.py`
- [x] Ski Jumping: `/skijump/polars/elo_predict.py`
- [x] Ski Jumping relay: `/skijump/polars/relay/elo_predict.py`
- [x] Alpine: `/alpine/polars/elo_predict.py`

## Notes
- Alpine does not have a relay folder
- The `k_finder` function can remain in the file (it's no longer called but doesn't need to be removed)
- The base path for all files is `~/ski/elo/python/`
