# Current Work Summary

## 1. Race Probability Fixes in weekly-picks2.R

**File:** `~/blog/daehl-e/content/post/cross-country/drafts/weekly-picks2.R`

### Problem
- FIS list athletes (like Pellegrino) were getting probability 0 instead of 1
- Race2_Prob was 0 for everyone instead of being calculated

### Root Cause
The `config_non_included` block was setting probabilities to 0 for athletes where `Config_Nation == TRUE && In_Config == FALSE`, without checking `In_FIS_List`. This overwrote FIS athletes' probabilities.

Additionally, the preservation check `existing_prob %in% c(0, 1)` was preserving ALL 0s, including default 0s from Python for non-config nation athletes who should have their probability calculated.

### Solution Applied (around line 337-360)
1. **config_non_included block**: Sets prob=0 ONLY for `Config_Nation && !In_Config && !In_FIS_List`
   - Athletes from major nations who aren't confirmed to race get 0
   - FIS list athletes keep their probability of 1

2. **Preservation logic** (around line 384-410): Only preserve 0 if:
   - `prob == 1`: Always keep (confirmed racing)
   - `prob == 0` AND `Config_Nation && !In_Config && !In_FIS_List`: Keep (config nation not in config)
   - `prob == 0` AND `In_Config == TRUE`: Keep (explicit config no list)
   - Otherwise: Calculate probability using `get_race_probability()` based on historical participation

### Key Logic
- FIS athletes: prob = 1 (confirmed)
- Config athletes with yes list: prob = 1
- Config athletes with no list: prob = 0
- Config nation athletes NOT in config AND NOT in FIS: prob = 0
- Non-config nation athletes: CALCULATE based on history
- Config athletes without yes/no: CALCULATE based on history

---

## 2. Elo Score Source Fix in race-picks.R

**File:** `~/blog/daehl-e/content/post/cross-country/drafts/race-picks.R`

### Problem
The `prepare_startlist_data` function was getting Elo scores from `race_df`, which is filtered by distance and technique. For example, if the race is Sprint Freestyle and the last Sprint Freestyle was 6 months ago, the Elo values would be outdated.

### Solution Applied (around line 423-440)
Changed to read Elo scores from the chrono_pred files which have the most recent values:

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

- Uses **ID** instead of Skier for matching (handles duplicate names)
- Weighted points still come from filtered `race_df` (correct for race-type-specific points)
- Join: Elos by ID, points by Skier

---

## 3. Git Pre-commit Hook

**File:** `~/ski/.git/hooks/pre-commit`

Created a hook that automatically unstages files over 100MB before commit (instead of blocking):

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

Also added to `.gitignore`:
```
# Large chrono files (too big for GitHub)
elo/python/ski/polars/excel365/ladies_chrono_pred.csv
elo/python/ski/polars/excel365/men_chrono_pred.csv
```

---

## Key Files Reference

| Purpose | File Path |
|---------|-----------|
| Weekly race predictions (R) | `~/blog/daehl-e/content/post/cross-country/drafts/weekly-picks2.R` |
| Individual race predictions (R) | `~/blog/daehl-e/content/post/cross-country/drafts/race-picks.R` |
| Python startlist scraper | `~/ski/elo/python/ski/polars/startlist-scrape-weekend.py` |
| Python common utilities | `~/ski/elo/python/ski/polars/startlist_common.py` |
| Config (nation quotas, skiers) | `~/ski/elo/python/ski/polars/config.py` |
| Men's chrono pred data | `~/ski/elo/python/ski/polars/excel365/men_chrono_pred.csv` |
| Ladies' chrono pred data | `~/ski/elo/python/ski/polars/excel365/ladies_chrono_pred.csv` |

---

## Data Flow

1. **Python** (`startlist-scrape-weekend.py`):
   - Scrapes FIS startlist
   - Sets Race1_Prob = 1.0 for FIS athletes
   - Sets Race{n}_Prob based on config yes/no lists or None
   - Outputs `startlist_weekend_men.csv` / `startlist_weekend_ladies.csv`

2. **R** (`weekly-picks2.R`):
   - Reads startlist CSVs
   - Calculates probabilities for athletes without preset 0 or 1
   - Normalizes to nation quotas
   - Generates predictions

3. **R** (`race-picks.R`):
   - For individual race predictions
   - Reads Elo from chrono_pred files (most recent)
   - Reads weighted points from filtered chronos (race-type specific)
