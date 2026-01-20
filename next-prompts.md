# All-Competitions Pipeline Development

## Objective

Update each ski sport to support "all professional races" (not just World Cup) by creating parallel scraping and ELO pipelines.

## Sports Status

| Sport | Python Location | R Location | Status |
|-------|-----------------|------------|--------|
| Cross-country | `~/ski/elo/python/ski/polars/{relay}` | `~/blog/daehl-e/content/post/cross-country/drafts` | DONE (reference) |
| Biathlon | `~/ski/elo/python/biathlon/polars/{relay}` | `~/blog/daehl-e/content/post/biathlon/drafts` | DONE (reference) |
| Nordic Combined | `~/ski/elo/python/nordic-combined/polars/{relay}` | `~/blog/daehl-e/content/post/nordic-combined/drafts` | DONE |
| Ski Jumping | `~/ski/elo/python/skijump/polars/{relay}` | `~/blog/daehl-e/content/post/skijump/drafts` | TODO |
| Alpine | `~/ski/elo/python/alpine/polars/` (no relay) | `~/blog/daehl-e/content/post/alpine/drafts` | TODO |

**Use biathlon as primary reference** - its code structure is most similar to nordic-combined, skijump, and alpine.

---

## Files to Create/Update for Each Sport

### 1. Python Scraping Files (main and relay directories)

**Create these files** (should be near-copies of existing scrape.py/update_scrape.py):

| New File | Based On | Changes Only |
|----------|----------|--------------|
| `all_scrape.py` | `scrape.py` | URL adds `&hva=k`, output is `all_{gender}_scrape.csv` |
| `all_update_scrape.py` | `update_scrape.py` | URL adds `&hva=k`, output is `all_{gender}_scrape.csv` |

**IMPORTANT**: These files should be nearly identical to their source files. The ONLY differences should be:
1. The base URL (add `&hva=k` parameter to include all competitions)
2. The output filename prefix (`all_` instead of regular)

Do NOT change the processing logic, sex handling, metadata structures, etc.

### 2. Python ELO Files (main and relay directories)

**Create these files** (based on sport's elo.py + biathlon's elo_predict/elo_dynamic patterns):

| File | Input | Output | Description |
|------|-------|--------|-------------|
| `all_elo.py` | `all_{gender}_scrape.csv` | `all_{gender}.csv`, `all_{gender}_{type}.csv` | ELO for all-competitions data (WC-only athletes) |
| `all_elo_script.sh` | - | - | Runs all_elo.py for all race types |
| `elo_predict.py` | `all_{gender}_scrape.csv` + `{gender}.csv` (WC elo) | `pred_{gender}.csv`, `pred_{gender}_{type}.csv` | Predicted ELO (all athletes get ratings) |
| `elo_predict_script.sh` | - | - | Runs elo_predict.py for all race types |
| `elo_dynamic.py` | `all_{gender}_scrape.csv` + `{gender}.csv` (WC elo) | `dyn_{gender}.csv`, `dyn_{gender}_{type}.csv` | Dynamic ELO (WC skiers maintain rating through all data) |
| `elo_dynamic_script.sh` | - | - | Runs elo_dynamic.py for all race types |

**IMPORTANT**: `elo_predict.py` and `elo_dynamic.py` use the **regular WC elo file** (`{gender}.csv` from `elo.py`) as reference, NOT the all_elo.py output. This is because we need to identify which athletes have WC Elo ratings to apply the correct rating logic.

### 3. Python Chrono Files (main and relay directories)

| File | Input | Output |
|------|-------|--------|
| `chrono_predict.py` | `pred_{gender}*.csv` files | `{gender}_chrono_pred.csv` |
| `chrono_dynamic.py` | `dyn_{gender}*.csv` files | `{gender}_chrono_dyn.csv` |

### 4. Startlist Scrapers (main and relay directories)

**Update existing files** to read from `chrono_pred.csv` instead of `chrono.csv`:
- `startlist-scrape-races.py`
- `startlist-scrape-weekend.py`
- `startlist-scrape-champs.py`
- All relay startlist scrapers

### 5. R Prediction Files

**Update `prepare_startlist_data` function** in:
- `race-picks.R`
- `weekly-picks2.R`
- `champs-predictions.R`

**The fix**: Use Elo values already in the startlist (from Python scrapers) instead of re-fetching from race_df.

```r
# WRONG - re-fetches Elo from race_df:
most_recent_elos <- race_df %>%
  filter(Skier %in% base_df$Skier) %>%
  group_by(Skier) %>%
  slice_tail(n = 1) %>%
  select(Skier, any_of(elo_cols))

result_df <- base_df %>%
  left_join(most_recent_elos, by = "Skier") %>%
  left_join(recent_points, by = "Skier")

# CORRECT - use Elo already in startlist:
base_df <- startlist %>%
  select(Skier, ID, Nation, Sex, all_of(race_prob_cols), any_of(elo_cols))

result_df <- base_df %>%
  left_join(recent_points, by = "Skier")  # Only join points, Elos already in base_df
```

---

## Data Flow Diagram

```
scrape.py ──────────────────────────────────────────────────────────────→ {gender}_scrape.csv (WC only)
                                                                                    ↓
                                                                               elo.py
                                                                                    ↓
                                                                         {gender}.csv (WC ELO)
                                                                                    ↓
                                                                              chrono.py
                                                                                    ↓
                                                                         {gender}_chrono.csv


all_scrape.py (&hva=k URL) ──────────────────────────────────────────→ all_{gender}_scrape.csv (all races)
                                                                                    ↓
                                                          ┌─────────────────────────┼─────────────────────────┐
                                                          ↓                         ↓                         ↓
                                                     all_elo.py              elo_predict.py            elo_dynamic.py
                                                          ↓                         ↓                         ↓
                                                  all_{gender}.csv          pred_{gender}.csv         dyn_{gender}.csv
                                                  (WC-only ELO on           (all athletes get         (WC athletes maintain
                                                   all-races data)           predicted ELO)            ELO through all data)
                                                                                    ↓                         ↓
                                                                            chrono_predict.py         chrono_dynamic.py
                                                                                    ↓                         ↓
                                                                         {gender}_chrono_pred.csv   {gender}_chrono_dyn.csv
                                                                                    ↓
                                                                         startlist scrapers (read chrono_pred)
                                                                                    ↓
                                                                         startlist CSVs (with Elo scores)
                                                                                    ↓
                                                                         R prediction files (use Elo from startlist)
```

---

## Current Session Progress

### Nordic Combined - DONE (main + relay)

**Completed**:
- `all_scrape.py` - Created with `&hva=k` URL parameter, outputs `all_{gender}_scrape.csv`
- `all_update_scrape.py` - Created with schema_overrides for HillSize, Distance, RaceType, SJ_Pos, CC_Pos, TeamID (all as pl.String to handle "N/A" values)
- `all_elo.py` + `all_elo_script.sh` - ELO for all-competitions data
- `elo_predict.py` + `elo_predict_script.sh` - Predicted ELO (all athletes get ratings)
- `elo_dynamic.py` + `elo_dynamic_script.sh` - Dynamic ELO (WC skiers maintain rating)
- `chrono_predict.py` - Combines pred files into chrono format
- `chrono_dynamic.py` - Combines dyn files into chrono format

**Key fix applied**: `elo_predict.py` and `elo_dynamic.py` use `{file_string}.csv` (regular WC elo from elo.py) instead of `all_{file_string}.csv` for the WC elo lookup. The all_elo.py output is NOT used as reference - the regular elo.py output is.

**Relay directory** (`~/ski/elo/python/nordic-combined/polars/relay/`):
- Same files created with relay-specific race types: "Team", "Team Sprint"

### TODO Next

1. **Do Ski Jumping** (main + relay)
   - Location: `~/ski/elo/python/skijump/polars/{relay}`
   - Create all_scrape.py, all_update_scrape.py
   - Create all_elo.py, elo_predict.py, elo_dynamic.py and scripts
   - Create chrono_predict.py, chrono_dynamic.py
   - Update startlist scrapers
   - Update R files

2. **Do Alpine** (main only, no relay)
   - Location: `~/ski/elo/python/alpine/polars/`
   - Same as above but no relay directory

---

## Reference File Locations

### Biathlon (PRIMARY REFERENCE)
```
Python: ~/ski/elo/python/biathlon/polars/
├── scrape.py, update_scrape.py
├── all_scrape.py, all_update_scrape.py
├── elo.py, all_elo.py, all_elo_script.sh
├── elo_predict.py, elo_predict_script.sh
├── elo_dynamic.py, elo_dynamic_script.sh
├── chrono.py, chrono_predict.py, chrono_dynamic.py
├── startlist-scrape-*.py
└── relay/ (same structure)

R: ~/blog/daehl-e/content/post/biathlon/drafts/
├── race-picks.R
├── weekly-picks2.R
└── champs-predictions.R
```

### Cross-Country (SECONDARY REFERENCE)
```
Python: ~/ski/elo/python/ski/polars/
R: ~/blog/daehl-e/content/post/cross-country/drafts/
```
