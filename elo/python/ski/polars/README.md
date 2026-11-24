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

### Production source policy (`cross_country_source_policy.json`)

Official cross-country history uses classified results linked from the accepted
FIS schedule. Target-race eligibility comes from the official FIS startlist;
domestic participation alone never places an athlete in a prediction.

Russian domestic results are nevertheless required as a prediction-rating
bridge. They preserve evidence of form for returning athletes whose FIS history
has a multi-season gap and for new athletes who have raced only in Russia. A
new FLGR athlete starts at the `1300` baseline and earns a bridge rating through
verified races against the connected domestic field. Established athletes with
FIS history anchor that field and must also update through domestic results.
When a reviewed FLGR-to-FIS identity appears on an official FIS startlist, the
latest accepted bridge rating can be used for the prediction.

The legacy implementation remains quarantined while that bridge is rebuilt. It
fuzzy-matched identities (or created negative IDs), renumbered combined races,
did not consistently update established athletes through domestic results, and
had no as-of guard preventing future-dated rows. Its `russia_*_scrape.csv` and
`combined_*_scrape.csv` files therefore remain forbidden hosted inputs.

The target contract uses stable `flgr:<id>` event and athlete identities,
reviewed crosswalks, immutable source evidence, future-date rejection,
offseason regression, and explicit rating provenance. It creates a separate
prediction replay and never inserts domestic races into the official FIS
archive. The legacy scripts remain research references while this replacement
is implemented and backtested.

The first safe acquisition slice operates on a saved FLGR race page and writes
only to an empty candidate directory:

```bash
python cross_country_flgr_results.py \
  --race-html /path/to/flgr-race.html \
  --source-url https://flgr-results.ru/results/77/88 \
  --season 2027 \
  --sex M \
  --as-of 2026-11-23 \
  --output-dir /tmp/nordicnumbers-flgr-candidate
```

The command derives stable `flgr:<competition-id>:<race-id>` and
`flgr:<competitor-id>` identities, hashes the source page, maps an autumn race
in season 2027 to calendar year 2026, and rejects future-dated, incomplete,
duplicate-athlete, team, qualification, and roller-ski evidence. It produces an
isolated JSON manifest and normalized CSV; it performs no network request and
does not modify accepted FIS or prediction histories.

Resolve candidate identities in a second isolated directory with:

```bash
python cross_country_flgr_identity.py \
  --candidate /tmp/nordicnumbers-flgr-candidate/flgr-candidate.json \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --crosswalk excel365/flgr_identity_crosswalk.csv \
  --output-dir /tmp/nordicnumbers-flgr-identities
```

Every athlete retains the stable `flgr:<competitor-id>` bridge identity.
Reviewed crosswalk rows may attach an accepted canonical athlete ID for a FIS
rating anchor and later startlist join. Unmapped domestic athletes remain valid
`flgr_only` bridge participants, starting from the configured newcomer
baseline in the future replay. Deterministic Cyrillic-name similarity and birth
year comparisons are written only as review suggestions; suggestions are never
applied automatically. Conflicting names, birth years, reused canonical IDs,
unknown canonical athletes, or changed reviewed identities fail closed.

Replay one or more reviewed events chronologically into the isolated bridge
ledger with:

```bash
python cross_country_flgr_elo_replay.py \
  --identity-results /tmp/nordicnumbers-flgr-identities/flgr-identity-results.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --output-dir /tmp/nordicnumbers-flgr-replay \
  --k-factor 1.0
```

For a reviewed identity, the replay anchors from the latest accepted FIS row
strictly before that athlete's first supplied FLGR event. This prevents future
FIS results from leaking backward. It applies every missed offseason
regression, starts unmapped newcomers at `1300`, and updates both groups through
the same nine rating streams as official history. Stable FLGR event IDs are
retained and accepted chrono files are verified byte-for-byte unchanged.

The output is `flgr-bridge-ledger.csv` plus a provenance report. It is a
prediction-only candidate: the K factor is explicit and the report always
marks this slice as not publication eligible. It neither edits the official
FIS archive nor connects bridge ratings to live predictions.

Backtest multiple identically shaped replay candidates before choosing a K
factor:

```bash
python cross_country_flgr_bridge_backtest.py \
  --bridge-ledger k1=/tmp/replay-k1/flgr-bridge-ledger.csv \
  --bridge-ledger k5=/tmp/replay-k5/flgr-bridge-ledger.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --minimum-athletes 20 \
  --output-dir /tmp/nordicnumbers-flgr-backtest
```

For each reviewed athlete, the tool selects the first accepted FIS race after
their final supplied FLGR event. It evaluates stale FIS Elo and every candidate
against each opponent in that race using pairwise Brier error and directional
accuracy. An operator may also provide a normalized legacy ledger with
`--legacy-ledger`; legacy results remain explicitly quarantined benchmark data
and never become an input to production.

Candidate ledgers must cover identical athlete/event endpoints, source files
remain byte-for-byte unchanged, and a recommendation requires both the
configured minimum number of distinct returning athletes and lower Brier error
than stale FIS Elo. The detailed cases and report remain prediction-only and
always set `publication_eligible` to false.

The reviewed 2021 historical cohort met that experiment's threshold. Across 22
explicitly selected returning athletes and 1,661 pairwise comparisons, `K=2`
had the best candidate Brier score (`0.16962191`, versus `0.17069568` for stale
FIS Elo) and higher directional accuracy (`0.75436484`, versus `0.74714028`).
The immutable experiment record is `excel365/flgr_bridge_calibration.json`.
That `K=2` selection is now superseded because the experiment allowed verified
and unverified athletes to exchange Elo. The accepted fixed-anchor contract
keeps prior A-tier athletes immutable, ignores unknown-versus-unknown ordering,
and must be recalibrated before any bridge can become publication eligible.

The source-neutral contract is recorded in
`cross_country_prediction_elo_policy.json`. A-tier FIS categories (`WC`, Tour
stages, `WSC`, and `OWG`) remain the only accepted Elo inputs. Approved
lower-tier FIS categories provide prediction evidence for never-A-tier athletes
without entering official history. National federation sources are fallbacks
only for events absent from FIS and must use the identical comparison rules.

Acquire completed lower-tier FIS result evidence into an isolated directory:

```bash
python cross_country_fis_prediction_acquire.py \
  --season 2027 \
  --category SCAN \
  --category FIS \
  --accepted-schedule excel365/races.csv \
  --as-of 2026-11-23 \
  --output-dir /tmp/nordicnumbers-fis-prediction-evidence \
  --allow-network
```

The command discovers result pages only beneath explicitly approved FIS
cross-country calendar categories, skips future and team races, deduplicates
shared event pages and race IDs, and excludes every race ID already present in
the accepted A-tier schedule. It stores hashed raw evidence and normalized
classified results outside the accepted source tree. The output remains
identity-unresolved, prediction-only, and never publication eligible.

Classify those participants against accepted A-tier history as of each race:

```bash
python cross_country_fis_prediction_identity.py \
  --results /tmp/nordicnumbers-fis-prediction-evidence/fis-prediction-results.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --aliases excel365/fis_identity_aliases.csv \
  --output-dir /tmp/nordicnumbers-fis-prediction-identities
```

An athlete becomes a fixed anchor only when accepted history contains an
A-tier race strictly before the lower-tier evidence date. The classifier uses
the latest seasonally regressed official rating without changing it. Athletes
with no prior A-tier result retain `fis:<competitorid>` prediction identities.
It emits only unknown-to-anchor comparisons; unknown ordering is absent, so
two unknowns between the same verified finishers receive identical evidence.

Replay prediction ratings for never-A-tier athletes only:

```bash
python cross_country_fis_prediction_replay.py \
  --participants /tmp/nordicnumbers-fis-prediction-identities/fis-prediction-participants.csv \
  --comparisons /tmp/nordicnumbers-fis-prediction-identities/fis-anchor-comparisons.csv \
  --k-factor 4.0 \
  --output-dir /tmp/nordicnumbers-fis-prediction-replay
```

Each prediction identity starts from the private `1300` prior. Only direct
outcomes against fixed A-tier anchors update it; anchor-free appearances carry
the prior rating forward unchanged, and verified participants never enter the
prediction ledger. Matching discipline streams update independently and every
missed season applies the normal offseason regression. The reviewed calibration
selects K=4.0; the rating remains private until its shadow prediction outputs
have passed review.

Build a review queue from one or more acquired lower-tier result batches:

```bash
python cross_country_fis_prediction_cohort.py \
  --results /tmp/fis-2025/fis-prediction-results.csv \
  --results /tmp/fis-2026/fis-prediction-results.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --aliases excel365/fis_identity_aliases.csv \
  --minimum-athletes 20 \
  --output-dir /tmp/nordicnumbers-fis-calibration-cohort
```

The cohort builder keeps existing reviewed aliases reviewed, but an exact,
unique name match is only a `proposed_unique_name`. It records the last
lower-tier result before the athlete's first accepted A-tier race so an
operator can review the identity evidence. Proposed rows never count toward
the 20-athlete calibration gate until explicitly reviewed.

Backtest several identically shaped replay ledgers with reviewed identities:

```bash
python cross_country_fis_prediction_backtest.py \
  --candidate-ledger k0.5=/tmp/replay-k0.5/fis-prediction-ledger.csv \
  --candidate-ledger k1=/tmp/replay-k1/fis-prediction-ledger.csv \
  --candidate-ledger k2=/tmp/replay-k2/fis-prediction-ledger.csv \
  --identity-crosswalk /tmp/reviewed-fis-prediction-crosswalk.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --minimum-athletes 20 \
  --output-dir /tmp/nordicnumbers-fis-prediction-backtest
```

The crosswalk must explicitly review each prediction-only FIS identity against
the canonical athlete who later debuted at A tier. The harness rejects any
athlete already present in accepted A-tier history by their replay endpoint,
requires every K candidate to use identical evidence, and reads the K factor
from the ledger itself. It scores matching Elo streams against pairwise results
in the athlete's first A-tier race. Outputs remain research-only even when a
candidate beats the official prior; selecting a production K factor requires a
separate reviewed calibration decision.

Optionally refine the pre-startlist candidate pool with a reviewed announcement
artifact:

```bash
python cross_country_weekend_inputs.py \
  --schedule excel365/weekends.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --aliases excel365/fis_identity_aliases.csv \
  --group-date 2026-11-27 \
  --announced-rosters /path/to/reviewed-announced-rosters.json \
  --output-dir /tmp/weekend-inputs
```

The reviewed JSON uses
`nordicnumbers.cross-country-announced-rosters/v1`. Each entry binds an exact
canonical athlete ID to `confirmed` or `excluded`, one or more FIS race IDs (or
`"*"` for the whole weekend), an HTTPS evidence URL, and a timezone-aware
observation timestamp. Announcements overlay the existing latest-season pool;
they do not replace unmentioned countries or athletes. An official FIS
startlist always supersedes the entire announcement overlay for that event.

Acquire the underlying FantasyXC posts as publication-disabled review evidence:

```bash
python cross_country_fantasyxc_feed.py \
  --logical-date 2026-11-27 \
  --as-of 2026-11-23 \
  --output-dir /tmp/fantasyxc-evidence \
  --report /tmp/fantasyxc-evidence-report.json \
  --allow-network
```

The acquisition reads only the fixed public
`https://fxtwitter.com/FantasyXC/feed.xml` endpoint, retries transient failures,
caps the response at 2 MiB, filters posts to a bounded pre-weekend window, and
retains both the raw XML and normalized items with SHA-256 checksums. Its output
is `candidate_review`, never a reviewed roster and never publication eligible;
athlete identity and roster-claim resolution remain a separate guarded step.

Create a shadow-only startlist rating overlay after building weekend inputs:

```bash
python cross_country_prediction_elo_shadow.py \
  --startlist /tmp/weekend-inputs/men_startlist.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --prediction-ledger /tmp/replay-k4/fis-prediction-ledger.csv \
  --policy cross_country_prediction_elo_policy.json \
  --output-dir /tmp/weekend-prediction-elo-shadow
```

Run the weekend input builder with `--prediction-shadow` to retain an unmatched
official FIS starter under the stable `fis:<competitorid>` identity. Without
that explicit flag, the builder continues to fail closed. The overlay keeps
accepted A-tier Elo for known athletes, uses the latest K=4 ledger row strictly
before the target race for never-A-tier athletes, and gives a true newcomer the
private 1300 prior. Its report always disables publication.

Build the current private ledger reproducibly from the allowlisted FIS
categories before creating that overlay:

```bash
python cross_country_prediction_elo_pipeline.py \
  --season 2027 \
  --as-of 2026-11-23 \
  --accepted-schedule excel365/races.csv \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --aliases excel365/fis_identity_aliases.csv \
  --policy cross_country_prediction_elo_policy.json \
  --output-dir /tmp/current-fis-prediction-elo \
  --allow-network
```

The command composes bounded acquisition, as-of identity classification, and
the calibrated K=4 replay. It verifies that accepted sources remain unchanged,
copies the final `fis-prediction-ledger.csv` to the output root, records every
stage checksum, and always disables publication. Omitting `--category` uses
the complete policy allowlist; one or more explicit `--category` arguments may
be supplied for a bounded diagnostic run.

Acquire the explicitly approved historical pages into an operator-owned
directory outside Git with:

```bash
python cross_country_flgr_evidence_acquire.py \
  --manifest excel365/flgr_historical_evidence_manifest.csv \
  --as-of 2026-11-23 \
  --output-dir /tmp/nordicnumbers-flgr-evidence
```

The manifest contains 15 classified post-ban holdout races in the final three
domestic competition blocks before Saveliy Korostelev and Dariya Nepryaeva
returned to FIS racing on 2025-12-13, plus four 2021 historical calibration
races covering 22 explicitly reviewed returning athletes. Each URL is HTTPS
and explicit; the downloader never crawls arbitrary links. It validates the
current FLGR page markup, requires the expected source athlete to be
classified, hashes and stores the raw page, writes a normalized candidate, and
produces a review index. Proposed canonical IDs remain
`proposed_requires_review`; acquisition never edits the reviewed crosswalk or
applies a mapping.

A manifest may repeat an approved race URL for multiple explicitly named
target athletes when constructing the historical calibration cohort. The
cohort, season, and sex must agree for every row sharing that URL. Acquisition
fetches and hashes the page once, writes one candidate, and emits one review
index row per target; resolution likewise processes the event once while
requiring every proposed target to match an exact reviewed crosswalk row.

Raw pages are intentionally excluded from Git and must be written to a new,
empty external directory. The checked-in manifest is only the reproducible
locator and review intent. The two post-ban returners remain an independent
holdout cohort and are not selected as historical calibration targets.

After acquisition, recheck every raw checksum and resolve all candidates into
one chronological replay input with:

```bash
python cross_country_flgr_evidence_resolve.py \
  --evidence-dir /tmp/nordicnumbers-flgr-evidence \
  --men-chrono excel365/men_chrono.csv \
  --ladies-chrono excel365/ladies_chrono.csv \
  --crosswalk excel365/flgr_identity_crosswalk.csv \
  --cohort historical_calibration \
  --output-dir /tmp/nordicnumbers-flgr-identities
```

Always resolve calibration and post-ban holdout cohorts separately. Selecting
`--cohort historical_calibration` prevents a reviewed athlete's later domestic
results from leaking into the historical endpoint used to evaluate their next
accepted FIS race.

Within a selected cohort, only identities explicitly named by that cohort's
evidence rows receive reviewed canonical mappings. Other racers on the same
page remain `flgr_only` participants. This preserves the rating field without
silently pulling a holdout athlete into calibration.

The reviewed crosswalk now contains the two post-ban holdout identities:
`flgr:109411` maps to canonical FIS-history athlete `13188` (Saveliy
Korostelev), and `flgr:204470` maps to `6555` (Dariya Nepryaeva). The review
uses the FLGR profile English name and birthdate together with the accepted FIS
history name and birth year/date. No fuzzy suggestion was promoted.

The batch resolver prevents path traversal, rehashes every raw page, verifies
candidate metadata against the acquisition index, and requires each proposed
target to equal an exact reviewed crosswalk row. All other domestic athletes
retain stable `flgr:<id>` identities and remain `flgr_only`. Its combined CSV
is prediction-only replay input and cannot update the official FIS archive.

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

Before running a model, validate the paired team and individual startlists with
`relay/team_startlist_contract.py`. The contract requires exactly four members
per relay team or two per team-sprint team, confirms every athlete against the
FIS-derived individual list, rejects duplicate assignments, and verifies that
the team Elo equals the sum of its member ratings. The checked-in 2026 relay
and Olympic team-sprint startlists are regression fixtures for this contract.
The race builders enforce the contract before invoking R, write separate men’s
and women’s relay artifacts even when both races are dispatched together, and
refuse to run predictions when no valid output was generated. Set
`NORDIC_SKI_DATA_ROOT` and `NORDIC_TEAM_DATA_ROOT` to override the repository
data directories in hosted runs.

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
