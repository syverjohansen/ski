import polars as pl
import numpy as np
from datetime import datetime
import bisect
import time
import sys
import json
import warnings
warnings.filterwarnings('ignore')
pl.Config.set_tbl_cols(100)
start_time = time.time()

# Load data based on sex (M or L) using the combined scrape files (FIS + Russia):
def load_sex_data(df, sex_value):
    if sex_value == "M":
        df = pl.read_csv("~/ski/elo/python/ski/polars/relay/excel365/combined_men_scrape.csv", schema_overrides={"Distance": pl.Utf8})
    else:
        df = pl.read_csv("~/ski/elo/python/ski/polars/relay/excel365/combined_ladies_scrape.csv", schema_overrides={"Distance": pl.Utf8})
    
    # Cast columns to appropriate types
    df = df.with_columns([
        pl.col("Date").cast(pl.Utf8),
        pl.col("City").cast(pl.Utf8),
        pl.col("Country").cast(pl.Utf8),
        pl.col("Sex").cast(pl.Utf8),
        pl.col("Distance").cast(pl.Utf8),
        pl.col("Event").cast(pl.Utf8),
        pl.col("MS").cast(pl.Int64),
        pl.col("Technique").cast(pl.Utf8),
        pl.col("Place").cast(pl.Int64),
        pl.col("Skier").cast(pl.Utf8),
        pl.col("Nation").cast(pl.Utf8),
        pl.col("ID").cast(pl.Int64),
        pl.col("Season").cast(pl.Int64),
        pl.col("Race").cast(pl.Int64),
        pl.col("Birthday").cast(pl.Utf8),
        pl.col("Age").cast(pl.Utf8),
        pl.col("Exp").cast(pl.Int64)
    ])

    return df

def relay(df, relay):
    if relay == 1:
        return df
    else:
        df = df.filter(pl.col("Distance") != "Rel")
        df = df.filter(pl.col("Distance") != "Ts")
        return df


def date1(df, date1):
    df = df.filter(pl.col('Date') >= date1)
    return df

def date2(df, date2):
    df = df.filter(pl.col('Date') <= date2)
    return df

def city(df, cities):
    df = df.filter(pl.col('City').is_in(cities))
    return df

def country(df, countries):
    df = df.filter(pl.col('Country').is_in(countries))
    return df

def event(df, events):
    df = df.filter(pl.col('Event').is_in(events))
    return df

def distance(df, distances):
    if distances == "Sprint":
        df = df.filter((pl.col('Distance') == "Sprint") | (pl.col("Distance") == "Ts"))
    elif distances in df['Distance'].unique().to_list():
        df = df.filter(pl.col('Distance') == distances)
    else:
        df = df.filter((pl.col('Distance') != "Sprint") & (pl.col('Distance') != "Ts"))
    return df

def technique(df, technique):
    if technique == "F":
        df = df.filter(pl.col('Technique') == "F")
    elif technique == "P":
        df = df.filter(pl.col('Technique') == "P")
    else:
        # Classic technique: N/A or C, excluding relays
        df = df.filter(
            (pl.col('Technique') == "N/A") |
            ((pl.col("Technique") == "C") & (pl.col("Distance") != "Rel"))
        )
        df = df.filter(pl.col("Distance") != "0")
        df = df.filter((pl.col('Distance') != "Stage") & (pl.col('Distance') != "Etappeløp"))
    return df

def ms(df, ms):
    if(ms=="1"):
        df = df.filter(pl.col('MS') == 1)
    else:
        df = df.filter(pl.col('MS') == 0)
    return df

def place1(df, place1):
    df = df.filter(pl.col('Place') >= place1)
    return df

def place2(df, place2_val):
    df = df.filter(pl.col('Place') <= place2_val)
    return df

def names(df, names_list):
    df = df.filter(pl.col('Skier').is_in(names_list))
    return df

def season1(df, season1):
    df = df.filter(pl.col('Season') >= season1)
    return df

def season2(df, season2_val):
    df = df.filter(pl.col('Season') <= season2_val)
    return df

def nation(df, nations):
    df = df.filter(pl.col('Nation').is_in(nations))
    return df

def race1(df, pct1):
    """Filter to races at or after pct1 percentage of the season."""
    # Calculate max race per season and compute percentage
    df = df.with_columns([
        (pl.col('Race') / pl.col('Race').max().over('Season')).alias('race_pct')
    ])
    df = df.filter(pl.col('race_pct') >= pct1)
    df = df.drop('race_pct')
    return df

def race2(df, pct2):
    """Filter to races at or before pct2 percentage of the season."""
    # Calculate max race per season and compute percentage
    df = df.with_columns([
        (pl.col('Race') / pl.col('Race').max().over('Season')).alias('race_pct')
    ])
    df = df.filter(pl.col('race_pct') <= pct2)
    df = df.drop('race_pct')
    return df

def handle_value(key, value, df):
    if key == "sex":
        df = load_sex_data(df, value)
    elif key == "date1":
        df = date1(df, value)

    elif key == "date2":
        df = date2(df, value)
    elif key == "city":
        df = city(df, value)
    elif key == "country":
        df = country(df, value)
    elif key == "distance":
        df = distance(df, value)
    elif key == "ms":
        df = ms(df, value)
    elif key == "technique":
        df = technique(df, value)     
    elif key == "place1":
        df = place1(df, value)
    elif key == "place2":
        df = place2(df, value)
    elif key == "names":
        df = names(df, value)
    elif key == "season1":
        df = season1(df, value)
    elif key == "season2":
        df = season2(df, value)
    elif key == "nation":
        df = nation(df, value)
    elif key == "race1":
        df = race1(df, value)
    elif key == "race2":
        df = race2(df, value)     
    elif key == "relay":   
        df = relay(df, value)

    return df





def calc_Svec(Place_vector):
    '''
            convert the race results (place vector) into actual value for each athlete.
            Input:
                Place_vector: the place of each athlete, an array of sorted integer values, [P1, P2, ... Pn] so that P1 <= P2 ... <= Pn
            Output:
                S_vector: the actual score of each athlete (winning, drawing, or losing) against the (n-1) athletes, an array of float values, [S1, S2, ... Sn]
                    win = 1
                    draw = 1 / 2
    '''
    #Getting the length of the place vector and the set of the place_vector
    n, n_unique = len(Place_vector), len(set(Place_vector))
    
    #If no draws:
    if n==n_unique:
        #Reverses the order of the places to show how many wins a person got
        S_vector = np.arange(n)[::-1]
        return S_vector
    #If there are draws
    else:
        Place_vector, S_vector = np.array(Place_vector), list()
        for p in Place_vector:
            draws = np.count_nonzero(Place_vector == p)-1
            wins = (Place_vector>p).sum()
            score = wins*1 + draws*.5
            S_vector.append(score)
        return np.array(S_vector)


def calc_Evec(R_vector, basis=10, difference=400):
    '''
        compute the expected value of each athlete winning against all the other (n-1) athletes.
        Input:
            R_vector: the ELO rating of each athlete, an array of float values, [R1, R2, ... Rn]
        Output:
            E_vector: the expected value of each athlete winning against all the (n-1) athletes, an array of float values, [E1, E2, ... En]
    '''

    #R_vector is the list of previous elo scores for each skier.  Going to convert to an array and get its size
    R_vector = np.array(R_vector)
    n = R_vector.size

    #R_mat_col creates a matrix of the elo scores
    #Then we transpose the matrix.  Now each row is each skiers pelo, and each column represents all the pelos
    R_mat_col = np.tile(R_vector, (n,1))
    R_mat_row = np.transpose(R_mat_col)

    #Now this computes the expected score for the Elo matchup
    E_matrix = 1/(1+basis**((R_mat_row-R_mat_col)/difference))

    #We are not interested in self-matchups
    np.fill_diagonal(E_matrix, 0)

    #Sum of expected scores for each player against all other players
    E_vector = np.sum(E_matrix, axis=0)

    return E_vector

def calc_Svec_partial(Place_vector, has_real_elo, basis=10, difference=400):
    '''
        Calculate actual score only against opponents with real Elo.
        For each skier, count wins/draws/losses only against skiers who have real Elo.
        Vectorized implementation for performance.
    '''
    Place_vector = np.asarray(Place_vector)
    has_real_elo = np.asarray(has_real_elo, dtype=bool)
    n = len(Place_vector)

    # Create matrices for comparison: place_i vs place_j
    place_row = Place_vector.reshape(-1, 1)  # Column vector
    place_col = Place_vector.reshape(1, -1)  # Row vector

    # Wins: my_place < opponent_place (I beat them)
    wins_matrix = (place_row < place_col).astype(float)
    # Draws: my_place == opponent_place
    draws_matrix = (place_row == place_col).astype(float) * 0.5

    # Mask to only count comparisons against real Elo holders
    # has_real_elo[j] must be True for column j to count
    real_elo_mask = has_real_elo.reshape(1, -1)  # Broadcast across rows

    # Zero out self-comparisons
    np.fill_diagonal(wins_matrix, 0)
    np.fill_diagonal(draws_matrix, 0)

    # Apply mask and sum
    S_vector = np.sum((wins_matrix + draws_matrix) * real_elo_mask, axis=1)

    return S_vector

def calc_Evec_partial(R_vector, has_real_elo, basis=10, difference=400):
    '''
        Calculate expected score only against opponents with real Elo.
        For each skier, sum expected scores only against skiers who have real Elo.
        Vectorized implementation for performance.
    '''
    R_vector = np.asarray(R_vector, dtype=float)
    has_real_elo = np.asarray(has_real_elo, dtype=bool)
    n = R_vector.size

    # Create rating difference matrix: R[j] - R[i] for expected score formula
    R_row = R_vector.reshape(-1, 1)  # My rating (column vector)
    R_col = R_vector.reshape(1, -1)  # Opponent rating (row vector)

    # Expected score: 1 / (1 + 10^((R_opp - R_me) / 400))
    E_matrix = 1.0 / (1.0 + basis ** ((R_col - R_row) / difference))

    # Zero out self-comparisons
    np.fill_diagonal(E_matrix, 0)

    # Mask to only count comparisons against real Elo holders
    real_elo_mask = has_real_elo.reshape(1, -1)

    # Apply mask and sum
    E_vector = np.sum(E_matrix * real_elo_mask, axis=1)

    return E_vector

#Getting the K value for a season
def k_finder(season_df, max_racers):
    # RACERS APPROACH (current):
    # Counting total race entries (rows) in the season
    racers = season_df.height
    # K is max_racers / current_racers, with minimum of 1 and maximum of 5
    k = float(max_racers / racers)
    k = min(k, 5)  # cap at 5
    k = max(k, 1)  # floor at 1

    # RACES APPROACH (commented out):
    # #Figuring out how many races there are for a given season
    # races = season_df.select(pl.col("Race").n_unique()).item()
    # #Calculating the k-value for that season
    # #The lowest k-value is 1.  This is for the season with the most races
    # #The highest is 5
    # #What ever is smallest between 5, most races in a season/2, and most races in a season/races in that season
    # k = max(1, min(5, float(max_var_length/2), float(max_var_length/races)))

    return k

def init_elo_df():
    return pl.DataFrame(
        schema={
            "Date": pl.Utf8,
            "City": pl.Utf8,
            "Country": pl.Utf8,
            "Sex": pl.Utf8,
            "Distance": pl.Utf8,
            "Event": pl.Utf8,
            "MS": pl.Int64,
            "Technique": pl.Utf8,
            "Place": pl.Int64,
            "Skier": pl.Utf8,
            "Nation": pl.Utf8,
            "ID": pl.Int64,
            "Season": pl.Int64,
            "Race": pl.Int64,
            "Birthday": pl.Utf8,
            "Age": pl.Utf8,
            "Exp": pl.Int64,
            "Pelo": pl.Float64,
            "Elo": pl.Float64,
            "pred_Pelo": pl.Float64,
            "pred_Elo": pl.Float64
        }
    )


def batch_process_end_of_season(season_df, wc_lookup, pred_id_dict, discount, base_elo, seasons, season, sex):
    """
    Batch process all skiers for end-of-season records.
    Creates a single DataFrame for all skiers instead of one per skier.
    """
    endseasondate = str(datetime(seasons[season], 5, 1))

    # Get unique skiers with their last record in the season
    # Use group_by to get the last record per skier efficiently
    last_records = season_df.group_by('ID').agg([
        pl.col('Skier').last(),
        pl.col('Nation').last(),
        pl.col('Birthday').last(),
        pl.col('Age').last(),
        pl.col('Exp').last()
    ])

    n_skiers = last_records.height
    ids = last_records['ID'].to_list()

    # Build all the lists at once
    pelo_list = []
    elo_list = []
    pred_pelo_list = []
    pred_elo_list = []

    for idd in ids:
        # WC Elo lookup (Race 0 = end of season, so this gets the last race's Elo)
        endpelo, endelo = get_wc_elo_at_race(wc_lookup, idd, seasons[season], 0)
        pelo_list.append(endpelo)
        elo_list.append(endelo)

        # Predicted Elo discount
        end_pred_pelo = pred_id_dict[idd]
        end_pred_elo = end_pred_pelo * discount + base_elo * (1 - discount)
        pred_id_dict[idd] = end_pred_elo
        pred_pelo_list.append(end_pred_pelo)
        pred_elo_list.append(end_pred_elo)

    # Create single DataFrame for all skiers at once
    endf = pl.DataFrame({
        "Date": [endseasondate] * n_skiers,
        "City": ["Summer"] * n_skiers,
        "Country": ["Break"] * n_skiers,
        "Sex": [sex] * n_skiers,
        "Distance": ["0"] * n_skiers,
        "Event": ["Offseason"] * n_skiers,
        "MS": [0] * n_skiers,
        "Technique": [""] * n_skiers,
        "Place": [0] * n_skiers,
        "Skier": last_records['Skier'].to_list(),
        "Nation": last_records['Nation'].to_list(),
        "ID": ids,
        "Season": [seasons[season]] * n_skiers,
        "Race": [0] * n_skiers,
        "Birthday": [str(b) for b in last_records['Birthday'].to_list()],
        "Age": [str(a) for a in last_records['Age'].to_list()],
        "Exp": last_records['Exp'].to_list(),
        "Pelo": pelo_list,
        "Elo": elo_list,
        "pred_Pelo": pred_pelo_list,
        "pred_Elo": pred_elo_list
    })

    return endf, pred_id_dict

#Creating the elo function with prediction capability
#The initial score we are setting is 1300, arbitrary number that is subject to change from testing
#K score is 1 by default, we will change this, and I want to do testing to determine the best overall K eventually
#Discount is .85.  This is how much we will reduce an athletes elo by at the end of a season.  Again to be tested
def make_sort_key(season, race):
    """
    Create a sort key from season and race.
    Race 0 is end-of-season marker, so it should come after all other races.
    """
    return season * 1000 + (999 if race == 0 else race)

def build_wc_elo_lookup(wc_elo_df, all_races_df):
    """
    Build a lookup structure from WC elo data.
    Joins WC data with all-races data to get the all-races Race numbers.
    Returns a dict: {ID: (sort_keys_list, pelos_list, elos_list)} sorted by sort_key ascending.
    """
    if wc_elo_df is None or wc_elo_df.is_empty():
        return {}, set()

    # Ensure columns match types for joining
    wc_elo_df = wc_elo_df.with_columns([
        pl.col('ID').cast(pl.Int64),
        pl.col('Season').cast(pl.Int64),
        pl.col('Date').cast(pl.Utf8),
        pl.col('City').cast(pl.Utf8),
        pl.col('Country').cast(pl.Utf8),
        pl.col('Distance').cast(pl.Utf8),
        pl.col('Technique').cast(pl.Utf8)
    ])

    # Get just the columns we need from all-races for joining
    all_races_join = all_races_df.select([
        pl.col('ID').cast(pl.Int64),
        pl.col('Season').cast(pl.Int64),
        pl.col('Race').cast(pl.Int64).alias('all_races_Race'),
        pl.col('Date').cast(pl.Utf8),
        pl.col('City').cast(pl.Utf8),
        pl.col('Country').cast(pl.Utf8),
        pl.col('Distance').cast(pl.Utf8),
        pl.col('Technique').cast(pl.Utf8)
    ])

    # Join WC data with all-races data to get the all-races Race numbers
    join_cols = ['ID', 'City', 'Country', 'Date', 'Distance', 'Technique', 'Season']

    joined = wc_elo_df.join(
        all_races_join,
        on=join_cols,
        how='left'
    )

    # For end-of-season records (Race 0 in WC data), keep Race 0
    # For matched races, use the all-races Race number
    joined = joined.with_columns([
        pl.when(pl.col('Race') == 0)
        .then(pl.lit(0))
        .otherwise(pl.col('all_races_Race'))
        .alias('lookup_race')
    ])

    # Filter out any WC races that didn't match (shouldn't happen, but just in case)
    matched = joined.filter(pl.col('lookup_race').is_not_null())
    unmatched_count = joined.filter(pl.col('lookup_race').is_null() & (pl.col('Race') != 0)).height
    if unmatched_count > 0:
        print(f"Warning: {unmatched_count} WC races didn't match to all-races data")

    # Create sort_key column using all-races Race numbers
    # Race 0 (end of season) comes after all other races
    matched = matched.with_columns([
        (pl.col('Season') * 1000 + pl.when(pl.col('lookup_race') == 0).then(999).otherwise(pl.col('lookup_race'))).alias('sort_key')
    ])

    # Sort once by ID and sort_key
    matched = matched.sort(['ID', 'sort_key'])

    # Get list of WC skier IDs
    wc_ids = set(matched['ID'].unique().to_list())
    print(f"Built WC lookup with {len(wc_ids)} unique skier IDs")

    # Use partition_by to group data by ID efficiently (single pass)
    partitions = matched.partition_by('ID', maintain_order=True)

    lookup = {}
    for partition in partitions:
        skier_id = partition['ID'][0]
        # Extract columns as lists (already sorted by sort_key from the sort above)
        sort_keys = partition['sort_key'].to_list()
        pelos = partition['Pelo'].to_list()
        elos = partition['Elo'].to_list()
        lookup[skier_id] = (sort_keys, pelos, elos)

    return lookup, wc_ids

def get_wc_elo_at_race(lookup, skier_id, season, race):
    """
    Get a skier's WC Elo as of a given season/race.
    Returns (pelo, elo) from the most recent WC entry at or before the current race.
    The Pelo represents the Elo GOING INTO that race, Elo is AFTER.
    Uses binary search for O(log n) lookup instead of O(n).
    """
    if skier_id not in lookup:
        return None, None

    sort_keys, pelos, elos = lookup[skier_id]

    # Create sort_key for current race
    current_sort_key = make_sort_key(season, race)

    # Binary search for the rightmost sort_key <= current_sort_key
    # bisect_right gives us the insertion point AFTER any equal values
    idx = bisect.bisect_right(sort_keys, current_sort_key)

    if idx == 0:
        # All sort_keys are > current, no WC data available yet
        return None, None

    # idx-1 is the index of the last sort_key <= current_sort_key
    return pelos[idx - 1], elos[idx - 1]

def elo(df, wc_elo_df, base_elo=1300, K=1, discount=.85):
    # Get the sex value from data
    sex_val = df['Sex'][0]

    # Get column order from the schema
    column_order = list(init_elo_df().columns)

    # Sort the entire dataframe by Season, Race, and Place
    df = df.sort(['Season', 'Race', 'Place'])

    # Create a list of all IDs in the all-races df
    id_dict_list = df['ID'].unique().to_list()

    # Build WC Elo lookup structure (O(n) with partition_by + binary search)
    # Pass all-races df to join and get correct Race numbers
    wc_lookup, wc_ids = build_wc_elo_lookup(wc_elo_df, df)

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

    # Partition data by season once (instead of filtering repeatedly)
    df_by_season = {s: df.filter(pl.col('Season') == s) for s in seasons}

    # Collect all result DataFrames in a list (concat once at end)
    all_results = []

    for season_idx, season_year in enumerate(seasons):
        season_df = df_by_season[season_year]  # Already sorted by Race, Place from initial sort

        # Get K from WC-based values; default to 5 if no WC races in this season
        K = k_values.get(season_year, 5)
        print(f"({season_year}, {K})")

        # Partition season data by race (maintains order from initial sort)
        race_partitions = season_df.partition_by('Race', maintain_order=True)

        # Collect race results for this season
        season_results = []

        for race_df in race_partitions:
            # Already sorted by Place from initial sort

            # Extract data as numpy arrays/lists for fast processing
            ski_ids_r = race_df['ID'].to_numpy()
            places_arr = race_df['Place'].to_numpy()
            race_num = race_df['Race'][0]
            distance_type = race_df['Distance'][0]

            n_skiers = len(ski_ids_r)

            # Build arrays for this race using numpy for speed
            pelo_arr = np.empty(n_skiers, dtype=object)
            elo_arr = np.empty(n_skiers, dtype=object)
            pred_pelo_arr = np.empty(n_skiers, dtype=float)
            has_real_elo_arr = np.zeros(n_skiers, dtype=bool)

            for i, idd in enumerate(ski_ids_r):
                pred_pelo_arr[i] = pred_id_dict[idd]
                wc_pelo, wc_elo = get_wc_elo_at_race(wc_lookup, idd, season_year, race_num)
                if wc_pelo is not None:
                    pelo_arr[i] = wc_pelo
                    elo_arr[i] = wc_elo
                    has_real_elo_arr[i] = True
                else:
                    pelo_arr[i] = None
                    elo_arr[i] = None

            # Determine K modifier based on race type
            if distance_type == "Ts":
                K_mod = K / 2
            elif distance_type == "Rel":
                K_mod = K / 4
            else:
                K_mod = K

            # Calculate pred_Elo for all skiers
            n_real = has_real_elo_arr.sum()
            if n_real > 0:
                # Convert elo_arr to float array once (reused below)
                real_elo_floats = np.array([e if e is not None else 0 for e in elo_arr], dtype=float)

                # Build comparison elos array: WC skiers use real Elo, others use predicted
                comparison_elos = np.where(has_real_elo_arr, real_elo_floats, pred_pelo_arr)

                E_pred = calc_Evec_partial(comparison_elos, has_real_elo_arr)
                S_pred = calc_Svec_partial(places_arr, has_real_elo_arr)
                pred_elos = pred_pelo_arr + K_mod * (S_pred - E_pred)

                # Build pred_elo array: WC skiers get their WC Elo, others get calculated
                pred_elo_arr = np.where(has_real_elo_arr, real_elo_floats, pred_elos)

                # Update pred_id_dict
                for i, idd in enumerate(ski_ids_r):
                    pred_id_dict[idd] = pred_elo_arr[i]
            else:
                # No WC Elo holders in this race
                pred_elo_arr = pred_pelo_arr.copy()

            # Add columns to race_df efficiently
            race_df = race_df.with_columns([
                pl.Series(name="Pelo", values=pelo_arr.tolist()).cast(pl.Float64),
                pl.Series(name="Elo", values=elo_arr.tolist()).cast(pl.Float64),
                pl.Series(name="pred_Pelo", values=pred_pelo_arr),
                pl.Series(name="pred_Elo", values=pred_elo_arr)
            ]).select(column_order)

            season_results.append(race_df)

        # Concat all races for this season at once
        if season_results:
            all_results.append(pl.concat(season_results))

        # Add end of season records using batch processing
        end_of_season_df, pred_id_dict = batch_process_end_of_season(
            season_df, wc_lookup, pred_id_dict,
            discount, base_elo, seasons, season_idx, sex_val
        )
        all_results.append(end_of_season_df.select(column_order))

    # Single concat at the end (instead of repeated concats)
    elo_df = pl.concat(all_results)

    # Final sort
    elo_df = elo_df.sort(['Date', 'Season', 'Race', 'Place'])
    return elo_df

df = pl.DataFrame()

json_str = sys.argv[1]
data = json.loads(json_str)
#print(data)

# File string creation code remains the same
file_string = ""
for key, value in data.items():
    if key == "relay":
        if value == 1:
            file_string = file_string + "rel_"
        else:
            df = handle_value(key, 0, df)
    elif value is not None and value != "null":  # Skip null values
        file_string = file_string + value + "_"
        df = handle_value(key, value, df)

# Remove trailing underscore if it exists
file_string = file_string[:-1] if file_string.endswith('_') else file_string

# If file_string is empty, use just the sex value
if not file_string:
    file_string = data.get('sex', '')



#print(file_string)

# Load the WC elo data to identify real Elo holders
# The WC elo file should be based on sex (M_rel.csv or L_rel.csv for relay)
sex_value = data.get('sex', 'M')
wc_elo_path = f"~/ski/elo/python/ski/polars/relay/excel365/{sex_value}_rel.csv"

try:
    # Specify schema to avoid inference issues (Distance can be "Sprint", "50", etc.)
    wc_elo_df = pl.read_csv(
        wc_elo_path,
        schema_overrides={
            "Date": pl.Utf8,
            "Distance": pl.Utf8,
            "ID": pl.Int64,
            "Pelo": pl.Float64,
            "Elo": pl.Float64
        }
    )
    print(f"Loaded WC Elo data from {wc_elo_path} with {wc_elo_df.height} rows")
except Exception as e:
    print(f"Warning: Could not load WC Elo data from {wc_elo_path}: {e}")
    wc_elo_df = None

# Run elo calculation with WC data for real Elo identification
elo_df = elo(df, wc_elo_df)
#print(elo_df.filter(pl.col("City") == "Tour de Ski"))

# Base path for output files
base_path = "~/ski/elo/python/ski/polars/relay/excel365"

# Save CSV format with pred_ prefix to distinguish from WC elo files
elo_df.write_csv(f"{base_path}/pred_{file_string}.csv")
print(f"Saved to {base_path}/pred_{file_string}.csv")
print(time.time() - start_time)

