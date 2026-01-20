import polars as pl
import numpy as np
from datetime import datetime, date
import bisect
import time
import sys
import json
import os
import warnings
warnings.filterwarnings('ignore')
pl.Config.set_tbl_cols(100)
start_time = time.time()

# Define schema overrides for nordic-combined
schema_overrides = {
    'HillSize': pl.String,
    'Distance': pl.String,
    'RaceType': pl.String,
    'MassStart': pl.Int64,
    'TeamEvent': pl.Int64,
    'Season': pl.Int64,
    'Race': pl.Int64,
    'Place': pl.Int64,
    'Skier': pl.String,
    'Nation': pl.String,
    'ID': pl.String,
    'Birthday': pl.Datetime,
    'Leg': pl.Int64,
    'TeamID': pl.String,
    'SJ_Pos': pl.String,
    'CC_Pos': pl.String,
    'Age': pl.Float64,
    'Exp': pl.Int32
}

# Load data based on sex (M or L) using the all_scrape files:
def load_sex_data(df, sex_value):
    if sex_value == "M":
        df = pl.read_csv("~/ski/elo/python/nordic-combined/polars/relay/excel365/all_men_scrape.csv", schema_overrides=schema_overrides)
    else:
        df = pl.read_csv("~/ski/elo/python/nordic-combined/polars/relay/excel365/all_ladies_scrape.csv", schema_overrides=schema_overrides)

    # Cast columns to appropriate types
    df = df.with_columns([
        pl.col("Date").cast(pl.Date),
        pl.col("City").cast(pl.Utf8),
        pl.col("Country").cast(pl.Utf8),
        pl.col("Sex").cast(pl.Utf8),
        pl.col("Distance").cast(pl.Utf8),
        pl.col("RaceType").cast(pl.Utf8),
        pl.col("MassStart").cast(pl.Int64),
        pl.col("Event").cast(pl.Utf8),
        pl.col("Place").cast(pl.Int64),
        pl.col("Skier").cast(pl.Utf8),
        pl.col("Nation").cast(pl.Utf8),
        pl.col("ID").cast(pl.Utf8),
        pl.col("Season").cast(pl.Int64),
        pl.col("Race").cast(pl.Int64),
        pl.col("Birthday").cast(pl.Datetime),
        pl.col("Age").cast(pl.Float64),
        pl.col("Exp").cast(pl.Int64),
        pl.col("Leg").cast(pl.Int64)
    ])

    return df

def race_type(df, race_types):
    """Filter by race type"""
    if not race_types or race_types == "null":
        return df

    if isinstance(race_types, list):
        df = df.filter(pl.col('RaceType').is_in(race_types))
    else:
        df = df.filter(pl.col('RaceType') == race_types)

    return df

def team_filter(df, include_teams):
    """Filter to include or exclude team events"""
    if include_teams == 0:
        df = df.filter(~pl.col('RaceType').str.contains("Team"))
    return df

def event_filter(df, apply_filter):
    """Filter events to include only major competitions"""
    if not apply_filter or apply_filter == 0:
        return df

    pattern = "(?i)Olympic|World Cup|Championship"
    df = df.filter(
        pl.col('Event').str.contains(pattern)
    )

    return df

def date1(df, date1):
    """Filter by start date"""
    if not date1 or date1 == "null":
        return df
    df = df.filter(pl.col('Date') >= pl.lit(date1))
    return df

def date2(df, date2):
    """Filter by end date"""
    if not date2 or date2 == "null":
        return df
    df = df.filter(pl.col('Date') <= pl.lit(date2))
    return df

def city(df, cities):
    """Filter by city"""
    if not cities or cities == "null":
        return df
    if isinstance(cities, list):
        df = df.filter(pl.col('City').is_in(cities))
    else:
        df = df.filter(pl.col('City') == cities)
    return df

def country(df, countries):
    """Filter by country"""
    if not countries or countries == "null":
        return df
    if isinstance(countries, list):
        df = df.filter(pl.col('Country').is_in(countries))
    else:
        df = df.filter(pl.col('Country') == countries)
    return df

def place1(df, place1):
    """Filter by minimum place"""
    if not place1 or place1 == "null":
        return df
    df = df.filter(pl.col('Place') >= place1)
    return df

def place2(df, place2_val):
    """Filter by maximum place"""
    if not place2_val or place2_val == "null":
        return df
    df = df.filter(pl.col('Place') <= place2_val)
    return df

def names(df, names_list):
    """Filter by skier names"""
    if not names_list or names_list == "null":
        return df
    if isinstance(names_list, list):
        df = df.filter(pl.col('Skier').is_in(names_list))
    else:
        df = df.filter(pl.col('Skier') == names_list)
    return df

def season1(df, season1):
    """Filter by start season"""
    if not season1 or season1 == "null":
        return df
    df = df.filter(pl.col('Season') >= season1)
    return df

def season2(df, season2_val):
    """Filter by end season"""
    if not season2_val or season2_val == "null":
        return df
    df = df.filter(pl.col('Season') <= season2_val)
    return df

def nation(df, nations):
    """Filter by nation"""
    if not nations or nations == "null":
        return df
    if isinstance(nations, list):
        df = df.filter(pl.col('Nation').is_in(nations))
    else:
        df = df.filter(pl.col('Nation') == nations)
    return df

def handle_value(key, value, df):
    """Apply filter based on key and value"""
    if value is None or value == "null":
        return df

    if key == "sex":
        df = load_sex_data(df, value)
    elif key == "event_filter":
        df = event_filter(df, value)
    elif key == "race_type":
        df = race_type(df, value)
    elif key == "team_filter":
        df = team_filter(df, value)
    elif key == "date1":
        df = date1(df, value)
    elif key == "date2":
        df = date2(df, value)
    elif key == "city":
        df = city(df, value)
    elif key == "country":
        df = country(df, value)
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
    return df


def calc_Svec(Place_vector):
    '''
    Convert the race results (place vector) into actual value for each athlete.
    '''
    n, n_unique = len(Place_vector), len(set(Place_vector))

    if n==n_unique:
        S_vector = np.arange(n)[::-1]
        return S_vector
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
    Compute the expected value of each athlete winning against all the other (n-1) athletes.
    '''
    R_vector = np.array(R_vector)
    n = R_vector.size

    R_mat_col = np.tile(R_vector, (n,1))
    R_mat_row = np.transpose(R_mat_col)

    E_matrix = 1/(1+basis**((R_mat_row-R_mat_col)/difference))

    np.fill_diagonal(E_matrix, 0)

    E_vector = np.sum(E_matrix, axis=0)

    return E_vector

def calc_Svec_partial(Place_vector, has_real_elo, basis=10, difference=400):
    '''
    Calculate actual score only against opponents with real Elo.
    '''
    Place_vector = np.asarray(Place_vector)
    has_real_elo = np.asarray(has_real_elo, dtype=bool)
    n = len(Place_vector)

    place_row = Place_vector.reshape(-1, 1)
    place_col = Place_vector.reshape(1, -1)

    wins_matrix = (place_row < place_col).astype(float)
    draws_matrix = (place_row == place_col).astype(float) * 0.5

    real_elo_mask = has_real_elo.reshape(1, -1)

    np.fill_diagonal(wins_matrix, 0)
    np.fill_diagonal(draws_matrix, 0)

    S_vector = np.sum((wins_matrix + draws_matrix) * real_elo_mask, axis=1)

    return S_vector

def calc_Evec_partial(R_vector, has_real_elo, basis=10, difference=400):
    '''
    Calculate expected score only against opponents with real Elo.
    '''
    R_vector = np.asarray(R_vector, dtype=float)
    has_real_elo = np.asarray(has_real_elo, dtype=bool)
    n = R_vector.size

    R_row = R_vector.reshape(-1, 1)
    R_col = R_vector.reshape(1, -1)

    E_matrix = 1.0 / (1.0 + basis ** ((R_col - R_row) / difference))

    np.fill_diagonal(E_matrix, 0)

    real_elo_mask = has_real_elo.reshape(1, -1)

    E_vector = np.sum(E_matrix * real_elo_mask, axis=1)

    return E_vector

def k_finder(season_df, max_racers):
    """Calculate k-value for a season based on number of racers"""
    racers = season_df.height
    k = float(max_racers / racers)
    k = min(k, 5)
    k = max(k, 1)
    return k

def init_elo_df():
    """Initialize an empty DataFrame with the correct schema for ELO data"""
    return pl.DataFrame(
        schema={
            "Date": pl.Date,
            "City": pl.Utf8,
            "Country": pl.Utf8,
            "Sex": pl.Utf8,
            "Distance": pl.Utf8,
            "RaceType": pl.Utf8,
            "MassStart": pl.Int64,
            "Event": pl.Utf8,
            "Place": pl.Int64,
            "Skier": pl.Utf8,
            "Nation": pl.Utf8,
            "ID": pl.Utf8,
            "Season": pl.Int64,
            "Race": pl.Int64,
            "Birthday": pl.Datetime,
            "Age": pl.Float64,
            "Exp": pl.Int64,
            "Leg": pl.Int64,
            "Pelo": pl.Float64,
            "Elo": pl.Float64,
            "pred_Pelo": pl.Float64,
            "pred_Elo": pl.Float64
        }
    )


def batch_process_end_of_season(season_df, wc_lookup, pred_id_dict, discount, base_elo, seasons, season, sex):
    """
    Batch process all skiers for end-of-season records.
    """
    endseasondate = date(seasons[season], 5, 1)

    last_records = season_df.group_by('ID').agg([
        pl.col('Skier').last(),
        pl.col('Nation').last(),
        pl.col('Birthday').last(),
        pl.col('Age').last(),
        pl.col('Exp').last(),
        pl.col('Leg').last()
    ])

    n_skiers = last_records.height
    ids = last_records['ID'].to_list()

    pelo_list = []
    elo_list = []
    pred_pelo_list = []
    pred_elo_list = []

    for idd in ids:
        endpelo, endelo = get_wc_elo_at_race(wc_lookup, idd, seasons[season], 0)
        pelo_list.append(endpelo)
        elo_list.append(endelo)

        end_pred_pelo = pred_id_dict[idd]
        end_pred_elo = end_pred_pelo * discount + base_elo * (1 - discount)
        pred_id_dict[idd] = end_pred_elo
        pred_pelo_list.append(end_pred_pelo)
        pred_elo_list.append(end_pred_elo)

    endf = pl.DataFrame({
        "Date": [endseasondate] * n_skiers,
        "City": ["Summer"] * n_skiers,
        "Country": ["Break"] * n_skiers,
        "Sex": [sex] * n_skiers,
        "Distance": ["0"] * n_skiers,
        "RaceType": ["Offseason"] * n_skiers,
        "MassStart": [0] * n_skiers,
        "Event": ["Offseason"] * n_skiers,
        "Place": [0] * n_skiers,
        "Skier": last_records['Skier'].to_list(),
        "Nation": last_records['Nation'].to_list(),
        "ID": ids,
        "Season": [seasons[season]] * n_skiers,
        "Race": [0] * n_skiers,
        "Birthday": last_records['Birthday'].to_list(),
        "Age": last_records['Age'].to_list(),
        "Exp": last_records['Exp'].to_list(),
        "Leg": last_records['Leg'].to_list(),
        "Pelo": pelo_list,
        "Elo": elo_list,
        "pred_Pelo": pred_pelo_list,
        "pred_Elo": pred_elo_list
    })

    return endf, pred_id_dict

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
        pl.col('ID').cast(pl.Utf8),
        pl.col('Season').cast(pl.Int64),
        pl.col('Date').cast(pl.Date),
        pl.col('City').cast(pl.Utf8),
        pl.col('Country').cast(pl.Utf8),
        pl.col('RaceType').cast(pl.Utf8)
    ])

    # Get just the columns we need from all-races for joining
    all_races_join = all_races_df.select([
        pl.col('ID').cast(pl.Utf8),
        pl.col('Season').cast(pl.Int64),
        pl.col('Race').cast(pl.Int64).alias('all_races_Race'),
        pl.col('Date').cast(pl.Date),
        pl.col('City').cast(pl.Utf8),
        pl.col('Country').cast(pl.Utf8),
        pl.col('RaceType').cast(pl.Utf8)
    ])

    # Join WC data with all-races data to get the all-races Race numbers
    join_cols = ['ID', 'City', 'Country', 'Date', 'RaceType', 'Season']

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

    # Filter out any WC races that didn't match
    matched = joined.filter(pl.col('lookup_race').is_not_null())
    unmatched_count = joined.filter(pl.col('lookup_race').is_null() & (pl.col('Race') != 0)).height
    if unmatched_count > 0:
        print(f"Warning: {unmatched_count} WC races didn't match to all-races data")

    # Create sort_key column using all-races Race numbers
    matched = matched.with_columns([
        (pl.col('Season') * 1000 + pl.when(pl.col('lookup_race') == 0).then(999).otherwise(pl.col('lookup_race'))).alias('sort_key')
    ])

    # Sort once by ID and sort_key
    matched = matched.sort(['ID', 'sort_key'])

    # Get list of WC skier IDs
    wc_ids = set(matched['ID'].unique().to_list())
    print(f"Built WC lookup with {len(wc_ids)} unique skier IDs")

    # Use partition_by to group data by ID efficiently
    partitions = matched.partition_by('ID', maintain_order=True)

    lookup = {}
    for partition in partitions:
        skier_id = partition['ID'][0]
        sort_keys = partition['sort_key'].to_list()
        pelos = partition['Pelo'].to_list()
        elos = partition['Elo'].to_list()
        lookup[skier_id] = (sort_keys, pelos, elos)

    return lookup, wc_ids

def get_wc_elo_at_race(lookup, skier_id, season, race):
    """
    Get a skier's WC Elo as of a given season/race.
    Returns (pelo, elo) from the most recent WC entry at or before the current race.
    """
    if skier_id not in lookup:
        return None, None

    sort_keys, pelos, elos = lookup[skier_id]

    current_sort_key = make_sort_key(season, race)

    idx = bisect.bisect_right(sort_keys, current_sort_key)

    if idx == 0:
        return None, None

    return pelos[idx - 1], elos[idx - 1]

def elo(df, wc_elo_df, base_elo=1300, K=1, discount=.85):
    """Calculate ELO ratings with prediction capability"""
    if df.is_empty():
        print("Input DataFrame is empty, no data to process")
        return init_elo_df()

    sex_val = df['Sex'][0]

    column_order = list(init_elo_df().columns)

    # Sort the entire dataframe by Season, Race, and Place
    df = df.sort(['Season', 'Race', 'Place'])

    # Create a list of all IDs in the all-races df
    id_dict_list = df['ID'].unique().to_list()

    # Build WC Elo lookup structure
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

    # Partition data by season once
    df_by_season = {s: df.filter(pl.col('Season') == s) for s in seasons}

    # Collect all result DataFrames in a list
    all_results = []

    for season_idx, season_year in enumerate(seasons):
        season_df = df_by_season[season_year]

        # Get K from WC-based values; default to 5 if no WC races in this season
        K = k_values.get(season_year, 5)
        print(f"({season_year}, {K})")

        # Partition season data by race
        race_partitions = season_df.partition_by('Race', maintain_order=True)

        season_results = []

        for race_df in race_partitions:
            ski_ids_r = race_df['ID'].to_list()
            places_arr = race_df['Place'].to_numpy()
            race_num = race_df['Race'][0]
            race_type_val = race_df['RaceType'][0]

            n_skiers = len(ski_ids_r)

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
            if race_type_val == "Team":
                K_mod = K / 4
            elif race_type_val == "Team Sprint":
                K_mod = K / 2
            else:
                K_mod = K

            # Calculate pred_Elo for all skiers
            n_real = has_real_elo_arr.sum()
            if n_real > 0:
                real_elo_floats = np.array([e if e is not None else 0 for e in elo_arr], dtype=float)

                comparison_elos = np.where(has_real_elo_arr, real_elo_floats, pred_pelo_arr)

                E_pred = calc_Evec_partial(comparison_elos, has_real_elo_arr)
                S_pred = calc_Svec_partial(places_arr, has_real_elo_arr)
                pred_elos = pred_pelo_arr + K_mod * (S_pred - E_pred)

                pred_elo_arr = np.where(has_real_elo_arr, real_elo_floats, pred_elos)

                for i, idd in enumerate(ski_ids_r):
                    pred_id_dict[idd] = pred_elo_arr[i]
            else:
                pred_elo_arr = pred_pelo_arr.copy()

            # Add columns to race_df
            race_df = race_df.with_columns([
                pl.Series(name="Pelo", values=[float(x) if x is not None else None for x in pelo_arr]).cast(pl.Float64),
                pl.Series(name="Elo", values=[float(x) if x is not None else None for x in elo_arr]).cast(pl.Float64),
                pl.Series(name="pred_Pelo", values=pred_pelo_arr),
                pl.Series(name="pred_Elo", values=pred_elo_arr)
            ]).select(column_order)

            season_results.append(race_df)

        if season_results:
            all_results.append(pl.concat(season_results))

        # Add end of season records
        end_of_season_df, pred_id_dict = batch_process_end_of_season(
            season_df, wc_lookup, pred_id_dict,
            discount, base_elo, seasons, season_idx, sex_val
        )
        all_results.append(end_of_season_df.select(column_order))

    # Single concat at the end
    elo_df = pl.concat(all_results)

    # Final sort
    elo_df = elo_df.sort(['Date', 'Season', 'Race', 'Place'])
    return elo_df

# Main execution
df = pl.DataFrame()

json_str = sys.argv[1]
data = json.loads(json_str)

# Build file string and apply filters
file_string = ""
file_string += data.get('sex', '')
file_string += "_rel"  # Add relay suffix

# Add race_type if specified
if data.get('race_type') not in (None, "null"):
    race_type_str = data.get('race_type')
    race_type_str = race_type_str.replace(" ", "_")
    file_string += f"_{race_type_str}"

# Apply all filters
for key, value in data.items():
    if value is not None and value != "null":
        df = handle_value(key, value, df)

# Load the WC elo data to identify real Elo holders
# Use the regular WC elo file (from elo.py, not all_elo.py)
wc_elo_path = os.path.expanduser(f"~/ski/elo/python/nordic-combined/polars/relay/excel365/{file_string}.csv")

try:
    wc_elo_df = pl.read_csv(
        wc_elo_path,
        schema_overrides={
            "Date": pl.Date,
            "City": pl.Utf8,
            "Country": pl.Utf8,
            "Distance": pl.Utf8,
            "RaceType": pl.Utf8,
            "ID": pl.Utf8,
            "Season": pl.Int64,
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

# Base path for output files
base_path = os.path.expanduser("~/ski/elo/python/nordic-combined/polars/relay/excel365")

# Save CSV format with pred_ prefix to distinguish from WC elo files
elo_df.write_csv(f"{base_path}/pred_{file_string}.csv")
print(f"Saved to {base_path}/pred_{file_string}.csv")
print(time.time() - start_time)
