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

# Load data based on sex (M or L) using the all_scrape files:
def load_sex_data(df, sex_value):
    if sex_value == "M":
        df = pl.read_csv("~/ski/elo/python/alpine/polars/excel365/all_men_scrape.csv")
    else:
        df = pl.read_csv("~/ski/elo/python/alpine/polars/excel365/all_ladies_scrape.csv")

    # Cast columns to appropriate types (alpine schema)
    df = df.with_columns([
        pl.col("Date").cast(pl.Date),
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
        pl.col("Birthday").cast(pl.Datetime),
        pl.col("Age").cast(pl.Float64),
        pl.col("Exp").cast(pl.Int64)
    ])

    return df

def distance_filter(df, distances):
    """Filter by race distance type"""
    if not distances or distances == "null":
        return df

    if distances == "Speed":
        # Speed disciplines: Downhill and Super G
        df = df.filter(pl.col('Distance').is_in(["Downhill", "Super G"]))
    elif distances == "Tech":
        # Technical disciplines: Slalom and Giant Slalom
        df = df.filter(pl.col('Distance').is_in(["Slalom", "Giant Slalom"]))
    else:
        # Specific discipline
        df = df.filter(pl.col('Distance') == distances)

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
    if not date1 or date1 == "null":
        return df
    df = df.filter(pl.col('Date') >= pl.lit(date1))
    return df

def date2(df, date2):
    if not date2 or date2 == "null":
        return df
    df = df.filter(pl.col('Date') <= pl.lit(date2))
    return df

def city(df, cities):
    if not cities or cities == "null":
        return df
    if isinstance(cities, list):
        df = df.filter(pl.col('City').is_in(cities))
    else:
        df = df.filter(pl.col('City') == cities)
    return df

def country(df, countries):
    if not countries or countries == "null":
        return df
    if isinstance(countries, list):
        df = df.filter(pl.col('Country').is_in(countries))
    else:
        df = df.filter(pl.col('Country') == countries)
    return df

def place1(df, place1):
    if not place1 or place1 == "null":
        return df
    df = df.filter(pl.col('Place') >= place1)
    return df

def place2(df, place2_val):
    if not place2_val or place2_val == "null":
        return df
    df = df.filter(pl.col('Place') <= place2_val)
    return df

def names(df, names_list):
    if not names_list or names_list == "null":
        return df
    if isinstance(names_list, list):
        df = df.filter(pl.col('Skier').is_in(names_list))
    else:
        df = df.filter(pl.col('Skier') == names_list)
    return df

def season1(df, season1):
    if not season1 or season1 == "null":
        return df
    df = df.filter(pl.col('Season') >= season1)
    return df

def season2(df, season2_val):
    if not season2_val or season2_val == "null":
        return df
    df = df.filter(pl.col('Season') <= season2_val)
    return df

def nation(df, nations):
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
    elif key == "distance":
        df = distance_filter(df, value)
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
    R_vector = np.array(R_vector)
    n = R_vector.size

    R_mat_col = np.tile(R_vector, (n,1))
    R_mat_row = np.transpose(R_mat_col)

    E_matrix = 1/(1+basis**((R_mat_row-R_mat_col)/difference))

    np.fill_diagonal(E_matrix, 0)

    E_vector = np.sum(E_matrix, axis=0)

    return E_vector

def calc_Svec_partial(Place_vector, has_real_elo, basis=10, difference=400):
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
    racers = season_df.height
    k = float(max_racers / racers)
    k = min(k, 5)
    k = max(k, 1)
    return k

def init_elo_df():
    """Initialize an empty DataFrame with the correct schema for ELO data (alpine)"""
    return pl.DataFrame(
        schema={
            "Date": pl.Date,
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
            "Birthday": pl.Datetime,
            "Age": pl.Float64,
            "Exp": pl.Int64,
            "Pelo": pl.Float64,
            "Elo": pl.Float64,
            "pred_Pelo": pl.Float64,
            "pred_Elo": pl.Float64
        }
    )


def batch_process_end_of_season(season_df, wc_id_dict, pred_id_dict, discount, base_elo, seasons, season, sex):
    """
    Batch process all skiers for end-of-season records (alpine).
    For dynamic version: WC skiers use wc_id_dict, non-WC use pred_id_dict.
    """
    endseasondate = date(seasons[season], 5, 1)

    last_records = season_df.group_by('ID').agg([
        pl.col('Skier').last(),
        pl.col('Nation').last(),
        pl.col('Birthday').last(),
        pl.col('Age').last(),
        pl.col('Exp').last()
    ])

    n_skiers = last_records.height
    ids = last_records['ID'].to_list()

    pelo_list = []
    elo_list = []
    pred_pelo_list = []
    pred_elo_list = []

    for idd in ids:
        if idd in wc_id_dict:
            # WC skier: use dynamic Elo from wc_id_dict and apply discount
            end_pelo = wc_id_dict[idd]
            end_elo = end_pelo * discount + base_elo * (1 - discount)
            wc_id_dict[idd] = end_elo
            pelo_list.append(end_pelo)
            elo_list.append(end_elo)
            pred_pelo_list.append(end_pelo)
            pred_elo_list.append(end_elo)
        else:
            # Non-WC skier: use pred_id_dict
            pelo_list.append(None)
            elo_list.append(None)
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
        "Event": ["Offseason"] * n_skiers,
        "MS": [0] * n_skiers,
        "Technique": [""] * n_skiers,
        "Place": [0] * n_skiers,
        "Skier": last_records['Skier'].to_list(),
        "Nation": last_records['Nation'].to_list(),
        "ID": ids,
        "Season": [seasons[season]] * n_skiers,
        "Race": [0] * n_skiers,
        "Birthday": last_records['Birthday'].to_list(),
        "Age": last_records['Age'].to_list(),
        "Exp": last_records['Exp'].to_list(),
        "Pelo": pelo_list,
        "Elo": elo_list,
        "pred_Pelo": pred_pelo_list,
        "pred_Elo": pred_elo_list
    })

    return endf, wc_id_dict, pred_id_dict

def make_sort_key(season, race):
    return season * 1000 + (999 if race == 0 else race)

def build_wc_elo_lookup(wc_elo_df, all_races_df):
    """
    Build a lookup structure from WC elo data.
    Joins WC data with all-races data to get the all-races Race numbers.
    """
    if wc_elo_df is None or wc_elo_df.is_empty():
        return {}, set()

    wc_elo_df = wc_elo_df.with_columns([
        pl.col('ID').cast(pl.Int64),
        pl.col('Season').cast(pl.Int64),
        pl.col('Date').cast(pl.Date),
        pl.col('City').cast(pl.Utf8),
        pl.col('Country').cast(pl.Utf8),
        pl.col('Distance').cast(pl.Utf8)
    ])

    all_races_join = all_races_df.select([
        pl.col('ID').cast(pl.Int64),
        pl.col('Season').cast(pl.Int64),
        pl.col('Race').cast(pl.Int64).alias('all_races_Race'),
        pl.col('Date').cast(pl.Date),
        pl.col('City').cast(pl.Utf8),
        pl.col('Country').cast(pl.Utf8),
        pl.col('Distance').cast(pl.Utf8)
    ])

    join_cols = ['ID', 'City', 'Country', 'Date', 'Distance', 'Season']

    joined = wc_elo_df.join(
        all_races_join,
        on=join_cols,
        how='left'
    )

    joined = joined.with_columns([
        pl.when(pl.col('Race') == 0)
        .then(pl.lit(0))
        .otherwise(pl.col('all_races_Race'))
        .alias('lookup_race')
    ])

    matched = joined.filter(pl.col('lookup_race').is_not_null())
    unmatched_count = joined.filter(pl.col('lookup_race').is_null() & (pl.col('Race') != 0)).height
    if unmatched_count > 0:
        print(f"Warning: {unmatched_count} WC races didn't match to all-races data")

    matched = matched.with_columns([
        (pl.col('Season') * 1000 + pl.when(pl.col('lookup_race') == 0).then(999).otherwise(pl.col('lookup_race'))).alias('sort_key')
    ])

    matched = matched.sort(['ID', 'sort_key'])

    wc_ids = set(matched['ID'].unique().to_list())
    print(f"Built WC lookup with {len(wc_ids)} unique skier IDs")

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
    """Calculate dynamic ELO ratings (alpine)"""
    if df.is_empty():
        print("Input DataFrame is empty, no data to process")
        return init_elo_df()

    sex_val = df['Sex'][0]

    column_order = list(init_elo_df().columns)

    df = df.sort(['Season', 'Race', 'Place'])

    id_dict_list = df['ID'].unique().to_list()

    wc_lookup, wc_ids = build_wc_elo_lookup(wc_elo_df, df)

    pred_id_dict = {k: 1300.0 for k in id_dict_list}

    wc_id_dict = {}

    max_racers = df.group_by('Season').agg(pl.len().alias('count'))['count'].max()

    seasons = df['Season'].unique().sort().to_list()

    df_by_season = {s: df.filter(pl.col('Season') == s) for s in seasons}

    all_results = []

    for season_idx, season_year in enumerate(seasons):
        season_df = df_by_season[season_year]

        K = k_finder(season_df, max_racers)
        print(f"({season_year}, {K})")

        race_partitions = season_df.partition_by('Race', maintain_order=True)

        season_results = []

        for race_df in race_partitions:
            ski_ids_r = race_df['ID'].to_list()
            places_arr = race_df['Place'].to_numpy()
            race_num = race_df['Race'][0]
            distance_val = race_df['Distance'][0]

            n_skiers = len(ski_ids_r)

            pelo_arr = np.empty(n_skiers, dtype=object)
            elo_arr = np.empty(n_skiers, dtype=object)
            pred_pelo_arr = np.empty(n_skiers, dtype=float)
            has_real_elo_arr = np.zeros(n_skiers, dtype=bool)

            for i, idd in enumerate(ski_ids_r):
                pred_pelo_arr[i] = pred_id_dict[idd]

                if idd in wc_ids:
                    if idd not in wc_id_dict:
                        wc_pelo, _ = get_wc_elo_at_race(wc_lookup, idd, season_year, race_num)
                        if wc_pelo is not None:
                            wc_id_dict[idd] = wc_pelo
                        else:
                            wc_id_dict[idd] = 1300.0

                    pelo_arr[i] = wc_id_dict[idd]
                    elo_arr[i] = None
                    has_real_elo_arr[i] = True
                else:
                    pelo_arr[i] = None
                    elo_arr[i] = None

            # Determine K modifier based on race type (alpine: combined events)
            if distance_val == "Combined":
                K_mod = K * 0.8
            else:
                K_mod = K

            n_real = has_real_elo_arr.sum()
            if n_real > 0:
                wc_pelo_floats = np.array([p if p is not None else 0 for p in pelo_arr], dtype=float)

                comparison_elos = np.where(has_real_elo_arr, wc_pelo_floats, pred_pelo_arr)

                E_all = calc_Evec_partial(comparison_elos, has_real_elo_arr)
                S_all = calc_Svec_partial(places_arr, has_real_elo_arr)

                new_elos = comparison_elos + K_mod * (S_all - E_all)

                for i, idd in enumerate(ski_ids_r):
                    if has_real_elo_arr[i]:
                        wc_id_dict[idd] = new_elos[i]
                        elo_arr[i] = new_elos[i]
                    else:
                        pred_id_dict[idd] = new_elos[i]

                pred_elo_arr = new_elos
            else:
                pred_elo_arr = pred_pelo_arr.copy()

            race_df = race_df.with_columns([
                pl.Series(name="Pelo", values=[float(x) if x is not None else None for x in pelo_arr]).cast(pl.Float64),
                pl.Series(name="Elo", values=[float(x) if x is not None else None for x in elo_arr]).cast(pl.Float64),
                pl.Series(name="pred_Pelo", values=pred_pelo_arr),
                pl.Series(name="pred_Elo", values=pred_elo_arr)
            ]).select(column_order)

            season_results.append(race_df)

        if season_results:
            all_results.append(pl.concat(season_results))

        end_of_season_df, wc_id_dict, pred_id_dict = batch_process_end_of_season(
            season_df, wc_id_dict, pred_id_dict,
            discount, base_elo, seasons, season_idx, sex_val
        )
        all_results.append(end_of_season_df.select(column_order))

    elo_df = pl.concat(all_results)

    elo_df = elo_df.sort(['Date', 'Season', 'Race', 'Place'])
    return elo_df

# Main execution
df = pl.DataFrame()

json_str = sys.argv[1]
data = json.loads(json_str)

# Build file strings for WC elo lookup and output
# WC elo files use abbreviated names (SuperG, GS, SL)
# Output files use original names with underscores (Super_G, Giant_Slalom, Slalom)
wc_file_string = ""
output_file_string = ""
wc_file_string += data.get('sex', '')
output_file_string += data.get('sex', '')

# Mapping from all_scrape Distance names to WC elo file naming convention
distance_to_wc_filename = {
    "Super G": "SuperG",
    "Giant Slalom": "GS",
    "Slalom": "SL",
    "Downhill": "Downhill",
    "Combined": "Combined",
    "Speed": "Speed",
    "Tech": "Tech"
}

# Add distance if specified
if data.get('distance') not in (None, "null"):
    distance_str = data.get('distance')
    # WC file uses abbreviated names
    wc_distance_str = distance_to_wc_filename.get(distance_str, distance_str.replace(" ", "_"))
    wc_file_string += f"_{wc_distance_str}"
    # Output file uses original names with underscores
    output_file_string += f"_{distance_str.replace(' ', '_')}"

# Apply all filters
for key, value in data.items():
    if value is not None and value != "null":
        df = handle_value(key, value, df)

# Load the WC elo data to identify real Elo holders
# Use the regular WC elo file (from elo.py, not all_elo.py)
wc_elo_path = os.path.expanduser(f"~/ski/elo/python/alpine/polars/excel365/{wc_file_string}.csv")

try:
    wc_elo_df = pl.read_csv(
        wc_elo_path,
        schema_overrides={
            "Date": pl.Date,
            "City": pl.Utf8,
            "Country": pl.Utf8,
            "Distance": pl.Utf8,
            "ID": pl.Int64,
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
base_path = os.path.expanduser("~/ski/elo/python/alpine/polars/excel365")

# Save CSV format with dyn_ prefix to distinguish from WC elo and pred files
elo_df.write_csv(f"{base_path}/dyn_{output_file_string}.csv")
print(f"Saved to {base_path}/dyn_{output_file_string}.csv")
print(time.time() - start_time)
