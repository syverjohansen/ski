import polars as pl
import pandas as pd
import numpy as np
from datetime import datetime, date
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import time
import logging
import sys
import json
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

pl.Config.set_tbl_cols(100)
start_time = time.time()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create output directory if it doesn't exist
output_dir = os.path.expanduser("~/ski/elo/python/nordic-combined/polars/relay/excel365")
os.makedirs(output_dir, exist_ok=True)

def sex(df, sex):
    """Load data for specified sex from all_scrape output files"""
    # Define schema overrides to handle "N/A" values
    schema_overrides = {
        'HillSize': pl.String,
        'Distance': pl.String,  # Handle "N/A" values
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

    if(sex=="M"):
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
    elif ',' in race_types:
        race_list = [r.strip() for r in race_types.split(',')]
        df = df.filter(pl.col('RaceType').is_in(race_list))
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

def place2(df, place2):
    """Filter by maximum place"""
    if not place2 or place2 == "null":
        return df
    df = df.filter(pl.col('Place') <= place2)
    return df

def names(df, names):
    """Filter by skier names"""
    if not names or names == "null":
        return df
    if isinstance(names, list):
        df = df.filter(pl.col('Skier').is_in(names))
    else:
        df = df.filter(pl.col('Skier') == names)
    return df

def season1(df, season1):
    """Filter by start season"""
    if not season1 or season1 == "null":
        return df
    df = df.filter(pl.col('Season') >= season1)
    return df

def season2(df, season2):
    """Filter by end season"""
    if not season2 or season2 == "null":
        return df
    df = df.filter(pl.col('Season') <= season2)
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
        df = sex(df, value)
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
    elif key == "name":
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

def k_finder(season_df, max_racers):
    """Calculate k-value for a season based on number of racers"""
    racers = season_df.height
    k = float(max_racers / racers)
    k = min(k, 5)
    k = max(k, 1)

    logger.info(f"K value for season: {k}")
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
            "Elo": pl.Float64
        }
    )

def process_skier(idd, season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df_columns):
    """Process offseason data for a single skier"""
    current_season_year = seasons[season]
    endseasondate = date(current_season_year, 5, 1)

    endskier = season_df.filter(pl.col('ID') == idd)
    if len(endskier) == 0:
        return None

    endname = endskier['Skier'][-1]
    endnation = endskier['Nation'][-1]
    endpelo = id_dict[idd]
    endelo = endpelo * discount + base_elo * (1 - discount)

    try:
        endbirthday = endskier['Birthday'][-1]
        if endbirthday is None:
            endbirthday = datetime(1700, 1, 1)
    except:
        endbirthday = datetime(1700, 1, 1)

    try:
        endage = endskier['Age'][-1] if 'Age' in endskier.columns else 0.0
        endage = float(endage) if endage is not None else 0.0
    except:
        endage = 0.0

    if 'Exp' in endskier.columns:
        endexp = endskier['Exp'][-1]
    else:
        endexp = 0

    if 'Leg' in endskier.columns:
        endleg = endskier['Leg'][-1]
    else:
        endleg = 0

    endf = pl.DataFrame({
        "Date": [endseasondate],
        "City": ["Summer"],
        "Country": ["Break"],
        "Sex": [sex],
        "Distance": ["0"],
        "RaceType": ["Offseason"],
        "MassStart": [0],
        "Event": ["Offseason"],
        "Place": [0],
        "Skier": [endname],
        "Nation": [endnation],
        "ID": [idd],
        "Season": [current_season_year],
        "Race": [0],
        "Birthday": [endbirthday],
        "Age": [endage],
        "Exp": [endexp],
        "Leg": [endleg],
        "Pelo": [endpelo],
        "Elo": [endelo]
    })

    if elo_df_columns:
        endf = endf.select(elo_df_columns)

    id_dict[idd] = endelo
    return endf

def parallel_process_skiers(season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df):
    """Process all skiers for a season in parallel"""
    ski_ids_s = season_df['ID'].unique().to_list()

    elo_df_columns = elo_df.columns if not elo_df.is_empty() else None

    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                process_skier,
                idd,
                season_df,
                id_dict,
                discount,
                base_elo,
                seasons,
                season,
                sex,
                elo_df_columns
            ): idd for idd in ski_ids_s
        }

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)

    if not results:
        return elo_df, id_dict

    new_rows = pl.concat(results)

    if elo_df.is_empty():
        return new_rows, id_dict

    for col in elo_df.columns:
        if col in new_rows.columns:
            col_type = elo_df.schema[col]
            new_rows = new_rows.with_columns(pl.col(col).cast(col_type))

    new_rows = new_rows.select(elo_df.columns)
    return pl.concat([elo_df, new_rows]), id_dict

def elo(df, base_elo=1300, K=1, discount=.85):
    """
    Calculate ELO ratings for all Nordic Combined team results in the dataset
    """
    if df.is_empty():
        logger.error("Input DataFrame is empty, no data to process")
        return init_elo_df()

    sex = df['Sex'][0]

    elo_df = init_elo_df()

    column_order = elo_df.columns

    df = df.sort(['Season', 'Race', 'Place'])

    id_dict_list = df['ID'].unique().to_list()

    id_dict = {k: 1300.0 for k in id_dict_list}

    max_races = df['Race'].max()

    seasons = df['Season'].unique().to_list()

    max_racers = 0
    for season in seasons:
        season_df = df.filter(pl.col('Season') == season)
        max_racers = max(max_racers, season_df.height)

    for season in range(len(seasons)):
        season_df = df.filter(pl.col('Season')==seasons[season]).sort(['Race', 'Place'])

        K = k_finder(season_df, max_racers)
        logger.info(f"Processing season {seasons[season]}")

        season_elo_df = init_elo_df()

        races = season_df['Race'].unique().to_list()
        for race in range(len(races)):
            race_df = season_df.filter(pl.col('Race')==races[race]).sort('Place')

            ski_ids_r = race_df['ID'].to_list()

            pelo_list = [id_dict[idd] for idd in ski_ids_r]

            places_list = race_df['Place'].to_list()

            race_df = race_df.with_columns([
                pl.Series(name="Pelo", values=pelo_list)
            ])

            race_type_val = race_df['RaceType'][0]

            # Apply different K factors for different race types
            if race_type_val == "Team":
                K_adj = K / 4
            elif race_type_val == "Team Sprint":
                K_adj = K / 2
            else:
                K_adj = K

            E = calc_Evec(pelo_list)
            S = calc_Svec(places_list)
            elo_list = np.array(pelo_list) + K_adj * (S - E)

            race_df = race_df.with_columns([
                pl.Series(name="Elo", values=elo_list)
            ])

            race_df = race_df.select(column_order)

            season_elo_df = pl.concat([season_elo_df, race_df])

            id_dict.update({idd: elo_list[i] for i, idd in enumerate(ski_ids_r)})

        elo_df = pl.concat([elo_df, season_elo_df])

        elo_df, id_dict = parallel_process_skiers(season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df)

    elo_df = elo_df.sort(['Date','Season', 'Race', 'Place'])
    return elo_df

def main():
    """Main function to run ELO calculations"""
    if len(sys.argv) > 1:
        try:
            json_str = sys.argv[1]
            data = json.loads(json_str)
            logger.info(f"Using configuration: {data}")
        except Exception as e:
            logger.error(f"Error parsing JSON: {e}")
            return
    else:
        data = {
            "sex": "M",
            "event_filter": 0,
            "race_type": None,
            "team_filter": 1,
            "date1": None,
            "date2": None,
            "city": None,
            "country": None,
            "place1": None,
            "place2": None,
            "name": None,
            "season1": None,
            "season2": None,
            "nation": None
        }

    df = pl.DataFrame()

    for key, value in data.items():
        df = handle_value(key, value, df)

    file_string = ""
    file_string += data.get('sex', '')
    file_string += "_rel"

    if data.get('race_type') not in (None, "null"):
        race_type_str = data.get('race_type')
        race_type_str = race_type_str.replace(" ", "_")
        file_string += f"_{race_type_str}"

    for key, value in data.items():
        if key not in ("sex", "race_type", "event_filter", "team_filter") and value not in (None, "null"):
            file_string += f"_{value}"

    # Add "all_" prefix for complete calendar data
    file_string = "all_" + file_string

    elo_df = elo(df)

    base_path = os.path.expanduser("~/ski/elo/python/nordic-combined/polars/relay/excel365")

    elo_df.write_csv(f"{base_path}/{file_string}.csv")

    execution_time = time.time() - start_time
    logger.info(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()
