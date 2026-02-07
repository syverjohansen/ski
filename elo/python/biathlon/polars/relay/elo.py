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
output_dir = os.path.expanduser("~/ski/elo/python/biathlon/polars/relay/excel365")
os.makedirs(output_dir, exist_ok=True)

def sex(df, sex):
    """Load data for specified sex"""
    if(sex=="M"):
        df = pl.read_csv("~/ski/elo/python/biathlon/polars/relay/excel365/men_scrape_update.csv")
    else:
        df = pl.read_csv("~/ski/elo/python/biathlon/polars/relay/excel365/ladies_scrape_update.csv")
    
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

def relay_filter(df, include_relays):
    """Filter to include or exclude relay events"""
    if include_relays == 0:
        # Exclude relay events
        df = df.filter(~pl.col('RaceType').str.contains("Relay"))
    # If include_relays is 1, include all races (no filtering needed)
    
    return df

def event_filter(df, apply_filter):
    """Filter events to include only major competitions"""
    if not apply_filter or apply_filter == 0:
        return df
        
    # Filter to include only Olympic Winter Games, World Championship, and World Cup
    df = df.filter(
        pl.col('Event').str.contains("Olympic|World Cup|Championship", ignore_case=True)
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
    elif key == "relay_filter":
        df = relay_filter(df, value)
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
    Input:
        Place_vector: the place of each athlete, an array of sorted integer values, [P1, P2, ... Pn] so that P1 <= P2 ... <= Pn
    Output:
        S_vector: the actual score of each athlete (winning, drawing, or losing) against the (n-1) athletes, an array of float values, [S1, S2, ... Sn]
            win = 1
            draw = 1 / 2
    '''
    # Getting the length of the place vector and the set of the place_vector
    n, n_unique = len(Place_vector), len(set(Place_vector))
    
    # If no draws:
    if n==n_unique:
        # Reverses the order of the places to show how many wins a person got
        S_vector = np.arange(n)[::-1]
        return S_vector
    # If there are draws
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
    Input:
        R_vector: the ELO rating of each athlete, an array of float values, [R1, R2, ... Rn]
    Output:
        E_vector: the expected value of each athlete winning against all the (n-1) athletes, an array of float values, [E1, E2, ... En]
    '''
    
    # R_vector is the list of previous elo scores for each skier.  Going to convert to an array and get its size
    R_vector = np.array(R_vector)
    n = R_vector.size
    
    # R_mat_col creates a matrix of the elo scores
    # Then we transpose the matrix.  Now each row is each skiers pelo, and each column represents all the pelos
    R_mat_col = np.tile(R_vector, (n,1))
    R_mat_row = np.transpose(R_mat_col)
    
    # Now this computes the expected score for the Elo matchup
    E_matrix = 1/(1+basis**((R_mat_row-R_mat_col)/difference))
    
    # We are not interested in self-matchups
    np.fill_diagonal(E_matrix, 0)
    
    # Sum of expected scores for each player against all other players
    E_vector = np.sum(E_matrix, axis=0)
    
    return E_vector

# Getting the K value for a season
def k_finder(season_df, max_racers):
    """Calculate k-value for a season based on number of racers"""
    # RACERS APPROACH (current):
    # Counting total race entries (rows) in the season
    racers = season_df.height
    # K is max_racers / current_racers, with minimum of 1 and maximum of 5
    k = float(max_racers / racers)
    k = min(k, 5)  # cap at 5
    k = max(k, 1)  # floor at 1

    # RACES APPROACH (commented out):
    # """Calculate k-value for a season based on number of races"""
    # # Figuring out how many races there are for a given season
    # races = season_df.select(pl.col("Race").n_unique()).item()
    # # Calculating the k-value for that season
    # # The lowest k-value is 1. This is for the season with the most races
    # # The highest is 5
    # # Whatever is smallest between 5, most races in a season/2, and most races in a season/races in that season
    # k = max(1, min(5, float(max_var_length/2), float(max_var_length/races)))

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
    # Create end of season date as a Date object (not Datetime)
    # Using the current season's year for May 1st (not the next year)
    current_season_year = seasons[season]
    endseasondate = date(current_season_year, 5, 1)  # End of season is May 1st of CURRENT year
    
    # Get skier information
    endskier = season_df.filter(pl.col('ID') == idd)
    if len(endskier) == 0:
        # Skip if skier not found in season data
        return None
        
    endname = endskier['Skier'][-1]
    endnation = endskier['Nation'][-1]
    endpelo = id_dict[idd]
    endelo = endpelo * discount + base_elo * (1 - discount)
    
    # Safely handle birthday data - this is likely the source of the error
    try:
        endbirthday = endskier['Birthday'][-1]
        # If birthday is None, use a default value
        if endbirthday is None:
            # Use a very old date that's unlikely to cause issues
            endbirthday = datetime(1700, 1, 1)
    except:
        # If any error, use a default date
        endbirthday = datetime(1700, 1, 1)
    
    # Safely handle other fields
    try:
        endage = endskier['Age'][-1] if 'Age' in endskier.columns else 0.0
        # Ensure age is a float
        endage = float(endage) if endage is not None else 0.0
    except:
        endage = 0.0
        
    if 'Exp' in endskier.columns:
        endexp = endskier['Exp'][-1]
    else:
        endexp = 0  # Default if experience isn't available
        
    if 'Leg' in endskier.columns:
        endleg = endskier['Leg'][-1]
    else:
        endleg = 0  # Default if leg isn't available
    
    # Create DataFrame with all columns in the correct order
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
        "Season": [current_season_year],  # Use current season, not next season
        "Race": [0],
        "Birthday": [endbirthday],
        "Age": [endage],
        "Exp": [endexp],
        "Leg": [endleg],
        "Pelo": [endpelo],
        "Elo": [endelo]
    })
    
    # Ensure column order matches main DataFrame
    if elo_df_columns:
        endf = endf.select(elo_df_columns)
    
    id_dict[idd] = endelo
    return endf

def parallel_process_skiers(season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df):
    """Process all skiers for a season in parallel"""
    ski_ids_s = season_df['ID'].unique().to_list()
    
    # Get column names from main DataFrame if it exists
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
            if result is not None:  # Only add valid results
                results.append(result)
    
    # Concatenate all the DataFrames returned by the parallel processing
    if not results:
        return elo_df, id_dict  # If no results, return the original elo_df
        
    new_rows = pl.concat(results)
    
    # If elo_df is empty, return new_rows directly
    if elo_df.is_empty():
        return new_rows, id_dict
    
    # Ensure both DataFrames have the same column order and types before concatenation
    for col in elo_df.columns:
        if col in new_rows.columns:
            col_type = elo_df.schema[col]
            new_rows = new_rows.with_columns(pl.col(col).cast(col_type))
    
    new_rows = new_rows.select(elo_df.columns)
    return pl.concat([elo_df, new_rows]), id_dict

def elo(df, base_elo=1300, K=1, discount=.85):
    """
    Calculate ELO ratings for all biathletes in the dataset
    
    Args:
        df: DataFrame with race results
        base_elo: Base ELO rating for new biathletes
        K: K-factor for ELO calculation
        discount: Season-end discount factor
    
    Returns:
        DataFrame with ELO ratings
    """
    # Check if df is empty
    if df.is_empty():
        logger.error("Input DataFrame is empty, no data to process")
        return init_elo_df()  # Return empty DataFrame with correct schema
    
    # Get the sex
    sex = df['Sex'][0]
    
    # Create an empty DataFrame with the correct schema
    elo_df = init_elo_df()
    
    # Get column order from the initialized DataFrame
    column_order = elo_df.columns
    
    # Sort the entire dataframe by Season, Race, and Place
    df = df.sort(['Season', 'Race', 'Place'])
    
    # Create a list of the IDs in the df
    id_dict_list = df['ID'].unique().to_list()
    
    # Assign everyone a value of 1300 to start out with
    id_dict = {k: 1300.0 for k in id_dict_list}
    
    # Getting the maximum number of races in the df
    max_races = df['Race'].max()
    
    # Getting a list of all the seasons in the df
    seasons = df['Season'].unique().to_list()

    # RACERS APPROACH (current):
    # Calculate max_racers (total race entries) across all seasons
    max_racers = 0
    for season in seasons:
        season_df = df.filter(pl.col('Season') == season)
        max_racers = max(max_racers, season_df.height)

    # RACES APPROACH (commented out):
    # max_var_length = 0
    # for season in seasons:
    #     season_df = df.filter(pl.col('Season') == season)
    #     max_var_length = max(max_var_length, season_df['Race'].unique().shape[0])

    for season in range(len(seasons)):
        # Creating a season df and sorting by Race and Place
        season_df = df.filter(pl.col('Season')==seasons[season]).sort(['Race', 'Place'])

        # Get the K value based on number of racers
        K = k_finder(season_df, max_racers)
        # RACES APPROACH: K = k_finder(season_df, max_var_length)
        logger.info(f"Processing season {seasons[season]}")

        # Initialize season_elo_df with the same schema
        season_elo_df = init_elo_df()
        
        races = season_df['Race'].unique().to_list()
        for race in range(len(races)):
            # Isolate the race into a df and sort by Place
            race_df = season_df.filter(pl.col('Race')==races[race]).sort('Place')
            
            # Create a list of all the IDs in that race
            ski_ids_r = race_df['ID'].to_list()
            
            # Get the most recent elo score for each skier in the race
            pelo_list = [id_dict[idd] for idd in ski_ids_r]
            
            # Get a list of all the places in the race
            places_list = race_df['Place'].to_list()
            
            # Create a column called Pelo that has all the previous elo values
            race_df = race_df.with_columns([
                pl.Series(name="Pelo", values=pelo_list)
            ])

            # Get race type - determine K adjustment based on race type
            race_type = race_df['RaceType'][0]
            
            # Apply different K factors for different race types
            if race_type in ["Relay", "Mixed Relay"]:
                # Regular relays have weight divided by 4
                K_adj = K / 4
            elif race_type == "Single Mixed Relay":
                # Single mixed relays have weight divided by 2
                K_adj = K / 2
            else:
                # Standard races use normal K
                K_adj = K
                
            # Calculate ELO values
            E = calc_Evec(pelo_list)
            S = calc_Svec(places_list)
            elo_list = np.array(pelo_list) + K_adj * (S - E)

            # Add Elo column
            race_df = race_df.with_columns([
                pl.Series(name="Elo", values=elo_list)
            ])

            # Ensure race_df has all required columns in correct order
            race_df = race_df.select(column_order)
            
            # Concatenate to season_elo_df
            season_elo_df = pl.concat([season_elo_df, race_df])
            
            # Update id_dict with new elo values
            id_dict.update({idd: elo_list[i] for i, idd in enumerate(ski_ids_r)})

        # Concatenate season results to main DataFrame
        elo_df = pl.concat([elo_df, season_elo_df])
        
        # Add end of season records
        elo_df, id_dict = parallel_process_skiers(season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df)
    
    # Final sort of the entire DataFrame
    elo_df = elo_df.sort(['Date','Season', 'Race', 'Place'])
    return elo_df

def main():
    """Main function to run ELO calculations"""
    # Check if we have command line arguments
    if len(sys.argv) > 1:
        # Parse JSON configuration
        try:
            json_str = sys.argv[1]
            data = json.loads(json_str)
            logger.info(f"Using configuration: {data}")
        except Exception as e:
            logger.error(f"Error parsing JSON: {e}")
            return
    else:
        # Default configuration for processing all data
        data = {
            "sex": "M",  # Default to men
            "event_filter": 1,  # Default to filtering major events
            "race_type": None,
            "relay_filter": 1,  # Default to excluding relays
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
    
    # Apply filters based on configuration
    for key, value in data.items():
        df = handle_value(key, value, df)
    
    # Generate output file name
    file_string = ""
    file_string += data.get('sex', '')  # Start with sex
    
    # Add race_type if specified
    if data.get('race_type') not in (None, "null"):
        race_type_str = data.get('race_type')
        # Clean up race type for filename
        race_type_str = race_type_str.replace(" ", "_")
        file_string += f"_{race_type_str}"
    
    # Add other filters to filename if needed
    for key, value in data.items():
        if key not in ("sex", "race_type", "event_filter", "relay_filter") and value not in (None, "null"):
            file_string += f"_{value}"
    
    # Calculate ELO ratings
    elo_df = elo(df)
    
    # Save results
    base_path = os.path.expanduser("~/ski/elo/python/biathlon/polars/relay/excel365")
    
    # Save CSV format
    elo_df.write_csv(f"{base_path}/{file_string}.csv")
    
    # Log execution time
    execution_time = time.time() - start_time
    logger.info(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()