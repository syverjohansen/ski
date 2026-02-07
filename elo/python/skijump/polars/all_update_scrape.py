import polars as pl
import logging
from datetime import datetime
import sys
from typing import Dict, Set, Tuple, Optional, List, Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import concurrent.futures

# Import from all_scrape.py (uses &hva=k parameter for all competitions)
from all_scrape import (setup_cache_structure, fetch_season_links,
                   get_race_data, get_race_results, construct_historical_df, format_skier_name)

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def get_existing_metadata(men_df: pl.DataFrame, ladies_df: pl.DataFrame) -> Dict:
    """Extract metadata from existing DataFrames"""
    try:
        # Get unique races from both men and ladies datasets
        men_races = set(men_df.select(["Date", "City", "RaceType"]).unique().rows())
        ladies_races = set(ladies_df.select(["Date", "City", "RaceType"]).unique().rows())
        all_races = men_races.union(ladies_races)
        
        # Get max experience per ID for men
        men_max_exp = (men_df.group_by('ID')
                     .agg(pl.col('Exp').max().alias('max_exp')))
        men_exp_dict = dict(zip(men_max_exp['ID'], men_max_exp['max_exp']))
        
        # Get max experience per ID for ladies
        ladies_max_exp = (ladies_df.group_by('ID')
                        .agg(pl.col('Exp').max().alias('max_exp')))
        ladies_exp_dict = dict(zip(ladies_max_exp['ID'], ladies_max_exp['max_exp']))
        
        return {
            'races': all_races,
            'men_experience': men_exp_dict,
            'ladies_experience': ladies_exp_dict
        }
    except Exception as e:
        logging.error(f"Error extracting metadata: {e}")
        return {'races': set(), 'men_experience': {}, 'ladies_experience': {}}

def load_and_process_data() -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame], Dict]:
    """Load existing data and extract metadata"""
    try:
        # Define file paths - using skijump path instead of nordic-combined
        base_path = Path("~/ski/elo/python/skijump/polars/excel365").expanduser()
        men_path = base_path / "all_men_scrape.csv"
        ladies_path = base_path / "all_ladies_scrape.csv"
        
        # Check if files exist
        men_df = None
        ladies_df = None

        # Define schema overrides to handle "N/A" values
        schema_overrides = {
            'HillSize': pl.String,
            'RaceType': pl.String,
            'TeamID': pl.String
        }

        if men_path.exists():
            men_df = pl.scan_csv(men_path, schema_overrides=schema_overrides).collect()
            logging.info(f"Loaded men's data with {len(men_df)} rows")
        else:
            logging.warning("No existing men's data file found")

        if ladies_path.exists():
            ladies_df = pl.scan_csv(ladies_path, schema_overrides=schema_overrides).collect()
            logging.info(f"Loaded ladies' data with {len(ladies_df)} rows")
        else:
            logging.warning("No existing ladies' data file found")
            
        # Generate metadata
        if men_df is not None and ladies_df is not None:
            metadata = get_existing_metadata(men_df, ladies_df)
        elif men_df is not None:
            # Only men's data exists
            empty_ladies = pl.DataFrame(schema=men_df.schema)
            metadata = get_existing_metadata(men_df, empty_ladies)
        elif ladies_df is not None:
            # Only ladies' data exists
            empty_men = pl.DataFrame(schema=ladies_df.schema)
            metadata = get_existing_metadata(empty_men, ladies_df)
        else:
            metadata = {'races': set(), 'men_experience': {}, 'ladies_experience': {}}
        
        return men_df, ladies_df, metadata
        
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None, None, {'races': set(), 'men_experience': {}, 'ladies_experience': {}}

def process_race(link: List[Any], metadata: Dict) -> Optional[Tuple]:
    """Process a single race with its results"""
    try:
        # Extract info from link
        is_team_event = link[3] if len(link) > 3 else False
        sex = link[4] if len(link) > 4 else 'M'  # Default to men if not specified
        
        # Get race data
        table_data = get_race_data(link)
        if table_data is None:
            return None
            
        # Check if race exists - ski jumping uses different indices than nordic combined
        # ski jumping: [date, city, country, event, hill_size, race_type, team_event, season, race_num, sex]
        race_key = (table_data[0], table_data[1], table_data[5])  # Date, City, RaceType
        if race_key in metadata['races']:
            logging.info(f"Skipping existing race: {race_key}")
            return None
            
        # Get race results
        race_results = get_race_results(link)
        if not race_results:
            return None
            
        return (table_data, race_results)
        
    except Exception as e:
        logging.error(f"Error processing race {link}: {e}")
        return None

def process_new_season(metadata: Dict) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
    """Process current season's races in parallel"""
    now = datetime.now()
    # Winter sports season starts in October, so if it's October or later, 
    # we look for next calendar year's season
    current_year = now.year + 1 if now.month >= 10 else now.year
    all_tables = []
    all_results = []
    
    # Set up thread pool for parallel processing
    max_workers = min(32, multiprocessing.cpu_count() * 4)
    
    # Process men's races
    logging.info(f"Processing men's races for current season")
    men_season_links = fetch_season_links(current_year, 'M')
    
    if men_season_links:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create tasks for each race
            future_to_link = {
                executor.submit(process_race, link, metadata): link 
                for link in men_season_links
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_link):
                result = future.result()
                if result is not None:
                    all_tables.append(result[0])
                    all_results.append(result[1])
    
    # Process women's races separately
    logging.info(f"Processing women's races for current season")
    women_season_links = fetch_season_links(current_year, 'L')  # 'L' for Ladies
    
    if women_season_links:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create tasks for each race
            future_to_link = {
                executor.submit(process_race, link, metadata): link 
                for link in women_season_links
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_link):
                result = future.result()
                if result is not None:
                    all_tables.append(result[0])
                    all_results.append(result[1])
    
    if not all_tables:
        return None, None
        
    # Construct historical DataFrame from new results
    return construct_historical_df(all_tables, all_results)

def update_experience(df: pl.DataFrame, exp_dict: Dict, sex: str) -> pl.DataFrame:
    """Update experience values for new data based on sex"""
    if df is None or len(df) == 0:
        return df
        
    return df.with_columns([
        (pl.col('ID')
         .map_elements(lambda x: exp_dict.get(x, 0))
         .add(pl.col('Exp'))
         .alias('Exp'))
    ])

def merge_and_save(old_df: Optional[pl.DataFrame], 
                  new_df: Optional[pl.DataFrame], 
                  sex: str) -> Optional[pl.DataFrame]:
    """Merge and save data efficiently"""
    if new_df is None or len(new_df) == 0:
        logging.info(f"No new data to add for {sex}")
        return old_df
        
    try:
        # If no old data, just return new data
        if old_df is None or len(old_df) == 0:
            return new_df
            
        # Ensure consistent types before concatenation - ski jumping specific schema
        type_mapping = {
            'Date': pl.Date,
            'City': pl.Utf8,
            'Country': pl.Utf8,
            'Event': pl.Utf8,
            'HillSize': pl.Utf8,
            'RaceType': pl.Utf8,
            'TeamEvent': pl.Int64,
            'Season': pl.Int64,
            'Race': pl.Int64,
            'Place': pl.Int64,
            'Skier': pl.Utf8,
            'Nation': pl.Utf8,
            'ID': pl.Utf8,
            'Sex': pl.Utf8,
            'Birthday': pl.Datetime,
            'Age': pl.Float64,
            'Exp': pl.Int32,
            'Leg': pl.Int64,
            'TeamID': pl.Utf8,
            'Length1': pl.Float64,
            'Length2': pl.Float64,
            'Points': pl.Float64
        }
        
        # Cast both dataframes to ensure consistent types
        old_df = old_df.with_columns([pl.col(col).cast(dtype) for col, dtype in type_mapping.items() 
                                     if col in old_df.columns])
        new_df = new_df.with_columns([pl.col(col).cast(dtype) for col, dtype in type_mapping.items() 
                                     if col in new_df.columns])
        
        # Concatenate and remove duplicates
        final_df = pl.concat([old_df, new_df])
        
        # Remove duplicates based on key columns that uniquely identify a race result
        # For ski jumping, include relevant columns
        before_dedup = len(final_df)
        final_df = final_df.unique(subset=['Date', 'City', 'Event', 'HillSize', 'ID', 'Place', 'Leg', 'TeamID'])
        after_dedup = len(final_df)
        
        if before_dedup > after_dedup:
            logging.info(f"Removed {before_dedup - after_dedup} duplicate rows for {sex}")
        
        # Sort the final dataframe
        final_df = final_df.sort(['ID', 'Date'])
        
        return final_df
        
    except Exception as e:
        logging.error(f"Error merging data for {sex}: {e}")
        return old_df

def save_data(men_df, ladies_df):
    """Save the final dataframes"""
    try:
        base_path = Path("~/ski/elo/python/skijump/polars/excel365").expanduser()
        
        if men_df is not None and len(men_df) > 0:
            men_df.write_csv(base_path / "all_men_scrape.csv")
            logging.info(f"Saved updated men's data with {len(men_df)} rows")

        if ladies_df is not None and len(ladies_df) > 0:
            ladies_df.write_csv(base_path / "all_ladies_scrape.csv")
            logging.info(f"Saved updated ladies' data with {len(ladies_df)} rows")
            
    except Exception as e:
        logging.error(f"Error saving data: {e}")

def main():
    setup_logging()
    setup_cache_structure()
    
    # Load existing data and metadata
    old_men_df, old_ladies_df, metadata = load_and_process_data()
    
    # Process new season
    logging.info("Processing current season")
    new_men_df, new_ladies_df = process_new_season(metadata)
    
    # Update and merge data
    if new_men_df is not None and len(new_men_df) > 0:
        logging.info(f"Found {len(new_men_df)} new men's results")
        # Update experience values for men
        new_men_df = update_experience(new_men_df, metadata['men_experience'], 'M')
        # Merge with existing data
        final_men_df = merge_and_save(old_men_df, new_men_df, 'M')
    else:
        final_men_df = old_men_df
        logging.info("No new men's data to add")
    
    if new_ladies_df is not None and len(new_ladies_df) > 0:
        logging.info(f"Found {len(new_ladies_df)} new ladies' results")
        # Update experience values for ladies
        new_ladies_df = update_experience(new_ladies_df, metadata['ladies_experience'], 'L')
        # Merge with existing data
        final_ladies_df = merge_and_save(old_ladies_df, new_ladies_df, 'L')
    else:
        final_ladies_df = old_ladies_df
        logging.info("No new ladies' data to add")
    
    # Save the final data
    save_data(final_men_df, final_ladies_df)
    
    logging.info("Update process completed")

if __name__ == '__main__':
    main()