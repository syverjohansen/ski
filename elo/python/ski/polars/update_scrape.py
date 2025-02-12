import polars as pl
import logging
from datetime import datetime
import sys
from typing import Dict, Set, Tuple, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import concurrent.futures
from typing import List, Any, Optional, Tuple

# Import from scrape.py
from scrape import (setup_cache_structure, fetch_season_links, get_race_data, 
                   get_race_results, construct_historical_df)

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def get_existing_metadata(df: pl.DataFrame) -> Dict:
    """Extract metadata from existing DataFrame"""
    try:
        # Get unique races
        races = set(df.select(["Date", "City", "Event"]).unique().rows())
        
        # Get max experience per ID
        max_exp = (df.group_by('ID')
                  .agg(pl.col('Exp').max().alias('max_exp')))
        exp_dict = dict(zip(max_exp['ID'], max_exp['max_exp']))
        
        return {
            'races': races,
            'experience': exp_dict
        }
    except Exception as e:
        logging.error(f"Error extracting metadata: {e}")
        return {'races': set(), 'experience': {}}

def load_and_process_data(sex: str) -> Tuple[Optional[pl.DataFrame], Dict]:
    """Load existing data and extract metadata"""
    try:
        filename = f"{'men' if sex=='M' else 'ladies'}_scrape.feather"
        path = Path(f"~/ski/elo/python/ski/polars/excel365/{filename}").expanduser()
        
        if not path.exists():
            logging.warning(f"No existing data file found for {sex}")
            return None, {'races': set(), 'experience': {}}
        
        # Load data in streaming mode for memory efficiency
        df = pl.scan_ipc(path).collect()
        metadata = get_existing_metadata(df)
        
        logging.info(f"Loaded existing {sex} data with {len(df)} rows")
        return df, metadata
        
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None, {'races': set(), 'experience': {}}

def process_race(link: List[Any], sex: str, metadata: Dict) -> Optional[Tuple]:
    """Process a single race with its results"""
    try:
        table_data = get_race_data(link)
        if table_data is None:
            return None
            
        # Check if race exists
        race_key = (table_data[0], table_data[1], table_data[3])
        if race_key in metadata['races']:
            logging.info(f"Skipping existing race: {race_key}")
            return None
            
        race_results = get_race_results(link, sex)
        if not race_results:
            return None
            
        return (table_data, race_results)
        
    except Exception as e:
        logging.error(f"Error processing race {link}: {e}")
        return None

def process_new_season(sex: str, metadata: Dict) -> Optional[pl.DataFrame]:
    """Process current season's races in parallel"""
    current_year = 2025
    
    # Get current season links
    season_links = fetch_season_links(current_year, sex)
    if not season_links:
        logging.warning(f"No races found for {current_year} ({sex})")
        return None
    
    # Set up thread pool for parallel processing
    max_workers = min(32, multiprocessing.cpu_count() * 4)
    
    # Process races in parallel
    tables = []
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks for each race
        future_to_link = {
            executor.submit(process_race, link, sex, metadata): link 
            for link in season_links
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_link):
            result = future.result()
            if result is not None:
                table_data, race_results = result
                tables.append(table_data)
                results.append(race_results)
    
    if not tables:
        return None
        
    return construct_historical_df(tables, results, sex)

def update_experience(new_df: pl.DataFrame, exp_dict: Dict) -> pl.DataFrame:
    """Update experience values for new data"""
    return new_df.with_columns([
        (pl.col('ID')
         .map_elements(lambda x: exp_dict.get(x, 0))
         .add(pl.col('Exp'))
         .alias('Exp'))
    ])

def merge_and_save(old_df: Optional[pl.DataFrame], 
                  new_df: Optional[pl.DataFrame], 
                  sex: str) -> None:
    """Merge and save data efficiently"""
    if new_df is None:
        logging.info(f"No new data to add for {sex}")
        return
        
    try:
        # If no old data, just save new data
        if old_df is None:
            final_df = new_df
        else:
            # Ensure consistent types before concatenation
            type_mapping = {
                'Date': pl.Date,
                'City': pl.Utf8,
                'Country': pl.Utf8,
                'Event': pl.Utf8,
                'Distance': pl.Utf8,
                'MS': pl.Int32,
                'Technique': pl.Utf8,
                'Season': pl.Int32,
                'Race': pl.Int32,
                'Sex': pl.Utf8,
                'Place': pl.Int32,
                'Skier': pl.Utf8,
                'Nation': pl.Utf8,
                'ID': pl.Int32,
                'Birthday': pl.Datetime,
                'Age': pl.Float64,
                'Exp': pl.Int32
            }
            
            # Cast both dataframes to ensure consistent types
            old_df = old_df.with_columns([pl.col(col).cast(dtype) for col, dtype in type_mapping.items()])
            new_df = new_df.with_columns([pl.col(col).cast(dtype) for col, dtype in type_mapping.items()])
            
            # Concatenate and sort
            final_df = pl.concat([old_df, new_df])
            final_df = final_df.sort(['ID', 'Date'])
        
        # Save results
        base_path = Path("~/ski/elo/python/ski/polars/excel365").expanduser()
        prefix = 'men' if sex == 'M' else 'ladies'
        
        final_df.write_ipc(base_path / f"{prefix}_scrape_update.feather")
        final_df.write_csv(base_path / f"{prefix}_scrape_update.csv")
        
        logging.info(f"Saved updated {sex} data with {len(final_df)} rows")
        
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        print(f"Detailed error: {str(e)}")  # Added for more debug info

def main():
    setup_logging()
    setup_cache_structure()
    
    for sex in ['M', 'L']:
        # Load existing data and metadata
        old_df, metadata = load_and_process_data(sex)
        
        # Process new season
        logging.info(f"Processing current season for {sex}")
        new_df = process_new_season(sex, metadata)
        
        if new_df is not None:
            # Update experience values
            new_df = update_experience(new_df, metadata['experience'])
            
            # Merge and save
            merge_and_save(old_df, new_df, sex)
        else:
            logging.info(f"No new data to add for {sex}")

if __name__ == '__main__':
    main()