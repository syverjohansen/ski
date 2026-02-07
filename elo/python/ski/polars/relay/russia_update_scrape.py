"""
russia_update_scrape.py - Update scraper for current season Russian ski results

This script updates the Russian ski results for the current season only.
Run russia_scrape.py first for historical data (2022 through last season).
"""

import polars as pl
import logging
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional, List, Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Import from russia_scrape.py
from russia_scrape import (
    fetch_season_race_links,
    process_single_race,
    load_fis_reference_data,
    build_id_mapping,
    construct_dataframe,
    save_id_mapping,
    FIS_BAN_DATE,
)


def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def get_current_season_year() -> int:
    """
    Determine the current season year based on UTC date.
    Ski seasons are named for the year they end in:
    - Oct 1 - Dec 31: Year + 1 (season 2026 starts in Oct 2025)
    - Jan 1 - Sep 30: Year (season 2026 runs through spring 2026)
    """
    now = datetime.now(timezone.utc)
    if now.month >= 10:  # Oct-Dec: new season has started
        return now.year + 1
    else:  # Jan-Sep
        return now.year


def get_existing_metadata(df: pl.DataFrame) -> Dict:
    """Extract metadata from existing DataFrame"""
    try:
        # Get unique races (Date, City, Event)
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
        filename = f"russia_{'men' if sex == 'M' else 'ladies'}_scrape.csv"
        path = Path(f"~/ski/elo/python/ski/polars/relay/excel365/{filename}").expanduser()

        if not path.exists():
            logging.warning(f"No existing data file found for {sex}")
            return None, {'races': set(), 'experience': {}}

        df = pl.read_csv(path)
        metadata = get_existing_metadata(df)

        logging.info(f"Loaded existing {sex} data with {len(df)} rows")
        return df, metadata

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None, {'races': set(), 'experience': {}}


def process_race(race_info: Dict[str, Any], year: int, metadata: Dict) -> Optional[Dict]:
    """Process a single race with its results, skipping if already exists"""
    try:
        result = process_single_race(race_info, year)
        if result is None:
            return None

        # Check if race exists based on Date, City, Event
        race_date = result['metadata'].get('Date')
        race_city = result['metadata'].get('City')
        race_event = result['metadata'].get('Event')

        if race_date:
            race_key = (race_date, race_city, race_event)
            if race_key in metadata['races']:
                logging.info(f"Skipping existing race: {race_key}")
                return None

        return result

    except Exception as e:
        logging.error(f"Error processing race {race_info.get('url', 'unknown')}: {e}")
        return None


def process_new_season(sex: str, metadata: Dict) -> Tuple[List[Dict], List[Dict]]:
    """Process current season's races in parallel, returns (races, unique_athletes)"""
    current_year = get_current_season_year()

    # Get current season race links
    race_links = fetch_season_race_links(current_year, sex)
    if not race_links:
        logging.warning(f"No races found for {current_year} ({sex})")
        return [], []

    # Set up thread pool for parallel processing
    max_workers = min(50, multiprocessing.cpu_count() * 4)

    all_races = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {
            executor.submit(process_race, race_info, current_year, metadata): race_info
            for race_info in race_links
        }

        for future in as_completed(future_to_link):
            result = future.result()
            if result is not None:
                all_races.append(result)

    if not all_races:
        return [], []

    # Sort by date
    all_races.sort(key=lambda x: x['metadata'].get('Date') or '9999-99-99')

    # Collect unique athletes
    athlete_map = {}
    for race in all_races:
        for result in race['results']:
            rid = result['Russian_ID']
            if rid not in athlete_map:
                athlete_map[rid] = {
                    'Russian_ID': rid,
                    'Skier': result['Skier'],
                    'Birth_Year': result['Birth_Year']
                }

    return all_races, list(athlete_map.values())


def update_experience(new_df: pl.DataFrame, exp_dict: Dict) -> pl.DataFrame:
    """Update experience values for new data based on existing experience"""
    return new_df.with_columns([
        (pl.col('ID')
         .map_elements(lambda x: exp_dict.get(x, 0), return_dtype=pl.Int32)
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

            # Concatenate
            final_df = pl.concat([old_df, new_df])

            # Remove duplicates based on key columns
            before_dedup = len(final_df)
            final_df = final_df.unique(subset=['Date', 'City', 'Event', 'Distance', 'ID', 'Place'])
            after_dedup = len(final_df)

            if before_dedup > after_dedup:
                logging.info(f"Removed {before_dedup - after_dedup} duplicate rows for {sex}")

            # Sort the final dataframe
            final_df = final_df.sort(['ID', 'Date'])

        # Save results
        base_path = Path("~/ski/elo/python/ski/polars/relay/excel365").expanduser()
        prefix = f"russia_{'men' if sex == 'M' else 'ladies'}"

        final_df.write_csv(base_path / f"{prefix}_scrape.csv")

        logging.info(f"Saved updated {sex} data with {len(final_df)} rows")

    except Exception as e:
        logging.error(f"Error saving data: {e}")
        print(f"Detailed error: {str(e)}")


def main():
    setup_logging()

    current_season = get_current_season_year()
    logging.info(f"Updating Russia results for season {current_season}")

    base_path = Path("~/ski/elo/python/ski/polars/relay/excel365").expanduser()

    for sex in ['M', 'L']:
        sex_label = 'Men' if sex == 'M' else 'Ladies'
        logging.info(f"\n{'='*50}\nProcessing {sex_label}\n{'='*50}")

        # Load existing data and metadata
        old_df, metadata = load_and_process_data(sex)

        # Process new season races
        logging.info(f"Processing current season for {sex}")
        new_races, unique_athletes = process_new_season(sex, metadata)

        if not new_races:
            logging.info(f"No new races to add for {sex}")
            continue

        # Include existing unmatched athletes for consistent ID mapping
        if old_df is not None:
            existing_ids = {a['Russian_ID'] for a in unique_athletes}
            for row in old_df.iter_rows(named=True):
                if row['ID'] < 0:
                    rid = -row['ID']
                    if rid not in existing_ids:
                        unique_athletes.append({
                            'Russian_ID': rid,
                            'Skier': row['Skier'],
                            'Birth_Year': None
                        })

        # Build ID mapping
        fis_athletes = load_fis_reference_data(sex, str(base_path))
        id_mapping, name_mapping, birthday_mapping = build_id_mapping(
            unique_athletes, fis_athletes, threshold=90
        )

        # Save ID mapping
        save_id_mapping(id_mapping, sex, str(base_path))

        # Construct DataFrame for new races
        new_df = construct_dataframe(new_races, id_mapping, name_mapping, birthday_mapping, sex)

        if new_df is not None:
            # Update experience values based on existing data
            new_df = update_experience(new_df, metadata['experience'])

            # Merge and save
            merge_and_save(old_df, new_df, sex)
        else:
            logging.info(f"No valid results for {sex}")


if __name__ == '__main__':
    main()
