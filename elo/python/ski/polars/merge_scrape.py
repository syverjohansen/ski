"""
merge_scrape.py - Merge FIS and Russian ski race results

Combines all_*_scrape.csv (FIS data) with russia_*_scrape.csv (Russian domestic data)
into combined_*_scrape.csv files.

Race numbering:
- Each season starts with Race 1
- Original race order is preserved within each source
- Races are sorted by Date, with same-date races grouped by source
"""

import polars as pl
import logging
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_csv(filepath: Path) -> Optional[pl.DataFrame]:
    """Load a CSV file if it exists."""
    if not filepath.exists():
        logging.warning(f"File not found: {filepath}")
        return None
    try:
        df = pl.read_csv(filepath)
        logging.info(f"Loaded {len(df)} rows from {filepath.name}")
        return df
    except Exception as e:
        logging.error(f"Error loading {filepath}: {e}")
        return None


def merge_dataframes(fis_df: Optional[pl.DataFrame],
                     russia_df: Optional[pl.DataFrame],
                     sex: str) -> Optional[pl.DataFrame]:
    """
    Merge FIS and Russia dataframes while preserving race order.

    Strategy:
    1. Add source identifier and keep original Race number
    2. Sort by (Season, Date, Source, Original_Race) to preserve order
    3. Reassign Race numbers sequentially within each season
    4. Recalculate Exp across combined data
    """
    if fis_df is None and russia_df is None:
        return None

    # Handle single source case
    if fis_df is None:
        df = russia_df.with_columns(pl.lit("russia").alias("_source"))
    elif russia_df is None:
        df = fis_df.with_columns(pl.lit("fis").alias("_source"))
    else:
        # Add source identifier to each dataframe
        fis_df = fis_df.with_columns(pl.lit("fis").alias("_source"))
        russia_df = russia_df.with_columns(pl.lit("russia").alias("_source"))

        # Ensure consistent column types before concat
        type_mapping = {
            'Date': pl.Date,
            'City': pl.Utf8,
            'Country': pl.Utf8,
            'Event': pl.Utf8,
            'Distance': pl.Utf8,
            'MS': pl.Int64,
            'Technique': pl.Utf8,
            'Season': pl.Int64,
            'Race': pl.Int64,
            'Sex': pl.Utf8,
            'Place': pl.Int64,
            'Skier': pl.Utf8,
            'Nation': pl.Utf8,
            'ID': pl.Int64,
            'Birthday': pl.Datetime,
            'Age': pl.Float64,
            'Exp': pl.Int64,
            '_source': pl.Utf8,
        }

        # Cast columns that exist
        for col, dtype in type_mapping.items():
            if col in fis_df.columns:
                fis_df = fis_df.with_columns(pl.col(col).cast(dtype))
            if col in russia_df.columns:
                russia_df = russia_df.with_columns(pl.col(col).cast(dtype))

        # Concatenate
        df = pl.concat([fis_df, russia_df])

    # Keep original Race number for ordering
    df = df.with_columns(pl.col("Race").alias("_original_race"))

    # Sort by Season, Date, Source (FIS first), Original Race
    # This preserves the order of races within each source on the same date
    df = df.sort(["Season", "Date", "_source", "_original_race"])

    # Reassign Race numbers within each season (starting at 1)
    # First, get unique races per season in order
    race_keys = (df
        .select(["Season", "Date", "_source", "_original_race"])
        .unique()
        .sort(["Season", "Date", "_source", "_original_race"]))

    # Assign new race numbers within each season
    race_keys = race_keys.with_columns(
        pl.col("Season").cum_count().over("Season").alias("_new_race")
    )

    # Join back to main dataframe
    df = df.join(
        race_keys,
        on=["Season", "Date", "_source", "_original_race"],
        how="left"
    )

    # Replace Race with new sequential numbers
    df = df.with_columns(pl.col("_new_race").alias("Race"))

    # Remove duplicates (same athlete, same race details)
    # Keep FIS version if duplicate exists (FIS comes first in sort)
    before_dedup = len(df)
    df = df.unique(subset=["Date", "City", "Event", "Distance", "ID", "Place"], keep="first")
    after_dedup = len(df)
    if before_dedup > after_dedup:
        logging.info(f"Removed {before_dedup - after_dedup} duplicate rows")

    # Recalculate Exp (cumulative race count per athlete)
    df = df.sort(["ID", "Season", "Race"]).with_columns(
        pl.col("ID").cum_count().over("ID").cast(pl.Int64).alias("Exp")
    )

    # Drop temporary columns
    df = df.drop(["_source", "_original_race", "_new_race"])

    # Final sort by Season, Race, Place
    df = df.sort(["Season", "Race", "Place"])

    logging.info(f"Merged dataframe has {len(df)} rows")
    return df


def main():
    logging.info("Starting merge of FIS and Russia ski results")

    base_path = Path("~/ski/elo/python/ski/polars/excel365").expanduser()

    for sex in ['M', 'L']:
        sex_label = 'Men' if sex == 'M' else 'Ladies'
        sex_name = 'men' if sex == 'M' else 'ladies'

        logging.info(f"\n{'='*50}\nMerging {sex_label}\n{'='*50}")

        # Load FIS data
        fis_path = base_path / f"all_{sex_name}_scrape.csv"
        fis_df = load_csv(fis_path)

        # Load Russia data
        russia_path = base_path / f"russia_{sex_name}_scrape.csv"
        russia_df = load_csv(russia_path)

        # Merge
        combined_df = merge_dataframes(fis_df, russia_df, sex)

        if combined_df is not None:
            # Save combined output
            output_path = base_path / f"combined_{sex_name}_scrape.csv"
            combined_df.write_csv(output_path)
            logging.info(f"Saved {len(combined_df)} rows to {output_path.name}")

            # Print summary stats
            seasons = combined_df.select("Season").unique().height
            races = combined_df.select(["Season", "Race"]).unique().height
            athletes = combined_df.select("ID").unique().height
            logging.info(f"Summary: {seasons} seasons, {races} races, {athletes} athletes")
        else:
            logging.warning(f"No data to merge for {sex_label}")

    logging.info("\nMerge complete!")


if __name__ == '__main__':
    main()
