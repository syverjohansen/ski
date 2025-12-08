import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def merge_elevation_data(chrono_df, elevation_df):
    """
    Merge elevation data with chronological results
    
    Parameters:
    chrono_df: DataFrame containing chronological results
    elevation_df: DataFrame containing city elevation data
    
    Returns:
    DataFrame with added elevation column
    """
    # Create copies to avoid warning
    chrono_df = chrono_df.copy()
    elevation_df = elevation_df.copy()
    
    # Standardize city names (trim whitespace, convert to title case)
    chrono_df['City'] = chrono_df['City'].str.strip().str.title()
    elevation_df['City'] = elevation_df['City'].str.strip().str.title()
    
    # Create a mapping dict from city to elevation
    city_to_elevation = dict(zip(elevation_df['City'], elevation_df['Elevation']))
    
    # Log the number of unique cities in each dataset
    chrono_cities = set(chrono_df['City'].dropna().unique())
    elevation_cities = set(elevation_df['City'].unique())
    
    logging.info(f"Number of unique cities in chronological data: {len(chrono_cities)}")
    logging.info(f"Number of unique cities in elevation data: {len(elevation_cities)}")
    
    # Find cities that are in chrono data but not in elevation data
    missing_cities = chrono_cities - elevation_cities
    if missing_cities:
        logging.warning(f"Cities in chronological data without elevation info: {len(missing_cities)}")
        logging.warning(f"First 10 missing cities: {list(missing_cities)[:10]}")
    
    # Add elevation column using the mapping, with default of 0 for missing values
    chrono_df['Elevation'] = chrono_df['City'].map(city_to_elevation).fillna(0)
    
    # Log the number of rows where default elevation was applied
    default_elevation_count = (chrono_df['Elevation'] == 0).sum()
    total_rows = len(chrono_df)
    
    logging.info(f"Rows with default elevation (0): {default_elevation_count} ({default_elevation_count/total_rows*100:.2f}%)")
    
    return chrono_df

def main():
    # Define paths
    base_dir = os.path.expanduser('~/ski/elo/python/biathlon/polars')
    excel365_dir = os.path.join(base_dir, 'excel365')
    
    # Input files
    mens_chrono = os.path.join(base_dir, 'excel365', 'men_chrono.csv')
    ladies_chrono = os.path.join(base_dir, 'excel365', 'ladies_chrono.csv')
    elevation_csv = os.path.join(base_dir, 'excel365', 'elevation.csv')
    
    # Output files
    mens_output = os.path.join(excel365_dir, 'men_chrono_elevation.csv')
    ladies_output = os.path.join(excel365_dir, 'ladies_chrono_elevation.csv')
    
    # Read elevation data
    try:
        logging.info("Reading elevation data")
        elevation_df = pd.read_csv(elevation_csv)
        logging.info(f"Elevation data shape: {elevation_df.shape}")
    except Exception as e:
        logging.error(f"Error reading elevation data: {str(e)}")
        return
    
    # Process men's data
    try:
        logging.info("Processing men's data")
        mens_df = pd.read_csv(mens_chrono)
        logging.info(f"Men's chronological data shape: {mens_df.shape}")
        
        mens_with_elevation = merge_elevation_data(mens_df, elevation_df)
        
        # Save to CSV
        mens_with_elevation.to_csv(mens_output, index=False)
        logging.info(f"Men's data with elevation saved to {mens_output}")
        
    except Exception as e:
        logging.error(f"Error processing men's data: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    
    # Process women's data
    try:
        logging.info("Processing women's data")
        ladies_df = pd.read_csv(ladies_chrono)
        logging.info(f"Women's chronological data shape: {ladies_df.shape}")
        
        ladies_with_elevation = merge_elevation_data(ladies_df, elevation_df)
        
        # Save to CSV
        ladies_with_elevation.to_csv(ladies_output, index=False)
        logging.info(f"Women's data with elevation saved to {ladies_output}")
        
    except Exception as e:
        logging.error(f"Error processing women's data: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    
    logging.info("Processing complete!")

if __name__ == "__main__":
    main()