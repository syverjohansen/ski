import pandas as pd
import numpy as np
import os

def standardize_date(date_series):
    """
    Convert dates to YYYYMMDD format as integers
    Handles both string dates ('YYYY-MM-DD') and integer dates (YYYYMMDD)
    """
    try:
        # Convert to string first to handle any format
        date_series = date_series.astype(str)
        
        # Remove any hyphens or other separators
        date_series = date_series.str.replace('-', '')
        date_series = date_series.str.replace('/', '')
        
        # Convert to integer, keeping NA values as NA
        return pd.to_numeric(date_series, errors='coerce')
        
    except Exception as e:
        print(f"Error converting dates: {e}")
        return date_series

def merge_race_data(race_df, chrono_df, gender):
    """
    Merge race data with chronological results
    
    Parameters:
    race_df: DataFrame containing race information (from CSV)
    chrono_df: DataFrame containing individual results (from feather)
    gender: 'M' or 'W' for logging purposes
    
    Returns:
    Merged DataFrame
    """
    # Create copies of the dataframes to avoid warnings
    race_df = race_df.copy()
    chrono_df = chrono_df.copy()
    
    print(f"\nProcessing {gender}'s data:")
    print(f"Race data shape: {race_df.shape}")
    print(f"Chrono data shape: {chrono_df.shape}")
    
    # Check column names
    print("\nRace data columns:", race_df.columns.tolist())
    print("Chrono data columns:", chrono_df.columns.tolist())
    
    # Standardize the Date column in both dataframes
    print("\nStandardizing date formats...")
    
    # Convert dates in both dataframes
    race_df['Date'] = standardize_date(race_df['Date'])
    chrono_df['Date'] = standardize_date(chrono_df['Date'])
    
    # Remove rows with NA dates before conversion to int64
    race_df = race_df.dropna(subset=['Date'])
    chrono_df = chrono_df.dropna(subset=['Date'])
    
    # Ensure both Date columns are int64
    race_df['Date'] = race_df['Date'].astype('int64')
    chrono_df['Date'] = chrono_df['Date'].astype('int64')
    
    print(f"After date standardization:")
    print(f"Race data shape: {race_df.shape}")
    print(f"Chrono data shape: {chrono_df.shape}")
    
    # Select only the needed columns from race_df
    selected_columns = [
        'Race', 'Date', 'total_race_climb', 'Actual Distance', 
        'Average Grade', 'Difficulty Score', 'Grade Adjusted', 'Altitude'
    ]
    race_columns = [col for col in selected_columns if col in race_df.columns]
    race_df = race_df[race_columns]
    
    print(f"Selected columns from race data: {race_columns}")
    
    # Merge the dataframes
    merged_df = pd.merge(
        chrono_df,
        race_df,
        on=['Race', 'Date'],
        how='left'
    )
    
    # Print merge results
    print(f"\nMerge results:")
    print(f"Total rows in merged dataset: {len(merged_df)}")
    print(f"Rows with missing race data: {merged_df['total_race_climb'].isna().sum() if 'total_race_climb' in merged_df.columns else 'N/A'}")
    
    return merged_df

def main():
    # Define paths
    current_dir = os.path.expanduser('~/ski/elo/python/elo/polars')
    excel365_dir = os.path.join(current_dir, 'excel365')
    
    # Process men's data
    mens_csv = os.path.join(excel365_dir, 'processed/unique_mens_races.csv')
    mens_chrono = os.path.join(excel365_dir, 'men_chrono.feather')
    
    # Process women's data
    womens_csv = os.path.join(excel365_dir, 'processed/unique_womens_races.csv')
    womens_chrono = os.path.join(excel365_dir, 'ladies_chrono.feather')
    
    # Rest of the function remains the same
    
    # Process men's data
    try:
        print("\n=== Processing men's data ===")
        mens_race_df = pd.read_csv(mens_csv)
        mens_chrono_df = pd.read_feather(mens_chrono)
        
        mens_merged = merge_race_data(mens_race_df, mens_chrono_df, 'Men')
        
        # Save as both feather and csv
        mens_merged.to_feather(os.path.join(excel365_dir, 'mens_merged.feather'))
        mens_merged.to_csv(os.path.join(excel365_dir, 'mens_merged.csv'), index=False)
        
        print(f"Men's merged data shape: {mens_merged.shape}")
        print("Men's data saved as both .feather and .csv")
        
    except Exception as e:
        print(f"Error processing men's data: {str(e)}")
        import traceback
        print(traceback.format_exc())
    
    # Process women's data
    try:
        print("\n=== Processing women's data ===")
        womens_race_df = pd.read_csv(womens_csv)
        womens_chrono_df = pd.read_feather(womens_chrono)
        
        womens_merged = merge_race_data(womens_race_df, womens_chrono_df, 'Women')
        
        # Save as both feather and csv
        womens_merged.to_feather(os.path.join(excel365_dir, 'womens_merged.feather'))
        womens_merged.to_csv(os.path.join(excel365_dir, 'womens_merged.csv'), index=False)
        
        print(f"Women's merged data shape: {womens_merged.shape}")
        print("Women's data saved as both .feather and .csv")
        
    except Exception as e:
        print(f"Error processing women's data: {str(e)}")
        import traceback
        print(traceback.format_exc())
    
    print("\nProcessing complete!")

if __name__ == "__main__":
    main()