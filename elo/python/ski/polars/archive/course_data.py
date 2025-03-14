import pandas as pd
import logging
import os

# Set up logging
logging.basicConfig(
    filename='race_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

def process_race_data(main_file, race_data_file, gender):
    """
    Process race data from main and race_data files to create a unified dataset of unique races.
    
    Parameters:
    main_file (str): Path to the M.feather or L.feather file
    race_data_file (str): Path to the race_data_M or race_data_W file
    gender (str): 'M' or 'W' to indicate which gender's data is being processed
    
    Returns:
    pd.DataFrame: Processed race data with additional columns from race_data file
    """
    try:
        # Read the main file
        main_df = pd.read_feather(main_file)
        
        # Filter for races from 2002 onwards
        main_df = main_df[main_df['Season'] >= 2002]
        
        # Get unique races using multiple columns to properly identify unique races
        unique_races = main_df.drop_duplicates(
            subset=['Date', 'Distance', 'Technique', 'Race']
        )[
            ['Race', 'Date', 'City', 'Country', 'Distance', 'Technique']
        ]
        
        # Convert the yyyy-mm-dd date to yyyymmdd format to match race_data
        unique_races['date'] = pd.to_datetime(unique_races['Date']).dt.date.astype(str).str.replace('-', '')
        unique_races = unique_races.drop('Date', axis=1)  # Remove the original Date column
        
        # Read the race data file
        race_data = pd.read_feather(race_data_file)
        
        # Columns to get from race_data
        race_data_cols = [
            'date',
            'height_difference',
            'maximum_climb',
            'total_climb',
            'lap_length',
            'number_of_laps',
            'total_race_climb'
        ]
        
        # Select only needed columns from race_data
        race_data = race_data[race_data_cols]
        
        # Convert race_data date to string if it's not already
        race_data['date'] = race_data['date'].astype(str)
        
        # Merge the dataframes on date
        final_df = pd.merge(
            unique_races,
            race_data,
            on='date',
            how='left'
        )
        
        # Calculate Average Grade
        # First multiply lap_length by number_of_laps to get total distance in meters
        final_df['total_distance_m'] = final_df['lap_length'] * final_df['number_of_laps']
        
        # Calculate Average Grade (as percentage)
        final_df['Average Grade'] = (final_df['total_race_climb'] / final_df['total_distance_m'] * 100).round(2)
        
        # Calculate Difficulty Score
        # Formula: (lap_length * number_of_laps / 1000) × (1 + total_race_climb / 1000)
        final_df['Difficulty Score'] = (
            (final_df['lap_length'] * final_df['number_of_laps'] / 1000) * 
            (1 + final_df['total_race_climb'] / 1000)
        ).round(2)
        
        # Calculate Grade Adjusted Distance
        final_df['Grade Adjusted'] = final_df['Distance'] + (final_df['total_race_climb'] / 1000)
        
        # Add Actual Distance column (in meters converted to km)
        final_df['Actual Distance'] = final_df['total_distance_m'] / 1000
        
        # Drop the temporary total_distance_m column
        final_df = final_df.drop('total_distance_m', axis=1)
        
        # Log missing dates
        missing_dates = final_df[final_df['height_difference'].isna()]['date']
        if len(missing_dates) > 0:
            logging.info(f"\nMissing race data for {gender} races on dates:")
            for date in missing_dates:
                logging.info(f"- {date}")
        
        # Log races per date to check for multiple races
        races_per_date = final_df.groupby('date').size()
        multiple_races = races_per_date[races_per_date > 1]
        
        if len(multiple_races) > 0:
            logging.info(f"\nDates with multiple {gender} races:")
            for date, count in multiple_races.items():
                logging.info(f"- {date}: {count} races")
                races_on_date = final_df[final_df['date'] == date]
                for _, race in races_on_date.iterrows():
                    logging.info(f"  * {race['Race']}: {race['Distance']}km {race['Technique']}")
        
        # Sort by date and other relevant columns
        final_df = final_df.sort_values(['date', 'Distance', 'Technique'])
        
        return final_df
    
    except Exception as e:
        logging.error(f"Error processing {gender} data: {str(e)}")
        raise


def main():
    # Define the data directory
    data_dir = os.path.expanduser('~/ski/elo/python/elo/polars/excel365')
    
    # Define file paths
    m_file = os.path.join(data_dir, 'M.feather')
    l_file = os.path.join(data_dir, 'L.feather')
    race_data_m = os.path.join(data_dir, 'race_data_M.feather')
    race_data_w = os.path.join(data_dir, 'race_data_W.feather')
    
    # Rest of the function remains the same
    
    # Process men's data
    men_races = process_race_data(
        m_file,
        race_data_m,
        'M'
    )
    
    # Process women's data
    women_races = process_race_data(
        l_file,
        race_data_w,
        'W'
    )
    
    # Create output directory if it doesn't exist
    output_dir = os.path.join(data_dir, 'processed')
    os.makedirs(output_dir, exist_ok=True)
    
    # Save results
    men_races.to_csv(os.path.join(output_dir, 'unique_mens_races.csv'), index=False)
    women_races.to_csv(os.path.join(output_dir, 'unique_womens_races.csv'), index=False)
    
    print(f"Processing complete. Results saved in {output_dir}")
    print("Check race_processing.log for any missing dates and multiple races per date")

if __name__ == "__main__":
    main()