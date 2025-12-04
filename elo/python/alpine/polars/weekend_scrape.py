import pandas as pd
from datetime import datetime
import os

def create_weekends_from_races(input_file="excel365/races.csv", output_file="excel365/weekends.csv"):
    """
    Create weekends.csv from races.csv by grouping races into weekends.
    
    Logic:
    1. For regular races (Championship=0): Group by city, assign earliest race date as weekend Date
    2. For championship races (Championship=1): Group by city AND discipline, assign race date as weekend Date
    
    Args:
        input_file: Path to races.csv
        output_file: Path to output weekends.csv
    """
    
    try:
        # Read the races CSV file
        print(f"Reading races data from {input_file}...")
        df = pd.read_csv(input_file)
        print(f"Found {len(df)} races")
        
        # Step 1: Rename Date to Race_Date
        df = df.rename(columns={'Date': 'Race_Date'})
        
        # Step 2: Convert to datetime for processing
        df['Race_Date_dt'] = pd.to_datetime(df['Race_Date'], format='%m/%d/%Y')
        
        # Step 3: Create the new Date column based on weekend logic
        df['Date'] = ''
        
        # For regular races (Championship = 0): use earliest date in each city
        regular_mask = df['Championship'] == 0
        championship_mask = df['Championship'] == 1
        
        print(f"Processing {regular_mask.sum()} regular races and {championship_mask.sum()} championship races")
        
        # Process regular races - group by city but handle time gaps
        for city in df[regular_mask]['City'].unique():
            city_races = df[(regular_mask) & (df['City'] == city)].copy()
            city_races = city_races.sort_values('Race_Date_dt')
            
            # Group races that are within 14 days of each other
            weekend_groups = []
            current_group = []
            
            for _, race in city_races.iterrows():
                if not current_group:
                    current_group = [race]
                else:
                    # Check if this race is within 14 days of the last race in current group
                    last_race_date = current_group[-1]['Race_Date_dt']
                    current_race_date = race['Race_Date_dt']
                    days_diff = (current_race_date - last_race_date).days
                    
                    if days_diff <= 14:  # Same weekend/period
                        current_group.append(race)
                    else:  # Start a new weekend group
                        weekend_groups.append(current_group)
                        current_group = [race]
            
            # Add the last group
            if current_group:
                weekend_groups.append(current_group)
            
            # Assign weekend dates for each group
            for group in weekend_groups:
                earliest_date = min(race['Race_Date_dt'] for race in group).strftime('%m/%d/%Y')
                for race in group:
                    df.loc[race.name, 'Date'] = earliest_date
        
        # Process championship races (group by city AND discipline)
        for city in df[championship_mask]['City'].unique():
            city_champ_races = df[(championship_mask) & (df['City'] == city)]
            for discipline in city_champ_races['Distance'].unique():
                discipline_races = city_champ_races[city_champ_races['Distance'] == discipline]
                earliest_date = discipline_races['Race_Date_dt'].min().strftime('%m/%d/%Y')
                df.loc[(championship_mask) & (df['City'] == city) & (df['Distance'] == discipline), 'Date'] = earliest_date
        
        # Step 4: Reorder columns - Date first, then Race_Date, then everything else
        other_cols = [col for col in df.columns if col not in ['Date', 'Race_Date', 'Race_Date_dt']]
        df_final = df[['Date', 'Race_Date'] + other_cols]
        
        # Step 5: Sort by Date, then Race_Date (using datetime for proper sorting)
        df_final['Date_dt'] = pd.to_datetime(df_final['Date'], format='%m/%d/%Y')
        df_final['Race_Date_dt_sort'] = pd.to_datetime(df_final['Race_Date'], format='%m/%d/%Y')
        df_final = df_final.sort_values(['Date_dt', 'Race_Date_dt_sort'])
        
        # Remove the temporary sorting columns
        df_final = df_final.drop(['Date_dt', 'Race_Date_dt_sort'], axis=1)
        
        # Save to output file
        df_final.to_csv(output_file, index=False)
        print(f"Saved {len(df_final)} races with weekend dates to {output_file}")
        
        # Print summary
        weekend_count = len(df_final['Date'].unique())
        print(f"Created {weekend_count} unique weekends")
        
        # Show sample of the output
        print("\nFirst 10 rows of output:")
        print(df_final.head(10)[['Date', 'Race_Date', 'City', 'Distance', 'Championship']].to_string(index=False))
        
        return df_final
        
    except Exception as e:
        print(f"Error creating weekends file: {e}")
        return None

if __name__ == "__main__":
    print("Creating weekends.csv from races.csv...")
    
    # Check if races.csv exists
    input_file = "excel365/races.csv"
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Please run race_scrape.py first.")
    else:
        result = create_weekends_from_races()
        if result is not None:
            print("Weekend creation complete!")
        else:
            print("Failed to create weekends file.")