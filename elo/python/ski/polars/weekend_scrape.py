import pandas as pd
from datetime import datetime
import os

def create_weekends_from_races(input_file="excel365/races.csv", output_file="excel365/weekends.csv"):
    """
    Create weekends.csv from races.csv by grouping cross-country races into weekends.
    
    Logic:
    1. For regular races (Championship=0): Group by city with 14-day window, assign earliest race date as weekend Date
    2. For championship races (Championship=1): Group by city AND discipline combination
    
    Args:
        input_file: Path to races.csv
        output_file: Path to output weekends.csv
    """
    
    try:
        # Read the races CSV file
        print(f"Reading cross-country races data from {input_file}...")
        df = pd.read_csv(input_file)
        print(f"Found {len(df)} races")
        
        # Step 1: Rename Date to Race_Date
        df = df.rename(columns={'Date': 'Race_Date'})
        
        # Step 2: Convert to datetime for processing
        df['Race_Date_dt'] = pd.to_datetime(df['Race_Date'], format='%m/%d/%Y')
        
        # Step 3: Create the new Date column based on weekend logic
        df['Date'] = ''
        
        # Process races based on championship status and period
        regular_mask = df['Championship'] == 0
        championship_mask = df['Championship'] == 1
        period2_mask = df['Period'] == 2
        
        print(f"Processing {regular_mask.sum()} regular races, {championship_mask.sum()} championship races, and {period2_mask.sum()} Period 2 races")
        
        # Process regular races (excluding Period 2) - group by city, event type, and time gaps  
        regular_non_period2_mask = regular_mask & ~period2_mask
        for city in df[regular_non_period2_mask]['City'].unique():
            city_races = df[(regular_non_period2_mask) & (df['City'] == city)].copy()
            
            # Group by event type within the city
            individual_races = city_races[~city_races['Distance'].isin(['Rel', 'Ts'])]
            relay_races = city_races[city_races['Distance'] == 'Rel']
            team_sprint_races = city_races[city_races['Distance'] == 'Ts']
            
            # Process individual races with 14-day grouping
            if not individual_races.empty:
                individual_races = individual_races.sort_values('Race_Date_dt')
                weekend_groups = []
                current_group = []
                
                for _, race in individual_races.iterrows():
                    if not current_group:
                        current_group = [race]
                    else:
                        last_race_date = current_group[-1]['Race_Date_dt']
                        current_race_date = race['Race_Date_dt']
                        days_diff = (current_race_date - last_race_date).days
                        
                        if days_diff <= 14:
                            current_group.append(race)
                        else:
                            weekend_groups.append(current_group)
                            current_group = [race]
                
                if current_group:
                    weekend_groups.append(current_group)
                
                # Assign weekend dates for individual race groups
                for group in weekend_groups:
                    earliest_date = min(race['Race_Date_dt'] for race in group).strftime('%m/%d/%Y')
                    for race in group:
                        df.loc[race.name, 'Date'] = earliest_date
            
            # Process relay races - group within 14 days
            if not relay_races.empty:
                relay_races = relay_races.sort_values('Race_Date_dt')
                weekend_groups = []
                current_group = []
                
                for _, race in relay_races.iterrows():
                    if not current_group:
                        current_group = [race]
                    else:
                        last_race_date = current_group[-1]['Race_Date_dt']
                        current_race_date = race['Race_Date_dt']
                        days_diff = (current_race_date - last_race_date).days
                        
                        if days_diff <= 14:
                            current_group.append(race)
                        else:
                            weekend_groups.append(current_group)
                            current_group = [race]
                
                if current_group:
                    weekend_groups.append(current_group)
                
                # Assign weekend dates for relay race groups
                for group in weekend_groups:
                    earliest_date = min(race['Race_Date_dt'] for race in group).strftime('%m/%d/%Y')
                    for race in group:
                        df.loc[race.name, 'Date'] = earliest_date
            
            # Process team sprint races - group within 14 days
            if not team_sprint_races.empty:
                team_sprint_races = team_sprint_races.sort_values('Race_Date_dt')
                weekend_groups = []
                current_group = []
                
                for _, race in team_sprint_races.iterrows():
                    if not current_group:
                        current_group = [race]
                    else:
                        last_race_date = current_group[-1]['Race_Date_dt']
                        current_race_date = race['Race_Date_dt']
                        days_diff = (current_race_date - last_race_date).days
                        
                        if days_diff <= 14:
                            current_group.append(race)
                        else:
                            weekend_groups.append(current_group)
                            current_group = [race]
                
                if current_group:
                    weekend_groups.append(current_group)
                
                # Assign weekend dates for team sprint race groups
                for group in weekend_groups:
                    earliest_date = min(race['Race_Date_dt'] for race in group).strftime('%m/%d/%Y')
                    for race in group:
                        df.loc[race.name, 'Date'] = earliest_date
        
        # Process Period 2 races - Date = Race_Date
        for idx in df[period2_mask].index:
            df.loc[idx, 'Date'] = df.loc[idx, 'Race_Date']
        
        # Process championship races (group by city, event type, AND discipline combination)
        for city in df[championship_mask]['City'].unique():
            city_champ_races = df[(championship_mask) & (df['City'] == city)]
            
            # Group by event type first, then by discipline
            individual_champ = city_champ_races[~city_champ_races['Distance'].isin(['Rel', 'Ts'])]
            relay_champ = city_champ_races[city_champ_races['Distance'] == 'Rel'] 
            team_sprint_champ = city_champ_races[city_champ_races['Distance'] == 'Ts']
            
            # Process individual championship races by distance+technique
            if not individual_champ.empty:
                individual_champ = individual_champ.copy()
                individual_champ['Technique'] = individual_champ['Technique'].fillna('N/A')
                individual_champ['Technique'] = individual_champ['Technique'].replace('', 'N/A')
                
                for (distance, technique), group in individual_champ.groupby(['Distance', 'Technique']):
                    earliest_date = group['Race_Date_dt'].min().strftime('%m/%d/%Y')
                    for idx in group.index:
                        df.loc[idx, 'Date'] = earliest_date
            
            # Process relay championship races - all relays together
            if not relay_champ.empty:
                earliest_date = relay_champ['Race_Date_dt'].min().strftime('%m/%d/%Y')
                for idx in relay_champ.index:
                    df.loc[idx, 'Date'] = earliest_date
            
            # Process team sprint championship races - all team sprints together
            if not team_sprint_champ.empty:
                earliest_date = team_sprint_champ['Race_Date_dt'].min().strftime('%m/%d/%Y')
                for idx in team_sprint_champ.index:
                    df.loc[idx, 'Date'] = earliest_date
        
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
        print(df_final.head(10)[['Date', 'Race_Date', 'City', 'Distance', 'Technique', 'Championship']].to_string(index=False))
        
        return df_final
        
    except Exception as e:
        print(f"Error creating weekends file: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("Creating cross-country weekends.csv from races.csv...")
    
    # Check if races.csv exists
    input_file = "excel365/races.csv"
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Please run race_scrape.py first.")
    else:
        result = create_weekends_from_races()
        if result is not None:
            print("Cross-country weekend creation complete!")
        else:
            print("Failed to create weekends file.")