#!/usr/bin/env python3
import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional
import warnings
from datetime import datetime
import traceback
import subprocess
warnings.filterwarnings('ignore')

# Import common utility functions
from startlist_common import *

# Import config for nation quotas
from config import get_nation_quota, get_additional_skiers


# Add this function to each main script file to call the appropriate R script
def call_r_script(script_type: str, race_type: str = None, gender: str = None) -> None:
    """
    Call the appropriate R script after processing race data
    
    Args:
        script_type: 'weekend' or 'races' (determines weekly picks or race picks)
        race_type: 'standard', 'team_sprint', 'relay', or 'mixed_relay'
        gender: 'men', 'ladies', or None for mixed events
    """
    import subprocess
    import os
    
    # Set the base path to the R scripts
    r_script_base_path = "~/blog/daehl-e/content/post/cross-country/drafts"
    
    # Determine which R script to call based on script type and race type
    if script_type == 'weekend':
        # Weekly picks scripts
        if race_type == 'standard':
            r_script = "weekly-picks2.R"
        elif race_type == 'team_sprint':
            r_script = "weekly-picks-team-sprint.R"
        elif race_type == 'relay':
            r_script = "weekly-picks-relay.R"
        elif race_type == 'mixed_relay':
            r_script = "weekly-picks-mixed-relay.R"
        else:
            print(f"Unknown race type: {race_type}")
            return
    elif script_type == 'races':
        # Race picks scripts
        if race_type == 'standard':
            r_script = "race-picks.R"
        elif race_type == 'team_sprint':
            r_script = "race-picks-team-sprint.R"
        elif race_type == 'relay':
            r_script = "race-picks-relay.R"
        elif race_type == 'mixed_relay':
            r_script = "race-picks-mixed-relay.R"
        else:
            print(f"Unknown race type: {race_type}")
            return
    else:
        print(f"Unknown script type: {script_type}")
        return
    
    # Full path to the R script
    r_script_path = os.path.expanduser(f"{r_script_base_path}/{r_script}")
    
    # Command to execute the R script
    command = ["Rscript", r_script_path]
    
    # Add gender parameter if specified
    if gender:
        command.append(gender)
    
    print(f"Calling R script: {' '.join(command)}")
    
    try:
        # Call the R script
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"R script output:\n{result.stdout}")
        if result.stderr:
            print(f"R script error:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Error calling R script: {e}")
        print(f"Script output: {e.stdout}")
        print(f"Script error: {e.stderr}")
    except FileNotFoundError:
        print(f"R script not found: {r_script_path}")

def is_relay_event(race: pd.Series) -> bool:
    """
    Determine if a race is a relay event
    Relay types: 'Rel' (standard relay), 'Ts' (team sprint), 'Mix' (mixed relay)
    """
    distance = str(race['Distance']).strip() if 'Distance' in race else ""
    return distance in ['Rel', 'Ts']

def get_relay_type(race: pd.Series) -> str:
    """Return the type of relay based on race data"""
    distance = str(race['Distance']).strip() if 'Distance' in race else ""
    sex = str(race['Sex']).strip() if 'Sex' in race else ""
    
    if distance == 'Rel' and sex != "Mixed":
        return 'relay'
    elif distance == 'Ts' and sex != "Mixed":
        return 'team_sprint'
    elif distance == 'Rel' and sex == "Mixed":
        return 'mixed_relay'
    elif distance == 'Ts' and sex == "Mixed":
        return 'mixed_team_sprint'
    else:
        return 'unknown'

def handle_relay_races(races_df: pd.DataFrame) -> bool:
    """
    Handle relay races by calling the appropriate relay script
    Returns True if relay races were handled, False otherwise
    """
    if races_df.empty:
        return False
    
    # Check if all races are relay events
    all_relays = all(is_relay_event(race) for _, race in races_df.iterrows())
    
    if not all_relays:
        # If any non-relay races exist, let the main script handle it
        return False
    
    # Get unique relay types present in the dataframe
    relay_types = set(get_relay_type(race) for _, race in races_df.iterrows())
    
    # Log the relay types detected
    print(f"Relay events detected: {relay_types}")
    
    # Call the appropriate relay script for each type
    success = False
    for relay_type in relay_types:
        if relay_type != 'unknown':
            relay_script_path = f"relay/startlist_scrape_weekend_{relay_type}.py"
            
            # Filter races for this relay type
            relay_distance = {
                'relay': 'Rel', 
                'team_sprint': 'Ts', 
                'mixed_relay': 'Rel',
                'mixed_team_sprint': 'Ts'
            }[relay_type]
            
            # Additional filtering for mixed events
            if relay_type in ['mixed_relay', 'mixed_team_sprint']:
                relay_specific_races = races_df[(races_df['Distance'] == relay_distance) & 
                                              (races_df['Sex'] == 'Mixed')]
            else:
                relay_specific_races = races_df[(races_df['Distance'] == relay_distance) & 
                                              (races_df['Sex'] != 'Mixed')]
            
            # Save to temporary CSV for the relay script to use
            temp_csv_path = f"/tmp/weekend_relay_{relay_type}_races.csv"
            relay_specific_races.to_csv(temp_csv_path, index=False)
            
            print(f"Calling relay script for {relay_type}: {relay_script_path}")
            try:
                # Call the relay script with the temporary CSV as an argument
                result = subprocess.run(
                    ["python3", relay_script_path, temp_csv_path], 
                    check=True, 
                    capture_output=True, 
                    text=True
                )
                print(f"Relay script output:\n{result.stdout}")
                if result.stderr:
                    print(f"Relay script error:\n{result.stderr}")
                
                success = True
            except subprocess.CalledProcessError as e:
                print(f"Error calling relay script: {e}")
                print(f"Script output: {e.stdout}")
                print(f"Script error: {e.stderr}")
            except FileNotFoundError:
                print(f"Relay script not found: {relay_script_path}")
                
    return success

def process_weekend_races() -> None:
    """Main function to process weekend races"""
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/ski/polars/excel365/weekends.csv')
        print(f"Successfully read weekends.csv with {len(weekends_df)} rows")
    except Exception as e:
        print(f"Error reading weekends.csv: {e}")
        traceback.print_exc()
        return
    
    # Find next race date
    next_date = find_next_race_date(weekends_df)
    print(f"Next race date is {next_date}")
    
    # Filter races for the next date
    next_races = filter_races_by_date(weekends_df, next_date)
    print(f"Found {len(next_races)} races on {next_date}")
    
    # Check if all races are relay events
    if all(is_relay_event(race) for _, race in next_races.iterrows()):
        print("All races for next date are relay events.")
        if handle_relay_races(next_races):
            print("Relay races successfully handled by relay scripts.")
            return
        else:
            print("Failed to handle relay races with relay scripts, continuing with standard processing...")
    
    # Filter out relays and team sprints for normal processing
    valid_races = next_races[(next_races['Distance'] != 'Rel') & 
                             (next_races['Distance'] != 'Ts')] 
    print(f"Found {len(valid_races)} individual races after filtering out relays and team sprints")
    
    if valid_races.empty:
        print("No valid individual races found for the next date")
        return
    
    # Sort races by Race_Date (earliest first)
    try:
        # Convert Race_Date to datetime for proper sorting
        valid_races['Race_Date'] = pd.to_datetime(valid_races['Race_Date'])
        sorted_races = valid_races.sort_values('Race_Date')
        print(f"Sorted races by Race_Date, first race date: {sorted_races['Race_Date'].iloc[0]}")
        
        # Determine host nation from the Country column
        host_nation = sorted_races.iloc[0]['Country']
        print(f"Host nation for this weekend: {host_nation}")
        
        # Select up to two races to scrape (prioritizing earliest race dates)
        races_to_process = sorted_races.head(2)
        print(f"Processing {len(races_to_process)} races with earliest dates")
        
    except Exception as e:
        print(f"Error sorting races: {e}")
        traceback.print_exc()
        return
    
    # Get gender mapping for internal reference
    gender_mapping = {'M': 'men', 'L': 'ladies'}
    
    # Process each gender separately
    men_races = races_to_process[races_to_process['Sex'] == 'M']
    ladies_races = races_to_process[races_to_process['Sex'] == 'L']
    
    print(f"Found {len(men_races)} men's races and {len(ladies_races)} ladies' races to process")
    
    # Process men's races
    if not men_races.empty:
        try:
            process_gender_races(men_races, gender_mapping['M'], host_nation, next_races)
        except Exception as e:
            print(f"Error processing men's races: {e}")
            traceback.print_exc()

    # Process ladies' races
    if not ladies_races.empty:
        try:
            process_gender_races(ladies_races, gender_mapping['L'], host_nation, next_races)
        except Exception as e:
            print(f"Error processing ladies' races: {e}")
            traceback.print_exc()
    call_r_script('weekend', 'standard')

def process_gender_races(races_df: pd.DataFrame, gender: str, host_nation: str, all_next_races: pd.DataFrame) -> None:
    """Process races for a specific gender"""
    print(f"\nProcessing {gender} races...")
    
    # Get total number of races for this gender on the next date
    # This counts ALL races for the gender, not just the ones we'll process
    total_gender_races = len(all_next_races[all_next_races['Sex'] == ('M' if gender == 'men' else 'L')])
    
    # Set the column names based on total number of races for this gender
    if total_gender_races == 1:
        prob_columns = ['Race1_Prob', None]
    else:
        # Create enough probability columns for all races, even if we only process 2
        prob_columns = [f'Race{i+1}_Prob' for i in range(total_gender_races)]
        
        # Limit to the races we're actually processing
        prob_columns = prob_columns[:len(races_df)]
    
    # Get the ELO path
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_elevation.csv"
    
    # Initialize a consolidated dataframe
    consolidated_df = None
    
    # Process each race
    for i, (_, race) in enumerate(races_df.iterrows()):
        url = race['Startlist']
        
        if not url or pd.isna(url):
            print(f"No startlist URL for race {i+1}, skipping")
            continue
        
        print(f"Processing race {i+1} from URL: {url}")
        
        # Create startlist for this race
        race_df = create_weekend_startlist(
            fis_url=url,
            elo_path=elo_path,
            gender=gender,
            host_nation=host_nation,
            prob_column=prob_columns[i]
        )
        
        if race_df is None:
            print(f"Failed to create startlist for race {i+1}")
            continue
        
        # If this is the first race, initialize the consolidated dataframe
        if consolidated_df is None:
            consolidated_df = race_df
        else:
            # Merge with existing data (ensuring we don't lose any athletes)
            consolidated_df = merge_race_dataframes(consolidated_df, race_df, prob_columns[i])
    
    # If we successfully processed at least one race
    if consolidated_df is not None:
        # Save the consolidated dataframe
        output_path = f"~/ski/elo/python/ski/polars/excel365/startlist_weekend_{gender}.csv"
        print(f"Saving consolidated {gender} startlist to {output_path}")
        consolidated_df.to_csv(output_path, index=False)
        print(f"Saved {len(consolidated_df)} {gender} athletes")
        #call_r_script('weekend', 'standard', gender)

def create_weekend_startlist(fis_url: str, elo_path: str, gender: str, 
                           host_nation: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with startlist, prices and ELO scores for weekend races"""
    try:
        # Get data from all sources
        fis_athletes = get_fis_startlist(fis_url)
        print(f"Found {len(fis_athletes)} athletes in FIS startlist")
        fantasy_prices = get_fantasy_prices()
        
        if not fis_athletes:
            print("FIS startlist is empty, using fallback logic with 2025 season skiers")
            return create_fallback_startlist(elo_path, gender, host_nation, prob_column, fantasy_prices)
        

        elo_scores = get_latest_elo_scores(elo_path)
        
        # Get list of nations in config
        ADDITIONAL_SKIERS = get_additional_skiers(gender)
        config_nations = list(ADDITIONAL_SKIERS.keys())
        
        # Create data for DataFrame
        data = []
        processed_names = set()
        
        # Process FIS athletes first
        for fis_name, fis_nation_code in fis_athletes:
            print(f"\nProcessing FIS athlete: {fis_name}")
            
            # STEP 1: Name Processing
            # Check manual mappings first
            if fis_name in MANUAL_NAME_MAPPINGS:
                processed_name = MANUAL_NAME_MAPPINGS[fis_name]
                print(f"Found direct manual mapping: {fis_name} -> {processed_name}")
            else:
                # Convert to First Last format
                first_last = convert_to_first_last(fis_name)
                if first_last in MANUAL_NAME_MAPPINGS:
                    processed_name = MANUAL_NAME_MAPPINGS[first_last]
                    print(f"Found manual mapping after conversion: {first_last} -> {processed_name}")
                else:
                    processed_name = first_last
                    print(f"Using converted name: {processed_name}")
            
            if processed_name in processed_names:
                print(f"Skipping {processed_name} - already processed")
                continue
            
            # STEP 2: Get Fantasy Price
            # Try with original FIS name first
            price = get_fantasy_price(fis_name, fantasy_prices)
            if price == 0:
                # If no price found, try with processed name
                price = get_fantasy_price(processed_name, fantasy_prices)
            
            # STEP 3: Match with ELO scores
            # Try exact match first
            elo_match = None
            if processed_name in elo_scores['Skier'].values:
                elo_match = processed_name
                print(f"Found exact ELO match for: {processed_name}")
            else:
                # Try fuzzy matching if no exact match
                elo_match = fuzzy_match_name(processed_name, elo_scores['Skier'].tolist())
                if elo_match:
                    print(f"Found fuzzy ELO match: {processed_name} -> {elo_match}")
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                original_name = elo_match
                skier_id = elo_data['ID']
                nation = elo_data['Nation']
            else:
                print(f"No ELO match found for: {processed_name}")
                elo_data = {}
                original_name = processed_name
                skier_id = None
                nation = fis_nation_code
            
            # Calculate nation quota
            base_quota = get_nation_quota(nation, gender)
            is_host = (nation == host_nation)
            total_quota = get_nation_quota(nation, gender, is_host=is_host)
            
            row_data = {
                'FIS_Name': fis_name,
                'Skier': original_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': True,
                'Config_Nation': nation in config_nations,
                'In_Config': False,
                'Price': price,
                'Elo': elo_data.get('Elo', None),
                'Distance_Elo': elo_data.get('Distance_Elo', None),
                'Distance_C_Elo': elo_data.get('Distance_C_Elo', None),
                'Distance_F_Elo': elo_data.get('Distance_F_Elo', None),
                'Sprint_Elo': elo_data.get('Sprint_Elo', None),
                'Sprint_C_Elo': elo_data.get('Sprint_C_Elo', None),
                'Sprint_F_Elo': elo_data.get('Sprint_F_Elo', None),
                'Classic_Elo': elo_data.get('Classic_Elo', None),
                'Freestyle_Elo': elo_data.get('Freestyle_Elo', None),
                'Base_Quota': base_quota,
                'Is_Host_Nation': is_host,
                'Quota': total_quota
            }
            
            # Set race probability
            if prob_column:
                row_data[prob_column] = 1.0  # In FIS list = 100% for this race
            
            data.append(row_data)
            processed_names.add(original_name)
        
        # Process additional skiers from config
        for nation, skiers in ADDITIONAL_SKIERS.items():
            print(f"\nProcessing additional skiers for {nation}")
            for skier_entry in skiers:
                name = skier_entry if isinstance(skier_entry, str) else skier_entry['name']
                
                print(f"\nProcessing additional skier: {name}")
                
                if name in processed_names:
                    print(f"Skipping {name} - already processed")
                    continue
                
                # Check manual mappings for config skiers too
                if name in MANUAL_NAME_MAPPINGS:
                    processed_name = MANUAL_NAME_MAPPINGS[name]
                    print(f"Found manual mapping for config skier: {name} -> {processed_name}")
                else:
                    processed_name = name
                
                price = get_fantasy_price(processed_name, fantasy_prices)
                print(f"Final price for {processed_name}: {price}")
                
                # Match with ELO scores using processed name
                elo_match = None
                if processed_name in elo_scores['Skier'].values:
                    elo_match = processed_name
                else:
                    elo_match = fuzzy_match_name(processed_name, elo_scores['Skier'].tolist())
                
                if elo_match:
                    elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                    original_name = elo_match
                    skier_id = elo_data['ID']
                    skier_nation = elo_data['Nation']
                else:
                    elo_data = {}
                    original_name = processed_name
                    skier_id = None
                    skier_nation = nation
                
                # Calculate nation quota
                base_quota = get_nation_quota(skier_nation, gender)
                is_host = (skier_nation == host_nation)
                total_quota = get_nation_quota(skier_nation, gender, is_host=is_host)
                
                row_data = {
                    'FIS_Name': name,
                    'Skier': original_name,
                    'ID': skier_id,
                    'Nation': skier_nation,
                    'In_FIS_List': False,
                    'Config_Nation': skier_nation in config_nations,
                    'In_Config': True,
                    'Price': price,
                    'Elo': elo_data.get('Elo', None),
                    'Distance_Elo': elo_data.get('Distance_Elo', None),
                    'Distance_C_Elo': elo_data.get('Distance_C_Elo', None),
                    'Distance_F_Elo': elo_data.get('Distance_F_Elo', None),
                    'Sprint_Elo': elo_data.get('Sprint_Elo', None),
                    'Sprint_C_Elo': elo_data.get('Sprint_C_Elo', None),
                    'Sprint_F_Elo': elo_data.get('Sprint_F_Elo', None),
                    'Classic_Elo': elo_data.get('Classic_Elo', None),
                    'Freestyle_Elo': elo_data.get('Freestyle_Elo', None),
                    'Base_Quota': base_quota,
                    'Is_Host_Nation': is_host,
                    'Quota': total_quota
                }
                
                # Set race probability
                if prob_column:
                    row_data[prob_column] = 0.0  # Not in FIS list = 0% for this race
                
                data.append(row_data)
                processed_names.add(original_name)
        
        # Add processing for additional skiers from chronos data
        try:
            # Get chronos data for finding additional national skiers
            # Use more robust options for reading the CSV
            try:
                chronos = pl.read_csv(
                    elo_path,
                    infer_schema_length=10000,  # Increase schema inference length
                    ignore_errors=True,  # Ignore parsing errors
                    null_values=["", "NA", "NULL", "Sprint"]  # Add "Sprint" to null values
                ).to_pandas()
            except Exception as csv_err:
                print(f"Error with polars: {csv_err}")
                # Fallback to pandas if polars fails
                chronos = pd.read_csv(elo_path, low_memory=False)
            
            # Get all nations from 2025 season (the max year)
            all_2025_nations = set(chronos[chronos['Season'] == 2025]['Nation'].unique())
            
            # Find nations that aren't in config
            non_config_nations = {nation for nation in all_2025_nations if nation not in config_nations}
            
            print(f"Found {len(non_config_nations)} non-config nations from 2025 season")
            
            # Process each non-config nation
            for nation in non_config_nations:
                print(f"Processing all 2025 skiers for non-config nation: {nation}")
                
                # Get current skiers for this nation (if any)
                current_skiers = {row['Skier'] for row in data if row['Nation'] == nation}
                
                # Get all skiers from this nation who competed in 2025
                nation_skiers = chronos[(chronos['Nation'] == nation) & 
                                      (chronos['Season'] == 2025)]['Skier'].unique()
                
                print(f"Found {len(nation_skiers)} skiers from {nation} who competed in 2025")
                
                # Process each skier from this nation
                for skier_name in nation_skiers:
                    if skier_name in current_skiers:
                        print(f"Skipping {skier_name} - already processed")
                        continue
                    
                    # Match with ELO scores
                    elo_match = None
                    if skier_name in elo_scores['Skier'].values:
                        elo_match = skier_name
                        print(f"Found exact ELO match for: {skier_name}")
                    else:
                        # Try fuzzy matching if no exact match
                        elo_match = fuzzy_match_name(skier_name, elo_scores['Skier'].tolist())
                        if elo_match:
                            print(f"Found fuzzy ELO match: {skier_name} -> {elo_match}")
                    
                    if elo_match:
                        elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                        original_name = elo_match
                        skier_id = elo_data.get('ID', None)
                    else:
                        print(f"No ELO match found for: {skier_name}")
                        elo_data = {}
                        original_name = skier_name
                        skier_id = None
                    
                    # Get fantasy price
                    price = get_fantasy_price(original_name, fantasy_prices)
                    
                    # Calculate nation quota
                    base_quota = get_nation_quota(nation, gender)
                    is_host = (nation == host_nation)
                    total_quota = get_nation_quota(nation, gender, is_host=is_host)
                    
                    # Create skier data
                    skier_data = {
                        'FIS_Name': original_name,
                        'Skier': original_name,
                        'ID': skier_id,
                        'Nation': nation,
                        'In_FIS_List': False,
                        'Config_Nation': False,  # Explicitly set to False for non-config nations
                        'In_Config': False,
                        'Price': price,
                        'Elo': elo_data.get('Elo', None),
                        'Distance_Elo': elo_data.get('Distance_Elo', None),
                        'Distance_C_Elo': elo_data.get('Distance_C_Elo', None),
                        'Distance_F_Elo': elo_data.get('Distance_F_Elo', None),
                        'Sprint_Elo': elo_data.get('Sprint_Elo', None),
                        'Sprint_C_Elo': elo_data.get('Sprint_C_Elo', None),
                        'Sprint_F_Elo': elo_data.get('Sprint_F_Elo', None),
                        'Classic_Elo': elo_data.get('Classic_Elo', None),
                        'Freestyle_Elo': elo_data.get('Freestyle_Elo', None),
                        'Base_Quota': base_quota,
                        'Is_Host_Nation': is_host,
                        'Quota': total_quota
                    }
                    
                    # Set race probability
                    if prob_column:
                        skier_data[prob_column] = 0.0  # Not in FIS list = 0% for this race
                        
                    print(f"Adding skier {original_name} from non-config nation {nation}")
                    data.append(skier_data)
                    processed_names.add(original_name)
        except Exception as e:
            print(f"Error processing additional skiers: {e}")
            traceback.print_exc()
        
        # Create DataFrame and sort by price
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=['Skier'], keep='first')
        df = df.sort_values(['Price', 'Elo'], ascending=[False, False])
        
        print(f"Processed startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating weekend startlist: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_fallback_startlist(elo_path: str, gender: str, host_nation: str, 
                             prob_column: str, fantasy_prices: Dict) -> pd.DataFrame:
    """Creates fallback startlist with all skiers from the 2025 season"""
    try:
        print(f"Creating fallback startlist with all skiers from the current season season for {gender}")
        
        # Get the most recent ELO scores
        elo_scores = get_latest_elo_scores(elo_path)
        
        # Get chronos data for finding 2025 skiers
        try:
            chronos = pl.read_csv(
                elo_path,
                infer_schema_length=10000,
                ignore_errors=True,
                null_values=["", "NA", "NULL", "Sprint"]
            ).to_pandas()
        except Exception as csv_err:
            print(f"Error with polars: {csv_err}")
            # Fallback to pandas if polars fails
            chronos = pd.read_csv(elo_path, low_memory=False)
        
        # Get all skiers from 2025 season
        skiers_2025 = chronos[chronos['Season'] == max(chronos['Season'])]['Skier'].unique()
        print(f"Found {len(skiers_2025)} skiers from the 2025 season")
        
        # Get list of nations in config
        ADDITIONAL_SKIERS = get_additional_skiers(gender)
        config_nations = list(ADDITIONAL_SKIERS.keys())
        
        # Create data for DataFrame
        data = []
        processed_names = set()
        
        # Define Elo columns
        elo_columns = [
            'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
            'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
            'Classic_Elo', 'Freestyle_Elo'
        ]
        
        # Process each 2025 skier
        for skier_name in skiers_2025:
            if skier_name in processed_names:
                continue
            
            # Get the most recent record for this skier
            recent_records = chronos[chronos['Skier'] == skier_name].sort_values('Date', ascending=False)
            
            if recent_records.empty:
                continue
                
            # Get the most recent record
            recent_record = recent_records.iloc[0]
            nation = recent_record['Nation']
            skier_id = recent_record['ID']
            
            # Match with ELO scores
            elo_match = None
            if skier_name in elo_scores['Skier'].values:
                elo_match = skier_name
            else:
                # Try fuzzy matching if no exact match
                elo_match = fuzzy_match_name(skier_name, elo_scores['Skier'].tolist())
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
            else:
                # Use Elo values directly from chronos data as fallback
                elo_data = {}
                # Get Elo values directly
                for elo_col in elo_columns:
                    if elo_col in recent_record and not pd.isna(recent_record[elo_col]):
                        elo_data[elo_col] = float(recent_record[elo_col])
                    else:
                        elo_data[elo_col] = None
            
            # Get fantasy price
            price = get_fantasy_price(skier_name, fantasy_prices)
            
            # Calculate nation quota
            base_quota = get_nation_quota(nation, gender)
            is_host = (nation == host_nation)
            total_quota = get_nation_quota(nation, gender, is_host=is_host)
            
            # Create skier data
            skier_data = {
                'FIS_Name': skier_name,
                'Skier': skier_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': False,  # Not in FIS list since this is fallback
                'Config_Nation': nation in config_nations,
                'In_Config': False,
                'Price': price,
                'Elo': elo_data.get('Elo', None),
                'Distance':elo_data.get('Distance_Elo', None),
                # Continuing from where we left off
                'Distance_Elo': elo_data.get('Distance_Elo', None),
                'Distance_C_Elo': elo_data.get('Distance_C_Elo', None),
                'Distance_F_Elo': elo_data.get('Distance_F_Elo', None),
                'Sprint_Elo': elo_data.get('Sprint_Elo', None),
                'Sprint_C_Elo': elo_data.get('Sprint_C_Elo', None),
                'Sprint_F_Elo': elo_data.get('Sprint_F_Elo', None),
                'Classic_Elo': elo_data.get('Classic_Elo', None),
                'Freestyle_Elo': elo_data.get('Freestyle_Elo', None),
                'Base_Quota': base_quota,
                'Is_Host_Nation': is_host,
                'Quota': total_quota
            }
            
            # Set race probability
            if prob_column:
                # Set lower probability for fallback skiers
                # You might want to adjust this based on various factors
                skier_data[prob_column] = 0.0
            
            data.append(skier_data)
            processed_names.add(skier_name)
        
        # Create DataFrame and sort by Elo
        df = pd.DataFrame(data)
        df = df.sort_values(['Price', 'Elo'], ascending=[False, False])
        
        print(f"Created fallback startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating fallback startlist: {e}")
        traceback.print_exc()
        return None

def merge_race_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, prob_column: str) -> pd.DataFrame:
    """Merge two race dataframes, preserving unique athletes and combining probability columns"""
    # Create a copy of df1 to avoid modifying the original
    result = df1.copy()
    
    # Create a set of existing skiers in df1
    existing_skiers = set(df1['Skier'])
    
    # For skiers that exist in both dataframes, update the probability column
    common_skiers = set(df2['Skier']) & existing_skiers
    for skier in common_skiers:
        # Find the row index in result for this skier
        idx = result[result['Skier'] == skier].index[0]
        
        # Get probability from df2
        prob_value = df2[df2['Skier'] == skier][prob_column].values[0]
        
        # Update the probability in result
        result.loc[idx, prob_column] = prob_value
    
    # For skiers that only exist in df2, add them to result
    new_skiers = set(df2['Skier']) - existing_skiers
    new_rows = df2[df2['Skier'].isin(new_skiers)]
    
    # Append the new rows to result
    result = pd.concat([result, new_rows], ignore_index=True)
    
    # Sort the result by price and elo
    result = result.sort_values(['Price', 'Elo'], ascending=[False, False])
    
    return result

if __name__ == "__main__":
    process_weekend_races()