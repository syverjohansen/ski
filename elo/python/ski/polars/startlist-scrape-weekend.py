#!/usr/bin/env python3
import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional
import warnings
from datetime import datetime, timezone
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
    
    # Get unique relay types present in the dataframe
    relay_types = set(get_relay_type(race) for _, race in races_df.iterrows())
    relay_types.discard('unknown')  # Remove unknown types
    
    # Log the relay types detected
    print(f"Relay events detected: {relay_types}")
    
    # Call the appropriate relay script for each type
    success = False
    for relay_type in relay_types:
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
        
        if relay_specific_races.empty:
            print(f"No {relay_type} races found after filtering")
            continue
            
        # Save to temporary CSV for the relay script to use
        temp_csv_path = f"/tmp/weekend_relay_{relay_type}_races.csv"
        relay_specific_races.to_csv(temp_csv_path, index=False)
        
        print(f"Calling relay script for {relay_type}: {relay_script_path}")
        print(f"Processing {len(relay_specific_races)} {relay_type} races")
        
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

def get_today_utc_date() -> str:
    """Get today's date in UTC timezone as MM/DD/YYYY string to match CSV format"""
    return datetime.now(timezone.utc).strftime('%m/%d/%Y')

def process_weekend_races() -> None:
    """Main function to process weekend races - relays first, then individuals"""
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/ski/polars/excel365/weekends.csv')
        print(f"Successfully read weekends.csv with {len(weekends_df)} rows")
    except Exception as e:
        print(f"Error reading weekends.csv: {e}")
        traceback.print_exc()
        return
    
    # Get today's date in UTC
    today_utc = get_today_utc_date()
    print(f"Looking for races on today's date (UTC): {today_utc}")
    
    # Filter races for today's date
    today_races = weekends_df[weekends_df['Date'] == today_utc]
    
    if today_races.empty:
        print(f"No races found for today ({today_utc})")
        return
    
    print(f"Found {len(today_races)} races for today ({today_utc})")
    
    # DEBUG: Show all races found for today
    print("\n===== ALL RACES FOR TODAY =====")
    for idx, (_, race) in enumerate(today_races.iterrows()):
        race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else 'No ID'
        print(f"  {idx+1}. {race['Distance']} {race['Sex']} in {race.get('City', 'Unknown')} - Race ID: {race_id}")
        print(f"      Startlist: {race.get('Startlist', 'No URL')}")
    
    # Check for duplicate race IDs
    race_ids = []
    for _, race in today_races.iterrows():
        race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else None
        if race_id:
            race_ids.append(race_id)
    
    duplicate_ids = [race_id for race_id in race_ids if race_ids.count(race_id) > 1]
    if duplicate_ids:
        print(f"\n⚠️  WARNING: Found duplicate race IDs: {duplicate_ids}")
        # Remove duplicate races based on race ID
        print("Removing duplicate races...")
        seen_race_ids = set()
        unique_races = []
        for _, race in today_races.iterrows():
            race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else None
            if race_id is None or race_id not in seen_race_ids:
                unique_races.append(race)
                if race_id:
                    seen_race_ids.add(race_id)
        
        today_races = pd.DataFrame(unique_races)
        print(f"After removing duplicates: {len(today_races)} races")
    
    # STEP 1: Process all relay races first
    relay_races = today_races[today_races['Distance'].isin(['Rel', 'Ts'])]
    
    if not relay_races.empty:
        print(f"\n===== PROCESSING {len(relay_races)} RELAY RACES FIRST =====")
        for _, race in relay_races.iterrows():
            race_type = get_relay_type(race)
            race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else 'No ID'
            print(f"  {race_type}: {race['Distance']} {race['Sex']} in {race.get('City', 'Unknown')} - Race ID: {race_id}")
        
        # Handle all relay races
        relay_success = handle_relay_races(relay_races)
        if relay_success:
            print("All relay races processed successfully")
        else:
            print("Some relay races may not have been processed")
    else:
        print("No relay races found for today")
    
    # STEP 2: Process individual races after all relays are done
    individual_races = today_races[(today_races['Distance'] != 'Rel') & 
                                   (today_races['Distance'] != 'Ts')]
    
    if not individual_races.empty:
        print(f"\n===== PROCESSING {len(individual_races)} INDIVIDUAL RACES =====")
        
        # Debug: Show all individual races found
        for _, race in individual_races.iterrows():
            race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else 'No ID'
            print(f"  Individual race: {race['Distance']} {race['Sex']} in {race.get('City', 'Unknown')} - Race ID: {race_id}")
            print(f"    Race_Date: {race.get('Race_Date', 'Unknown')}")
            print(f"    Startlist: {race.get('Startlist', 'No URL')}")
        
        # Check for duplicate individual races by gender and distance
        print("\n--- Checking for duplicate individual races ---")
        individual_summary = individual_races.groupby(['Sex', 'Distance']).size()
        print("Individual races by Sex and Distance:")
        print(individual_summary)
        
        duplicates_found = False
        for (sex, distance), count in individual_summary.items():
            if count > 1:
                print(f"⚠️  WARNING: Found {count} {sex} {distance} races (expected 1)")
                duplicates_found = True
        
        if duplicates_found:
            print("\nRemoving duplicate individual races (keeping first occurrence)...")
            # Remove duplicates by keeping first occurrence of each Sex+Distance combination
            individual_races = individual_races.drop_duplicates(subset=['Sex', 'Distance'], keep='first')
            print(f"After removing duplicates: {len(individual_races)} individual races")
            
            # Show final individual races
            print("Final individual races to process:")
            for _, race in individual_races.iterrows():
                race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else 'No ID'
                print(f"  {race['Distance']} {race['Sex']} in {race.get('City', 'Unknown')} - Race ID: {race_id}")
        
        # Sort individual races by Race_Date (earliest first)
        try:
            individual_races['Race_Date'] = pd.to_datetime(individual_races['Race_Date'])
            sorted_races = individual_races.sort_values('Race_Date')
            print(f"Sorted individual races by Race_Date, earliest: {sorted_races['Race_Date'].iloc[0]}")
            
            # Determine host nation from the first race
            host_nation = sorted_races.iloc[0]['Country']
            print(f"Host nation for this weekend: {host_nation}")
            
            # Use the deduplicated races
            races_to_process = sorted_races
            print(f"Processing {len(races_to_process)} individual races")
            
        except Exception as e:
            print(f"Error sorting individual races: {e}")
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
                process_gender_races(men_races, gender_mapping['M'], host_nation, today_races)
            except Exception as e:
                print(f"Error processing men's races: {e}")
                traceback.print_exc()

        # Process ladies' races
        if not ladies_races.empty:
            try:
                process_gender_races(ladies_races, gender_mapping['L'], host_nation, today_races)
            except Exception as e:
                print(f"Error processing ladies' races: {e}")
                traceback.print_exc()
    else:
        print("No individual races found for today")
    
    # STEP 3: Call the main R script only if we processed individual races
    if not individual_races.empty:
        print(f"\n===== CALLING MAIN R SCRIPT =====")
        call_r_script('weekend', 'standard')
    else:
        print("No individual races to process - skipping main R script")
    
    print("Weekend processing complete!")
def process_gender_races(races_df: pd.DataFrame, gender: str, host_nation: str, all_today_races: pd.DataFrame) -> None:
    """Process races for a specific gender with comprehensive debugging"""
    
    print(f"\n=== DEBUGGING {gender.upper()} RACE PROCESSING ===")
    
    # Debug: Show what races_df contains
    print(f"races_df contains {len(races_df)} races:")
    for i, (_, race) in enumerate(races_df.iterrows()):
        print(f"  {i+1}. {race['Distance']} {race['Sex']} on {race.get('Race_Date', 'Unknown')}")
    
    # Use the actual number of races being processed instead of counting from all_today_races
    # This fixes the bug where total_individual_races != len(races_df)
    num_races_to_process = len(races_df)
    print(f"Number of races to process: {num_races_to_process}")
    
    # Create exactly the number of probability columns we need
    if num_races_to_process == 1:
        prob_columns = ['Race1_Prob']
    else:
        prob_columns = [f'Race{i+1}_Prob' for i in range(num_races_to_process)]
    
    print(f"Probability columns for {gender}: {prob_columns}")
    
    # Get the ELO path - use chrono_pred for most recent Elo values
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    
    # Check base ELO file for existing probability columns
    print(f"\n=== CHECKING BASE ELO FILE ===")
    try:
        base_elo_df = pd.read_csv(elo_path)
        base_prob_cols = [col for col in base_elo_df.columns if 'Race' in col and 'Prob' in col]
        if base_prob_cols:
            print(f"⚠️  BASE FILE ALREADY HAS PROBABILITY COLUMNS: {base_prob_cols}")
            print("This could be the source of extra columns!")
        else:
            print("✓ Base ELO file contains no probability columns")
    except Exception as e:
        print(f"Could not read base ELO file: {e}")
    
    # Initialize a consolidated dataframe
    consolidated_df = None
    
    # Process each race with detailed debugging
    for i, (_, race) in enumerate(races_df.iterrows()):
        url = race['Startlist']
        
        if not url or pd.isna(url):
            print(f"No startlist URL for race {i+1}, skipping")
            continue
        
        print(f"\n=== PROCESSING RACE {i+1} ===")
        print(f"URL: {url}")
        print(f"Expected prob_column: {prob_columns[i]}")
        
        # Create startlist for this race
        print(f"Calling create_weekend_startlist...")
        race_df = create_weekend_startlist(
            fis_url=url,
            elo_path=elo_path,
            gender=gender,
            host_nation=host_nation,
            prob_column=prob_columns[i]
        )
        
        if race_df is None:
            print(f"❌ create_weekend_startlist returned None for race {i+1}")
            continue
        
        # Check what columns this individual race created
        print(f"✓ create_weekend_startlist returned DataFrame with {len(race_df)} rows, {len(race_df.columns)} columns")
        race_prob_cols = [col for col in race_df.columns if 'Race' in col and 'Prob' in col]
        print(f"Probability columns returned by race {i+1}: {race_prob_cols}")
        
        # Validate this race's output
        if len(race_prob_cols) != 1:
            print(f"⚠️  RACE {i+1} PROBLEM: Expected 1 probability column, got {len(race_prob_cols)}")
            print(f"This is likely where extra columns are coming from!")
            print(f"All columns in race_df: {list(race_df.columns)}")
        elif race_prob_cols[0] != prob_columns[i]:
            print(f"⚠️  RACE {i+1} COLUMN NAME MISMATCH:")
            print(f"Expected: '{prob_columns[i]}'")
            print(f"Got: '{race_prob_cols[0]}'")
        else:
            print(f"✓ Race {i+1} returned correct single probability column")
        
        # Merge logic with debugging
        if consolidated_df is None:
            print(f"Initializing consolidated_df with race {i+1}")
            consolidated_df = race_df.copy()
            init_prob_cols = [col for col in consolidated_df.columns if 'Race' in col and 'Prob' in col]
            print(f"Initial consolidated_df probability columns: {init_prob_cols}")
            print(f"Initial consolidated_df has {len(consolidated_df)} rows, {len(consolidated_df.columns)} columns")
        else:
            # Debug merge operation
            print(f"Merging race {i+1} data with consolidated_df...")
            
            # Before merge state
            before_prob_cols = [col for col in consolidated_df.columns if 'Race' in col and 'Prob' in col]
            before_total_cols = len(consolidated_df.columns)
            print(f"Before merge - probability columns: {before_prob_cols}")
            print(f"Before merge - total columns: {before_total_cols}")
            
            # Perform the merge
            print(f"Calling merge_race_dataframes with prob_column: '{prob_columns[i]}'")
            consolidated_df = merge_race_dataframes(consolidated_df, race_df, prob_columns[i])
            
            # After merge state
            after_prob_cols = [col for col in consolidated_df.columns if 'Race' in col and 'Prob' in col]
            after_total_cols = len(consolidated_df.columns)
            print(f"After merge - probability columns: {after_prob_cols}")
            print(f"After merge - total columns: {after_total_cols}")
            
            # Validate merge results
            expected_prob_cols = len(before_prob_cols) + 1
            actual_prob_cols = len(after_prob_cols)
            
            if actual_prob_cols != expected_prob_cols:
                print(f"⚠️  MERGE PROBLEM!")
                print(f"Expected {expected_prob_cols} probability columns after merge, got {actual_prob_cols}")
                
                # Show what changed
                new_cols = set(after_prob_cols) - set(before_prob_cols)
                removed_cols = set(before_prob_cols) - set(after_prob_cols)
                
                if new_cols:
                    print(f"Columns added during merge: {new_cols}")
                if removed_cols:
                    print(f"Columns removed during merge: {removed_cols}")
                    
                print("This suggests merge_race_dataframes is the source of extra columns!")
            else:
                print(f"✓ Merge correctly added 1 probability column")
    
    # Final validation and save
    if consolidated_df is not None:
        final_prob_cols = [col for col in consolidated_df.columns if 'Race' in col and 'Prob' in col]
        print(f"\n=== FINAL VALIDATION ===")
        print(f"Expected {len(prob_columns)} probability columns: {prob_columns}")
        print(f"Actually got {len(final_prob_cols)} probability columns: {final_prob_cols}")
        
        # Check for problems
        if len(final_prob_cols) != len(prob_columns):
            print(f"⚠️  FINAL COLUMN COUNT MISMATCH!")
            extra_cols = set(final_prob_cols) - set(prob_columns)
            missing_cols = set(prob_columns) - set(final_prob_cols)
            
            if extra_cols:
                print(f"Extra columns that shouldn't be there: {extra_cols}")
            if missing_cols:
                print(f"Missing expected columns: {missing_cols}")
        else:
            print(f"✓ Final column count is correct")
        
        # Check column names match exactly
        if set(final_prob_cols) != set(prob_columns):
            print(f"⚠️  COLUMN NAMES DON'T MATCH EXACTLY!")
            print(f"Expected: {sorted(prob_columns)}")
            print(f"Got: {sorted(final_prob_cols)}")
        else:
            print(f"✓ All column names match exactly")
        
        # Save the result
        output_path = f"~/ski/elo/python/ski/polars/excel365/startlist_weekend_{gender}.csv"
        print(f"\nSaving consolidated {gender} startlist to {output_path}")
        consolidated_df.to_csv(output_path, index=False)
        print(f"Saved {len(consolidated_df)} {gender} athletes with {len(consolidated_df.columns)} columns")
    else:
        print(f"No data to save for {gender}")

def create_weekend_startlist(fis_url: str, elo_path: str, gender: str, 
                           host_nation: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with startlist, prices and ELO scores for weekend races"""
    try:
        # Get data from all sources
        fis_athletes = get_fis_startlist(fis_url)
        print(f"Found {len(fis_athletes)} athletes in FIS startlist")
        fantasy_prices = get_fantasy_prices()
        
        if not fis_athletes:
            print("FIS startlist is empty, using fallback logic with current season skiers")
            return create_fallback_startlist(elo_path, gender, host_nation, prob_column, fantasy_prices)
        

        elo_scores = get_latest_elo_scores(elo_path)

        # Get list of nations in config
        ADDITIONAL_SKIERS = get_additional_skiers(gender)
        config_nations = list(ADDITIONAL_SKIERS.keys())

        # Build lookup of config athletes with their yes/no lists
        config_skier_lookup = {}  # name -> {'yes': [], 'no': [], 'nation': str}
        for nation, skiers in ADDITIONAL_SKIERS.items():
            for skier_entry in skiers:
                if isinstance(skier_entry, str):
                    config_skier_lookup[skier_entry] = {'yes': [], 'no': [], 'nation': nation}
                else:
                    config_skier_lookup[skier_entry['name']] = {
                        'yes': skier_entry.get('yes', []),
                        'no': skier_entry.get('no', []),
                        'nation': nation
                    }

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

            # Check if this FIS athlete is also in the config (check multiple name variants)
            in_config = False
            config_info = None
            for name_variant in [fis_name, processed_name, original_name]:
                if name_variant in config_skier_lookup:
                    in_config = True
                    config_info = config_skier_lookup[name_variant]
                    break

            row_data = {
                'FIS_Name': fis_name,
                'Skier': original_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': True,
                'Config_Nation': nation in config_nations,
                'In_Config': in_config,
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

            # Set race probability - FIS list athletes get 1.0 for this race
            if prob_column:
                row_data[prob_column] = 1.0

            data.append(row_data)
            processed_names.add(original_name)
        
        # Extract current race number from prob_column (e.g., 'Race1_Prob' -> 1)
        current_race_num = int(prob_column.replace('Race', '').replace('_Prob', '')) if prob_column else None

        # Process additional skiers from config
        for nation, skiers in ADDITIONAL_SKIERS.items():
            print(f"\nProcessing additional skiers for {nation}")
            for skier_entry in skiers:
                # Handle both string format and dict format with yes/no lists
                if isinstance(skier_entry, str):
                    name = skier_entry
                    yes_races = []  # Empty = no confirmed races
                    no_races = []   # Empty = no confirmed non-races
                else:
                    name = skier_entry['name']
                    yes_races = skier_entry.get('yes', [])  # Confirmed racing
                    no_races = skier_entry.get('no', [])    # Confirmed NOT racing

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
                
                # Set race probability based on yes/no lists
                # yes = confirmed racing (1.0), no = confirmed NOT racing (0.0), neither = calculate (None/NA)
                if prob_column:
                    if current_race_num in yes_races:
                        row_data[prob_column] = 1.0  # Confirmed racing this race
                    elif current_race_num in no_races:
                        row_data[prob_column] = 0.0  # Confirmed NOT racing this race
                    else:
                        row_data[prob_column] = None  # Unknown - R will calculate from history

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
            
            # Get current season and all nations from that season
            current_season = get_current_season_from_chronos(chronos)
            all_current_nations = set(chronos[chronos['Season'] == current_season]['Nation'].unique())
            
            # Find nations that aren't in config
            non_config_nations = {nation for nation in all_current_nations if nation not in config_nations}
            
            print(f"Found {len(non_config_nations)} non-config nations from {current_season} season")
            
            # Process each non-config nation
            for nation in non_config_nations:
                print(f"Processing all {current_season} skiers for non-config nation: {nation}")
                
                # Get current skiers for this nation (if any)
                current_skiers = {row['Skier'] for row in data if row['Nation'] == nation}
                
                # Get all skiers from this nation who competed in current season
                nation_skiers = chronos[(chronos['Nation'] == nation) & 
                                      (chronos['Season'] == current_season)]['Skier'].unique()
                
                print(f"Found {len(nation_skiers)} skiers from {nation} who competed in {current_season}")
                
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
    """Creates fallback startlist when no FIS startlist is available.

    Probability logic:
    - Config athletes (In_Config=True): based on yes/no lists, or None if neither
    - Config nation but not in config (Config_Nation=True, In_Config=False): 0.0
    - Non-config nation athletes: None (R calculates from history)
    """
    try:
        print(f"Creating fallback startlist (no FIS startlist) for {gender}")

        # Get the most recent ELO scores
        elo_scores = get_latest_elo_scores(elo_path)

        # Get chronos data for finding current season skiers
        try:
            chronos = pl.read_csv(
                elo_path,
                infer_schema_length=10000,
                ignore_errors=True,
                null_values=["", "NA", "NULL", "Sprint"]
            ).to_pandas()
        except Exception as csv_err:
            print(f"Error with polars: {csv_err}")
            chronos = pd.read_csv(elo_path, low_memory=False)

        # Get all skiers from current season
        current_season = get_current_season_from_chronos(chronos)
        current_season_skiers = set(chronos[chronos['Season'] == current_season]['Skier'].unique())
        print(f"Found {len(current_season_skiers)} skiers from the {current_season} season")

        # Get config data
        ADDITIONAL_SKIERS = get_additional_skiers(gender)
        config_nations = list(ADDITIONAL_SKIERS.keys())

        # Build lookup of config athletes with their yes/no lists
        config_skier_lookup = {}
        for nation, skiers in ADDITIONAL_SKIERS.items():
            for skier_entry in skiers:
                if isinstance(skier_entry, str):
                    config_skier_lookup[skier_entry] = {'yes': [], 'no': [], 'nation': nation}
                else:
                    config_skier_lookup[skier_entry['name']] = {
                        'yes': skier_entry.get('yes', []),
                        'no': skier_entry.get('no', []),
                        'nation': nation
                    }

        # Extract current race number from prob_column
        current_race_num = int(prob_column.replace('Race', '').replace('_Prob', '')) if prob_column else None

        # Create data for DataFrame
        data = []
        processed_names = set()

        # Define Elo columns
        elo_columns = [
            'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo',
            'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo',
            'Classic_Elo', 'Freestyle_Elo'
        ]

        def get_elo_data_for_skier(skier_name):
            """Helper to get ELO data for a skier"""
            if skier_name in elo_scores['Skier'].values:
                return elo_scores[elo_scores['Skier'] == skier_name].iloc[0].to_dict(), skier_name
            elo_match = fuzzy_match_name(skier_name, elo_scores['Skier'].tolist())
            if elo_match:
                return elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict(), elo_match
            return {}, skier_name

        # STEP 1: Process config athletes first
        print(f"Processing {len(config_skier_lookup)} config athletes...")
        for config_name, config_info in config_skier_lookup.items():
            if config_name in processed_names:
                continue

            yes_races = config_info['yes']
            no_races = config_info['no']
            config_nation = config_info['nation']

            elo_data, matched_name = get_elo_data_for_skier(config_name)
            skier_id = elo_data.get('ID', None)
            nation = elo_data.get('Nation', config_nation)

            price = get_fantasy_price(matched_name, fantasy_prices)
            base_quota = get_nation_quota(nation, gender)
            is_host = (nation == host_nation)
            total_quota = get_nation_quota(nation, gender, is_host=is_host)

            skier_data = {
                'FIS_Name': config_name,
                'Skier': matched_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': False,
                'Config_Nation': True,
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

            # Set probability based on yes/no lists
            if prob_column and current_race_num:
                if current_race_num in yes_races:
                    skier_data[prob_column] = 1.0
                elif current_race_num in no_races:
                    skier_data[prob_column] = 0.0
                else:
                    skier_data[prob_column] = None  # R calculates from history

            data.append(skier_data)
            processed_names.add(matched_name)

        # STEP 2: Process remaining current season skiers
        print(f"Processing remaining current season skiers...")
        for skier_name in current_season_skiers:
            if skier_name in processed_names:
                continue

            recent_records = chronos[chronos['Skier'] == skier_name].sort_values('Date', ascending=False)
            if recent_records.empty:
                continue

            recent_record = recent_records.iloc[0]
            nation = recent_record['Nation']
            skier_id = recent_record['ID']

            elo_data, matched_name = get_elo_data_for_skier(skier_name)
            if not elo_data:
                elo_data = {}
                for elo_col in elo_columns:
                    if elo_col in recent_record and not pd.isna(recent_record[elo_col]):
                        elo_data[elo_col] = float(recent_record[elo_col])

            price = get_fantasy_price(skier_name, fantasy_prices)
            base_quota = get_nation_quota(nation, gender)
            is_host = (nation == host_nation)
            total_quota = get_nation_quota(nation, gender, is_host=is_host)

            is_config_nation = nation in config_nations

            skier_data = {
                'FIS_Name': skier_name,
                'Skier': skier_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': False,
                'Config_Nation': is_config_nation,
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

            # Set probability based on config nation status
            if prob_column:
                if is_config_nation:
                    # Config nation but not in config = not announced = 0.0
                    skier_data[prob_column] = 0.0
                else:
                    # Non-config nation = R calculates from history
                    skier_data[prob_column] = None

            data.append(skier_data)
            processed_names.add(skier_name)

        # Create DataFrame and sort
        df = pd.DataFrame(data)
        df = df.sort_values(['Price', 'Elo'], ascending=[False, False])

        print(f"Created fallback startlist with {len(df)} athletes ({len(config_skier_lookup)} from config)")
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