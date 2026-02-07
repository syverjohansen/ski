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


def process_tds_races() -> None:
    """Main function to process Tour de Ski period races"""
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/ski/polars/excel365/weekends.csv')
        print(f"Successfully read weekends.csv with {len(weekends_df)} rows")
    except Exception as e:
        print(f"Error reading weekends.csv: {e}")
        traceback.print_exc()
        return
    
    # Filter for Period == 2 races, excluding "Tour de Ski" city
    print("Filtering for Period == 2 races (excluding Tour de Ski summary)...")
    tds_races = weekends_df[
        (weekends_df['Period'] == 2) & 
        (weekends_df['City'] != 'Tour de Ski')
    ]
    
    if tds_races.empty:
        print("No Tour de Ski races found in Period == 2")
        return
    
    print(f"Found {len(tds_races)} Tour de Ski races")
    
    # DEBUG: Show all TdS races found
    print("\n===== ALL TOUR DE SKI RACES =====")
    for idx, (_, race) in enumerate(tds_races.iterrows()):
        race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else 'No ID'
        print(f"  {idx+1}. {race['Distance']} {race['Sex']} in {race.get('City', 'Unknown')} - Race ID: {race_id} - Race: {race['Race']}")
        print(f"      Date: {race.get('Race_Date', 'Unknown')}")
        print(f"      Startlist: {race.get('Startlist', 'No URL')}")
    
    # Get unique gender and date combinations to determine minimum race for each gender
    gender_mapping = {'M': 'men', 'L': 'ladies'}
    
    for gender_code, gender_name in gender_mapping.items():
        print(f"\n===== PROCESSING {gender_name.upper()} TOUR DE SKI RACES =====")
        
        # Filter races for this gender
        gender_races = tds_races[tds_races['Sex'] == gender_code]
        
        if gender_races.empty:
            print(f"No {gender_name} races found")
            continue
        
        # Find the minimum Race value for this gender
        min_race = gender_races['Race'].min()
        print(f"Minimum race number for {gender_name}: {min_race}")
        
        # Sort races by Race number for proper Race_1, Race_2, etc. logic
        gender_races = gender_races.sort_values('Race')
        
        print(f"Found {len(gender_races)} {gender_name} races to process:")
        for _, race in gender_races.iterrows():
            race_id = parse_fis_race_id(race['Startlist']) if 'Startlist' in race else 'No ID'
            print(f"  Race {race['Race']}: {race['Distance']} {race['Sex']} in {race.get('City', 'Unknown')} - Race ID: {race_id}")
        
        # Determine host nation from the first race
        host_nation = gender_races.iloc[0]['Country']
        print(f"Host nation for {gender_name} Tour de Ski: {host_nation}")
        
        # Process the races for this gender
        try:
            process_tds_gender_races(gender_races, gender_name, host_nation, min_race)
        except Exception as e:
            print(f"Error processing {gender_name} races: {e}")
            traceback.print_exc()
    
    # Call the TdS R script
    print(f"\n===== CALLING TDS R SCRIPT =====")
    try:
        # Set the base path to the R scripts
        r_script_base_path = "~/blog/daehl-e/content/post/cross-country/drafts"
        r_script_path = os.path.expanduser(f"{r_script_base_path}/tds-picks.R")
        
        # Command to execute the R script
        command = ["Rscript", r_script_path]
        
        print(f"Calling TdS R script: {' '.join(command)}")
        
        # Call the R script
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"TdS R script output:\n{result.stdout}")
        if result.stderr:
            print(f"TdS R script error:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Error calling TdS R script: {e}")
        print(f"Script output: {e.stdout}")
        print(f"Script error: {e.stderr}")
    except FileNotFoundError:
        print(f"TdS R script not found: {r_script_path}")
    
    print("Tour de Ski processing complete!")


def process_tds_gender_races(races_df: pd.DataFrame, gender: str, host_nation: str, min_race: int) -> None:
    """Process Tour de Ski races for a specific gender"""
    
    print(f"\n=== PROCESSING {gender.upper()} TDS RACES ===")
    
    # Debug: Show what races_df contains
    print(f"races_df contains {len(races_df)} races:")
    for i, (_, race) in enumerate(races_df.iterrows()):
        print(f"  {i+1}. Race {race['Race']}: {race['Distance']} {race['Sex']} on {race.get('Race_Date', 'Unknown')}")
    
    # Create probability column names based on race numbers relative to minimum
    num_races_to_process = len(races_df)
    print(f"Number of races to process: {num_races_to_process}")
    
    # Create Race_1, Race_2, etc. based on order within TdS, not absolute race numbers
    prob_columns = [f'Race{i+1}_Prob' for i in range(num_races_to_process)]
    print(f"Probability columns for {gender}: {prob_columns}")
    
    # Get the ELO path
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    
    # Initialize a consolidated dataframe
    consolidated_df = None
    
    # Process each race
    for i, (_, race) in enumerate(races_df.iterrows()):
        url = race['Startlist']
        
        if not url or pd.isna(url):
            print(f"No startlist URL for race {i+1}, skipping")
            continue
        
        print(f"\n=== PROCESSING RACE {i+1} (Race {race['Race']}) ===")
        print(f"URL: {url}")
        print(f"Expected prob_column: {prob_columns[i]}")
        
        # Create startlist for this race
        print(f"Calling create_tds_startlist...")
        race_df = create_tds_startlist(
            fis_url=url,
            elo_path=elo_path,
            gender=gender,
            host_nation=host_nation,
            prob_column=prob_columns[i]
        )
        
        if race_df is None:
            print(f"❌ create_tds_startlist returned None for race {i+1}")
            continue
        
        # Check what columns this individual race created
        print(f"✓ create_tds_startlist returned DataFrame with {len(race_df)} rows, {len(race_df.columns)} columns")
        race_prob_cols = [col for col in race_df.columns if 'Race' in col and 'Prob' in col]
        print(f"Probability columns returned by race {i+1}: {race_prob_cols}")
        
        # Merge logic
        if consolidated_df is None:
            print(f"Initializing consolidated_df with race {i+1}")
            consolidated_df = race_df.copy()
            print(f"Initial consolidated_df has {len(consolidated_df)} rows, {len(consolidated_df.columns)} columns")
        else:
            # Debug merge operation
            print(f"Merging race {i+1} data with consolidated_df...")
            
            # Perform the merge
            print(f"Calling merge_race_dataframes with prob_column: '{prob_columns[i]}'")
            consolidated_df = merge_race_dataframes(consolidated_df, race_df, prob_columns[i])
            
            print(f"After merge - total columns: {len(consolidated_df.columns)}")
    
    # Save the result
    if consolidated_df is not None:
        output_path = f"~/ski/elo/python/ski/polars/excel365/startlist_tds_{gender}.csv"
        print(f"\nSaving consolidated {gender} Tour de Ski startlist to {output_path}")
        consolidated_df.to_csv(output_path, index=False)
        print(f"Saved {len(consolidated_df)} {gender} athletes with {len(consolidated_df.columns)} columns")
    else:
        print(f"❌ No data to save for {gender}")
    
    print(f"=== END {gender.upper()} TDS PROCESSING ===\n")


def create_tds_startlist(fis_url: str, elo_path: str, gender: str, 
                        host_nation: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with startlist, prices and ELO scores for Tour de Ski races"""
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
                row_data[prob_column] = 1.0  # Set all TdS race probabilities to 100%
            
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
                    row_data[prob_column] = 1.0  # Set all TdS race probabilities to 100%
                
                data.append(row_data)
                processed_names.add(original_name)
        
        # Add processing for additional skiers from chronos data (same as weekend script)
        try:
            # Get chronos data for finding additional national skiers
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
                        skier_data[prob_column] = 1.0  # Set all TdS race probabilities to 100%
                        
                    print(f"Adding skier {original_name} from non-config nation {nation}")
                    data.append(skier_data)
                    processed_names.add(original_name)
        except Exception as e:
            print(f"Error processing additional skiers: {e}")
            traceback.print_exc()
        
        # Create DataFrame and sort by price
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=['Skier'], keep='first')
        
        # Check if there are any athletes with In_FIS_List = True
        fis_athletes_exist = (df['In_FIS_List'] == True).any()
        
        if fis_athletes_exist:
            print(f"Found athletes in FIS list - filtering to only include those athletes")
            # Filter to only include athletes with In_FIS_List = True
            df = df[df['In_FIS_List'] == True]
            print(f"After filtering to FIS athletes only: {len(df)} athletes")
        else:
            print(f"No athletes found in FIS list - including all athletes as usual")
        
        df = df.sort_values(['Price', 'Elo'], ascending=[False, False])
        
        print(f"Processed TdS startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating TdS startlist: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_fallback_startlist(elo_path: str, gender: str, host_nation: str, 
                             prob_column: str, fantasy_prices: Dict) -> pd.DataFrame:
    """Creates fallback startlist with all skiers from the current season"""
    try:
        print(f"Creating fallback startlist with all skiers from the current season for {gender}")
        
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
            # Fallback to pandas if polars fails
            chronos = pd.read_csv(elo_path, low_memory=False)
        
        # Get all skiers from current season
        current_season = get_current_season_from_chronos(chronos)
        current_season_skiers = chronos[chronos['Season'] == current_season]['Skier'].unique()
        print(f"Found {len(current_season_skiers)} skiers from the {current_season} season")
        
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
        
        # Process each current season skier
        for skier_name in current_season_skiers:
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
                # Set all TdS race probabilities to 100%
                skier_data[prob_column] = 1.0
            
            data.append(skier_data)
            processed_names.add(skier_name)
        
        # Create DataFrame and sort by Elo
        df = pd.DataFrame(data)
        df = df.sort_values(['Price', 'Elo'], ascending=[False, False])
        
        print(f"Created fallback TdS startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating fallback TdS startlist: {e}")
        traceback.print_exc()
        return None


def merge_race_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, prob_column: str) -> pd.DataFrame:
    """Merge two race dataframes, preserving unique athletes and combining probability columns"""
    # Create a copy of df1 to avoid modifying the original
    result = df1.copy()
    
    # Check if we should only include FIS athletes (if df1 has any FIS athletes)
    fis_athletes_exist_in_result = (result['In_FIS_List'] == True).any()
    
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
    
    # Apply FIS filtering to new rows if we're in FIS-only mode
    if fis_athletes_exist_in_result and not new_rows.empty:
        print(f"Filtering {len(new_rows)} new skiers to only include FIS athletes")
        new_rows = new_rows[new_rows['In_FIS_List'] == True]
        print(f"After FIS filtering: {len(new_rows)} new skiers remaining")
    
    # Append the new rows to result
    result = pd.concat([result, new_rows], ignore_index=True)
    
    # Sort the result by price and elo
    result = result.sort_values(['Price', 'Elo'], ascending=[False, False])
    
    return result


if __name__ == "__main__":
    process_tds_races()