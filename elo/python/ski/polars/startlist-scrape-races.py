#!/usr/bin/env python3
import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional
import warnings
from datetime import datetime, timedelta
import traceback
import subprocess
warnings.filterwarnings('ignore')

# Add parent directories to path for shared config
sys.path.insert(0, os.path.expanduser('~/ski/elo/python'))

# Import pipeline config for TEST_MODE and file paths
from pipeline_config import TEST_MODE, get_races_file

# Import common utility functions
from startlist_common import *

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
        elif race_type == 'final_climb':
            r_script = "final_climb.R"
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
    return distance in ['Rel', 'Ts', 'Mix']

def check_for_final_climb_today(races_df: pd.DataFrame) -> bool:
    """Check if today's races include any Final Climb events"""
    try:
        today = datetime.now()
        print(f"Checking for Final Climb events on {today.strftime('%Y-%m-%d')}")
        
        # Parse dates in the dataframe
        if 'Date' in races_df.columns:
            print(f"Found {len(races_df)} total races in data")
            
            # Handle different date formats (M/D/YYYY or YYYY-MM-DD)
            races_df_copy = races_df.copy()
            
            # Try to parse dates with multiple formats
            def parse_date_flexible(date_str):
                try:
                    # Try M/D/YYYY format first
                    if '/' in str(date_str):
                        return pd.to_datetime(date_str, format='%m/%d/%Y')
                    # Try YYYY-MM-DD format
                    else:
                        return pd.to_datetime(date_str)
                except:
                    return None
            
            races_df_copy['Date_parsed'] = races_df_copy['Date'].apply(parse_date_flexible)
            
            # Filter out rows where date parsing failed
            valid_dates = races_df_copy.dropna(subset=['Date_parsed'])
            print(f"Successfully parsed {len(valid_dates)} race dates")
            
            # Check for today's date (allowing for some flexibility)
            today_races = valid_dates[valid_dates['Date_parsed'].dt.date == today.date()]
            
            print(f"Found {len(today_races)} races for today ({today.strftime('%Y-%m-%d')})")
            
            if today_races.empty:
                # Debug: show what dates we do have
                if len(valid_dates) > 0:
                    print("Available race dates:")
                    unique_dates = valid_dates['Date_parsed'].dt.date.unique()[:5]  # Show first 5
                    for date in unique_dates:
                        print(f"  - {date}")
                return False
            
            # Check for Final_Climb column and if any race has Final_Climb = 1
            if 'Final_Climb' in today_races.columns:
                # Convert Final_Climb to numeric to handle string/int issues
                today_races_copy = today_races.copy()
                today_races_copy['Final_Climb'] = pd.to_numeric(today_races_copy['Final_Climb'], errors='coerce')
                
                final_climb_races = today_races_copy[today_races_copy['Final_Climb'] == 1]
                
                if not final_climb_races.empty:
                    print(f"Found {len(final_climb_races)} Final Climb races today:")
                    for _, race in final_climb_races.iterrows():
                        print(f"  - {race.get('Distance', 'Unknown')} {race.get('Sex', 'Unknown')} in {race.get('City', 'Unknown')}")
                    return True
                else:
                    print("No Final Climb races found for today")
                    print(f"Final_Climb values in today's races: {today_races['Final_Climb'].unique()}")
                    return False
            else:
                print("Final_Climb column not found in races data")
                print(f"Available columns: {list(today_races.columns)}")
                return False
                
    except Exception as e:
        print(f"Error checking for Final Climb events: {e}")
        traceback.print_exc()
        return False

def get_relay_type(race: pd.Series) -> str:
    """Return the type of relay based on race data"""
    distance = str(race['Distance']).strip() if 'Distance' in race else ""
    sex = str(race['Sex']).strip() if 'Sex' in race else ""
    
    # Check for mixed relay (Rel with Mixed sex)
    if distance == 'Rel' and (sex == "Mixed" or sex == "Mix"):
        return 'mixed_relay'
    # Check for standard relay (Rel with M or L sex)
    elif distance == 'Rel':
        return 'relay'
    # Check for team sprint
    elif distance == 'Ts':
        return 'team_sprint'
    # Legacy format (not used anymore)
    elif distance == 'Mix':
        return 'mixed_relay'
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
            relay_script_path = f"relay/startlist_scrape_races_{relay_type}.py"
            
            # Determine which distance value corresponds to this relay type
            relay_distance = None
            if relay_type == 'relay' or relay_type == 'mixed_relay':
                relay_distance = 'Rel'
            elif relay_type == 'team_sprint':
                relay_distance = 'Ts'
            
            if relay_distance:
                # For mixed relay, filter by both Distance and Sex
                if relay_type == 'mixed_relay':
                    relay_specific_races = races_df[
                        (races_df['Distance'] == relay_distance) & 
                        (races_df['Sex'].isin(['Mixed', 'Mix']))
                    ]
                else:
                    # For standard relay, filter just by Distance
                    relay_specific_races = races_df[
                        (races_df['Distance'] == relay_distance) &
                        (~races_df['Sex'].isin(['Mixed', 'Mix']))
                    ]
            
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

def process_races() -> None:
    """Main function to process individual races"""
    # Read races CSV (uses test file if TEST_MODE is enabled in .env)
    races_file = get_races_file('ski')
    print(f"Reading {races_file}..." + (" [TEST MODE]" if TEST_MODE else ""))
    try:
        races_df = pd.read_csv(races_file)
        print(f"Successfully read {races_file.name} with {len(races_df)} rows")
    except Exception as e:
        print(f"Error reading {races_file}: {e}")
        traceback.print_exc()
        return
    
    # Check for mixed gender events first
    mixed_races = races_df[races_df['Sex'].isin(['Mixed', 'Mix'])]
    if not mixed_races.empty:
        next_date = find_next_race_date(mixed_races)
        if next_date:
            mixed_next_races = filter_races_by_date(mixed_races, next_date)
            if not mixed_next_races.empty:
                print(f"Found {len(mixed_next_races)} mixed gender races on {next_date}")
                # Process mixed gender races
                if handle_relay_races(mixed_next_races):
                    print("Mixed relay races successfully handled")
    
    # Track if we have any standard races that need processing
    has_standard_races = False
    
    # Find next race dates for each gender separately
    men_standard = process_gender_specific_races(races_df, 'men')
    ladies_standard = process_gender_specific_races(races_df, 'ladies')
    
    # Only call the standard race picks script if we have standard races
    if men_standard or ladies_standard:
        # Check if today's races include Final Climb events
        if check_for_final_climb_today(races_df):
            print("Final Climb event detected - calling final_climb.R instead of race-picks.R")
            call_r_script('races', 'final_climb')
        else:
            call_r_script('races', 'standard')

def process_gender_specific_races(races_df: pd.DataFrame, target_gender: str) -> bool:
    """
    Process races for a specific gender, finding the next available race for that gender
    Returns True if standard races were processed, False otherwise
    """
    print(f"\n===== Processing {target_gender.upper()} races =====")
    
    # Standardize gender values
    gender_map = {
        'men': ['men', 'M', 'Men'],
        'ladies': ['ladies', 'L', 'Ladies', 'Women', 'F']
    }
    
    # Find races for the specified gender
    gender_races = races_df[races_df['Sex'].isin(gender_map[target_gender])]
    if gender_races.empty:
        print(f"No {target_gender} races found in the data")
        return False
    
    # Find next race date for this gender
    next_date = find_next_race_date(gender_races)
    print(f"Next {target_gender} race date is {next_date}")
    
    # Filter races for the next date
    next_races = filter_races_by_date(gender_races, next_date)
    print(f"Found {len(next_races)} {target_gender} races on {next_date}")
    
    # Check if no races were found, or if we should look ahead to tomorrow
    if next_races.empty or should_look_ahead(next_date):
        # Try looking ahead to find next available race
        print(f"Looking ahead to find next available {target_gender} race")
        found_race = False
        look_ahead_days = 7  # Look up to a week ahead
        
        for days_ahead in range(1, look_ahead_days + 1):
            # Calculate the date to check
            check_date = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
            print(f"Checking for {target_gender} races on {check_date}...")
            
            # Find races for this date
            check_races = filter_races_by_date(gender_races, check_date)
            if not check_races.empty:
                next_date = check_date
                next_races = check_races
                found_race = True
                print(f"Found {len(next_races)} {target_gender} races on {next_date}")
                break
        
        if not found_race:
            print(f"No upcoming {target_gender} races found within the next {look_ahead_days} days")
            return False
    
    # Check if all next races are relay events
    if all(is_relay_event(race) for _, race in next_races.iterrows()):
        print(f"All next {target_gender} races are relay events.")
        if handle_relay_races(next_races):
            print(f"Relay races successfully handled by relay scripts.")
            return False  # No standard races to process
        else:
            print(f"Failed to handle relay races with relay scripts, continuing with standard processing...")
    
    # Filter out relays and team sprints
    valid_races = next_races[(next_races['Distance'] != 'Rel') & (next_races['Distance'] != 'Ts') & (next_races['Distance'] != 'Mix')]
    print(f"Found {len(valid_races)} individual {target_gender} races after filtering out relays and team sprints")
    
    if valid_races.empty:
        print(f"No valid individual {target_gender} races found for the next date")
        return False
    
    # Process the races for this gender
    try:
        process_gender_races(valid_races, target_gender)
        return True  # Successfully processed standard races
    except Exception as e:
        print(f"Error processing {target_gender} races: {e}")
        traceback.print_exc()
        return False  # Failed to process races

def should_look_ahead(date_str: str) -> bool:
    """Determine if we should look ahead to the next day based on current time"""
    try:
        # Convert date to datetime
        race_date = pd.to_datetime(date_str)
        today = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
        
        # Get current hour
        current_hour = datetime.now().hour
        
        # If the race date is today and it's after noon, look ahead
        #if race_date == today and current_hour >= 12:
         #   print(f"Current time ({current_hour}:00) is after noon, looking ahead to next day's races")
          #  return True
            
        # If the race date is in the past, look ahead
        if race_date < today:
            print(f"Race date ({date_str}) is in the past, looking ahead to next day's races")
            return True
            
        return False
    except Exception as e:
        print(f"Error in should_look_ahead: {e}")
        return False

def process_gender_races(races_df: pd.DataFrame, gender: str) -> None:
    """Process races for a specific gender"""
    print(f"\nProcessing {gender} races...")
    
    # Get total number of races for this gender
    total_gender_races = len(races_df)
    print(f"Total {gender} races: {total_gender_races}")
    
    # Create enough probability columns for all races
    all_prob_columns = [f'Race{i+1}_Prob' for i in range(total_gender_races)]
    print(f"All probability columns: {all_prob_columns}")
    
    # Get the ELO path - use chrono_pred for most recent Elo values
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    print(f"Using ELO path: {elo_path}")
    
    # Initialize a consolidated dataframe
    consolidated_df = None
    
    # Process each race
    for i, (_, race) in enumerate(races_df.iterrows()):
        url = race['Startlist']
        prob_column = all_prob_columns[i]
        
        print(f"Processing race {i+1} with probability column: {prob_column}")
        
        if not url or pd.isna(url):
            print(f"No startlist URL for race {i+1}, using fallback logic with current season skiers")
            
            # Create fallback startlist with all skiers from current season
            race_df = create_race_fallback_startlist(
                elo_path=elo_path,
                gender=gender,
                race_info=race,
                fantasy_prices=get_fantasy_prices(),
                prob_column=prob_column
            )
        else:
            print(f"Processing race {i+1} from URL: {url}")
            
            # Create startlist for this race from URL
            race_df = create_race_startlist(
                fis_url=url,
                elo_path=elo_path,
                race_info=race,
                gender=gender,
                prob_column=prob_column
            )
        
        if race_df is None:
            print(f"Failed to create startlist for race {i+1}")
            continue
        
        # If this is the first race, initialize the consolidated dataframe
        if consolidated_df is None:
            consolidated_df = race_df
        else:
            # Merge with existing data (ensuring we don't lose any athletes)
            consolidated_df = merge_race_dataframes(consolidated_df, race_df, prob_column)
    
    # If we successfully processed at least one race
    if consolidated_df is not None:
        # Ensure all race probability columns exist in the final dataframe
        for i, col in enumerate(all_prob_columns):
            if col not in consolidated_df.columns:
                print(f"Adding missing column {col}")
                consolidated_df[col] = 0.0
        
        # Create a comprehensive fallback startlist to ensure all current season skiers are included
        print("Creating comprehensive fallback startlist to ensure all current season skiers are included")
        fallback_df = create_race_fallback_startlist(
            elo_path=elo_path,
            gender=gender,
            race_info=races_df.iloc[0],  # Use first race for race info
            fantasy_prices=get_fantasy_prices(),
            prob_column="temp_prob"  # Temporary column
        )
        
        if fallback_df is not None:
            # Get skiers already in the consolidated dataframe
            existing_skiers = set(consolidated_df['Skier'])
            
            # Find skiers from fallback who aren't in the consolidated dataframe
            missing_skiers_df = fallback_df[~fallback_df['Skier'].isin(existing_skiers)]
            print(f"Found {len(missing_skiers_df)} skiers from current season not already in the startlist")
            
            # Drop the temp_prob column
            missing_skiers_df = missing_skiers_df.drop(columns=['temp_prob'])
            
            # Add all race probability columns with 0
            for col in all_prob_columns:
                missing_skiers_df[col] = 0.0
            
            # Combine with the consolidated dataframe
            if len(missing_skiers_df) > 0:
                consolidated_df = pd.concat([consolidated_df, missing_skiers_df], ignore_index=True)
                print(f"Added {len(missing_skiers_df)} skiers from current season to the startlist")
        
        # Make sure In_FIS_List is FALSE for skiers not on any startlist
        # If a skier has 0 probability for all races, they weren't on any startlist
        # Check row by row if all probability values are 0
        consolidated_df.loc[consolidated_df[all_prob_columns].sum(axis=1) == 0, 'In_FIS_List'] = False
        
        # Save the consolidated dataframe
        output_path = f"~/ski/elo/python/ski/polars/excel365/startlist_races_{gender}.csv"
        print(f"Saving consolidated {gender} startlist to {output_path}")
        consolidated_df.to_csv(output_path, index=False)
        print(f"Saved {len(consolidated_df)} {gender} athletes")
    else:
        print(f"No startlist data was generated for {gender}")

def create_race_startlist(fis_url: str, elo_path: str, race_info: pd.Series, gender: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with startlist, prices and ELO scores for an individual race"""
    try:
        # Get race information
        city = race_info['City']
        country = race_info['Country']
        distance = race_info['Distance']
        technique = race_info['Technique']
        
        print(f"Race details: {distance}km {technique} in {city}, {country}")
        
        # Get data from all sources
        fis_athletes = get_fis_startlist(fis_url)
        print(f"Found {len(fis_athletes)} athletes in FIS startlist")
        
        fantasy_prices = get_fantasy_prices()
        
        if not fis_athletes:
            print("FIS startlist is empty, using fallback logic with current season skiers")
            return create_race_fallback_startlist(elo_path, gender, race_info, fantasy_prices, prob_column)
        
        elo_scores = get_latest_elo_scores(elo_path)
        
        # Create data for DataFrame
        data = []
        processed_names = set()
        
        # Process FIS athletes
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
            if 'Skier' in elo_scores.columns and processed_name in elo_scores['Skier'].values:
                elo_match = processed_name
                print(f"Found exact ELO match for: {processed_name}")
            elif 'Skier' in elo_scores.columns:
                # Try fuzzy matching if no exact match
                elo_match = fuzzy_match_name(processed_name, elo_scores['Skier'].tolist())
                if elo_match:
                    print(f"Found fuzzy ELO match: {processed_name} -> {elo_match}")
            
            if elo_match and 'Skier' in elo_scores.columns:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                original_name = elo_match
                skier_id = elo_data.get('ID', None)
                nation = elo_data.get('Nation', fis_nation_code)
            else:
                print(f"No ELO match found for: {processed_name}")
                elo_data = {}
                original_name = processed_name
                skier_id = None
                nation = fis_nation_code
            
            # Determine which ELO columns to prioritize based on race type
            try:
                elo_priority = get_elo_priority(distance, technique)
            except Exception as e:
                print(f"Error determining ELO priority: {e}")
                elo_priority = ['Elo']
            
            # Build base row data
            row_data = {
                'FIS_Name': fis_name,
                'Skier': original_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': True,
                'Price': price
            }
            
            # Add ELO columns if available
            for col in ['Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
                      'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 'Freestyle_Elo']:
                row_data[col] = elo_data.get(col, None)
            
            # Add race-specific ELO and race type
            row_data['Race_Elo'] = get_race_specific_elo(elo_data, elo_priority)
            row_data['Race_Type'] = f"{distance}km {technique}"
            
            # Set race probability
            if prob_column:
                row_data[prob_column] = 1.0  # In startlist = 100% for this race
            
            data.append(row_data)
            processed_names.add(original_name)
        
        # Add race info to all rows
        race_info_dict = {
            'City': city,
            'Country': country,
            'Distance': distance,
            'Technique': technique,
            'Race_Date': race_info['Date']
        }
        
        # Add chronos data for additional analysis and Last_5 features
        try:
            chronos = pd.read_csv(elo_path)
            
            # Identify skiers who competed in previous races
            recent_competitors = get_recent_competitors(chronos, race_info_dict)
            
            # Add recent competitor flag to dataframe
            for row in data:
                skier = row['Skier']
                row['Recent_Competitor'] = skier in recent_competitors
                
        except Exception as e:
            print(f"Error processing chronos data: {e}")
            traceback.print_exc()
            # Set defaults if we can't process chronos
            for row in data:
                row['Recent_Competitor'] = False
        
        # Create DataFrame and add race info columns
        df = pd.DataFrame(data)
        for key, value in race_info_dict.items():
            df[key] = value
            
        # Sort by relevant ELO and price
        if 'Race_Elo' in df.columns and 'Price' in df.columns:
            df = df.sort_values(['Race_Elo', 'Price'], ascending=[False, False])
        
        print(f"Processed startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating race startlist: {e}")
        traceback.print_exc()
        return None

def create_race_fallback_startlist(elo_path: str, gender: str, race_info: pd.Series, fantasy_prices: Dict, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates fallback startlist with all skiers from the current season for race predictions"""
    try:
        print(f"Creating fallback race startlist with all skiers from the current season for {gender}")
        
        # Get race information
        city = race_info['City']
        country = race_info['Country']
        distance = race_info['Distance']
        technique = race_info['Technique']
        
        print(f"Race details: {distance}km {technique} in {city}, {country}")
        
        # Get the most recent ELO scores
        elo_scores = get_latest_elo_scores(elo_path)
        
        # Get chronos data for finding current season skiers
        try:
            import polars as pl
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
        
        # Create data for DataFrame
        data = []
        processed_names = set()
        
        # Define Elo columns
        elo_columns = [
            'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
            'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
            'Classic_Elo', 'Freestyle_Elo'
        ]
        
        # Determine which ELO columns to prioritize based on race type
        try:
            elo_priority = get_elo_priority(distance, technique)
        except Exception as e:
            print(f"Error determining ELO priority: {e}")
            elo_priority = ['Elo']
        
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
            if 'Skier' in elo_scores.columns and skier_name in elo_scores['Skier'].values:
                elo_match = skier_name
            elif 'Skier' in elo_scores.columns:
                # Try fuzzy matching if no exact match
                elo_match = fuzzy_match_name(skier_name, elo_scores['Skier'].tolist())
            
            if elo_match and 'Skier' in elo_scores.columns:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
            else:
                # Use Elo values directly from chronos data as fallback
                elo_data = {}
                # Get Elo values directly from recent record
                for elo_col in elo_columns:
                    if elo_col in recent_record and not pd.isna(recent_record[elo_col]):
                        elo_data[elo_col] = float(recent_record[elo_col])
                    else:
                        elo_data[elo_col] = None
            
            # Get fantasy price
            price = get_fantasy_price(skier_name, fantasy_prices)
            
            # Build row data
            row_data = {
                'FIS_Name': skier_name,
                'Skier': skier_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': False,  # Not in FIS list since this is fallback
                'Price': price
            }
            
            # Add ELO columns
            for col in elo_columns:
                row_data[col] = elo_data.get(col, None)
            
            # Add race-specific ELO and race type
            row_data['Race_Elo'] = get_race_specific_elo(elo_data, elo_priority)
            row_data['Race_Type'] = f"{distance}km {technique}"
            
            # Set race probability for fallback - will be calculated later in R
            if prob_column:
                row_data[prob_column] = 0.0  # Default to 0 for fallback, R will calculate realistic probability
            
            # Add race info
            row_data.update({
                'City': city,
                'Country': country,
                'Distance': distance,
                'Technique': technique,
                'Race_Date': race_info['Date']
            })
            
            # Mark as not a recent competitor for fallback (we don't have startlist info)
            row_data['Recent_Competitor'] = False
            
            data.append(row_data)
            processed_names.add(skier_name)
        
        
        # Create DataFrame and sort by race-specific ELO and price
        df = pd.DataFrame(data)
        if 'Race_Elo' in df.columns and 'Price' in df.columns:
            df = df.sort_values(['Race_Elo', 'Price'], ascending=[False, False])
        
        print(f"Created fallback race startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating fallback race startlist: {e}")
        traceback.print_exc()
        return None

def get_elo_priority(distance: str, technique: str) -> List[str]:
    """Determine which ELO columns to prioritize based on race type"""
    # Handle sprint races (distance < 2)
    try:
        is_sprint = False
        if isinstance(distance, str):
            if distance.strip() == 'SP':
                is_sprint = True
            else:
                # Remove km and try to convert to float
                clean_distance = distance.replace('km', '').strip()
                if clean_distance.replace('.', '', 1).isdigit():
                    is_sprint = float(clean_distance) < 2
    except Exception as e:
        print(f"Error determining if race is sprint: {e}")
        is_sprint = False
    
    if is_sprint:
        if technique == 'C':
            return ['Sprint_C_Elo', 'Sprint_Elo', 'Classic_Elo', 'Elo']
        elif technique == 'F':
            return ['Sprint_F_Elo', 'Sprint_Elo', 'Freestyle_Elo', 'Elo']
        else:
            return ['Sprint_Elo', 'Elo']
    else:
        if technique == 'C':
            return ['Distance_C_Elo', 'Distance_Elo', 'Classic_Elo', 'Elo']
        elif technique == 'F':
            return ['Distance_F_Elo', 'Distance_Elo', 'Freestyle_Elo', 'Elo']
        else:
            return ['Distance_Elo', 'Elo']

def get_race_specific_elo(elo_data: Dict, priority_columns: List[str]) -> float:
    """Get the most relevant ELO score based on priority list"""
    for col in priority_columns:
        if col in elo_data and elo_data[col] is not None:
            try:
                return float(elo_data[col])
            except (TypeError, ValueError):
                continue
    return 0.0  # Default if no matching ELO found

def get_recent_competitors(chronos: pd.DataFrame, race_info: Dict) -> set:
    """Get set of skiers who have competed in similar races recently"""
    try:
        # Filter to current season if Season column exists
        if 'Season' in chronos.columns:
            current_season = get_current_season_from_chronos(chronos)
            current_season_data = chronos[chronos['Season'] == current_season]
        else:
            current_season_data = chronos
        
        # Find races similar to current one
        similar_races = current_season_data
        
        # Filter by Distance if possible
        if 'Distance' in current_season_data.columns:
            similar_races = similar_races[similar_races['Distance'].astype(str) == str(race_info['Distance'])]
        
        # Filter by Technique if possible
        if 'Technique' in current_season_data.columns:
            similar_races = similar_races[similar_races['Technique'] == race_info['Technique']]
        
        # Get unique skiers
        if 'Skier' in similar_races.columns:
            return set(similar_races['Skier'].unique())
        else:
            return set()
    except Exception as e:
        print(f"Error getting recent competitors: {e}")
        return set()


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
    
    # Sort the result by ELO and price
    result = result.sort_values(['Race_Elo', 'Price'], ascending=[False, False])
    
    return result


if __name__ == "__main__":
    process_races()