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
    # Read races CSV
    print("Reading races.csv...")
    try:
        races_df = pd.read_csv('~/ski/elo/python/ski/polars/excel365/races.csv')
        print(f"Successfully read races.csv with {len(races_df)} rows")
    except Exception as e:
        print(f"Error reading races.csv: {e}")
        traceback.print_exc()
        try:
            # Try relative path as fallback
            races_df = pd.read_csv('races.csv')
            print(f"Successfully read races.csv with {len(races_df)} rows")
        except Exception as e:
            print(f"Error reading races.csv with relative path: {e}")
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
    
    # Get the ELO path
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_elevation.csv"
    print(f"Using ELO path: {elo_path}")
    
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
        race_df = create_race_startlist(
            fis_url=url,
            elo_path=elo_path,
            race_info=race
        )
        
        if race_df is None:
            print(f"Failed to create startlist for race {i+1}")
            continue
        
        # If this is the first race, initialize the consolidated dataframe
        if consolidated_df is None:
            consolidated_df = race_df
        else:
            # Merge with existing data
            consolidated_df = pd.concat([consolidated_df, race_df], ignore_index=True)
            consolidated_df = consolidated_df.drop_duplicates(subset=['Skier'], keep='first')
    
    # If we successfully processed at least one race
    if consolidated_df is not None:
        # Save the consolidated dataframe
        output_path = f"~/ski/elo/python/ski/polars/excel365/startlist_races_{gender}.csv"
        print(f"Saving consolidated {gender} startlist to {output_path}")
        consolidated_df.to_csv(output_path, index=False)
        print(f"Saved {len(consolidated_df)} {gender} athletes")
    else:
        print(f"No startlist data was generated for {gender}")

def create_race_startlist(fis_url: str, elo_path: str, race_info: pd.Series) -> Optional[pd.DataFrame]:
    """Creates DataFrame with startlist, prices and ELO scores for an individual race"""
    try:
        # Get race information
        city = race_info['City']
        country = race_info['Country']
        distance = race_info['Distance']
        technique = race_info['Technique']
        gender = race_info['Sex']
        # Convert gender to standard format if needed
        if gender in ['M', 'Men']:
            gender = 'men'
        elif gender in ['L', 'Ladies', 'Women', 'F']:
            gender = 'ladies'
        
        print(f"Race details: {distance}km {technique} in {city}, {country}")
        
        # Get data from all sources
        fis_athletes = get_fis_startlist(fis_url)
        print(f"Found {len(fis_athletes)} athletes in FIS startlist")
        
        if not fis_athletes:
            print("FIS startlist is empty, skipping")
            return None
        
        fantasy_prices = get_fantasy_prices()
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
        
        # Add chronos data for additional analysis
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
            # Set all as not recent competitors if we can't process chronos
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

if __name__ == "__main__":
    process_races()