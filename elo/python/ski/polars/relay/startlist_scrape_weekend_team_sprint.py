#!/usr/bin/env python3
import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional
import warnings
from datetime import datetime
import traceback
import subprocess
import requests
warnings.filterwarnings('ignore')

# Import common utility functions
from startlist_common import *

# Import team sprint functions
from startlist_scrape_races_team_sprint import get_team_sprint_teams

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

def process_weekend_team_sprint_races(races_file: str = None) -> None:
    """
    Process team sprint races for weekend, creating both team and individual CSVs
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing weekend team sprint races with team and individual output")
    
    # Load races from file if provided, otherwise from standard location
    if races_file and os.path.exists(races_file):
        races_df = pd.read_csv(races_file)
        print(f"Loaded {len(races_df)} races from {races_file}")
    else:
        # Default to weekends.csv
        races_path = '~/ski/elo/python/ski/polars/excel365/weekends.csv'
        
        try:
            races_df = pd.read_csv(races_path)
            print(f"Loaded {len(races_df)} races from {races_path}")
            
            # Filter to only team sprint races
            races_df = races_df[races_df['Distance'] == 'Ts']
            print(f"Filtered to {len(races_df)} team sprint races")
            
            # Find next race date
            next_date = find_next_race_date(races_df)
            
            # Filter to races on the next date
            races_df = filter_races_by_date(races_df, next_date)
            print(f"Found {len(races_df)} team sprint races on {next_date}")
            
        except Exception as e:
            print(f"Error loading races from {races_path}: {e}")
            traceback.print_exc()
            return
    
    # Process each gender separately
    men_races = races_df[races_df['Sex'] == 'M']
    ladies_races = races_df[races_df['Sex'] == 'L']
    
    if not men_races.empty:
        process_gender_team_sprint_races(men_races, 'men')
    
    if not ladies_races.empty:
        process_gender_team_sprint_races(ladies_races, 'ladies')

def process_gender_team_sprint_races(races_df: pd.DataFrame, gender: str) -> None:
    """Process team sprint races for a specific gender, creating team and individual CSVs"""
    print(f"\nProcessing {gender} team sprint races")
    
    # Get paths for output files
    team_output_path = f"~/ski/elo/python/ski/polars/relay/excel365/startlist_team_sprint_teams_{gender}.csv"
    individual_output_path = f"~/ski/elo/python/ski/polars/relay/excel365/startlist_team_sprint_individuals_{gender}.csv"
    
    # Create output directories if they don't exist
    os.makedirs(os.path.dirname(os.path.expanduser(team_output_path)), exist_ok=True)
    
    # Get the ELO scores
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    elo_scores = get_latest_elo_scores(elo_path)
    
    # Get fantasy prices including team prices
    fantasy_prices = get_fantasy_prices()
    fantasy_teams = get_fantasy_teams(gender)
    
    # Process each team sprint race
    all_teams_data = []
    all_individuals_data = []
    
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}")
            continue
        
        print(f"Processing team sprint race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the FIS startlist
        teams = get_team_sprint_teams(startlist_url)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams and individuals
            fallback_teams, fallback_individuals = generate_fallback_data(
                gender, fantasy_teams, elo_scores, race
            )
            
            all_teams_data.extend(fallback_teams)
            all_individuals_data.extend(fallback_individuals)
        else:
            # Process the teams
            team_data, individual_data = process_teams_for_csv(
                teams, race, gender, elo_scores, fantasy_prices, fantasy_teams
            )
            
            all_teams_data.extend(team_data)
            all_individuals_data.extend(individual_data)
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(os.path.expanduser(team_output_path), index=False)
        print(f"Saved {len(team_df)} {gender} team sprint teams to {team_output_path}")
    else:
        print(f"No team data generated for {gender}")
    
    # Save individual data
    if all_individuals_data:
        individual_df = pd.DataFrame(all_individuals_data)
        individual_df.to_csv(os.path.expanduser(individual_output_path), index=False)
        print(f"Saved {len(individual_df)} {gender} individual team sprint athletes to {individual_output_path}")
    else:
        print(f"No individual data generated for {gender}")

def get_fantasy_teams(gender: str) -> Dict[str, Dict]:
    """Get team data from Fantasy XC API with gender filter"""
    try:
        response = requests.get('https://www.fantasyxc.se/api/athletes')
        response.raise_for_status()
        
        athletes = response.json()
        
        # Filter for teams (is_team=true) and the specified gender
        gender_code = 'm' if gender == 'men' else 'f'
        teams = {
            athlete['name']: athlete 
            for athlete in athletes 
            if athlete.get('is_team', False) and athlete.get('gender', '') == gender_code
        }
        
        print(f"Found {len(teams)} {gender} teams in Fantasy XC")
        return teams
        
    except Exception as e:
        print(f"Error getting Fantasy XC teams: {e}")
        return {}

# Update the process_teams_for_csv function in startlist_scrape_weekend_team_sprint.py
# Update the process_teams_for_csv function to use team_number
def process_teams_for_csv(
    teams: List[Dict], 
    race: pd.Series, 
    gender: str, 
    elo_scores: pd.DataFrame,
    fantasy_prices: Dict[str, int],
    fantasy_teams: Dict[str, Dict]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Process teams for both team CSV and individual CSV
    
    Returns:
        tuple: (team_data, individual_data)
    """
    team_data = []
    individual_data = []
    
    # Define the list of known team names (from the teams spreadsheet)
    known_teams = [
        "SWEDEN I", "FINLAND I", "NORWAY I", "SWITZERLAND I", "GERMANY I", 
        "UNITED STATES OF AMERICA I", "ITALY I", "FRANCE I", "CZECH REPUBLIC I", 
        "CANADA I", "ESTONIA I", "AUSTRALIA I", "AUSTRIA I", "POLAND I", 
        "KAZAKHSTAN I", "UKRAINE I", "LATVIA I", "CROATIA I", "SLOVAKIA I", 
        "ARGENTINA I", "BRAZIL I", "LITHUANIA I", "GREECE I", "GREAT BRITAIN I", 
        "RUSSIA I", "NORTH MACEDONIA I", "ICELAND I", "PEOPLES REPUBLIC OF CHINA I", 
        "SLOVENIA I", "SERBIA I", "MONGOLIA I", "KOREA I", "TURKEY I", "BELARUS I", "JAPAN I"
    ]
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
        'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Calculate first quartile for each Elo column for imputation
    quartiles = {}
    for col in elo_columns:
        if col in elo_scores.columns:
            numeric_values = pd.to_numeric(elo_scores[col], errors='coerce')
            quartiles[col] = numeric_values.quantile(0.25)
            print(f"First quartile for {col}: {quartiles[col]}")
        else:
            # If column doesn't exist, use default value
            quartiles[col] = 1000
            print(f"Column {col} not found, using default quartile value: 1000")
    
    # Process each team
    for team in teams:
        # Get nation code from team
        nation = team['nation']
        
        # Map to standardized country name
        team_name_part = map_country_to_team_name(nation)
        
        # Skip teams that don't have a match in the team spreadsheet
        if not team_name_part:
            print(f"Skipping team from {nation} - no matching country in team list")
            continue
            
        # Use team number to determine correct suffix
        team_number = team.get('team_number', 1)
        if team_number == 1:
            team_suffix = " I"
        elif team_number == 2:
            team_suffix = " II"
        elif team_number == 3:
            team_suffix = " III"
        elif team_number == 4:
            team_suffix = " IV"
        else:
            team_suffix = f" {team_number}"
            
        # Use exact format from team spreadsheet WITH the correct suffix
        team_name = f"{team_name_part}{team_suffix}"
        print(f"Processing team from {nation} as {team_name}")
        
        # Initialize team info with default values for all Elo types
        team_info = {
            'Team_Name': team_name,
            'Nation': team_name_part,  # Use the standardized country name
            'Team_Rank': team['team_rank'],
            'Race_Date': race['Date'],
            'City': race['City'],
            'Country': race['Country'],
            'Price': 0,
            'Is_Present': True  # This team is in the actual startlist
        }
        
        # Initialize all Elo sums to 0
        for col in elo_columns:
            team_info[col] = 0
        
        # Get team price from fantasy API if available
        for api_team_name, api_team_info in fantasy_teams.items():
            if team_name.lower() == api_team_name.lower():
                team_info['Price'] = api_team_info['price']
                team_info['Team_API_ID'] = api_team_info['athlete_id']
                break
        
        team_members = []
        team_elos = {}
        
        # Initialize Elo sums for each type
        for col in elo_columns:
            team_elos[col] = []
        
        # Track position numbers to avoid duplicates in case of malformed bibs
        position_counter = 0
        position_numbers_used = set()
        
        # Team sprint teams have exactly 2 members
        if len(team['members']) > 2:
            print(f"Warning: Team {team_name} has {len(team['members'])} members, expected 2.")
        
        # Process each member
        for member in team['members']:
            # Handle the bib parsing more safely to deal with incomplete bib formats
            bib = member.get('bib', '')
            try:
                # Split the bib and try to get the position number
                bib_parts = bib.split('-')
                if len(bib_parts) > 1 and bib_parts[1].strip():
                    position_number = int(bib_parts[1])
                else:
                    # If bib is malformed (like "0-"), assign sequential position
                    position_counter += 1
                    position_number = position_counter
                    print(f"Warning: Malformed bib '{bib}' for {member.get('name', 'Unknown')}. Assigning position {position_number}.")
            except (ValueError, IndexError):
                # Handle any parsing errors by assigning sequential position
                position_counter += 1
                position_number = position_counter
                print(f"Warning: Invalid bib format '{bib}' for {member.get('name', 'Unknown')}. Assigning position {position_number}.")
            
            # Ensure no duplicate position numbers
            while position_number in position_numbers_used:
                position_number += 1
            position_numbers_used.add(position_number)
            
            # For team sprint, make sure positions are 1 or 2
            if position_number > 2:
                print(f"Warning: Position {position_number} > 2 for team sprint. Setting to 2.")
                position_number = 2
            
            # Extract just the leg number
            leg_number = str(position_number)
            
            # Process the athlete to match with ELO scores and prices
            athlete_data = process_team_sprint_athlete(
                {
                    'FIS_Name': member['name'],
                    'Skier': member['name'],
                    'Nation': nation,
                    'In_FIS_List': True,
                    'Price': 0,
                    'Team_Name': team_name,  # Use exact format from team spreadsheet with correct suffix
                    'Team_Rank': team['team_rank'],
                    'Team_Time': team.get('team_time', ''),
                    'Team_Position': leg_number  # Just the leg number (1 or 2)
                },
                elo_scores,
                fantasy_prices,
                gender,
                quartiles  # Pass quartiles for imputation
            )
            
            # Add race information
            athlete_data['Race_Date'] = race['Date']
            athlete_data['City'] = race['City']
            athlete_data['Country'] = race['Country']
            
            # Add to individual data
            individual_data.append(athlete_data)
            
            # Extract member info for team record
            team_members.append(athlete_data['Skier'])
            
            # Set member in team info
            team_info[f'Member_{position_number}'] = athlete_data['Skier']
            team_info[f'Member_{position_number}_ID'] = athlete_data.get('ID', None)
            
            # Add all Elo values to team sums
            for col in elo_columns:
                if col in athlete_data and athlete_data[col] is not None:
                    member_elo = float(athlete_data[col])
                    team_info[f'Member_{position_number}_{col}'] = member_elo
                    team_elos[col].append(member_elo)
                else:
                    # Use quartile value if Elo is missing
                    team_info[f'Member_{position_number}_{col}'] = quartiles[col]
                    team_elos[col].append(quartiles[col])
        
        # Calculate combined Elo for each type
        for col in elo_columns:
            team_info[col] = sum(team_elos[col])
        
        # Add team record
        team_data.append(team_info)
    
    return team_data, individual_data

def process_team_sprint_athlete(
    row_data: Dict, 
    elo_scores: pd.DataFrame, 
    fantasy_prices: Dict[str, int],
    gender: str,
    quartiles: Dict[str, float] = None
) -> Dict:
    """Process an individual team sprint athlete to match with ELO scores and prices"""
    try:
        fis_name = row_data['FIS_Name']
        nation = row_data['Nation']
        
        # Define Elo columns to work with
        elo_columns = [
            'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
            'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
            'Classic_Elo', 'Freestyle_Elo'
        ]
        
        # Generate quartiles if not provided
        if quartiles is None:
            quartiles = {}
            for col in elo_columns:
                if col in elo_scores.columns:
                    numeric_values = pd.to_numeric(elo_scores[col], errors='coerce')
                    quartiles[col] = numeric_values.quantile(0.25)
                else:
                    quartiles[col] = 1000  # Default value
        
        # Map country to team name format from team spreadsheet
        team_name_part = map_country_to_team_name(nation)
        
        # IMPORTANT: Don't override the Team_Name that was passed in
        # The row_data['Team_Name'] already has the correct team name with suffix from the parent function
        
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
        
        # STEP 2: Get Fantasy Price
        # Try with original FIS name first
        price = get_fantasy_price(fis_name, fantasy_prices)
        if price == 0:
            # If no price found, try with processed name
            price = get_fantasy_price(processed_name, fantasy_prices)
        
        # Update row data with price
        row_data['Price'] = price
        
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
        
        # Initialize all Elo columns with quartile values
        for col in elo_columns:
            row_data[col] = quartiles[col]
        
        if elo_match:
            elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
            row_data['Skier'] = elo_match  # Update with matched name
            row_data['ID'] = elo_data.get('ID', None)
            
            # Add ELO columns if available, otherwise keep quartile values
            for col in elo_columns:
                if col in elo_data and not pd.isna(elo_data[col]):
                    row_data[col] = elo_data[col]
        else:
            print(f"No ELO match found for: {processed_name}")
            row_data['Skier'] = processed_name
            row_data['ID'] = None
            # Elo columns already initialized with quartile values
        
        return row_data
    
    except Exception as e:
        print(f"Error processing team sprint athlete: {e}")
        traceback.print_exc()
        return row_data  # Return original data if processing fails

# Update the generate_fallback_data function to handle team numbers
def generate_fallback_data(
    gender: str, 
    fantasy_teams: Dict[str, Dict], 
    elo_scores: pd.DataFrame,
    race: pd.Series
) -> Tuple[List[Dict], List[Dict]]:
    """
    Generate fallback team and individual data when startlist is empty
    
    Returns:
        tuple: (team_data, individual_data)
    """
    team_data = []
    individual_data = []
    
    print(f"Generating fallback data for {gender} team sprint teams")
    
    # Define the exact list of team names from the team spreadsheet
    known_teams = [
        "SWEDEN I", "FINLAND I", "NORWAY I", "SWITZERLAND I", "GERMANY I", 
        "UNITED STATES OF AMERICA I", "ITALY I", "FRANCE I", "CZECH REPUBLIC I", 
        "CANADA I", "ESTONIA I", "AUSTRALIA I", "AUSTRIA I", "POLAND I", 
        "KAZAKHSTAN I", "UKRAINE I", "LATVIA I", "CROATIA I", "SLOVAKIA I", 
        "ARGENTINA I", "BRAZIL I", "LITHUANIA I", "GREECE I", "GREAT BRITAIN I", 
        "RUSSIA I", "NORTH MACEDONIA I", "ICELAND I", "PEOPLES REPUBLIC OF CHINA I", 
        "SLOVENIA I", "SERBIA I", "MONGOLIA I", "KOREA I", "TURKEY I", "BELARUS I", "JAPAN I"
    ]
    
    # Also add known second teams
    known_second_teams = [
        "SWEDEN II", "FINLAND II", "NORWAY II", "SWITZERLAND II", "GERMANY II", 
        "UNITED STATES OF AMERICA II", "ITALY II", "FRANCE II", "CZECH REPUBLIC II", 
        "CANADA II", "ESTONIA II", "RUSSIA II", "NORWAY III", "SWEDEN III"
    ]
    
    # Combine known teams
    all_known_teams = known_teams + known_second_teams
    
    # Rest of the function remains the same
    # ...
    # Get chronos data for finding skiers who competed in the most recent season
    chronos_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
        'Classic_Elo', 'Freestyle_Elo'
    ]
    
    try:
        # Load the chronos data to get most recent Elo values
        chronos = pd.read_csv(chronos_path)
        
        # Find the maximum season in the data
        max_season = chronos['Season'].max()
        print(f"Found max season: {max_season}")
        
        # Get ALL skiers who competed in the most recent season
        recent_skiers = chronos[chronos['Season'] == max_season]
        
        # Calculate first quartile for each Elo column from recent skiers
        quartiles = {}
        for elo_col in elo_columns:
            if elo_col in recent_skiers.columns:
                numeric_values = pd.to_numeric(recent_skiers[elo_col], errors='coerce')
                quartiles[elo_col] = numeric_values.quantile(0.25)
                print(f"First quartile for {elo_col}: {quartiles[elo_col]}")
            else:
                # If column doesn't exist, use default value
                quartiles[elo_col] = 1000
                print(f"Column {elo_col} not found, using default quartile value: 1000")
        
        # Group skiers by nation
        nations_skiers = recent_skiers.groupby('Nation')['Skier'].unique().to_dict()
        print(f"Found {len(nations_skiers)} nations with skiers in season {max_season}")
        
        # Get the most recent Elo values for each skier
        skier_elo_values = {}
        
        # Sort chronos data by date (if available) to get most recent records
        if 'Date' in chronos.columns:
            chronos['Date'] = pd.to_datetime(chronos['Date'], errors='coerce')
            chronos = chronos.sort_values('Date')
        
        # Process all records to get the most recent Elo values for each skier
        for _, row in chronos.iterrows():
            skier = row['Skier']
            
            # Initialize skier data if not already present
            if skier not in skier_elo_values:
                skier_elo_values[skier] = {
                    'ID': row.get('ID'),
                    'Nation': row.get('Nation')
                }
                
                # Initialize with quartiles
                for elo_col in elo_columns:
                    skier_elo_values[skier][elo_col] = quartiles.get(elo_col, 1000)
            
            # Update with actual values from this row (which will be the most recent due to sorting)
            for elo_col in elo_columns:
                if elo_col in row and not pd.isna(row[elo_col]):
                    skier_elo_values[skier][elo_col] = float(row[elo_col])
        
        # First, create individual records for ALL skiers from the current year
        for nation, skier_list in nations_skiers.items():
            # Map nation to exact team name format from team spreadsheet
            team_name_part = map_country_to_team_name(nation)
            
            # Skip nations that don't have a team in the known list
            if not team_name_part:
                print(f"Skipping nation {nation} - no matching team found")
                continue
                
            # Use exact format from team spreadsheet
            team_name = f"{team_name_part} I"
            print(f"Including skiers from {nation} as {team_name}")
            
            for skier in skier_list:
                # Initialize individual record with default values
                individual_record = {
                    'FIS_Name': skier,
                    'Skier': skier,
                    'ID': skier_elo_values.get(skier, {}).get('ID'),
                    'Nation': nation,
                    'In_FIS_List': False,
                    'Price': 0,  # Individual prices not available
                    'Team_Name': team_name,  # Use exact format from team spreadsheet
                    'Team_Rank': 0,
                    'Team_Time': '',
                    'Team_Position': '',  # No specific position
                    'Race_Date': race['Date'],
                    'City': race['City'],
                    'Country': race['Country']
                }
                
                # Add Elo values directly
                if skier in skier_elo_values:
                    for elo_col in elo_columns:
                        individual_record[elo_col] = skier_elo_values[skier].get(elo_col, quartiles[elo_col])
                else:
                    # Use quartile values if no data available
                    for elo_col in elo_columns:
                        individual_record[elo_col] = quartiles[elo_col]
                
                individual_data.append(individual_record)
        
    except Exception as e:
        print(f"Error reading chronos data: {e}")
        traceback.print_exc()
        nations_skiers = {}
        skier_elo_values = {}
        
        # If an error occurred, set default quartiles
        quartiles = {elo_col: 1000 for elo_col in elo_columns}
    
    # Use the exact known team names from the team spreadsheet
    for team_name in known_teams:
        # Remove the "I" suffix to get the country name
        country_name = team_name[:-2].strip()
        
        # Create team record
        team_record = {
            'Team_Name': team_name,
            'Nation': country_name,
            'Team_Rank': 0,  # No rank since it's not from a startlist
            'Race_Date': race['Date'],
            'City': race['City'],
            'Country': race['Country'],
            'Is_Present': False  # This team is not in the actual startlist
        }
        
        # Initialize Elo sum columns for team
        for elo_col in elo_columns:
            team_record[elo_col] = 0
        
        # Find matching fantasy team if available
        fantasy_team = None
        for api_team_name, api_team_info in fantasy_teams.items():
            # Try to match team names
            if team_name.lower() == api_team_name.lower():
                fantasy_team = api_team_info
                break
        
        # Add fantasy data if available
        if fantasy_team:
            team_record['Price'] = fantasy_team['price']
            team_record['Team_API_ID'] = fantasy_team['athlete_id']
        else:
            team_record['Price'] = 0
            team_record['Team_API_ID'] = None
        
        # Find skiers for this country 
        nation_skiers_found = False
        nation_skier_list = []
        
        # Look for skiers from this country with different code variations
        for nation, skiers in nations_skiers.items():
            # Map nation to team format and check if it matches this team
            if map_country_to_team_name(nation) == country_name:
                nation_skier_list = skiers
                nation_skiers_found = True
                break
        
        # If we have skiers for this nation, add them to the team
        if nation_skiers_found and len(nation_skier_list) > 0:
            # Get Elo scores for these skiers
            nation_skiers_data = []
            
            # Get Elo scores for all skiers from this nation
            for skier in nation_skier_list:
                if skier in skier_elo_values:
                    # Get skier data with Elo values
                    skier_data = {
                        'name': skier,
                        'ID': skier_elo_values[skier].get('ID')
                    }
                    
                    # Add all Elo values
                    for elo_col in elo_columns:
                        skier_data[elo_col] = skier_elo_values[skier].get(elo_col, quartiles[elo_col])
                    
                    nation_skiers_data.append(skier_data)
            
            # Sort by main Elo (highest first) and take up to 2 skiers for team sprint
            nation_skiers_data.sort(key=lambda x: x.get('Elo', 0), reverse=True)
            selected_skiers = nation_skiers_data[:2]  # Only 2 skiers for team sprint
            
            # If we don't have 2 skiers, fill with dummy entries
            while len(selected_skiers) < 2:
                dummy_skier = {
                    'name': f"Unknown {country_name} Skier {len(selected_skiers) + 1}",
                    'ID': None
                }
                # Add quartile values for Elo columns
                for elo_col in elo_columns:
                    dummy_skier[elo_col] = quartiles[elo_col]
                    
                selected_skiers.append(dummy_skier)
            
            # Add skiers to team record and sum up Elo values
            for i, skier_data in enumerate(selected_skiers, 1):
                # Add skier info
                team_record[f'Member_{i}'] = skier_data['name']
                team_record[f'Member_{i}_ID'] = skier_data['ID']
                
                # Add individual Elo values and update team totals
                for elo_col in elo_columns:
                    team_record[f'Member_{i}_{elo_col}'] = skier_data.get(elo_col, quartiles.get(elo_col, 1000))
                    team_record[elo_col] += skier_data.get(elo_col, quartiles.get(elo_col, 1000))  # Add to team total
        else:
            # If no skiers found, add dummy entries
            for i in range(1, 3):  # Team sprint has 2 members
                team_record[f'Member_{i}'] = f"Unknown {country_name} Skier {i}"
                team_record[f'Member_{i}_ID'] = None
                
                # Add quartile values for all Elo columns
                for elo_col in elo_columns:
                    team_record[f'Member_{i}_{elo_col}'] = quartiles[elo_col]
                    team_record[elo_col] += quartiles[elo_col]  # Add to team total
        
        team_data.append(team_record)
    
    return team_data, individual_data

def map_country_to_team_name(country: str) -> str:
    """
    Map country names from individuals to exact team names from team spreadsheet
    Returns empty string if no match found
    """
    # Direct mapping from individual country names AND codes to team names (without "I" suffix)
    country_to_team = {
        # Full country names mapping
        "Andorra": "",  # Not in team list
        "Argentina": "ARGENTINA",
        "Armenia": "",  # Not in team list
        "Australia": "AUSTRALIA",
        "Austria": "AUSTRIA",
        "Bosnia&Herzegovina": "",  # Not in team list
        "Brazil": "BRAZIL",
        "Bulgaria": "",  # Not in team list
        "Canada": "CANADA",
        "Chile": "",  # Not in team list
        "China": "PEOPLES REPUBLIC OF CHINA", 
        "Croatia": "CROATIA",
        "Czechia": "CZECH REPUBLIC",
        "Estonia": "ESTONIA",
        "Finland": "FINLAND",
        "France": "FRANCE",
        "Germany": "GERMANY",
        "Greece": "GREECE",
        "Haiti": "",  # Not in team list
        "Hungary": "",  # Not in team list
        "Iceland": "ICELAND",
        "India": "",  # Not in team list
        "Iran": "",  # Not in team list
        "Italy": "ITALY",
        "Japan": "JAPAN",
        "Kazakhstan": "KAZAKHSTAN",
        "Latvia": "LATVIA",
        "Lebanon": "",  # Not in team list
        "Lithuania": "LITHUANIA",
        "Malaysia": "",  # Not in team list
        "Mexico": "",  # Not in team list
        "Mongolia": "MONGOLIA",
        "North Macedonia": "NORTH MACEDONIA",
        "Norway": "NORWAY",
        "Poland": "POLAND",
        "Romania": "",  # Not in team list
        "Serbia": "SERBIA",
        "Slovakia": "SLOVAKIA",
        "Slovenia": "SLOVENIA",
        "South Korea": "KOREA",
        "Sweden": "SWEDEN",
        "Switzerland": "SWITZERLAND",
        "Taiwan": "",  # Not in team list
        "Turkey": "TURKEY",
        "USA": "UNITED STATES OF AMERICA",
        "Ukraine": "UKRAINE",
        "Great Britain": "GREAT BRITAIN",
        "Belarus": "BELARUS",
        "Russia": "RUSSIA",
        "Korea": "KOREA",
        
        # 3-letter country codes mapping - needed for scrape data
        "AND": "",  # Not in team list
        "ARG": "ARGENTINA",
        "ARM": "",  # Not in team list
        "AUS": "AUSTRALIA",
        "AUT": "AUSTRIA",
        "BIH": "",  # Not in team list
        "BLR": "BELARUS",
        "BRA": "BRAZIL",
        "BUL": "",  # Not in team list
        "CAN": "CANADA",
        "CHI": "",  # Not in team list
        "CHN": "PEOPLES REPUBLIC OF CHINA",
        "CRO": "CROATIA",
        "CZE": "CZECH REPUBLIC",
        "EST": "ESTONIA",
        "FIN": "FINLAND",
        "FRA": "FRANCE",
        "GBR": "GREAT BRITAIN",
        "GER": "GERMANY",
        "GRE": "GREECE",
        "HAI": "",  # Not in team list
        "HUN": "",  # Not in team list
        "ISL": "ICELAND",
        "IND": "",  # Not in team list
        "IRI": "",  # Not in team list
        "ITA": "ITALY",
        "JPN": "JAPAN",
        "KAZ": "KAZAKHSTAN",
        "KOR": "KOREA",
        "LAT": "LATVIA",
        "LBN": "",  # Not in team list
        "LTU": "LITHUANIA",
        "MAS": "",  # Not in team list
        "MEX": "",  # Not in team list
        "MGL": "MONGOLIA",
        "MKD": "NORTH MACEDONIA",
        "NOR": "NORWAY",
        "POL": "POLAND",
        "ROU": "",  # Not in team list
        "RUS": "RUSSIA",
        "SRB": "SERBIA",
        "SVK": "SLOVAKIA",
        "SLO": "SLOVENIA",
        "SWE": "SWEDEN",
        "SUI": "SWITZERLAND",
        "TPE": "",  # Not in team list
        "TUR": "TURKEY",
        "USA": "UNITED STATES OF AMERICA",
        "UKR": "UKRAINE"
    }
    
    # Get mapped team name
    return country_to_team.get(country, "")

# Update format_team_name in startlist_scrape_weekend_team_sprint.py
def format_team_name(name: str) -> str:
    """
    Format team name to match API format.
    Preserve existing suffixes (I, II, III) or add I if none exists.
    """
    name = name.upper().strip()
    
    # Check for existing suffixes
    if ' I ' in name or name.endswith(' I'):
        return name
    elif ' II ' in name or name.endswith(' II'):
        return name
    elif ' III ' in name or name.endswith(' III'):
        return name
    elif ' IV ' in name or name.endswith(' IV'):
        return name
    else:
        return f"{name} I"

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Process team sprint races with the enhanced functionality
    process_weekend_team_sprint_races(races_file)
    call_r_script('weekend', 'team_sprint')