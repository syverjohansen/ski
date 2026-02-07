#!/usr/bin/env python3
import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional
import warnings
from datetime import datetime
import traceback
from bs4 import BeautifulSoup
import requests
warnings.filterwarnings('ignore')

# Import common utility functions
from startlist_common import *

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

def process_mixed_relay_races(races_file: str = None) -> None:
    """
    Main function to process mixed relay races
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print(f"Processing mixed relay races")
    
    # Load races from file if provided, otherwise from standard location
    if races_file and os.path.exists(races_file):
        races_df = pd.read_csv(races_file)
        print(f"Loaded {len(races_df)} races from {races_file}")
    else:
        # Default to standard races.csv or weekends.csv
        # Determine if this was called from weekend or races script based on file name
        script_name = os.path.basename(sys.argv[0])
        if 'weekend' in script_name:
            races_path = '~/ski/elo/python/ski/polars/excel365/weekends.csv'
        else:
            races_path = '~/ski/elo/python/ski/polars/excel365/races.csv'
        
        try:
            races_df = pd.read_csv(races_path)
            print(f"Loaded {len(races_df)} races from {races_path}")
            
            # Filter to only mixed relay races (Rel with gender code 'X' or mixed events)
            races_df = races_df[(races_df['Distance'] == 'Rel') & 
                                ((races_df['Sex'] == 'Mixed') | 
                                 (races_df['Sex'].str.contains('mixed', case=False, na=False)))]
            print(f"Filtered to {len(races_df)} mixed relay races")
            
            # Find next race date
            next_date = find_next_race_date(races_df)
            
            # Filter to races on the next date
            races_df = filter_races_by_date(races_df, next_date)
            print(f"Found {len(races_df)} mixed relay races on {next_date}")
            
        except Exception as e:
            print(f"Error loading races from {races_path}: {e}")
            traceback.print_exc()
            return
    
    # Initialize data collections for all races
    all_teams_data = []
    all_individuals_data = []
    
    # Process each mixed relay race
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}, skipping")
            continue
        
        print(f"Processing mixed relay race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the FIS startlist
        teams = get_mixed_relay_teams(startlist_url)
        if not teams:
            print(f"No teams found for race {idx+1}, skipping")
            continue
        
        # Process the teams and create team and individual data
        teams_data, individuals_data = process_mixed_relay_teams(teams, race)
        
        # Add the data to our collections
        all_teams_data.extend(teams_data)
        all_individuals_data.extend(individuals_data)
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        save_mixed_relay_team_data(team_df)
    else:
        print(f"No team data generated")
    
    # Save individual data
    if all_individuals_data:
        individual_df = pd.DataFrame(all_individuals_data)
        save_mixed_relay_individual_data(individual_df)
    else:
        print(f"No individual data generated")

def get_mixed_relay_teams(url: str, fantasy_prices: Dict[str, Dict] = None) -> List[Dict]:
    """
    Get teams from FIS mixed relay startlist
    
    Args:
        url: The URL of the FIS mixed relay startlist
        fantasy_prices: Optional dictionary of fantasy prices for athletes
    
    Returns list of teams with structure:
    [
        {
            'team_name': 'COUNTRY I',
            'nation': 'XXX',
            'team_rank': 1,
            'team_time': '54:45.3',
            'members': [
                {'name': 'ATHLETE NAME', 'nation': 'XXX', 'year': '1994', 'bib': '2-1', 'sex': 'M'},
                {'name': 'ATHLETE NAME', 'nation': 'XXX', 'year': '1997', 'bib': '2-2', 'sex': 'F'},
                ...
            ]
        },
        ...
    ]
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        teams = []
        
        # Track team numbers by nation
        nation_team_counts = {}
        
        # Find all team rows (main rows) - these have class 'table-row_theme_main'
        team_rows = soup.select('a.table-row.table-row_theme_main')
        
        print(f"Found {len(team_rows)} team rows")
        
        for team_row in team_rows:
            # Get team rank
            rank_elem = team_row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold')
            if not rank_elem:
                print("No rank element found, skipping row")
                continue
            
            team_rank = rank_elem.text.strip()
            
            # Get team name
            team_name_elem = team_row.select_one('.g-lg-14.g-md-14.g-sm-11.g-xs-10.justify-left.bold')
            if not team_name_elem:
                print("No team name element found, skipping row")
                continue
            
            team_name = team_name_elem.text.strip()
            
            # Get nation code
            nation_elem = team_row.select_one('.country__name-short')
            nation = nation_elem.text.strip() if nation_elem else ""
            
            if not nation:
                print(f"No nation found for team {team_name}, skipping")
                continue
            
            # Update team counter for this nation
            if nation not in nation_team_counts:
                nation_team_counts[nation] = 1
            else:
                nation_team_counts[nation] += 1
            
            # Get team number for this nation
            team_number = nation_team_counts[nation]
            
            # Get team time if available
            team_time = ""
            time_elem = team_row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs')
            if time_elem:
                team_time = time_elem.text.strip()
            
            team_data = {
                'team_name': team_name,
                'nation': nation,
                'team_rank': team_rank,
                'team_time': team_time,
                'team_number': team_number,
                'members': []
            }
            
            print(f"Processing team: {team_name} (Rank {team_rank}, Nation {nation})")
            
            # Find all athlete rows that follow this team row
            # Athlete rows have class 'table-row_theme_additional'
            current_element = team_row
            athlete_count = 0
            
            while True:
                current_element = current_element.find_next_sibling('a')
                if not current_element:
                    break
                
                # Check if this is an athlete row (additional theme) or next team row (main theme)
                if 'table-row_theme_main' in current_element.get('class', []):
                    # We've reached the next team, stop processing athletes
                    break
                
                if 'table-row_theme_additional' in current_element.get('class', []):
                    # This is an athlete row
                    try:
                        # Get athlete name
                        athlete_name_elem = current_element.select_one('.g-lg-14.g-md-14.g-sm-11.g-xs-10.justify-left.bold')
                        if not athlete_name_elem:
                            print("No athlete name found in athlete row")
                            continue
                        
                        athlete_name = athlete_name_elem.text.strip()
                        
                        # Get birth year
                        year_elem = current_element.select_one('.g-lg-1.g-md-1.g-sm-2.g-xs-3.justify-left')
                        year = year_elem.text.strip() if year_elem else ''
                        
                        # Get athlete bib
                        bib_elem = current_element.select_one('.bib')
                        bib = bib_elem.text.strip() if bib_elem else ''
                        
                        # Determine gender based on position in mixed relay
                        # Typically: positions 1/3 are men, 2/4 are women
                        try:
                            if '-' in bib:
                                leg_position = int(bib.split('-')[1])
                                sex = 'M' if leg_position % 2 == 1 else 'F'  # Odd positions: men, Even: women
                            else:
                                # Fallback: alternate based on order
                                sex = 'M' if athlete_count % 2 == 0 else 'F'
                        except (ValueError, IndexError):
                            # If we can't parse the bib, alternate based on order
                            sex = 'M' if athlete_count % 2 == 0 else 'F'
                        
                        # If we have fantasy prices, try to verify gender
                        if fantasy_prices:
                            for athlete_id, athlete_data in fantasy_prices.items():
                                if isinstance(athlete_data, dict) and athlete_data.get('name', '').lower() == athlete_name.lower():
                                    gender_code = athlete_data.get('gender')
                                    if gender_code == 'm':
                                        sex = 'M'
                                    elif gender_code == 'f':
                                        sex = 'F'
                                    break
                        
                        team_data['members'].append({
                            'name': athlete_name,
                            'nation': nation,
                            'year': year,
                            'bib': bib,
                            'sex': sex
                        })
                        
                        athlete_count += 1
                        print(f"  Added athlete: {athlete_name} (bib: {bib}, sex: {sex})")
                        
                    except Exception as e:
                        print(f"Error processing athlete row: {e}")
                        continue
            
            print(f"Team {team_name} has {len(team_data['members'])} members")
            teams.append(team_data)
        
        print(f"Found {len(teams)} teams with {sum(len(team['members']) for team in teams)} total athletes")
        return teams
        
    except Exception as e:
        print(f"Error getting mixed relay teams: {e}")
        import traceback
        traceback.print_exc()
        return []

def process_mixed_relay_teams(teams: List[Dict], race: pd.Series) -> Tuple[List[Dict], List[Dict]]:
    """
    Process mixed relay teams and create team and individual data
    
    Args:
        teams: List of teams with members
        race: Race information
        
    Returns:
        tuple: (team_data, individual_data)
    """
    # Initialize data for teams and individual athletes
    team_data = []
    individual_data = []
    
    # Get the ELO scores for men and women
    men_elo_path = "~/ski/elo/python/ski/polars/excel365/men_chrono_pred.csv"
    women_elo_path = "~/ski/elo/python/ski/polars/excel365/ladies_chrono_pred.csv"
    
    men_elo_scores = get_latest_elo_scores(men_elo_path)
    women_elo_scores = get_latest_elo_scores(women_elo_path)
    
    # Get fantasy prices including team prices
    fantasy_prices = get_fantasy_prices()
    mixed_fantasy_teams = get_fantasy_teams('mixed')
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
        'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Calculate first quartile for each Elo column for imputation - separately for men and women
    men_quartiles = {}
    women_quartiles = {}
    
    for col in elo_columns:
        # Men's quartiles
        if col in men_elo_scores.columns:
            numeric_values = pd.to_numeric(men_elo_scores[col], errors='coerce')
            men_quartiles[col] = numeric_values.quantile(0.25)
            print(f"Men first quartile for {col}: {men_quartiles[col]}")
        else:
            # If column doesn't exist, use default value
            men_quartiles[col] = 1000
            print(f"Men column {col} not found, using default quartile value: 1000")
            
        # Women's quartiles
        if col in women_elo_scores.columns:
            numeric_values = pd.to_numeric(women_elo_scores[col], errors='coerce')
            women_quartiles[col] = numeric_values.quantile(0.25)
            print(f"Women first quartile for {col}: {women_quartiles[col]}")
        else:
            # If column doesn't exist, use default value
            women_quartiles[col] = 1000
            print(f"Women column {col} not found, using default quartile value: 1000")
    
    # Process each team
    for team in teams:
        # Map to standardized country name
        nation = team['nation']
        
        # Get original team name from FIS data and extract suffix if present
        fis_team_name = team['team_name'].strip()
        team_suffix = " I"  # Default suffix
        
        # Determine if this is a secondary team (II, III, etc.)
        if " II" in fis_team_name:
            team_suffix = " II"
        elif " III" in fis_team_name:
            team_suffix = " III"
        elif " IV" in fis_team_name:
            team_suffix = " IV"
        
        team_name_part = map_country_to_team_name(nation)
        
        # Skip teams that don't have a match in the team spreadsheet
        if not team_name_part:
            print(f"Skipping team from {nation} - no matching country in team list")
            continue
            
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
            'Is_Present': True,  # This team is in the actual startlist
            'Race_Type': 'Mixed Relay'
        }
        
        # Initialize all Elo sums to 0
        for col in elo_columns:
            team_info[col] = 0
        
        # Get team price from fantasy API if available
        for api_team_name, api_team_info in mixed_fantasy_teams.items():
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
        
        # Process each member
        for member in team['members']:
            # Determine gender from the member data
            is_male = member.get('sex', 'M') == 'M'
            gender = 'men' if is_male else 'ladies'
            
            # Select the appropriate ELO scores and quartiles based on gender
            elo_scores = men_elo_scores if is_male else women_elo_scores
            quartiles = men_quartiles if is_male else women_quartiles
            
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
            
            # Extract just the leg number from the bib
            leg_number = str(position_number)
            
            # Prepare the base row data
            row_data = {
                'FIS_Name': member['name'],
                'Skier': member['name'],  # Will be updated if we find a match
                'Nation': nation,
                'In_FIS_List': True,
                'Price': 0,
                'Team_Name': team_name,  # Use exact format from team spreadsheet
                'Team_Rank': team['team_rank'],
                'Team_Time': team.get('team_time', ''),
                'Team_Position': leg_number,  # Just the leg number
                'Sex': member.get('sex', 'M'),  # Add sex to individual data
                'Race_Type': 'Mixed Relay'
            }
            
            # Process the athlete to match with ELO scores and prices
            processed_data = process_mixed_relay_athlete(
                row_data, 
                elo_scores, 
                fantasy_prices,
                gender,
                quartiles
            )
            
            # Add race information
            processed_data['Race_Date'] = race['Date']
            processed_data['City'] = race['City']
            processed_data['Country'] = race['Country']
            
            # Add to individual data
            individual_data.append(processed_data)
            
            # Extract member info for team record
            team_members.append(processed_data['Skier'])
            
            # Set member in team info
            team_info[f'Member_{position_number}'] = processed_data['Skier']
            team_info[f'Member_{position_number}_ID'] = processed_data.get('ID', None)
            team_info[f'Member_{position_number}_Sex'] = processed_data['Sex']
            
            # Add all Elo values to team sums
            for col in elo_columns:
                if col in processed_data and processed_data[col] is not None:
                    member_elo = float(processed_data[col])
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

def process_mixed_relay_athlete(
    row_data: Dict, 
    elo_scores: pd.DataFrame, 
    fantasy_prices: Dict[str, Dict],
    gender: str,
    quartiles: Dict[str, float] = None
) -> Dict:
    """Process an individual mixed relay athlete to match with ELO scores and prices"""
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
        # Try extracting price from fantasy data
        price = 0
        for athlete_id, athlete_data in fantasy_prices.items():
            # Skip if athlete_data is not a dictionary
            if not isinstance(athlete_data, dict):
                continue
                
            # Try exact match first
            if processed_name.lower() == athlete_data.get('name', '').lower():
                price = athlete_data.get('price', 0)
                break
            
            # Try alternate name formats if needed
            if fis_name.lower() == athlete_data.get('name', '').lower():
                price = athlete_data.get('price', 0)
                break
        
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
        print(f"Error processing mixed relay athlete: {e}")
        traceback.print_exc()
        return row_data  # Return original data if processing fails

def save_mixed_relay_individual_data(df: pd.DataFrame) -> None:
    """Save processed mixed relay individual data to a CSV file"""
    try:
        # Sort by team rank and position
        df = df.sort_values(['Team_Rank', 'Team_Position'])
        
        # Save to CSV
        output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_relay_races_individuals.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} mixed relay individual athletes to {output_path}")
    
    except Exception as e:
        print(f"Error saving mixed relay individual data: {e}")
        traceback.print_exc()

def save_mixed_relay_team_data(df: pd.DataFrame) -> None:
    """Save processed mixed relay team data to a CSV file"""
    try:
        # Sort by team rank
        df = df.sort_values(['Team_Rank'])
        
        # Save to CSV
        output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_relay_races_teams.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} mixed relay teams to {output_path}")
    
    except Exception as e:
        print(f"Error saving mixed relay team data: {e}")
        traceback.print_exc()

def get_fantasy_teams(gender: str) -> Dict[str, Dict]:
    """Get team data from Fantasy XC API with gender filter"""
    try:
        response = requests.get('https://www.fantasyxc.se/api/athletes')
        response.raise_for_status()
        
        athletes = response.json()
        
        # Filter for teams (is_team=true) and the specified gender
        gender_code = 'm' if gender == 'men' else 'f' if gender == 'ladies' else 'mixed'
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

def map_country_to_team_name(country: str) -> str:
    """
    Map country names from individuals to exact team names from team spreadsheet
    Returns empty string if no match found
    """
    # Direct mapping from individual country names AND codes to team names (without "I" suffix)
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

def reverse_map_country_code(code: str) -> str:
    """
    Map country codes to full country names
    
    Args:
        code: The country code (e.g., 'NOR', 'SWE')
    
    Returns:
        The full country name or the original code if not found
    """
    # Dictionary mapping country codes to full names
    code_to_country = {
        # 3-letter country codes
        "AND": "Andorra",
        "ARG": "Argentina",
        "ARM": "Armenia",
        "AUS": "Australia",
        "AUT": "Austria",
        "BIH": "Bosnia & Herzegovina",
        "BLR": "Belarus",
        "BRA": "Brazil",
        "BUL": "Bulgaria",
        "CAN": "Canada",
        "CHI": "Chile",
        "CHN": "China",
        "CRO": "Croatia",
        "CZE": "Czech Republic",
        "EST": "Estonia",
        "FIN": "Finland",
        "FRA": "France",
        "GBR": "Great Britain",
        "GER": "Germany",
        "GRE": "Greece",
        "HUN": "Hungary",
        "ISL": "Iceland",
        "IND": "India",
        "IRI": "Iran",
        "ITA": "Italy",
        "JPN": "Japan",
        "KAZ": "Kazakhstan",
        "KOR": "South Korea",
        "LAT": "Latvia",
        "LBN": "Lebanon",
        "LTU": "Lithuania",
        "MAS": "Malaysia",
        "MEX": "Mexico",
        "MGL": "Mongolia",
        "MKD": "North Macedonia",
        "NOR": "Norway",
        "POL": "Poland",
        "ROU": "Romania",
        "RUS": "Russia",
        "SRB": "Serbia",
        "SVK": "Slovakia",
        "SLO": "Slovenia",
        "SWE": "Sweden",
        "SUI": "Switzerland",
        "TPE": "Taiwan",
        "TUR": "Turkey",
        "USA": "United States of America",
        "UKR": "Ukraine"
    }
    
    # Return the full country name or the original code if not found
    return code_to_country.get(code, code)

def format_team_name(name: str) -> str:
    """
    Format team name to match API format.
    If the name doesn't have I or II in it, append I.
    """
    name = name.upper().strip()
    
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
    
    # Process mixed relay races
    process_mixed_relay_races(races_file)
    call_r_script('races', 'mixed_relay')
        # Full country names