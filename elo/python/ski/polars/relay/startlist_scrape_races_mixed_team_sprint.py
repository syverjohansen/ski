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

def process_mixed_team_sprint_races(races_file: str = None) -> None:
    """
    Main function to process mixed team sprint races
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print(f"Processing mixed team sprint races")
    
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
            
            # Filter to only mixed team sprint races
            races_df = races_df[races_df['Distance'] == 'Mts']
            print(f"Filtered to {len(races_df)} mixed team sprint races")
            
            # Find next race date
            next_date = find_next_race_date(races_df)
            
            # Filter to races on the next date
            races_df = filter_races_by_date(races_df, next_date)
            print(f"Found {len(races_df)} mixed team sprint races on {next_date}")
            
        except Exception as e:
            print(f"Error loading races from {races_path}: {e}")
            traceback.print_exc()
            return
    
    # Initialize data collections for all races
    all_teams_data = []
    all_individuals_data = []
    
    # Process each mixed team sprint race
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}, skipping")
            continue
        
        print(f"Processing mixed team sprint race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the FIS startlist
        teams = get_mixed_team_sprint_teams(startlist_url)
        if not teams:
            print(f"No teams found for race {idx+1}, skipping")
            continue
        
        # Process the teams and create team and individual data
        teams_data, individuals_data = process_mixed_team_sprint_teams(teams, race)
        
        # Add the data to our collections
        all_teams_data.extend(teams_data)
        all_individuals_data.extend(individuals_data)
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        save_mixed_team_sprint_team_data(team_df)
    else:
        print(f"No team data generated")
    
    # Save individual data
    if all_individuals_data:
        individual_df = pd.DataFrame(all_individuals_data)
        save_mixed_team_sprint_individual_data(individual_df)
    else:
        print(f"No individual data generated")

def get_mixed_team_sprint_teams(url: str) -> List[Dict]:
    """
    Get teams from FIS mixed team sprint startlist
    
    Returns list of teams with structure:
    [
        {
            'team_name': 'COUNTRY I',
            'nation': 'XXX',
            'team_rank': 1,
            'team_time': '5:21.43',
            'team_number': 1,  # Added to track multiple teams from same nation
            'members': [
                {'name': 'ATHLETE NAME', 'nation': 'XXX', 'year': '1997', 'bib': '1-1', 'sex': 'M'},
                {'name': 'ATHLETE NAME', 'nation': 'XXX', 'year': '1994', 'bib': '1-2', 'sex': 'F'}
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
        
        # Find all team rows (main rows)
        team_rows = soup.select('.table-row_theme_main')
        
        for team_row in team_rows:
            team_name_elem = team_row.select_one('.g-lg-14.g-md-14.g-sm-11.g-xs-10')
            if not team_name_elem:
                continue
                
            # Get nation code
            nation_elem = team_row.select_one('.country__name-short')
            nation = nation_elem.text.strip() if nation_elem else ""
            
            # Update team counter for this nation
            if nation not in nation_team_counts:
                nation_team_counts[nation] = 1
            else:
                nation_team_counts[nation] += 1
            
            # Get team number for this nation
            team_number = nation_team_counts[nation]
                
            team_data = {
                'team_name': team_name_elem.text.strip(),
                'nation': nation,
                'team_rank': team_row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold').text.strip(),
                'team_time': '',
                'team_number': team_number,  # Add team number
                'members': []
            }
            
            # Get team time if available
            time_elem = team_row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs')
            if time_elem:
                team_data['team_time'] = time_elem.text.strip()
            
            # Find the next athlete rows until the next team row
            current_element = team_row
            while True:
                current_element = current_element.find_next_sibling()
                if not current_element or 'table-row_theme_main' in current_element.get('class', []):
                    break
                
                if 'athlete-team-row' in current_element.select_one('.g-row').get('class', []):
                    # This is an athlete row for the current team
                    athlete_name_elem = current_element.select_one('.athlete-name')
                    if athlete_name_elem:
                        athlete_name = athlete_name_elem.text.strip()
                        year_elem = current_element.select_one('.g-lg-1.g-md-1.g-sm-2.g-xs-3')
                        year = year_elem.text.strip() if year_elem else ''
                        
                        # Get athlete bib
                        bib_elem = current_element.select_one('.bib')
                        bib = bib_elem.text.strip() if bib_elem else ''
                        
                        # For mixed team sprint, determine athlete sex
                        sex = determine_athlete_sex_from_bib_or_name(athlete_name, bib, team_data['members'])
                        
                        team_data['members'].append({
                            'name': athlete_name,
                            'nation': team_data['nation'],
                            'year': year,
                            'bib': bib,
                            'sex': sex
                        })
            
            teams.append(team_data)
        
        print(f"Found {len(teams)} teams with {sum(len(team['members']) for team in teams)} athletes")
        return teams
        
    except Exception as e:
        print(f"Error getting mixed team sprint teams: {e}")
        traceback.print_exc()
        return []

def determine_athlete_sex_from_bib_or_name(name: str, bib: str, existing_members: List[Dict]) -> str:
    """
    Determine athlete sex based on bib positioning, existing members, or name patterns
    
    Args:
        name: Athlete name
        bib: Athlete bib
        existing_members: List of team members already processed
    
    Returns:
        'M' for male, 'F' for female
    """
    # Try to determine from bib if available
    # In mixed team sprint, positions 1 are typically men and 2 are women
    if '-' in bib:
        position = int(bib.split('-')[-1])
        if position == 1:
            return 'M'
        elif position == 2:
            return 'F'
    
    # If we already have team members, alternate sex
    if existing_members:
        last_member = existing_members[-1]
        return 'F' if last_member.get('sex') == 'M' else 'M'
    
    # Try to determine from name (least reliable)
    # Common female name endings in various languages
    female_patterns = ['ova', 'ina', 'eva', 'aia', 'aya', 'kyte', 'iene', 'ienė', 'ová', 'ska']
    if any(name.lower().endswith(pattern) for pattern in female_patterns):
        return 'F'
    
    # Default to male if we can't determine
    print(f"Warning: Could not reliably determine gender for {name}, defaulting to 'M'")
    return 'M'

def process_mixed_team_sprint_teams(teams: List[Dict], race: pd.Series) -> Tuple[List[Dict], List[Dict]]:
    """
    Process mixed team sprint teams and create team and individual data
    
    Args:
        teams: List of teams with members
        race: Race information
        
    Returns:
        tuple: (team_data, individual_data)
    """
    # Initialize data for teams and individual athletes
    team_data = []
    individual_data = []
    
    # Get the ELO scores for both genders
    men_elo_path = "~/ski/elo/python/ski/polars/excel365/men_chrono_pred.csv"
    women_elo_path = "~/ski/elo/python/ski/polars/excel365/ladies_chrono_pred.csv"
    
    men_elo_scores = get_latest_elo_scores(men_elo_path)
    women_elo_scores = get_latest_elo_scores(women_elo_path)
    
    # Get fantasy prices and team prices
    fantasy_prices = get_fantasy_prices()
    mixed_fantasy_teams = get_fantasy_teams('mixed')
    men_fantasy_teams = get_fantasy_teams('men')
    women_fantasy_teams = get_fantasy_teams('ladies')
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
        'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Calculate first quartile for each Elo column for imputation - for each gender
    men_quartiles = {}
    women_quartiles = {}
    
    for col in elo_columns:
        # Men's quartiles
        if col in men_elo_scores.columns:
            numeric_values = pd.to_numeric(men_elo_scores[col], errors='coerce')
            men_quartiles[col] = numeric_values.quantile(0.25)
            print(f"Men first quartile for {col}: {men_quartiles[col]}")
        else:
            men_quartiles[col] = 1000
            print(f"Men column {col} not found, using default quartile value: 1000")
        
        # Women's quartiles
        if col in women_elo_scores.columns:
            numeric_values = pd.to_numeric(women_elo_scores[col], errors='coerce')
            women_quartiles[col] = numeric_values.quantile(0.25)
            print(f"Ladies first quartile for {col}: {women_quartiles[col]}")
        else:
            women_quartiles[col] = 1000
            print(f"Ladies column {col} not found, using default quartile value: 1000")
    
    # Process each team
    for team in teams:
        # Get nation code from team
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
        
        # Map to standardized country name
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
            'Race_Type': 'Mixed Team Sprint'
        }
        
        # Initialize all Elo sums to 0
        for col in elo_columns:
            team_info[col] = 0
        
        # Get team price from fantasy API - first try mixed teams (preferred)
        # Then fallback to men's or women's if needed
        
        # Try mixed fantasy teams first
        for api_team_name, api_team_info in mixed_fantasy_teams.items():
            if team_name.lower() == api_team_name.lower():
                team_info['Price'] = api_team_info['price']
                team_info['Team_API_ID'] = api_team_info['athlete_id']
                break
        
        # If no price found in mixed teams, try men's
        if team_info['Price'] == 0:
            for api_team_name, api_team_info in men_fantasy_teams.items():
                if team_name.lower() == api_team_name.lower():
                    team_info['Price'] = api_team_info['price']
                    team_info['Team_API_ID'] = api_team_info['athlete_id']
                    break
                
        # If still no price found, try women's teams
        if team_info['Price'] == 0:
            for api_team_name, api_team_info in women_fantasy_teams.items():
                if team_name.lower() == api_team_name.lower():
                    team_info['Price'] = api_team_info['price']
                    team_info['Team_API_ID'] = api_team_info['athlete_id']
                    break
        
        team_members = []
        team_elos = {}
        
        # Initialize Elo sums for each type
        for col in elo_columns:
            team_elos[col] = []
        
        # Mixed team sprint teams have exactly 2 members (1 male, 1 female)
        if len(team['members']) > 2:
            print(f"Warning: Team {team_name} has {len(team['members'])} members, expected 2.")
        
        # Process each member
        for member in team['members']:
            # Determine gender from the member data
            is_male = member.get('sex', 'M') == 'M'
            gender = 'men' if is_male else 'ladies'
            
            # Handle the bib parsing more safely to deal with incomplete bib formats
            bib = member.get('bib', '')
            try:
                # Split the bib and try to get the position number
                bib_parts = bib.split('-')
                if len(bib_parts) > 1 and bib_parts[1].strip():
                    position_number = int(bib_parts[1])
                    leg_number = bib_parts[1]  # Store the leg number as a string
                else:
                    # If bib is malformed (like "0-"), assign sequential position
                    position_number = 1 if is_male else 2  # In mixed team sprint, men=1, women=2
                    leg_number = str(position_number)  # Convert position number to string for leg number
                    print(f"Warning: Malformed bib '{bib}' for {member.get('name', 'Unknown')}. Assigning position {position_number}.")
            except (ValueError, IndexError):
                # Handle any parsing errors by assigning sequential position
                position_number = 1 if is_male else 2  # In mixed team sprint, men=1, women=2
                leg_number = str(position_number)  # Convert position number to string for leg number
                print(f"Warning: Invalid bib format '{bib}' for {member.get('name', 'Unknown')}. Assigning position {position_number}.")
            
            # Select the appropriate ELO scores and quartiles based on gender
            elo_scores = men_elo_scores if is_male else women_elo_scores
            quartiles = men_quartiles if is_male else women_quartiles
            
            # Process the athlete to match with ELO scores and prices
            athlete_data = process_mixed_team_sprint_athlete(
                {
                    'FIS_Name': member['name'],
                    'Skier': member['name'],
                    'Nation': nation,
                    'In_FIS_List': True,
                    'Price': 0,
                    'Team_Name': team_name,  # Use exact format from team spreadsheet
                    'Team_Rank': team['team_rank'],
                    'Team_Time': team.get('team_time', ''),
                    'Team_Position': leg_number,  # Just the leg number (1 or 2)
                    'Sex': 'M' if is_male else 'F',
                    'Race_Type': 'Mixed Team Sprint'
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
            team_members.append(athlete_data['Skier'])
            
            # Extract member info for team record
            team_info[f'Member_{position_number}'] = athlete_data['Skier']
            team_info[f'Member_{position_number}_ID'] = athlete_data.get('ID', None)
            team_info[f'Member_{position_number}_Sex'] = 'M' if is_male else 'F'
            
            # Add all Elo values to team sums
            for col in elo_columns:
                if col in athlete_data and athlete_data[col] is not None:
                    member_elo = float(athlete_data[col])
                    team_info[f'Member_{position_number}_{col}'] = member_elo
                    team_elos[col].append(member_elo)
                else:
                    # Use quartile value if Elo is missing
                    quartile_value = men_quartiles[col] if is_male else women_quartiles[col]
                    team_info[f'Member_{position_number}_{col}'] = quartile_value
                    team_elos[col].append(quartile_value)
        
        # Fill in any missing members (should have exactly one male and one female)
        male_found = False
        female_found = False
        
        for pos in [1, 2]:
            member_key = f'Member_{pos}'
            if member_key in team_info:
                sex_key = f'Member_{pos}_Sex'
                if sex_key in team_info:
                    if team_info[sex_key] == 'M':
                        male_found = True
                    elif team_info[sex_key] == 'F':
                        female_found = True
        
        # Add missing male if needed
        if not male_found:
            team_info['Member_1'] = f"Unknown {team_name_part} Male"
            team_info['Member_1_ID'] = None
            team_info['Member_1_Sex'] = 'M'
            
            # Add quartile values for all Elo columns
            for col in elo_columns:
                team_info[f'Member_1_{col}'] = men_quartiles[col]
                team_elos[col].append(men_quartiles[col])
        
        # Add missing female if needed
        if not female_found:
            team_info['Member_2'] = f"Unknown {team_name_part} Female"
            team_info['Member_2_ID'] = None
            team_info['Member_2_Sex'] = 'F'
            
            # Add quartile values for all Elo columns
            for col in elo_columns:
                team_info[f'Member_2_{col}'] = women_quartiles[col]
                team_elos[col].append(women_quartiles[col])
        
        # Calculate combined Elo for each type
        for col in elo_columns:
            team_info[col] = sum(team_elos[col]) if team_elos[col] else 0
        
        # Add team record if there are members
        if team_members:
            team_data.append(team_info)
    
    return team_data, individual_data

def process_mixed_team_sprint_athlete(
    row_data: Dict, 
    elo_scores: pd.DataFrame, 
    fantasy_prices: Dict[str, Dict],
    gender: str,
    quartiles: Dict[str, float] = None
) -> Dict:
    """Process an individual mixed team sprint athlete to match with ELO scores and prices"""
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
        print(f"Error processing mixed team sprint athlete: {e}")
        traceback.print_exc()
        return row_data  # Return original data if processing fails

def get_fantasy_teams(gender: str) -> Dict[str, Dict]:
    """Get team data from Fantasy XC API with gender filter"""
    try:
        response = requests.get('https://www.fantasyxc.se/api/athletes')
        response.raise_for_status()
        
        athletes = response.json()
        
        # Filter for teams (is_team=true) and the specified gender
        gender_code = gender
        if gender == 'men':
            gender_code = 'm'
        elif gender == 'ladies':
            gender_code = 'f'
        
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

def save_mixed_team_sprint_individual_data(df: pd.DataFrame) -> None:
    """Save processed mixed team sprint individual data to a CSV file"""
    try:
        # Sort by team rank and position
        df = df.sort_values(['Team_Rank', 'Team_Position'])
        
        # Save to CSV
        output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_team_sprint_individuals.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} mixed team sprint individual athletes to {output_path}")
    
    except Exception as e:
        print(f"Error saving mixed team sprint individual data: {e}")
        traceback.print_exc()

def save_mixed_team_sprint_team_data(df: pd.DataFrame) -> None:
    """Save processed mixed team sprint team data to a CSV file"""
    try:
        # Sort by team rank
        df = df.sort_values(['Team_Rank'])
        
        # Save to CSV
        output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_team_sprint_teams.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} mixed team sprint teams to {output_path}")
    
    except Exception as e:
        print(f"Error saving mixed team sprint team data: {e}")
        traceback.print_exc()

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

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Process mixed team sprint races
    process_mixed_team_sprint_races(races_file)