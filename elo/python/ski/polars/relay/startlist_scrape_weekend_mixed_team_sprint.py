#!/usr/bin/env python3
import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional, Union
import warnings
from datetime import datetime
import traceback
import subprocess
import requests
warnings.filterwarnings('ignore')

# Import common utility functions
from startlist_common import *

# Import mixed team sprint functions
from startlist_scrape_races_mixed_team_sprint import get_mixed_team_sprint_teams

def process_weekend_mixed_team_sprint_races(races_file: str = None) -> None:
    """
    Process mixed team sprint races for weekend, creating both team and individual CSVs
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing weekend mixed team sprint races with team and individual output")
    
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
    
    # Process mixed team sprint races
    if not races_df.empty:
        process_mixed_team_sprint_races(races_df)

def process_mixed_team_sprint_races(races_df: pd.DataFrame) -> None:
    """Process mixed team sprint races, creating team and individual CSVs"""
    print("\nProcessing mixed team sprint races")
    
    # Get paths for output files - one file for teams and one for individuals
    team_output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_team_sprint_teams.csv"
    individual_output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_team_sprint_individuals.csv"
    
    # Create output directories if they don't exist
    os.makedirs(os.path.dirname(os.path.expanduser(team_output_path)), exist_ok=True)
    
    # Get the ELO scores for both genders
    men_elo_path = "~/ski/elo/python/ski/polars/excel365/men_chrono_pred.csv"
    women_elo_path = "~/ski/elo/python/ski/polars/excel365/ladies_chrono_pred.csv"
    
    men_elo_scores = get_latest_elo_scores(men_elo_path)
    women_elo_scores = get_latest_elo_scores(women_elo_path)
    
    # Get fantasy prices including team prices and all fantasy athlete data
    fantasy_prices = get_fantasy_prices()
    mixed_fantasy_teams = get_all_fantasy_teams('mixed')  # Get mixed team sprint teams
    men_fantasy_teams = get_all_fantasy_teams('men')
    women_fantasy_teams = get_all_fantasy_teams('ladies')
    
    # Process each mixed team sprint race
    all_teams_data = []
    all_individuals_data = []
    
    # Flag to check if any valid startlist URLs were found
    any_valid_startlist = False
    
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}")
            continue
        
        # Mark that we found at least one valid startlist URL
        any_valid_startlist = True
        
        print(f"Processing mixed team sprint race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the FIS startlist
        teams = get_mixed_team_sprint_teams(startlist_url)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams and individuals
            fallback_teams, fallback_individuals = generate_fallback_data(
                mixed_fantasy_teams, men_fantasy_teams, women_fantasy_teams, men_elo_scores, women_elo_scores, race
            )
            
            all_teams_data.extend(fallback_teams)
            all_individuals_data.extend(fallback_individuals)
        else:
            # Process the teams
            team_data, individual_data = process_mixed_teams_for_csv(
                teams, race, men_elo_scores, women_elo_scores, fantasy_prices, mixed_fantasy_teams, men_fantasy_teams, women_fantasy_teams
            )
            
            all_teams_data.extend(team_data)
            all_individuals_data.extend(individual_data)
    
    # If no valid startlist URLs or no teams were found, use the API fallback
    if not any_valid_startlist or not all_teams_data:
        print(f"No valid startlist data found, using Fantasy XC API fallback")
        
        # Get any race info to use for context (use first race or create a dummy one)
        if not races_df.empty:
            race_info = races_df.iloc[0]
        else:
            # Create a dummy race with today's date
            race_info = pd.Series({
                'Date': datetime.now().strftime('%Y-%m-%d'),
                'City': 'Unknown',
                'Country': 'Unknown'
            })
        
        # Get all teams from the Fantasy XC API
        mixed_api_teams = get_all_fantasy_teams('mixed')  # Get mixed team sprint teams
        men_api_teams = get_all_fantasy_teams('men')
        women_api_teams = get_all_fantasy_teams('ladies')
        
        # Filter to only include first teams for each gender
        mixed_first_teams = filter_to_first_teams(mixed_api_teams)
        men_first_teams = filter_to_first_teams(men_api_teams)
        women_first_teams = filter_to_first_teams(women_api_teams)
        
        # Create team data with empty Elo and member fields
        api_team_data = create_empty_team_data(mixed_first_teams, men_first_teams, women_first_teams, race_info)
        
        # Replace any existing teams with API data
        all_teams_data = api_team_data
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(os.path.expanduser(team_output_path), index=False)
        print(f"Saved {len(team_df)} mixed team sprint teams to {team_output_path}")
    else:
        print(f"No team data generated")

    # Save individual data
    if all_individuals_data:
        individual_df = pd.DataFrame(all_individuals_data)
        individual_df.to_csv(os.path.expanduser(individual_output_path), index=False)
        print(f"Saved {len(individual_df)} individual mixed team sprint athletes to {individual_output_path}")
    else:
        print(f"No individual data generated")
        
    print("\nFinished processing mixed team sprint races")

def get_all_fantasy_teams(gender: str) -> List[Dict]:
    """
    Get all teams from Fantasy XC API for a given gender
    
    Args:
        gender: 'men', 'ladies', or 'mixed'
    
    Returns:
        List of dictionaries with team data
    """
    try:
        # Get team data from the Fantasy XC API
        response = requests.get('https://www.fantasyxc.se/api/athletes')
        response.raise_for_status()
        
        athletes = response.json()
        
        # Filter for teams (is_team=true) and the specified gender
        if gender == 'mixed':
            gender_code = 'mixed'
        else:
            gender_code = 'm' if gender == 'men' else 'f'
            
        fantasy_teams = [
            athlete for athlete in athletes 
            if athlete.get('is_team', False) and athlete.get('gender', '') == gender_code
        ]
        
        # Sort teams by price (highest to lowest)
        fantasy_teams.sort(key=lambda x: x.get('price', 0), reverse=True)
        
        print(f"Found {len(fantasy_teams)} {gender} teams in Fantasy XC API")
        return fantasy_teams
        
    except Exception as e:
        print(f"Error getting Fantasy XC teams: {e}")
        return []

def filter_to_first_teams(fantasy_teams: List[Dict]) -> List[Dict]:
    """
    Filter to only include first teams (teams with " I" suffix)
    
    Args:
        fantasy_teams: List of team dictionaries from Fantasy XC API
    
    Returns:
        List of dictionaries with only first teams
    """
    first_teams = []
    
    # Create a set to track which nations we've already seen
    nations_seen = set()
    
    for team in fantasy_teams:
        team_name = team.get('name', '').strip()
        
        # Extract the nation name (without I, II, etc.)
        if team_name.endswith(' I'):
            nation = team_name[:-2].strip()
        elif team_name.endswith(' II'):
            nation = team_name[:-3].strip()
        elif team_name.endswith(' III'):
            nation = team_name[:-4].strip()
        elif team_name.endswith(' IV'):
            nation = team_name[:-3].strip()
        else:
            # If no suffix, just use the full name as nation
            nation = team_name
        
        # For each nation, only include the first team we encounter
        # Since teams are sorted by price, this will be the most expensive team
        if nation not in nations_seen:
            nations_seen.add(nation)
            first_teams.append(team)
    
    print(f"Filtered to {len(first_teams)} first teams")
    return first_teams

def create_empty_team_data(mixed_fantasy_teams: List[Dict], men_fantasy_teams: List[Dict], women_fantasy_teams: List[Dict], race: pd.Series) -> List[Dict]:
    """
    Create team data with empty Elo and member fields
    
    Args:
        mixed_fantasy_teams: List of mixed team dictionaries from Fantasy XC API
        men_fantasy_teams: List of men's team dictionaries from Fantasy XC API
        women_fantasy_teams: List of women's team dictionaries from Fantasy XC API
        race: Race information
    
    Returns:
        List of dictionaries with team data
    """
    team_data = []
    
    # Define Elo columns to initialize as empty
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
        'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Try mixed teams first, then men's, for a comprehensive unique list
    nations_seen = set()
    all_teams = []
    
    # First prioritize mixed teams
    for team in mixed_fantasy_teams:
        team_name = team.get('name', '').strip()
        
        # Extract the nation name (without I, II, etc.)
        if team_name.endswith(' I'):
            nation = team_name[:-2].strip()
        elif team_name.endswith(' II'):
            nation = team_name[:-3].strip()
        elif team_name.endswith(' III'):
            nation = team_name[:-4].strip()
        elif team_name.endswith(' IV'):
            nation = team_name[:-3].strip()
        else:
            # If no suffix, just use the full name as nation
            nation = team_name
        
        if nation not in nations_seen:
            nations_seen.add(nation)
            all_teams.append(team)
    
    # Then add men's teams if nations not already covered
    for team in men_fantasy_teams:
        team_name = team.get('name', '').strip()
        
        # Extract the nation name (without I, II, etc.)
        if team_name.endswith(' I'):
            nation = team_name[:-2].strip()
        elif team_name.endswith(' II'):
            nation = team_name[:-3].strip()
        elif team_name.endswith(' III'):
            nation = team_name[:-4].strip()
        elif team_name.endswith(' IV'):
            nation = team_name[:-3].strip()
        else:
            # If no suffix, just use the full name as nation
            nation = team_name
        
        if nation not in nations_seen:
            nations_seen.add(nation)
            all_teams.append(team)
    
    # Create team records for unique country list
    for team in all_teams:
        team_name = team.get('name', '').strip()
        
        # Extract the nation name (without I, II, etc.)
        if team_name.endswith(' I'):
            nation = team_name[:-2].strip()
        elif team_name.endswith(' II'):
            nation = team_name[:-3].strip()
        elif team_name.endswith(' III'):
            nation = team_name[:-4].strip()
        elif team_name.endswith(' IV'):
            nation = team_name[:-3].strip()
        else:
            # If no suffix, just use the full name as nation
            nation = team_name
        
        # Create team record with empty values
        team_record = {
            'Team_Name': team_name,
            'Nation': nation,
            'Team_Rank': 0,  # No rank available
            'Race_Date': race['Date'],
            'City': race['City'],
            'Country': race['Country'],
            'Price': team.get('price', 0),
            'Team_API_ID': team.get('athlete_id', None),
            'Is_Present': False,  # Not in actual startlist
            'Race_Type': 'Mixed Team Sprint'
        }
        
        # Initialize all Elo columns as empty strings (not 0)
        for col in elo_columns:
            team_record[col] = ''
        
        # Add member fields for both men and women
        for i in range(1, 3):  # For mixed team sprint, typically 2 members (1 man, 1 woman)
            team_record[f'Member_{i}'] = ""
            team_record[f'Member_{i}_ID'] = ""
            team_record[f'Member_{i}_Sex'] = "M" if i == 1 else "F"  # Typically 1=M, 2=F
            
            # Set all member Elo values to empty strings
            for col in elo_columns:
                team_record[f'Member_{i}_{col}'] = ''
        
        team_data.append(team_record)
    
    return team_data

def process_mixed_teams_for_csv(
    teams: List[Dict], 
    race: pd.Series, 
    men_elo_scores: pd.DataFrame,
    women_elo_scores: pd.DataFrame,
    fantasy_prices: Dict[str, Dict],
    mixed_fantasy_teams: Union[List[Dict], Dict[str, Dict]],
    men_fantasy_teams: Union[List[Dict], Dict[str, Dict]],
    women_fantasy_teams: Union[List[Dict], Dict[str, Dict]]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Process mixed team sprint teams for both team CSV and individual CSV
    
    Returns:
        tuple: (team_data, individual_data)
    """
    team_data = []
    individual_data = []
    
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
    
    # Track team counts by nation to handle multiple teams
    nation_team_counts = {}
    
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
            
        # Track team number for this nation
        if nation not in nation_team_counts:
            nation_team_counts[nation] = 1
        else:
            nation_team_counts[nation] += 1
        
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
        if hasattr(mixed_fantasy_teams, 'items'):
            # If it's a dictionary with items() method
            for api_team_name, api_team_info in mixed_fantasy_teams.items():
                if team_name.lower() == api_team_name.lower():
                    team_info['Price'] = api_team_info['price']
                    team_info['Team_API_ID'] = api_team_info['athlete_id']
                    break
        else:
            # If it's a list
            for api_team_info in mixed_fantasy_teams:
                if team_name.lower() == api_team_info.get('name', '').lower():
                    team_info['Price'] = api_team_info['price']
                    team_info['Team_API_ID'] = api_team_info['athlete_id']
                    break
        
        # If no price found in mixed teams, try men's
        if team_info['Price'] == 0:
            if hasattr(men_fantasy_teams, 'items'):
                # If it's a dictionary with items() method
                for api_team_name, api_team_info in men_fantasy_teams.items():
                    if team_name.lower() == api_team_name.lower():
                        team_info['Price'] = api_team_info['price']
                        team_info['Team_API_ID'] = api_team_info['athlete_id']
                        break
            else:
                # If it's a list
                for api_team_info in men_fantasy_teams:
                    if team_name.lower() == api_team_info.get('name', '').lower():
                        team_info['Price'] = api_team_info['price']
                        team_info['Team_API_ID'] = api_team_info['athlete_id']
                        break
                
        # If still no price found, try women's teams
        if team_info['Price'] == 0:
            if hasattr(women_fantasy_teams, 'items'):
                # If it's a dictionary with items() method
                for api_team_name, api_team_info in women_fantasy_teams.items():
                    if team_name.lower() == api_team_name.lower():
                        team_info['Price'] = api_team_info['price']
                        team_info['Team_API_ID'] = api_team_info['athlete_id']
                        break
            else:
                # If it's a list
                for api_team_info in women_fantasy_teams:
                    if team_name.lower() == api_team_info.get('name', '').lower():
                        team_info['Price'] = api_team_info['price']
                        team_info['Team_API_ID'] = api_team_info['athlete_id']
                        break
        
        team_members = []
        team_elos = {}
        
        # Initialize Elo sums for each type
        for col in elo_columns:
            team_elos[col] = []
        
# Continuing from the existing code - the function process_mixed_teams_for_csv

        # Mixed team sprint teams have exactly 2 members (1 male, 1 female)
        if len(team['members']) > 2:
            print(f"Warning: Team {team_name} has {len(team['members'])} members, expected 2.")
        
        found_male = False
        found_female = False
        
        # Process each member
        for member in team['members']:
            # Determine gender from the member data
            is_male = member.get('sex', 'M') == 'M'
            gender = 'men' if is_male else 'ladies'
            
            # Update our gender tracking
            if is_male:
                found_male = True
            else:
                found_female = True
            
            # Handle the bib parsing more safely to deal with incomplete bib formats
            bib = member.get('bib', '')
            try:
                # Split the bib and try to get the position number
                bib_parts = bib.split('-')
                if len(bib_parts) > 1 and bib_parts[1].strip():
                    position_number = int(bib_parts[1])
                    leg_number = bib_parts[1]  # Store the leg number as a string
                else:
                    # If bib is malformed (like "0-"), assign position based on gender
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
        if not found_male:
            position_number = 1  # Men are typically position 1
            team_info[f'Member_{position_number}'] = f"Unknown {team_name_part} Male"
            team_info[f'Member_{position_number}_ID'] = None
            team_info[f'Member_{position_number}_Sex'] = 'M'
            
            # Add quartile values for all Elo columns
            for col in elo_columns:
                team_info[f'Member_{position_number}_{col}'] = men_quartiles[col]
                team_elos[col].append(men_quartiles[col])
        
        if not found_female:
            position_number = 2  # Women are typically position 2
            team_info[f'Member_{position_number}'] = f"Unknown {team_name_part} Female"
            team_info[f'Member_{position_number}_ID'] = None
            team_info[f'Member_{position_number}_Sex'] = 'F'
            
            # Add quartile values for all Elo columns
            for col in elo_columns:
                team_info[f'Member_{position_number}_{col}'] = women_quartiles[col]
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

def generate_fallback_data(
    mixed_fantasy_teams: Union[List[Dict], Dict[str, Dict]],
    men_fantasy_teams: Union[List[Dict], Dict[str, Dict]],
    women_fantasy_teams: Union[List[Dict], Dict[str, Dict]], 
    men_elo_scores: pd.DataFrame,
    women_elo_scores: pd.DataFrame,
    race: pd.Series
) -> Tuple[List[Dict], List[Dict]]:
    """
    Generate fallback team and individual data when startlist is empty
    
    Args:
        mixed_fantasy_teams: Mixed teams from Fantasy XC API
        men_fantasy_teams: Men's teams from Fantasy XC API
        women_fantasy_teams: Women's teams from Fantasy XC API
        men_elo_scores: Men's ELO scores
        women_elo_scores: Women's ELO scores
        race: Race information
    
    Returns:
        tuple: (team_data, individual_data)
    """
    team_data = []
    individual_data = []
    
    print(f"Generating fallback data for mixed team sprint teams")
    
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
    
    # Get chronos data for finding skiers who competed in the most recent season
    men_chronos_path = "~/ski/elo/python/ski/polars/excel365/men_chrono_pred.csv"
    women_chronos_path = "~/ski/elo/python/ski/polars/excel365/ladies_chrono_pred.csv"
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
        'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Initialize dictionaries for tracking skiers by nation for each gender
    men_nations_skiers = {}
    women_nations_skiers = {}
    men_skier_elo_values = {}
    women_skier_elo_values = {}
    
    # Calculate first quartiles for each gender
    men_quartiles = {}
    women_quartiles = {}
    
    try:
        # Load the men's chronos data
        men_chronos = pd.read_csv(men_chronos_path)
        
        # Find the maximum season in the data
        men_max_season = men_chronos['Season'].max()
        print(f"Found men's max season: {men_max_season}")
        
        # Get ALL male skiers who competed in the most recent season
        men_recent_skiers = men_chronos[men_chronos['Season'] == men_max_season]
        
        # Calculate first quartile for each men's Elo column
        for elo_col in elo_columns:
            if elo_col in men_recent_skiers.columns:
                numeric_values = pd.to_numeric(men_recent_skiers[elo_col], errors='coerce')
                men_quartiles[elo_col] = numeric_values.quantile(0.25)
                print(f"Men's first quartile for {elo_col}: {men_quartiles[elo_col]}")
            else:
                # If column doesn't exist, use default value
                men_quartiles[elo_col] = 1000
                print(f"Men's column {elo_col} not found, using default quartile value: 1000")
        
        # Group male skiers by nation
        men_nations_skiers = men_recent_skiers.groupby('Nation')['Skier'].unique().to_dict()
        print(f"Found {len(men_nations_skiers)} nations with male skiers in season {men_max_season}")
        
        # Get the most recent Elo values for each male skier
        if 'Date' in men_chronos.columns:
            men_chronos['Date'] = pd.to_datetime(men_chronos['Date'], errors='coerce')
            men_chronos = men_chronos.sort_values('Date')
        
        # Process all records to get the most recent Elo values for each male skier
        for _, row in men_chronos.iterrows():
            skier = row['Skier']
            
            # Initialize skier data if not already present
            if skier not in men_skier_elo_values:
                men_skier_elo_values[skier] = {
                    'ID': row.get('ID'),
                    'Nation': row.get('Nation')
                }
                
                # Initialize with quartiles
                for elo_col in elo_columns:
                    men_skier_elo_values[skier][elo_col] = men_quartiles.get(elo_col, 1000)
            
            # Update with actual values from this row (which will be the most recent due to sorting)
            for elo_col in elo_columns:
                if elo_col in row and not pd.isna(row[elo_col]):
                    men_skier_elo_values[skier][elo_col] = float(row[elo_col])
        
        # Load the women's chronos data
        women_chronos = pd.read_csv(women_chronos_path)
        
        # Find the maximum season in the data
        women_max_season = women_chronos['Season'].max()
        print(f"Found women's max season: {women_max_season}")
        
        # Get ALL female skiers who competed in the most recent season
        women_recent_skiers = women_chronos[women_chronos['Season'] == women_max_season]
        
        # Calculate first quartile for each women's Elo column
        for elo_col in elo_columns:
            if elo_col in women_recent_skiers.columns:
                numeric_values = pd.to_numeric(women_recent_skiers[elo_col], errors='coerce')
                women_quartiles[elo_col] = numeric_values.quantile(0.25)
                print(f"Women's first quartile for {elo_col}: {women_quartiles[elo_col]}")
            else:
                # If column doesn't exist, use default value
                women_quartiles[elo_col] = 1000
                print(f"Women's column {elo_col} not found, using default quartile value: 1000")
        
        # Group female skiers by nation
        women_nations_skiers = women_recent_skiers.groupby('Nation')['Skier'].unique().to_dict()
        print(f"Found {len(women_nations_skiers)} nations with female skiers in season {women_max_season}")
        
        # Get the most recent Elo values for each female skier
        if 'Date' in women_chronos.columns:
            women_chronos['Date'] = pd.to_datetime(women_chronos['Date'], errors='coerce')
            women_chronos = women_chronos.sort_values('Date')
        
        # Process all records to get the most recent Elo values for each female skier
        for _, row in women_chronos.iterrows():
            skier = row['Skier']
            
            # Initialize skier data if not already present
            if skier not in women_skier_elo_values:
                women_skier_elo_values[skier] = {
                    'ID': row.get('ID'),
                    'Nation': row.get('Nation')
                }
                
                # Initialize with quartiles
                for elo_col in elo_columns:
                    women_skier_elo_values[skier][elo_col] = women_quartiles.get(elo_col, 1000)
            
            # Update with actual values from this row (which will be the most recent due to sorting)
            for elo_col in elo_columns:
                if elo_col in row and not pd.isna(row[elo_col]):
                    women_skier_elo_values[skier][elo_col] = float(row[elo_col])
        
    except Exception as e:
        print(f"Error reading chronos data: {e}")
        traceback.print_exc()
        
        # If an error occurred, set default quartiles
        for elo_col in elo_columns:
            men_quartiles[elo_col] = 1000
            women_quartiles[elo_col] = 1000
    
    # Use the exact known team names from the team spreadsheet
    for team_name in known_teams:
        # Remove the "I" suffix to get the country name
        country_name = team_name[:-2].strip()
        
        # Create team record with default values
        team_record = {
            'Team_Name': team_name,
            'Nation': country_name,
            'Team_Rank': 0,  # No rank since it's not from a startlist
            'Race_Date': race['Date'],
            'City': race['City'],
            'Country': race['Country'],
            'Is_Present': False,  # This team is not in the actual startlist
            'Race_Type': 'Mixed Team Sprint'
        }
        
        # Initialize all Elo columns as 0
        for elo_col in elo_columns:
            team_record[elo_col] = 0
        
        # Find matching fantasy team for pricing if available
        mixed_fantasy_team = None
        men_fantasy_team = None
        women_fantasy_team = None
        
        # First check mixed fantasy teams (preferred)
        if hasattr(mixed_fantasy_teams, 'items'):
            # If it's a dictionary with items() method
            for api_team_name, api_team_info in mixed_fantasy_teams.items():
                # Try to match team names
                if team_name.lower() == api_team_name.lower():
                    mixed_fantasy_team = api_team_info
                    break
        else:
            # If it's a list
            for api_team_info in mixed_fantasy_teams:
                if team_name.lower() == api_team_info.get('name', '').lower():
                    mixed_fantasy_team = api_team_info
                    break
        
        # Check men's fantasy teams if no mixed team found
        if not mixed_fantasy_team:
            if hasattr(men_fantasy_teams, 'items'):
                # If it's a dictionary with items() method
                for api_team_name, api_team_info in men_fantasy_teams.items():
                    # Try to match team names
                    if team_name.lower() == api_team_name.lower():
                        men_fantasy_team = api_team_info
                        break
            else:
                # If it's a list
                for api_team_info in men_fantasy_teams:
                    if team_name.lower() == api_team_info.get('name', '').lower():
                        men_fantasy_team = api_team_info
                        break
        
        # Check women's fantasy teams if no mixed or men's team found
        if not mixed_fantasy_team and not men_fantasy_team:
            if hasattr(women_fantasy_teams, 'items'):
                # If it's a dictionary with items() method
                for api_team_name, api_team_info in women_fantasy_teams.items():
                    # Try to match team names
                    if team_name.lower() == api_team_name.lower():
                        women_fantasy_team = api_team_info
                        break
            else:
                # If it's a list
                for api_team_info in women_fantasy_teams:
                    if team_name.lower() == api_team_info.get('name', '').lower():
                        women_fantasy_team = api_team_info
                        break
        
        # Add fantasy data if available (prefer mixed team price if available)
        if mixed_fantasy_team:
            team_record['Price'] = mixed_fantasy_team.get('price', 0)
            team_record['Team_API_ID'] = mixed_fantasy_team.get('athlete_id', None)
        elif men_fantasy_team:
            team_record['Price'] = men_fantasy_team.get('price', 0)
            team_record['Team_API_ID'] = men_fantasy_team.get('athlete_id', None)
        elif women_fantasy_team:
            team_record['Price'] = women_fantasy_team.get('price', 0)
            team_record['Team_API_ID'] = women_fantasy_team.get('athlete_id', None)
        else:
            team_record['Price'] = 0
            team_record['Team_API_ID'] = None
        
        # Find the normalized nation code that matches our chronos data
        # First convert country name to possible nation codes used in chronos
        possible_nation_codes = []
        for country_code, team_code in map_country_to_team_name.__globals__.get('country_to_team', {}).items():
            if team_code == country_name:
                possible_nation_codes.append(country_code)
        
        # Add the country name itself as a possible match
        possible_nation_codes.append(country_name)
        
        # Try to find best skiers for this nation from each gender
        men_team_members = []
        women_team_members = []
        
        # For each possible nation code, try to find skiers
        for nation_code in possible_nation_codes:
            # Try to find male skiers for this nation
            if nation_code in men_nations_skiers:
                nation_men = men_nations_skiers[nation_code]
                # Sort by Elo score (highest to lowest)
                sorted_men = sorted(
                    [(skier, men_skier_elo_values.get(skier, {}).get('Elo', 0)) 
                     for skier in nation_men],
                    key=lambda x: x[1],
                    reverse=True
                )
                # Take top 1 male skier (for mixed team sprint)
                men_team_members = [m[0] for m in sorted_men[:1]]
                
                # If we found skiers, don't check other nation codes
                if men_team_members:
                    break
        
        # Do the same for women
        for nation_code in possible_nation_codes:
            # Try to find female skiers for this nation
            if nation_code in women_nations_skiers:
                nation_women = women_nations_skiers[nation_code]
                # Sort by Elo score (highest to lowest)
                sorted_women = sorted(
                    [(skier, women_skier_elo_values.get(skier, {}).get('Elo', 0)) 
                     for skier in nation_women],
                    key=lambda x: x[1],
                    reverse=True
                )
                # Take top 1 female skier (for mixed team sprint)
                women_team_members = [w[0] for w in sorted_women[:1]]
                
                # If we found skiers, don't check other nation codes
                if women_team_members:
                    break
        
        # Add members to team record and create individual records
        # For mixed team sprint, position 1 is usually male, 2 is female
        team_elos = {col: 0 for col in elo_columns}
        
        # Add male member in position 1
        if men_team_members:
            male_skier = men_team_members[0]
            pos = 1
            
            # Get skier Elo data
            skier_elo = men_skier_elo_values.get(male_skier, {})
            
            # Add member to team record
            team_record[f'Member_{pos}'] = male_skier
            team_record[f'Member_{pos}_ID'] = skier_elo.get('ID', None)
            team_record[f'Member_{pos}_Sex'] = 'M'
            
            # Add Elo values for this member
            for elo_col in elo_columns:
                member_elo = skier_elo.get(elo_col, men_quartiles.get(elo_col, 1000))
                team_record[f'Member_{pos}_{elo_col}'] = member_elo
                team_elos[elo_col] += member_elo
            
            # Create individual record
            individual_record = {
                'FIS_Name': male_skier,
                'Skier': male_skier,
                'ID': skier_elo.get('ID', None),
                'Nation': reverse_map_country_code(nation_code) if nation_code in men_nations_skiers else country_name,
                'In_FIS_List': False,
                'Price': 0,  # Individual prices not available
                'Team_Name': team_name,
                'Team_Rank': 0,
                'Team_Time': '',
                'Team_Position': f"{pos}",
                'Race_Date': race['Date'],
                'City': race['City'],
                'Country': race['Country'],
                'Race_Type': 'Mixed Team Sprint',
                'Sex': 'M'
            }
            
            # Add Elo columns
            for elo_col in elo_columns:
                individual_record[elo_col] = skier_elo.get(elo_col, men_quartiles.get(elo_col, 1000))
            
            individual_data.append(individual_record)
        else:
            # Add placeholder male member
            pos = 1
            placeholder_name = f"{country_name} Male"
            team_record[f'Member_{pos}'] = placeholder_name
            team_record[f'Member_{pos}_ID'] = None
            team_record[f'Member_{pos}_Sex'] = 'M'
            
            # Add quartile Elo values for this placeholder
            for elo_col in elo_columns:
                member_elo = men_quartiles.get(elo_col, 1000)
                team_record[f'Member_{pos}_{elo_col}'] = member_elo
                team_elos[elo_col] += member_elo
        
        # Add female member in position 2
        if women_team_members:
            female_skier = women_team_members[0]
            pos = 2
            
            # Get skier Elo data
            skier_elo = women_skier_elo_values.get(female_skier, {})
            
            # Add member to team record
            team_record[f'Member_{pos}'] = female_skier
            team_record[f'Member_{pos}_ID'] = skier_elo.get('ID', None)
            team_record[f'Member_{pos}_Sex'] = 'F'
            
            # Add Elo values for this member
            for elo_col in elo_columns:
                member_elo = skier_elo.get(elo_col, women_quartiles.get(elo_col, 1000))
                team_record[f'Member_{pos}_{elo_col}'] = member_elo
                team_elos[elo_col] += member_elo
            
            # Create individual record
            individual_record = {
                'FIS_Name': female_skier,
                'Skier': female_skier,
                'ID': skier_elo.get('ID', None),
                'Nation': reverse_map_country_code(nation_code) if nation_code in women_nations_skiers else country_name,
                'In_FIS_List': False,
                'Price': 0,  # Individual prices not available
                'Team_Name': team_name,
                'Team_Rank': 0,
                'Team_Time': '',
                'Team_Position': f"{pos}",
                'Race_Date': race['Date'],
                'City': race['City'],
                'Country': race['Country'],
                'Race_Type': 'Mixed Team Sprint',
                'Sex': 'F'
            }
            
            # Add Elo columns
            for elo_col in elo_columns:
                individual_record[elo_col] = skier_elo.get(elo_col, women_quartiles.get(elo_col, 1000))
            
            individual_data.append(individual_record)
        else:
            # Add placeholder female member
            pos = 2
            placeholder_name = f"{country_name} Female"
            team_record[f'Member_{pos}'] = placeholder_name
            team_record[f'Member_{pos}_ID'] = None
            team_record[f'Member_{pos}_Sex'] = 'F'
            
            # Add quartile Elo values for this placeholder
            for elo_col in elo_columns:
                member_elo = women_quartiles.get(elo_col, 1000)
                team_record[f'Member_{pos}_{elo_col}'] = member_elo
                team_elos[elo_col] += member_elo
        
        # Set team Elo totals
        for elo_col in elo_columns:
            team_record[elo_col] = team_elos[elo_col]
        
        # Add team to list
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
    
    # Process mixed team sprint races with the enhanced functionality
    process_weekend_mixed_team_sprint_races(races_file)