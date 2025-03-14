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

# Import mixed relay functions
from startlist_scrape_races_mixed_relay import get_mixed_relay_teams

def process_weekend_mixed_relay_races(races_file: str = None) -> None:
    """
    Process mixed relay races for weekend, creating both team and individual CSVs
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing weekend mixed relay races with team and individual output")
    
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
            
            # Filter to only mixed relay races
            races_df = races_df[races_df['Distance'] == 'Mix']
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
    
    # Process mixed relay races - no gender filter since these are mixed
    if not races_df.empty:
        process_mixed_relay_races(races_df)

def process_mixed_relay_races(races_df: pd.DataFrame) -> None:
    """Process mixed relay races, creating team and individual CSVs for both genders"""
    print("\nProcessing mixed relay races")
    
    # Get paths for output files - we need files for each gender
    men_team_output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_relay_teams_men.csv"
    women_team_output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_relay_teams_ladies.csv"
    men_individual_output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_relay_individuals_men.csv"
    women_individual_output_path = "~/ski/elo/python/ski/polars/relay/excel365/startlist_mixed_relay_individuals_ladies.csv"
    
    # Create output directories if they don't exist
    os.makedirs(os.path.dirname(os.path.expanduser(men_team_output_path)), exist_ok=True)
    
    # Get the ELO scores for both genders
    men_elo_path = "~/ski/elo/python/ski/polars/excel365/men_chrono_elevation.csv"
    women_elo_path = "~/ski/elo/python/ski/polars/excel365/ladies_chrono_elevation.csv"
    
    men_elo_scores = get_latest_elo_scores(men_elo_path)
    women_elo_scores = get_latest_elo_scores(women_elo_path)
    
    # Get fantasy prices including team prices and all fantasy athlete data
    fantasy_prices = get_fantasy_prices(get_full_athlete_data=True)
    men_fantasy_teams = get_fantasy_teams('men')
    women_fantasy_teams = get_fantasy_teams('ladies')
    
    # Process each mixed relay race
    all_men_teams_data = []
    all_women_teams_data = []
    all_men_individuals_data = []
    all_women_individuals_data = []
    
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}")
            continue
        
        print(f"Processing mixed relay race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the FIS startlist
        teams = get_mixed_relay_teams(startlist_url, fantasy_prices)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams and individuals for both genders
            men_fallback_teams, men_fallback_individuals = generate_fallback_data(
                'men', men_fantasy_teams, men_elo_scores, race
            )
            
            women_fallback_teams, women_fallback_individuals = generate_fallback_data(
                'ladies', women_fantasy_teams, women_elo_scores, race
            )
            
            all_men_teams_data.extend(men_fallback_teams)
            all_men_individuals_data.extend(men_fallback_individuals)
            all_women_teams_data.extend(women_fallback_teams)
            all_women_individuals_data.extend(women_fallback_individuals)
        else:
            # Process the teams, separating by gender
            men_team_data, women_team_data, men_individual_data, women_individual_data = process_mixed_teams_for_csv(
                teams, race, men_elo_scores, women_elo_scores, fantasy_prices, men_fantasy_teams, women_fantasy_teams
            )
            
            all_men_teams_data.extend(men_team_data)
            all_men_individuals_data.extend(men_individual_data)
            all_women_teams_data.extend(women_team_data)
            all_women_individuals_data.extend(women_individual_data)
    
    # Save men team data
    if all_men_teams_data:
        men_team_df = pd.DataFrame(all_men_teams_data)
        men_team_df.to_csv(os.path.expanduser(men_team_output_path), index=False)
        print(f"Saved {len(men_team_df)} men mixed relay teams to {men_team_output_path}")
    else:
        print(f"No men team data generated")
    
    # Save women team data
    if all_women_teams_data:
        women_team_df = pd.DataFrame(all_women_teams_data)
        women_team_df.to_csv(os.path.expanduser(women_team_output_path), index=False)
        print(f"Saved {len(women_team_df)} ladies mixed relay teams to {women_team_output_path}")
    else:
        print(f"No ladies team data generated")
    
    # Save men individual data
    if all_men_individuals_data:
        men_individual_df = pd.DataFrame(all_men_individuals_data)
        men_individual_df.to_csv(os.path.expanduser(men_individual_output_path), index=False)
        print(f"Saved {len(men_individual_df)} men individual mixed relay athletes to {men_individual_output_path}")
    else:
        print(f"No men individual data generated")
    
    # Save women individual data
    if all_women_individuals_data:
        women_individual_df = pd.DataFrame(all_women_individuals_data)
        women_individual_df.to_csv(os.path.expanduser(women_individual_output_path), index=False)
        print(f"Saved {len(women_individual_df)} ladies individual mixed relay athletes to {women_individual_output_path}")
    else:
        print(f"No ladies individual data generated")

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

# Update process_mixed_teams_for_csv in startlist_scrape_weekend_mixed_relay.py
def process_mixed_teams_for_csv(
    teams: List[Dict], 
    race: pd.Series, 
    men_elo_scores: pd.DataFrame,
    women_elo_scores: pd.DataFrame,
    fantasy_prices: Dict[str, int],
    men_fantasy_teams: Dict[str, Dict],
    women_fantasy_teams: Dict[str, Dict]
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Process mixed relay teams for both team CSV and individual CSV, separating by gender
    
    Returns:
        tuple: (men_team_data, women_team_data, men_individual_data, women_individual_data)
    """
    men_team_data = []
    women_team_data = []
    men_individual_data = []
    women_individual_data = []
    
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
        
        # Initialize team info for both genders with default values for all Elo types
        men_team_info = {
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
        
        women_team_info = {
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
            men_team_info[col] = 0
            women_team_info[col] = 0
        
        # Get team price from fantasy API if available - for both genders
        for api_team_name, api_team_info in men_fantasy_teams.items():
            if team_name.lower() == api_team_name.lower():
                men_team_info['Price'] = api_team_info['price']
                men_team_info['Team_API_ID'] = api_team_info['athlete_id']
                break
                
        for api_team_name, api_team_info in women_fantasy_teams.items():
            if team_name.lower() == api_team_name.lower():
                women_team_info['Price'] = api_team_info['price']
                women_team_info['Team_API_ID'] = api_team_info['athlete_id']
                break
        
        
        men_team_members = []
        women_team_members = []
        men_team_elos = {}
        women_team_elos = {}
        
        # Initialize Elo sums for each type
        for col in elo_columns:
            men_team_elos[col] = []
            women_team_elos[col] = []
        
        # Track position numbers by gender to avoid duplicates in case of malformed bibs
        men_position_counter = 0
        women_position_counter = 0
        men_position_numbers_used = set()
        women_position_numbers_used = set()
        
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
                else:
                    # If bib is malformed (like "0-"), assign sequential position
                    if is_male:
                        men_position_counter += 1
                        position_number = men_position_counter
                    else:
                        women_position_counter += 1
                        position_number = women_position_counter
                    print(f"Warning: Malformed bib '{bib}' for {member.get('name', 'Unknown')}. Assigning position {position_number}.")
            except (ValueError, IndexError):
                # Handle any parsing errors by assigning sequential position
                if is_male:
                    men_position_counter += 1
                    position_number = men_position_counter
                else:
                    women_position_counter += 1
                    position_number = women_position_counter
                print(f"Warning: Invalid bib format '{bib}' for {member.get('name', 'Unknown')}. Assigning position {position_number}.")
            
            # Ensure no duplicate position numbers within each gender
            if is_male:
                while position_number in men_position_numbers_used:
                    position_number += 1
                men_position_numbers_used.add(position_number)
            else:
                while position_number in women_position_numbers_used:
                    position_number += 1
                women_position_numbers_used.add(position_number)
            
            # Select the appropriate ELO scores and quartiles based on gender
            elo_scores = men_elo_scores if is_male else women_elo_scores
            quartiles = men_quartiles if is_male else women_quartiles
            
            # Process the athlete to match with ELO scores and prices
            athlete_data = process_mixed_relay_athlete(
                {
                    'FIS_Name': member['name'],
                    'Skier': member['name'],
                    'Nation': nation,
                    'In_FIS_List': True,
                    'Price': 0,
                    'Team_Name': team_name,  # Use exact format from team spreadsheet
                    'Team_Rank': team['team_rank'],
                    'Team_Time': team.get('team_time', ''),
                    'Team_Position': bib,  # Keep original bib for reference
                    'Sex': 'M' if is_male else 'F',
                    'Race_Type': 'Mixed Relay'
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
            
            # Add to individual data based on gender
            if is_male:
                men_individual_data.append(athlete_data)
                men_team_members.append(athlete_data['Skier'])
            else:
                women_individual_data.append(athlete_data)
                women_team_members.append(athlete_data['Skier'])
            
            # Extract member info for team record and add to appropriate gender team
            if is_male:
                men_team_info[f'Member_{position_number}'] = athlete_data['Skier']
                men_team_info[f'Member_{position_number}_ID'] = athlete_data.get('ID', None)
                
                # Add all Elo values to team sums
                for col in elo_columns:
                    if col in athlete_data and athlete_data[col] is not None:
                        member_elo = float(athlete_data[col])
                        men_team_info[f'Member_{position_number}_{col}'] = member_elo
                        men_team_elos[col].append(member_elo)
                    else:
                        # Use quartile value if Elo is missing
                        men_team_info[f'Member_{position_number}_{col}'] = men_quartiles[col]
                        men_team_elos[col].append(men_quartiles[col])
            else:
                women_team_info[f'Member_{position_number}'] = athlete_data['Skier']
                women_team_info[f'Member_{position_number}_ID'] = athlete_data.get('ID', None)
                
                # Add all Elo values to team sums
                for col in elo_columns:
                    if col in athlete_data and athlete_data[col] is not None:
                        member_elo = float(athlete_data[col])
                        women_team_info[f'Member_{position_number}_{col}'] = member_elo
                        women_team_elos[col].append(member_elo)
                    else:
                        # Use quartile value if Elo is missing
                        women_team_info[f'Member_{position_number}_{col}'] = women_quartiles[col]
                        women_team_elos[col].append(women_quartiles[col])
        
        # Calculate combined Elo for each type
        for col in elo_columns:
            men_team_info[col] = sum(men_team_elos[col])
            women_team_info[col] = sum(women_team_elos[col])
        
        # Add team records to appropriate gender lists
        if men_team_members:
            men_team_data.append(men_team_info)
        
        if women_team_members:
            women_team_data.append(women_team_info)
    
    return men_team_data, women_team_data, men_individual_data, women_individual_data

def process_mixed_relay_athlete(
    row_data: Dict, 
    elo_scores: pd.DataFrame, 
    fantasy_prices: Dict[str, int],
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
        
        # Update team name if we have a mapping, otherwise keep the original
        if team_name_part:
            row_data['Team_Name'] = f"{team_name_part} I"
        
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
        print(f"Error processing mixed relay athlete: {e}")
        traceback.print_exc()
        return row_data  # Return original data if processing fails

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
    
    print(f"Generating fallback data for {gender} mixed relay teams")
    
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
    chronos_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_elevation.csv"
    
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
                    'Country': race['Country'],
                    'Race_Type': 'Mixed Relay',
                    'Sex': 'M' if gender == 'men' else 'F'
                }
                
                # Add Elo values
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
            'Is_Present': False,  # This team is not in the actual startlist
            'Race_Type': 'Mixed Relay'
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
            
            # Sort by main Elo (highest first)
            # For mixed relay, we need to consider gender balance
            # In this fallback mode, we just take the top skiers
            # For mixed relays, we typically use 2 skiers per team per gender
            nation_skiers_data.sort(key=lambda x: x.get('Elo', 0), reverse=True)
            member_count = min(2, len(nation_skiers_data))  # Take up to 2 skiers for this gender
            
            selected_skiers = nation_skiers_data[:member_count]
            
            # If we don't have enough skiers, fill with dummy entries
            while len(selected_skiers) < 2:  # For mixed relay, each gender contributes 2 skiers
                dummy_skier = {
                    'name': f"Unknown {country_name} {gender.capitalize()} Skier {len(selected_skiers) + 1}",
                    'ID': None
                }
                # Add quartile values for Elo columns
                for elo_col in elo_columns:
                    dummy_skier[elo_col] = quartiles[elo_col]
                    
                selected_skiers.append(dummy_skier)
            
            # Add skiers to team record and sum up Elo values
            for i, skier_data in enumerate(selected_skiers, 1):
                # For mixed relay, the positions would typically alternate between genders
                # In this fallback mode, we'll just use sequential numbering for each gender
                
                # Add skier info
                team_record[f'Member_{i}'] = skier_data['name']
                team_record[f'Member_{i}_ID'] = skier_data['ID']
                
                # Add individual Elo values and update team totals
                for elo_col in elo_columns:
                    team_record[f'Member_{i}_{elo_col}'] = skier_data.get(elo_col, quartiles.get(elo_col, 1000))
                    team_record[elo_col] += skier_data.get(elo_col, quartiles.get(elo_col, 1000))  # Add to team total
        else:
            # If no skiers found, add dummy entries
            for i in range(1, 3):  # Each gender contributes 2 skiers to mixed relay
                team_record[f'Member_{i}'] = f"Unknown {country_name} {gender.capitalize()} Skier {i}"
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

# Update format_team_name in startlist_scrape_weekend_mixed_relay.py
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

def determine_athlete_sex(
    name: str, 
    bib: str, 
    existing_members: List[Dict], 
    fantasy_prices: Dict[str, Dict] = None
) -> str:
    """
    Determine athlete sex based on fantasy API data, bib positioning, name pattern, or existing team members
    
    Args:
        name: Athlete name
        bib: Athlete bib
        existing_members: List of team members already processed
        fantasy_prices: Dictionary of fantasy API data with gender information
    
    Returns:
        'M' for male, 'F' for female
    """
    # PREFERRED METHOD: Check fantasy API data
    if fantasy_prices:
        # Try to find athlete in fantasy data - first try exact match
        for athlete_id, athlete_data in fantasy_prices.items():
            if name.lower() == athlete_data.get('name', '').lower():
                # Return gender from API data
                gender = athlete_data.get('gender', '').lower()
                return 'M' if gender == 'm' else 'F' if gender == 'f' else None
        
        # If no exact match, try partial match on lastname
        last_name = name.split()[-1] if ' ' in name else name
        for athlete_id, athlete_data in fantasy_prices.items():
            api_name = athlete_data.get('name', '')
            if ' ' in api_name and last_name.lower() in api_name.lower():
                # Return gender from API data
                gender = athlete_data.get('gender', '').lower()
                return 'M' if gender == 'm' else 'F' if gender == 'f' else None
    
    # FALLBACK 1: Check bib position if available
    if '-' in bib:
        position = int(bib.split('-')[-1])
        # In mixed relays, positions 1,3 are typically men and 2,4 are women
        if position in [1, 3]:
            return 'M'
        elif position in [2, 4]:
            return 'F'
    
    # FALLBACK 2: If we already have team members, alternate sex
    if existing_members:
        last_member = existing_members[-1]
        return 'F' if last_member.get('sex') == 'M' else 'M'
    
    # FALLBACK 3: Try to determine from name (least reliable)
    # Common female name endings in various languages
    female_patterns = ['ova', 'ina', 'eva', 'aia', 'aya', 'kyte', 'iene', 'ienė', 'ová', 'ska']
    if any(name.lower().endswith(pattern) for pattern in female_patterns):
        return 'F'
    
    # Default to male if we can't determine
    print(f"Warning: Could not reliably determine gender for {name}, defaulting to 'M'")
    return 'M'

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Process mixed relay races
    process_weekend_mixed_relay_races(races_file)
            # Initialize skier data if not already present

