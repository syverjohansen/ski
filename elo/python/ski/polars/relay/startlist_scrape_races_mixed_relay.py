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

def process_mixed_relay_races(races_file: str = None, gender: str = None) -> None:
    """
    Main function to process mixed relay races
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
        gender: Optional gender filter ('men' or 'ladies')
    """
    print(f"Processing mixed relay races{' for ' + gender if gender else ''}")
    
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
    
    # Initialize data collections for all races by gender
    all_men_teams_data = []
    all_men_individuals_data = []
    all_women_teams_data = []
    all_women_individuals_data = []
    
    # Process each mixed relay race
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}, skipping")
            continue
        
        print(f"Processing mixed relay race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the FIS startlist
        # Get fantasy athlete data for gender determination
        fantasy_athlete_data = get_fantasy_prices(get_full_athlete_data=True)
        teams = get_mixed_relay_teams(startlist_url, fantasy_athlete_data)
        
        if not teams:
            print(f"No teams found for race {idx+1}, skipping")
            continue
        
        # Process the teams and create team and individual data by gender
        men_team_data, women_team_data, men_individual_data, women_individual_data = process_mixed_relay_teams(
            teams, race
        )
        
        # Add to respective gender collections
        if men_team_data and (not gender or gender == 'men'):
            all_men_teams_data.extend(men_team_data)
        if men_individual_data and (not gender or gender == 'men'):
            all_men_individuals_data.extend(men_individual_data)
        if women_team_data and (not gender or gender == 'ladies'):
            all_women_teams_data.extend(women_team_data)
        if women_individual_data and (not gender or gender == 'ladies'):
            all_women_individuals_data.extend(women_individual_data)
    
    # Save collected data by gender
    # Men's data
    if all_men_teams_data and (not gender or gender == 'men'):
        men_team_df = pd.DataFrame(all_men_teams_data)
        save_mixed_relay_team_data(men_team_df, 'men')
    else:
        if not gender or gender == 'men':
            print("No men's team data generated")
    
    if all_men_individuals_data and (not gender or gender == 'men'):
        men_individual_df = pd.DataFrame(all_men_individuals_data)
        save_mixed_relay_individual_data(men_individual_df, 'men')
    else:
        if not gender or gender == 'men':
            print("No men's individual data generated")
    
    # Women's data
    if all_women_teams_data and (not gender or gender == 'ladies'):
        women_team_df = pd.DataFrame(all_women_teams_data)
        save_mixed_relay_team_data(women_team_df, 'ladies')
    else:
        if not gender or gender == 'ladies':
            print("No ladies' team data generated")
    
    if all_women_individuals_data and (not gender or gender == 'ladies'):
        women_individual_df = pd.DataFrame(all_women_individuals_data)
        save_mixed_relay_individual_data(women_individual_df, 'ladies')
    else:
        if not gender or gender == 'ladies':
            print("No ladies' individual data generated")

def get_mixed_relay_teams(url: str, fantasy_prices: Dict = None) -> List[Dict]:
    """
    Get teams from FIS mixed relay startlist with improved gender determination
    
    Returns list of teams with structure:
    [
        {
            'team_name': 'COUNTRY I',
            'nation': 'XXX',
            'team_rank': 1,
            'team_time': '54:45.3',
            'members': [
                {'name': 'ATHLETE NAME', 'nation': 'XXX', 'year': '1994', 'bib': '1-1', 'sex': 'M'},
                {'name': 'ATHLETE NAME', 'nation': 'XXX', 'year': '1997', 'bib': '1-2', 'sex': 'F'},
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
        
        # Find all team rows (main rows)
        team_rows = soup.select('.table-row_theme_main')
        
        for team_row in team_rows:
            team_name_elem = team_row.select_one('.g-lg-14.g-md-14.g-sm-11.g-xs-10')
            if not team_name_elem:
                continue
                
            team_data = {
                'team_name': team_name_elem.text.strip(),
                'nation': team_row.select_one('.country__name-short').text.strip(),
                'team_rank': team_row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold').text.strip(),
                'team_time': '',
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
                        
                        # Determine athlete sex using API data when available
                        sex = determine_athlete_sex(
                            athlete_name, 
                            bib, 
                            team_data['members'], 
                            fantasy_prices
                        )
                        
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
        print(f"Error getting mixed relay teams: {e}")
        traceback.print_exc()
        return []

def get_fantasy_prices(get_full_athlete_data: bool = False) -> Dict:
    """
    Get prices from Fantasy XC API
    
    Args:
        get_full_athlete_data: If True, returns full athlete data including gender info
                              If False, returns only prices keyed by athlete name
    
    Returns:
        Dictionary of prices keyed by athlete name or full athlete data dictionary
    """
    try:
        response = requests.get('https://www.fantasyxc.se/api/athletes')
        response.raise_for_status()
        
        athletes = response.json()
        
        if get_full_athlete_data:
            # Return full athlete data keyed by athlete ID and including gender info
            athlete_data = {
                athlete['athlete_id']: {
                    'name': athlete['name'],
                    'price': athlete['price'],
                    'gender': athlete['gender'],
                    'country': athlete.get('country', ''),
                    'is_team': athlete.get('is_team', False)
                }
                for athlete in athletes
                if not athlete.get('is_team', False)  # Filter out teams
            }
            print(f"Got data for {len(athlete_data)} athletes from Fantasy XC")
            return athlete_data
        else:
            # Return just prices keyed by name for backward compatibility
            prices = {
                athlete['name']: athlete['price'] 
                for athlete in athletes 
                if not athlete.get('is_team', False)  # Filter out teams
            }
            print(f"Got prices for {len(prices)} athletes from Fantasy XC")
            return prices
        
    except Exception as e:
        print(f"Error getting Fantasy XC prices: {e}")
        return {}

def determine_athlete_sex(
    name: str, 
    bib: str, 
    existing_members: List[Dict], 
    fantasy_prices: Dict = None
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

def process_mixed_relay_teams(teams: List[Dict], race: pd.Series) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Process mixed relay teams and create separate team and individual data by gender
    
    Args:
        teams: List of teams with members
        race: Race information
        
    Returns:
        tuple: (men_team_data, women_team_data, men_individual_data, women_individual_data)
    """
    # Initialize data for men and women
    men_team_data = []
    women_team_data = []
    men_individual_data = []
    women_individual_data = []
    
    # Get the ELO scores for both genders
    men_elo_path = "~/ski/elo/python/ski/polars/excel365/men_chrono_elevation.csv"
    women_elo_path = "~/ski/elo/python/ski/polars/excel365/ladies_chrono_elevation.csv"
    
    men_elo_scores = get_latest_elo_scores(men_elo_path)
    women_elo_scores = get_latest_elo_scores(women_elo_path)
    
    # Get fantasy prices and team prices
    fantasy_prices = get_fantasy_prices()
    men_fantasy_teams = get_fantasy_teams('men')
    women_fantasy_teams = get_fantasy_teams('ladies')
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 
        'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Calculate quartiles for each gender
    men_quartiles = {}
    women_quartiles = {}
    
    for col in elo_columns:
        # Men's quartiles
        if col in men_elo_scores.columns:
            numeric_values = pd.to_numeric(men_elo_scores[col], errors='coerce')
            men_quartiles[col] = numeric_values.quantile(0.25)
        else:
            men_quartiles[col] = 1000
            
        # Women's quartiles
        if col in women_elo_scores.columns:
            numeric_values = pd.to_numeric(women_elo_scores[col], errors='coerce')
            women_quartiles[col] = numeric_values.quantile(0.25)
        else:
            women_quartiles[col] = 1000
    
    # Process each team
    for team in teams:
        # Map to standardized country name
        nation = team['nation']
        team_name_part = map_country_to_team_name(nation)
        
        # Skip teams that don't have a match in the team spreadsheet
        if not team_name_part:
            print(f"Skipping team from {nation} - no matching country in team list")
            continue
            
        # Use exact format from team spreadsheet
        team_name = f"{team_name_part} I"
        print(f"Processing team from {nation} as {team_name}")
        
        # Initialize team info for both genders
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
        
        # Get team price from fantasy API if available
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
        
        # Track position numbers by gender to avoid duplicates
        men_position_counter = 0
        women_position_counter = 0
        men_position_numbers_used = set()
        women_position_numbers_used = set()
        
        # Process each member
        for member in team['members']:
            # Determine if male or female
            is_male = member.get('sex', 'M') == 'M'
            
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
            
            # Ensure no duplicate position numbers within gender
            if is_male:
                while position_number in men_position_numbers_used:
                    position_number += 1
                men_position_numbers_used.add(position_number)
            else:
                while position_number in women_position_numbers_used:
                    position_number += 1
                women_position_numbers_used.add(position_number)
            
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
                'Team_Position': bib,  # Keep original bib for reference
                'Race_Type': 'Mixed Relay',
                'Sex': 'M' if is_male else 'F'
            }
            
            # Process based on gender
            if is_male:
                # Process male athlete
                processed_data = process_mixed_relay_athlete(
                    row_data, 
                    men_elo_scores, 
                    fantasy_prices, 
                    'men',
                    men_quartiles
                )
                
                # Add race information
                processed_data['Race_Date'] = race['Date']
                processed_data['City'] = race['City']
                processed_data['Country'] = race['Country']
                
                # Add to individual data
                men_individual_data.append(processed_data)
                
                # Extract member info for team record
                men_team_members.append(processed_data['Skier'])
                
                # Set member in team info
                men_team_info[f'Member_{position_number}'] = processed_data['Skier']
                men_team_info[f'Member_{position_number}_ID'] = processed_data.get('ID', None)
                
                # Add all Elo values to team sums
                for col in elo_columns:
                    if col in processed_data and processed_data[col] is not None:
                        member_elo = float(processed_data[col])
                        men_team_info[f'Member_{position_number}_{col}'] = member_elo
                        men_team_elos[col].append(member_elo)
                    else:
                        # Use quartile value if Elo is missing
                        men_team_info[f'Member_{position_number}_{col}'] = men_quartiles[col]
                        men_team_elos[col].append(men_quartiles[col])
            else:
                # Process female athlete
                processed_data = process_mixed_relay_athlete(
                    row_data, 
                    women_elo_scores, 
                    fantasy_prices, 
                    'ladies',
                    women_quartiles
                )
                
                # Add race information
                processed_data['Race_Date'] = race['Date']
                processed_data['City'] = race['City']
                processed_data['Country'] = race['Country']
                
                # Add to individual data
                women_individual_data.append(processed_data)
                
                # Extract member info for team record
                women_team_members.append(processed_data['Skier'])
                
                # Set member in team info
                women_team_info[f'Member_{position_number}'] = processed_data['Skier']
                women_team_info[f'Member_{position_number}_ID'] = processed_data.get('ID', None)
                
                # Add all Elo values to team sums
                for col in elo_columns:
                    if col in processed_data and processed_data[col] is not None:
                        member_elo = float(processed_data[col])
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
        
        # Add team records if they have members
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

def save_mixed_relay_individual_data(df: pd.DataFrame, gender: str) -> None:
    """Save processed mixed relay individual data to a CSV file"""
    try:
        # Sort by team rank and position
        df = df.sort_values(['Team_Rank', 'Team_Position'])
        
        # Save to CSV
        output_path = f"~/ski/elo/python/ski/polars/relay/excel365/startlist_relay_mixed_races_individuals_{gender}.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} {gender} individual athletes to {output_path}")
    
    except Exception as e:
        print(f"Error saving mixed relay individual data: {e}")
        traceback.print_exc()

def save_mixed_relay_team_data(df: pd.DataFrame, gender: str) -> None:
    """Save processed mixed relay team data to a CSV file"""
    try:
        # Sort by team rank
        df = df.sort_values(['Team_Rank'])
        
        # Save to CSV
        output_path = f"~/ski/elo/python/ski/polars/relay/excel365/startlist_relay_mixed_races_teams_{gender}.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} {gender} teams to {output_path}")
    
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
    else:
        return f"{name} I"

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Check if gender was specified
    gender = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Process mixed relay races
    process_mixed_relay_races(races_file, gender)