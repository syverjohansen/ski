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

def process_team_sprint_races(races_file: str = None, gender: str = None) -> None:
    """
    Main function to process team sprint races

    Args:
        races_file: Optional path to a CSV containing specific races to process
        gender: Optional gender filter ('men' or 'ladies')
    """
    print(f"Processing team sprint races{' for ' + gender if gender else ''}")

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

    # Process each gender separately to avoid mixing data
    if gender:
        # If gender specified, only process that gender
        gender_code = 'M' if gender == 'men' else 'L'
        gender_races = races_df[races_df['Sex'] == gender_code]
        if not gender_races.empty:
            process_gender_team_sprint_races(gender_races, gender)
    else:
        # Process both genders separately
        men_races = races_df[races_df['Sex'] == 'M']
        ladies_races = races_df[races_df['Sex'] == 'L']

        if not men_races.empty:
            process_gender_team_sprint_races(men_races, 'men')

        if not ladies_races.empty:
            process_gender_team_sprint_races(ladies_races, 'ladies')


def process_gender_team_sprint_races(races_df: pd.DataFrame, gender: str) -> None:
    """
    Process team sprint races for a specific gender

    Args:
        races_df: DataFrame containing races for this gender
        gender: 'men' or 'ladies'
    """
    print(f"\n{'='*50}\nProcessing {gender} team sprint races\n{'='*50}")

    # Initialize data collections for this gender
    all_teams_data = []
    all_individuals_data = []

    # Process each team sprint race
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}, skipping")
            continue

        print(f"Processing {gender} team sprint race {idx+1}: {race['City']} ({race['Date']})")

        # Get teams from the FIS startlist
        teams = get_team_sprint_teams(startlist_url)
        if not teams:
            print(f"No teams found for race {idx+1}, skipping")
            continue

        # Process the teams and create team and individual data
        teams_data, individuals_data = process_team_sprint_teams(teams, race, gender)

        # Add the data to our collections
        all_teams_data.extend(teams_data)
        all_individuals_data.extend(individuals_data)

    # Save team data for this gender
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        save_team_sprint_team_data(team_df, gender)
    else:
        print(f"No team data generated for {gender}")

    # Save individual data for this gender
    if all_individuals_data:
        individual_df = pd.DataFrame(all_individuals_data)
        save_team_sprint_individual_data(individual_df, gender)
    else:
        print(f"No individual data generated for {gender}")

# Update the get_team_sprint_teams function to add team_number
def get_team_sprint_teams(url: str) -> List[Dict]:
    """
    Get teams from FIS team sprint startlist - FIXED VERSION

    Returns list of teams with structure:
    [
        {
            'team_name': 'ITALY',
            'nation': 'ITA',
            'team_rank': '1',
            'team_time': '7:53.89',
            'team_number': 1,
            'members': [
                {'name': 'GANZ Caterina', 'nation': 'ITA', 'year': '1995', 'bib': '7-1'},
                {'name': 'CASSOL Federica', 'nation': 'ITA', 'year': '2000', 'bib': '7-2'}
            ]
        },
        ...
    ]
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        teams = []

        # Track team numbers by nation
        nation_team_counts = {}

        # Find all team rows (main rows) - these have class 'table-row_theme_main'
        team_rows = soup.select('.table-row.table-row_theme_main')

        print(f"Found {len(team_rows)} team rows")

        for team_row in team_rows:
            try:
                # Get team rank
                rank_elem = team_row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold.pr-1')
                if not rank_elem:
                    print("No rank element found, skipping row")
                    continue
                team_rank = rank_elem.text.strip()

                # Get team name (country name)
                team_name_elem = team_row.select_one('.g-lg-14.g-md-14.g-sm-11.g-xs-10.justify-left.bold')
                if not team_name_elem:
                    print("No team name element found, skipping row")
                    continue
                team_name = team_name_elem.text.strip()

                # Get nation code
                nation_elem = team_row.select_one('.country__name-short')
                if not nation_elem:
                    print(f"No nation code found for team {team_name}, skipping")
                    continue
                nation = nation_elem.text.strip()

                # Get team time
                team_time = ""
                time_elem = team_row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs')
                if time_elem:
                    team_time = time_elem.text.strip()

                # Update team counter for this nation
                if nation not in nation_team_counts:
                    nation_team_counts[nation] = 1
                else:
                    nation_team_counts[nation] += 1

                team_number = nation_team_counts[nation]

                print(f"Processing team: {team_name} ({nation}) - Rank {team_rank}, Time: {team_time}")

                team_data = {
                    'team_name': team_name,
                    'nation': nation,
                    'team_rank': team_rank,
                    'team_time': team_time,
                    'team_number': team_number,
                    'members': []
                }

                # Find the athlete rows that follow this team row
                # Athlete rows have class 'table-row_theme_additional' and contain 'athlete-team-row'
                current_element = team_row
                athletes_found = 0

                while current_element and athletes_found < 2:  # Team sprint has exactly 2 athletes
                    current_element = current_element.find_next_sibling('a')

                    if not current_element:
                        break

                    # Check if this is an athlete row
                    if 'table-row_theme_additional' in current_element.get('class', []):
                        athlete_row_div = current_element.select_one('.athlete-team-row')
                        if athlete_row_div:
                            # This is an athlete row for the current team
                            # Get just the athlete name, not the constructor name
                            athlete_name_elem = current_element.select_one('.athlete-name')
                            if athlete_name_elem:
                                athlete_name = athlete_name_elem.text.strip()

                                # Get birth year
                                year_elem = current_element.select_one('.g-lg-1.g-md-1.g-sm-2.g-xs-3.justify-left')
                                year = year_elem.text.strip() if year_elem else ''

                                # Get bib number
                                bib_elem = current_element.select_one('.bib')
                                bib = bib_elem.text.strip() if bib_elem else ''

                                team_data['members'].append({
                                    'name': athlete_name,
                                    'nation': nation,  # Use team's nation
                                    'year': year,
                                    'bib': bib
                                })

                                athletes_found += 1
                                print(f"  Added athlete: {athlete_name} (bib: {bib}, year: {year})")

                    elif 'table-row_theme_main' in current_element.get('class', []):
                        # We've reached the next team, stop looking for athletes
                        break

                if len(team_data['members']) > 0:
                    teams.append(team_data)
                    print(f"Added team {team_name} with {len(team_data['members'])} members")
                else:
                    print(f"Warning: No athletes found for team {team_name}")

            except Exception as e:
                print(f"Error processing team row: {e}")
                continue

        print(f"Found {len(teams)} teams with {sum(len(team['members']) for team in teams)} total athletes")

        # If primary method found no teams, try fallback method for individual athlete listing
        if not teams:
            print("Primary method found no teams. Trying fallback method for individual athlete listing...")
            teams = get_team_sprint_teams_fallback(soup)

        return teams

    except Exception as e:
        print(f"Error getting team sprint teams: {e}")
        traceback.print_exc()
        return []


def get_team_sprint_teams_fallback(soup: BeautifulSoup) -> List[Dict]:
    """
    Fallback method for parsing team sprint startlists where athletes are listed individually.

    In this format, each athlete is a separate row with an order number. With N total athletes,
    there are N/2 teams. Skier at position i pairs with skier at position i + N/2.

    For example, with 46 skiers (23 teams):
    - Position 1 pairs with position 24
    - Position 2 pairs with position 25
    - Position 23 pairs with position 46
    """
    try:
        teams = []
        athletes = []

        # Find all athlete rows - these are 'a' tags with class 'table-row' that link to athlete biographies
        athlete_rows = soup.select('a.table-row[href*="athlete-biography"]')

        print(f"Fallback: Found {len(athlete_rows)} athlete biography rows")

        for row in athlete_rows:
            try:
                # Get order/position number - try multiple selectors
                order_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.pr-1.bold')
                if not order_elem:
                    order_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold.pr-1')
                if not order_elem:
                    # Try finding any element with justify-right and bold that contains a number
                    for elem in row.select('.justify-right.bold'):
                        text = elem.text.strip()
                        if text.isdigit():
                            order_elem = elem
                            break
                if not order_elem:
                    print(f"Fallback: No order element found for row")
                    continue

                order = order_elem.text.strip()
                if not order.isdigit():
                    continue
                order = int(order)

                # Get athlete name - try multiple selectors
                name_elem = row.select_one('.athlete-name')
                if not name_elem:
                    # Try the actual class from FIS HTML
                    name_elem = row.select_one('.g-lg-19.g-md-19.g-sm-17.g-xs-16.justify-left.bold')
                if not name_elem:
                    # Try finding any large column with justify-left and bold
                    for elem in row.select('.justify-left.bold'):
                        text = elem.text.strip()
                        # Name should be longer than just a number and contain letters
                        if len(text) > 2 and any(c.isalpha() for c in text):
                            name_elem = elem
                            break
                if not name_elem:
                    print(f"Fallback: No name element found for order {order}")
                    continue
                name = name_elem.text.strip()

                # Get nation
                nation_elem = row.select_one('.country__name-short')
                if not nation_elem:
                    print(f"Fallback: No nation element found for {name}")
                    continue
                nation = nation_elem.text.strip()

                # Get birth year (may not be present in all formats)
                year_elem = row.select_one('.g-lg-1.g-md-1.g-sm-2.g-xs-3.justify-left')
                year = year_elem.text.strip() if year_elem else ''

                athletes.append({
                    'order': order,
                    'name': name,
                    'nation': nation,
                    'year': year
                })
                print(f"Fallback: Parsed athlete {order}: {name} ({nation})")

            except Exception as e:
                print(f"Fallback: Error parsing athlete row: {e}")
                continue

        if not athletes:
            print("Fallback: No athletes found")
            return []

        # Sort by order to ensure correct pairing
        athletes.sort(key=lambda x: x['order'])

        n_athletes = len(athletes)
        n_teams = n_athletes // 2

        print(f"Fallback: Found {n_athletes} athletes, forming {n_teams} teams")

        if n_athletes % 2 != 0:
            print(f"Fallback: Warning - odd number of athletes ({n_athletes}), last athlete will be ignored")

        # Track team numbers by nation
        nation_team_counts = {}

        # Pair athletes: position i pairs with position i + n_teams
        for i in range(n_teams):
            athlete1 = athletes[i]
            athlete2 = athletes[i + n_teams]

            # Both athletes should be from the same nation for a team sprint
            nation = athlete1['nation']
            if athlete1['nation'] != athlete2['nation']:
                print(f"Fallback: Warning - athletes from different nations paired: {athlete1['name']} ({athlete1['nation']}) and {athlete2['name']} ({athlete2['nation']})")
                # Use the first athlete's nation

            # Update team counter for this nation
            if nation not in nation_team_counts:
                nation_team_counts[nation] = 1
            else:
                nation_team_counts[nation] += 1

            team_number = nation_team_counts[nation]

            # Generate team name from nation
            team_name = nation  # Will be mapped to full name later

            team_data = {
                'team_name': team_name,
                'nation': nation,
                'team_rank': str(i + 1),  # Use position as initial rank
                'team_time': '',
                'team_number': team_number,
                'members': [
                    {
                        'name': athlete1['name'],
                        'nation': nation,
                        'year': athlete1['year'],
                        'bib': f"{i+1}-1"  # Generate bib from position
                    },
                    {
                        'name': athlete2['name'],
                        'nation': nation,
                        'year': athlete2['year'],
                        'bib': f"{i+1}-2"
                    }
                ]
            }

            teams.append(team_data)
            print(f"Fallback: Created team {nation} #{team_number}: {athlete1['name']} + {athlete2['name']}")

        print(f"Fallback: Created {len(teams)} teams with {sum(len(team['members']) for team in teams)} total athletes")
        return teams

    except Exception as e:
        print(f"Error in fallback team sprint parsing: {e}")
        traceback.print_exc()
        return []

# Update process_team_sprint_teams in startlist_scrape_races_team_sprint.py
def process_team_sprint_teams(teams: List[Dict], race: pd.Series, gender: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Process team sprint teams and create team and individual data
    
    Args:
        teams: List of teams with members
        race: Race information
        gender: 'men' or 'ladies'
        
    Returns:
        tuple: (team_data, individual_data)
    """
    # Initialize data for teams and individual athletes
    team_data = []
    individual_data = []
    
    # Get the ELO scores
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    elo_scores = get_latest_elo_scores(elo_path)
    
    # Get fantasy prices and team prices
    fantasy_prices = get_fantasy_prices()
    fantasy_teams = get_fantasy_teams(gender)
    
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
            'Race_Type': 'Team Sprint'
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
                'Team_Position': leg_number,  # Just the leg number (1 or 2)
                'Race_Type': 'Team Sprint'
            }
            
            # Process the athlete to match with ELO scores and prices
            processed_data = process_team_sprint_athlete(
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
        
        # Update team name if we have a mapping, otherwise keep the original
        # Don't modify the team name - it already has the correct suffix from the parent function
        # The row_data['Team_Name'] should already have the correct team name with suffix
        
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

def save_team_sprint_individual_data(df: pd.DataFrame, gender: str) -> None:
    """Save processed team sprint individual data to a CSV file"""
    try:
        # Sort by team rank and position
        df = df.sort_values(['Team_Rank', 'Team_Position'])
        
        # Save to CSV
        output_path = f"~/ski/elo/python/ski/polars/relay/excel365/startlist_team_sprint_races_individuals_{gender}.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} {gender} individual athletes to {output_path}")
    
    except Exception as e:
        print(f"Error saving team sprint individual data: {e}")
        traceback.print_exc()

def save_team_sprint_team_data(df: pd.DataFrame, gender: str) -> None:
    """Save processed team sprint team data to a CSV file"""
    try:
        # Sort by team rank
        df = df.sort_values(['Team_Rank'])
        
        # Save to CSV
        output_path = f"~/ski/elo/python/ski/polars/relay/excel365/startlist_team_sprint_races_teams_{gender}.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
        
        df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(df)} {gender} teams to {output_path}")
    
    except Exception as e:
        print(f"Error saving team sprint team data: {e}")
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
    
    # Process team sprint races
    process_team_sprint_races(races_file, gender)
    call_r_script('races', 'team_sprint')