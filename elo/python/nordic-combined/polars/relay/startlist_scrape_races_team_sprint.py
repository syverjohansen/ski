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

def process_races_team_sprint(races_file: str = None) -> None:
    """
    Process team sprint races from races.csv, creating team CSV files
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing Nordic Combined team sprint races")
    
    # Load races from file if provided, otherwise from standard location
    if races_file and os.path.exists(races_file):
        races_df = pd.read_csv(races_file)
        print(f"Loaded {len(races_df)} races from {races_file}")
    else:
        # Default to races.csv
        races_path = '~/ski/elo/python/nordic-combined/polars/excel365/races.csv'
        
        try:
            races_df = pd.read_csv(races_path)
            print(f"Loaded {len(races_df)} races from {races_path}")
            
            # Get today's date in the same format as the CSV
            from datetime import datetime
            today_date = datetime.now().strftime('%m/%d/%Y')
            print(f"Today's date: {today_date}")
            
            # Filter to only TODAY'S races first
            today_races = races_df[races_df['Date'] == today_date]
            print(f"Filtered to {len(today_races)} races for today")
            
            # Filter to only team sprint races from today
            team_sprint_races = today_races[today_races['RaceType'].str.contains("Team Sprint", na=False)]
            races_df = team_sprint_races
            print(f"Filtered to {len(races_df)} team sprint races for today")
            
            # Add deduplication step
            if not races_df.empty:
                # Deduplicate races based on Date, City, and RaceType
                races_df = races_df.drop_duplicates(subset=['Date', 'City', 'RaceType'])
                print(f"After deduplication: {len(races_df)} team sprint races")            
            
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
    """Process team sprint races for a specific gender, creating team CSV"""
    print(f"\nProcessing {gender} team sprint races")
    
    # Get path for output file
    team_output_path = f"~/ski/elo/python/nordic-combined/polars/relay/excel365/startlist_team_sprint_races_{gender}.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.expanduser(team_output_path)), exist_ok=True)
    
    # Get the ELO scores
    elo_path = f"~/ski/elo/python/nordic-combined/polars/excel365/{gender}_chrono_pred.csv"
    elo_scores = get_latest_elo_scores(elo_path)
    
    # Process each team sprint race
    all_teams_data = []
    
    # Flag to check if any valid startlist URLs were found
    any_valid_startlist = False
    
    for idx, (_, race) in enumerate(races_df.iterrows()):
        race_id = None
        if 'Startlist' in race and not pd.isna(race['Startlist']):
            race_id = extract_race_id_from_url(race['Startlist'])
        
        if not race_id:
            print(f"No race ID for race {idx+1}")
            continue
        
        # Mark that we found at least one valid startlist URL
        any_valid_startlist = True
        
        print(f"Processing team sprint race {idx+1}: {race['City']} ({race['Date']}) with ID: {race_id}")
        
        # Get teams data from FIS website
        teams, race_meta = get_fis_race_data(race_id)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams
            fallback_teams = generate_fallback_data_team_sprint(gender, elo_scores, race)
            
            all_teams_data.extend(fallback_teams)
        else:
            # Process the teams to calculate ELO scores
            teams_data = process_team_sprint_teams(teams, race, gender, elo_scores)
            
            all_teams_data.extend(teams_data)
    
    # If no valid startlist URLs or no teams were found, use the fallback
    if not any_valid_startlist or not all_teams_data:
        print(f"No valid startlist data found, using fallback method")
        
        # Get any race info to use for context (use first race or create a dummy one)
        if not races_df.empty:
            race_info = races_df.iloc[0]
        else:
            # Create a dummy race with today's date
            race_info = pd.Series({
                'Date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                'City': 'Unknown',
                'Country': 'Unknown',
                'RaceType': 'Team Sprint'
            })
        
        # Generate fallback data
        fallback_teams = generate_fallback_data_team_sprint(gender, elo_scores, race_info)
        
        all_teams_data = fallback_teams  # Replace any existing teams with fallback data
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(os.path.expanduser(team_output_path), index=False)
        print(f"Saved {len(team_df)} {gender} team sprint entries to {team_output_path}")
    else:
        print(f"No team data generated for {gender}")

def process_team_sprint_teams(teams: List[Dict], race: pd.Series, gender: str, elo_scores: pd.DataFrame) -> List[Dict]:
    """
    Process team sprint teams and calculate team ELO scores
    
    Args:
        teams: List of teams with members from extract_team_results
        race: Race information
        gender: 'men' or 'ladies'
        elo_scores: DataFrame with ELO scores
        
    Returns:
        list: team_data with ELO calculations
    """
    team_data = []
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'MassStart_Elo', 'IndividualCompact_Elo'
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
        # Team data comes from extract_team_results function
        nation = team.get('Nation', '')
        team_name = team.get('TeamName', nation)
        
        print(f"Processing team from {nation}: {team_name}")
        
        # Initialize team info
        team_info = {
            'Team_Name': team_name,
            'Nation': nation,
            'Team_Rank': team.get('Rank', ''),
            'Race_Date': race['Date'],
            'City': race['City'],
            'Country': race['Country'],
            'Race_Type': race['RaceType'],
            'Is_Present': True,  # This team is in the actual startlist
            'Race1_Prob': 1.0    # 100% probability for this race
        }
        
        # Add team-level times and points if available
        team_info['Team_Time'] = team.get('Time', '')
        team_info['Team_TimeDiff'] = team.get('TimeDiff', '')
        team_info['Team_Points'] = team.get('Points', '')
        
        # Initialize all Elo sums and averages to 0
        for col in elo_columns:
            team_info[f'Total_{col}'] = 0
            team_info[f'Avg_{col}'] = 0
        
        valid_member_count = 0
        
        # For Nordic Combined team sprint, team data is structured like regular teams
        # Look for member data in the Members list
        team_members = []
        
        # Extract members from the team dictionary
        # Check if team has a Members list (proper structure)
        if 'Members' in team and team['Members']:
            team_members = team['Members']
        else:
            # Look for member data as individual keys in the team dictionary (fallback)
            member_indices = set()
            for key in team.keys():
                if key.startswith('Member_') and '_' in key:
                    # Extract member number from keys like 'Member_1', 'Member_1_ID', etc.
                    parts = key.split('_')
                    if len(parts) >= 2 and parts[1].isdigit():
                        member_indices.add(int(parts[1]))
            
            # Create member objects from the indexed data
            for member_num in sorted(member_indices):
                member_name = team.get(f'Member_{member_num}', '')
                if member_name and member_name != team.get('Nation', ''):  # Skip if it's just the team name
                    member_data = {
                        'Name': member_name,
                        'ID': team.get(f'Member_{member_num}_ID', ''),
                        'FisCode': team.get(f'Member_{member_num}_FisCode', ''),
                        'Year': team.get(f'Member_{member_num}_Year', ''),
                        'Leg': str(member_num),
                        'Jump': team.get(f'Member_{member_num}_Jump', ''),
                        'Points': team.get(f'Member_{member_num}_Points', '')
                    }
                    team_members.append(member_data)
        
        print(f"Found {len(team_members)} team members")
        
        # Debug: print the team structure to understand what we're getting
        print(f"Team keys: {list(team.keys())}")
        
        # Process each member (team sprint typically has 2 members)
        for i, member in enumerate(team_members):
            member_name = member.get('Name', '')
            
            # Extract all available member data
            member_id = member.get('ID', '')
            fis_code = member.get('FisCode', '')
            leg = member.get('Leg', str(i + 1))
            jump_distance = member.get('Jump', '')
            jump_points = member.get('Points', '')
            year = member.get('Year', '')
            
            print(f"Processing member {i+1}: {member_name} (Leg: {leg})")
            
            # Match with ELO scores
            elo_match = None
            if member_name in elo_scores['Skier'].values:
                elo_match = member_name
                print(f"Found exact ELO match for: {member_name}")
            else:
                # Try fuzzy matching if no exact match
                elo_match = fuzzy_match_name(member_name, elo_scores['Skier'].tolist())
                if elo_match:
                    print(f"Found fuzzy ELO match: {member_name} -> {elo_match}")
            
            # Use 1-indexed position
            leg_position = i + 1
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                
                # Add member info to team record
                team_info[f'Member_{leg_position}'] = elo_match
                team_info[f'Member_{leg_position}_ID'] = elo_data.get('ID', member_id)
                team_info[f'Member_{leg_position}_FisCode'] = fis_code
                team_info[f'Member_{leg_position}_Year'] = year
                team_info[f'Member_{leg_position}_Leg'] = leg
                team_info[f'Member_{leg_position}_Jump'] = jump_distance
                team_info[f'Member_{leg_position}_Points'] = jump_points
                
                # Add up ELO values for team totals
                for col in elo_columns:
                    if col in elo_data and elo_data[col] is not None:
                        try:
                            member_elo = float(elo_data[col])
                            team_info[f'Member_{leg_position}_{col}'] = member_elo
                            team_info[f'Total_{col}'] += member_elo
                        except (ValueError, TypeError):
                            # Use quartile value if conversion fails
                            team_info[f'Member_{leg_position}_{col}'] = quartiles[col]
                            team_info[f'Total_{col}'] += quartiles[col]
                    else:
                        # Use quartile value if Elo is missing
                        team_info[f'Member_{leg_position}_{col}'] = quartiles[col]
                        team_info[f'Total_{col}'] += quartiles[col]
                
                valid_member_count += 1
            else:
                print(f"No ELO match found for: {member_name}")
                
                # Add placeholder details
                team_info[f'Member_{leg_position}'] = member_name
                team_info[f'Member_{leg_position}_ID'] = member_id
                team_info[f'Member_{leg_position}_FisCode'] = fis_code
                team_info[f'Member_{leg_position}_Year'] = year
                team_info[f'Member_{leg_position}_Leg'] = leg
                team_info[f'Member_{leg_position}_Jump'] = jump_distance
                team_info[f'Member_{leg_position}_Points'] = jump_points
                
                # Use quartile values for all ELO columns
                for col in elo_columns:
                    team_info[f'Member_{leg_position}_{col}'] = quartiles[col]
                    team_info[f'Total_{col}'] += quartiles[col]
                
                valid_member_count += 1
        
        # Ensure all positions are filled (team sprint typically has 2 members)
        max_members = 2  # Standard team size for Nordic Combined team sprint
        for i in range(valid_member_count + 1, max_members + 1):
            team_info[f'Member_{i}'] = f"Unknown {nation} Athlete {i}"
            team_info[f'Member_{i}_ID'] = ''
            team_info[f'Member_{i}_FisCode'] = ''
            team_info[f'Member_{i}_Year'] = ''
            team_info[f'Member_{i}_Leg'] = str(i)
            team_info[f'Member_{i}_Jump'] = ''
            team_info[f'Member_{i}_Points'] = ''
            
            # Use quartile values for all ELO columns
            for col in elo_columns:
                team_info[f'Member_{i}_{col}'] = quartiles[col]
                team_info[f'Total_{col}'] += quartiles[col]
            
            valid_member_count += 1
        
        # Calculate average ELO values
        if valid_member_count > 0:
            for col in elo_columns:
                team_info[f'Avg_{col}'] = team_info[f'Total_{col}'] / valid_member_count
        
        # Add team record
        team_data.append(team_info)
    
    return team_data

def generate_fallback_data_team_sprint(gender: str, elo_scores: pd.DataFrame, race: pd.Series) -> List[Dict]:
    """
    Generate fallback team data when startlist is empty
    
    Args:
        gender: 'men' or 'ladies'
        elo_scores: DataFrame with ELO scores
        race: Race information
    
    Returns:
        list: Fallback team data
    """
    team_data = []
    
    # Define possible nations based on common participating countries
    common_nations = [
        "NORWAY", "GERMANY", "AUSTRIA", "JAPAN", "FINLAND",
        "FRANCE", "ITALY", "SLOVENIA", "SWITZERLAND", "CZECH REPUBLIC",
        "POLAND", "ESTONIA", "USA", "CANADA", "KOREA"
    ]
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'MassStart_Elo', 'IndividualCompact_Elo'
    ]
    
    # For each common nation, create a team entry
    for team_name in common_nations:
        # Create team record with empty values
        team_record = {
            'Team_Name': team_name,
            'Nation': team_name,
            'Team_Rank': 0,  # No rank since it's not from a startlist
            'Race_Date': race['Date'],
            'City': race['City'],
            'Country': race['Country'],
            'Race_Type': race['RaceType'],
            'Is_Present': False,  # This team is not in the actual startlist
            'Race1_Prob': 0.0     # 0% probability since not in startlist
        }
        
        # Add team-level fields that may be empty
        team_record['Team_Time'] = ''
        team_record['Team_TimeDiff'] = ''
        team_record['Team_Points'] = ''
        
        # Initialize all Elo sums and averages as empty strings
        for col in elo_columns:
            team_record[f'Total_{col}'] = ''
            team_record[f'Avg_{col}'] = ''
        
        # Initialize empty member fields for team sprint (typically 2 members)
        for i in range(1, 3):  # Team sprint has 2 members
            team_record[f'Member_{i}'] = ''
            team_record[f'Member_{i}_ID'] = ''
            team_record[f'Member_{i}_FisCode'] = ''
            team_record[f'Member_{i}_Year'] = ''
            team_record[f'Member_{i}_Leg'] = ''
            team_record[f'Member_{i}_Jump'] = ''
            team_record[f'Member_{i}_Points'] = ''
            
            # Set all member Elo values to empty strings
            for col in elo_columns:
                team_record[f'Member_{i}_{col}'] = ''
        
        # Add team record
        team_data.append(team_record)
    
    return team_data

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Process team sprint races
    process_races_team_sprint(races_file)
    
    # Only run race-picks.R if environment variable is not set
    if not os.environ.get('SKIP_RACE_PICKS'):
        print("\nChecking if races are today and need to run race-picks.R...")
        check_and_run_weekly_picks()