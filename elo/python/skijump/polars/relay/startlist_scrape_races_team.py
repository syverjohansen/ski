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

def process_races_team(races_file: str = None) -> None:
    """
    Process team races from races.csv, creating team CSV files
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing Ski Jumping team races")
    
    # Load races from file if provided, otherwise from standard location
    if races_file and os.path.exists(races_file):
        races_df = pd.read_csv(races_file)
        print(f"Loaded {len(races_df)} races from {races_file}")
    else:
        # Default to races.csv
        races_path = '~/ski/elo/python/skijump/polars/excel365/races.csv'
        
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
            
            # Filter to only team races (not mixed team) from today
            team_races = today_races[(today_races['RaceType'].str.contains("Team", na=False)) & 
                          (~today_races['RaceType'].str.contains("Mixed", na=False)) &
                          (today_races['Sex'] != 'Mixed')]
            races_df = team_races
            print(f"Filtered to {len(races_df)} team races for today")
            
            # Add deduplication step
            if not races_df.empty:
                # Deduplicate races based on Date, City, and RaceType
                races_df = races_df.drop_duplicates(subset=['Date', 'City', 'RaceType'])
                print(f"After deduplication: {len(races_df)} team races for today")            
            
        except Exception as e:
            print(f"Error loading races from {races_path}: {e}")
            traceback.print_exc()
            return
    
    # Process each gender separately
    men_races = races_df[races_df['Sex'] == 'M']
    ladies_races = races_df[races_df['Sex'] == 'L']
    
    if not men_races.empty:
        process_gender_team_races(men_races, 'men')
    
    if not ladies_races.empty:
        process_gender_team_races(ladies_races, 'ladies')

def process_gender_team_races(races_df: pd.DataFrame, gender: str) -> None:
    """Process team races for a specific gender, creating team CSV"""
    print(f"\nProcessing {gender} team races")
    
    # Get path for output file
    team_output_path = f"~/ski/elo/python/skijump/polars/relay/excel365/startlist_team_races_{gender}.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.expanduser(team_output_path)), exist_ok=True)
    
    # Get the ELO scores (chrono_pred files are in the main excel365 directory, not relay)
    elo_path = f"~/ski/elo/python/skijump/polars/excel365/{gender}_chrono_pred.csv"
    elo_scores = get_latest_elo_scores(elo_path)
    
    # Process each team race
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
        
        print(f"Processing team race {idx+1}: {race['City']} ({race['Date']}) with ID: {race_id}")
        
        # Get teams data from FIS website
        teams, race_meta = get_fis_race_data(race_id)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams
            fallback_teams = generate_fallback_data_team(gender, elo_scores, race)
            
            all_teams_data.extend(fallback_teams)
        else:
            # Process the teams to calculate ELO scores
            teams_data = process_teams(teams, race, gender, elo_scores)
            
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
                'RaceType': 'Team'
            })
        
        # Generate fallback data
        fallback_teams = generate_fallback_data_team(gender, elo_scores, race_info)
        
        all_teams_data = fallback_teams  # Replace any existing teams with fallback data
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(os.path.expanduser(team_output_path), index=False)
        print(f"Saved {len(team_df)} {gender} team entries to {team_output_path}")
    else:
        print(f"No team data generated for {gender}")

def process_teams(teams: List[Dict], race: pd.Series, gender: str, elo_scores: pd.DataFrame) -> List[Dict]:
    """
    Process teams and calculate team ELO scores
    
    Args:
        teams: List of teams with members from extract_team_results
        race: Race information
        gender: 'men' or 'ladies'
        elo_scores: DataFrame with ELO scores
        
    Returns:
        list: team_data with ELO calculations
    """
    team_data = []
    
    # Define Elo columns to work with for ski jumping
    elo_columns = [
        'Elo', 'Small_Elo', 'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'
    ]
    
    # Determine hill size for race-specific ELO
    hill_size = race.get('HillSize', determine_hill_size_from_race_type(race.get('RaceType', '')))
    
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
            'Hill_Size': hill_size,
            'Is_Present': True,  # This team is in the actual startlist
            'Race1_Prob': 1.0    # 100% probability for this race
        }
        
        # Add team-level points if available
        team_info['Team_Points'] = team.get('Points', '')
        
        # Initialize all Elo sums and averages to 0
        for col in elo_columns:
            team_info[f'Total_{col}'] = 0
            team_info[f'Avg_{col}'] = 0
        
        valid_member_count = 0
        
        # For ski jumping team events, team data is structured with Members list
        team_members = team.get('Members', [])
        print(f"Found {len(team_members)} team members")
        
        for i, member in enumerate(team_members):
            member_name = member.get('Name', '')
            
            # Extract all available member data
            member_id = member.get('ID', '')
            fis_code = member.get('FisCode', '')
            length1 = member.get('Length1', '')
            length2 = member.get('Length2', '')
            member_points = member.get('Points', '')
            year = member.get('Year', '')
            
            print(f"Processing member {i+1}: {member_name}")
            
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
            position = i + 1
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                
                # Add member info to team record
                team_info[f'Member_{position}'] = elo_match
                team_info[f'Member_{position}_ID'] = elo_data.get('ID', member_id)
                team_info[f'Member_{position}_FisCode'] = fis_code
                team_info[f'Member_{position}_Year'] = year
                team_info[f'Member_{position}_Length1'] = length1
                team_info[f'Member_{position}_Length2'] = length2
                team_info[f'Member_{position}_Points'] = member_points
                
                # Add up ELO values for team totals
                for col in elo_columns:
                    if col in elo_data and elo_data[col] is not None:
                        try:
                            member_elo = float(elo_data[col])
                            team_info[f'Member_{position}_{col}'] = member_elo
                            team_info[f'Total_{col}'] += member_elo
                        except (ValueError, TypeError):
                            # Use quartile value if conversion fails
                            team_info[f'Member_{position}_{col}'] = quartiles[col]
                            team_info[f'Total_{col}'] += quartiles[col]
                    else:
                        # Use quartile value if Elo is missing
                        team_info[f'Member_{position}_{col}'] = quartiles[col]
                        team_info[f'Total_{col}'] += quartiles[col]
                
                valid_member_count += 1
            else:
                print(f"No ELO match found for: {member_name}")
                
                # Add placeholder details
                team_info[f'Member_{position}'] = member_name
                team_info[f'Member_{position}_ID'] = member_id
                team_info[f'Member_{position}_FisCode'] = fis_code
                team_info[f'Member_{position}_Year'] = year
                team_info[f'Member_{position}_Length1'] = length1
                team_info[f'Member_{position}_Length2'] = length2
                team_info[f'Member_{position}_Points'] = member_points
                
                # Use quartile values for all ELO columns
                for col in elo_columns:
                    team_info[f'Member_{position}_{col}'] = quartiles[col]
                    team_info[f'Total_{col}'] += quartiles[col]
                
                valid_member_count += 1
        
        # Ensure all positions are filled (usually 4 members per team in ski jumping)
        max_members = 4  # Standard team size for ski jumping
        for i in range(valid_member_count + 1, max_members + 1):
            team_info[f'Member_{i}'] = f"Unknown {nation} Athlete {i}"
            team_info[f'Member_{i}_ID'] = ''
            team_info[f'Member_{i}_FisCode'] = ''
            team_info[f'Member_{i}_Year'] = ''
            team_info[f'Member_{i}_Length1'] = ''
            team_info[f'Member_{i}_Length2'] = ''
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

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Process team races
    process_races_team(races_file)
    
    # Only run race-picks.R if environment variable is not set
    if not os.environ.get('SKIP_RACE_PICKS'):
        print("\nChecking if races are today and need to run race-picks.R...")
        check_and_run_weekly_picks()