#!/usr/bin/env python3
import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional
import warnings
from datetime import datetime
import traceback
import subprocess
warnings.filterwarnings('ignore')

# Import common utility functions
from startlist_common import *

def process_weekend_relay_races(races_file: str = None) -> None:
    """
    Process relay races for weekend, creating team CSV files
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing weekend relay races")
    
    # Load races from file if provided, otherwise from standard location
    if races_file and os.path.exists(races_file):
        races_df = pd.read_csv(races_file)
        print(f"Loaded {len(races_df)} races from {races_file}")
    else:
        # Default to weekends.csv
        races_path = '~/ski/elo/python/biathlon/polars/excel365/weekends.csv'
        
        try:
            races_df = pd.read_csv(races_path)
            print(f"Loaded {len(races_df)} races from {races_path}")
            
            # Filter to only standard relay races
            races_df = races_df[races_df['RaceType'] == 'Relay']
            print(f"Filtered to {len(races_df)} standard relay races")
            
            # Find next race date
            next_date = find_next_race_date(races_df)
            
            # Filter to races on the next date
            races_df = filter_races_by_date(races_df, next_date)
            print(f"Found {len(races_df)} relay races on {next_date}")
# Add this deduplication step:
            if not races_df.empty:
                # Deduplicate races based on Date, City, and RaceType
                races_df = races_df.drop_duplicates(subset=['Date', 'City', 'RaceType'])
                print(f"After deduplication: {len(races_df)} relay races")            
            
        except Exception as e:
            print(f"Error loading races from {races_path}: {e}")
            traceback.print_exc()
            return
    
    # Process each gender separately
    men_races = races_df[races_df['Sex'] == 'M']
    ladies_races = races_df[races_df['Sex'] == 'L']
    
    if not men_races.empty:
        process_gender_relay_races(men_races, 'men')
    
    if not ladies_races.empty:
        process_gender_relay_races(ladies_races, 'ladies')

def process_gender_relay_races(races_df: pd.DataFrame, gender: str) -> None:
    """Process relay races for a specific gender, creating team CSV"""
    print(f"\nProcessing {gender} relay races")
    
    # Get path for output file
    team_output_path = f"~/ski/elo/python/biathlon/polars/relay/excel365/startlist_relay_weekend_teams_{gender}.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.expanduser(team_output_path)), exist_ok=True)
    
    # Get the ELO scores
    elo_path = f"~/ski/elo/python/biathlon/polars/relay/excel365/{gender}_chrono_pred.csv"
    elo_scores = get_latest_elo_scores(elo_path)
    
    # Process each relay race
    all_teams_data = []
    
    # Flag to check if any valid startlist URLs were found
    any_valid_startlist = False
    
    for idx, (_, race) in enumerate(races_df.iterrows()):
        startlist_url = race['Startlist']
        if pd.isna(startlist_url) or not startlist_url:
            print(f"No startlist URL for race {idx+1}")
            continue
        
        # Mark that we found at least one valid startlist URL
        any_valid_startlist = True
        
        print(f"Processing relay race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the biathlon startlist
        teams = get_biathlon_relay_teams(startlist_url)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams
            fallback_teams = generate_fallback_data(gender, elo_scores, race)
            
            all_teams_data.extend(fallback_teams)
        else:
            # Process the teams to calculate ELO scores
            teams_data = process_relay_teams(teams, race, gender)
            
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
                'Date': datetime.now().strftime('%Y-%m-%d'),
                'City': 'Unknown',
                'Country': 'Unknown'
            })
        
        # Generate fallback data
        fallback_teams = generate_fallback_data(gender, elo_scores, race_info)
        
        all_teams_data = fallback_teams  # Replace any existing teams with fallback data
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(os.path.expanduser(team_output_path), index=False)
        print(f"Saved {len(team_df)} {gender} relay teams to {team_output_path}")
    else:
        print(f"No team data generated for {gender}")

def process_relay_teams(teams: List[Dict], race: pd.Series, gender: str) -> List[Dict]:
    """
    Process relay teams and calculate team ELO scores
    
    Args:
        teams: List of teams with members
        race: Race information
        gender: 'men' or 'ladies'
        
    Returns:
        list: team_data with ELO calculations
    """
    team_data = []
    
    # Get the ELO scores
    elo_path = f"~/ski/elo/python/biathlon/polars/relay/excel365/{gender}_chrono_pred.csv"
    elo_scores = get_latest_elo_scores(elo_path)
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'
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
        nation = team['nation']
        team_name = team['team_name']
        
        print(f"Processing team from {nation}: {team_name}")
        
        # Initialize team info
        team_info = {
            'Team_Name': team_name,
            'Nation': nation,
            'Team_Rank': team['team_rank'],
            'Race_Date': race['Date'],
            'City': race['City'],
            'Country': race['Country'],
            'Race_Type': 'Relay',
            'Is_Present': True  # This team is in the actual startlist
        }
        
        # Initialize all Elo sums and averages to 0
        for col in elo_columns:
            team_info[f'Total_{col}'] = 0
            team_info[f'Avg_{col}'] = 0
        
        valid_member_count = 0
        team_members = []
        
        # Process each member
        for member in team['members']:
            member_name = member['name']
            team_members.append(member_name)
            
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
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                leg_position = member.get('leg', valid_member_count + 1)
                
                # Add member info to team record
                team_info[f'Member_{leg_position}'] = elo_match
                team_info[f'Member_{leg_position}_ID'] = elo_data.get('ID', member.get('ibu_id', ''))
                
                # Add up ELO values for team totals
                for col in elo_columns:
                    if col in elo_data and elo_data[col] is not None:
                        member_elo = float(elo_data[col])
                        team_info[f'Member_{leg_position}_{col}'] = member_elo
                        team_info[f'Total_{col}'] += member_elo
                    else:
                        # Use quartile value if Elo is missing
                        team_info[f'Member_{leg_position}_{col}'] = quartiles[col]
                        team_info[f'Total_{col}'] += quartiles[col]
                
                valid_member_count += 1
            else:
                print(f"No ELO match found for: {member_name}")
                leg_position = member.get('leg', valid_member_count + 1)
                
                # Add placeholder details
                team_info[f'Member_{leg_position}'] = member_name
                team_info[f'Member_{leg_position}_ID'] = member.get('ibu_id', '')
                
                # Use quartile values for all ELO columns
                for col in elo_columns:
                    team_info[f'Member_{leg_position}_{col}'] = quartiles[col]
                    team_info[f'Total_{col}'] += quartiles[col]
                
                valid_member_count += 1
        
        # Calculate average ELO values
        if valid_member_count > 0:
            for col in elo_columns:
                team_info[f'Avg_{col}'] = team_info[f'Total_{col}'] / valid_member_count
        
        # Add team record
        team_data.append(team_info)
    
    return team_data

def generate_fallback_data(gender: str, elo_scores: pd.DataFrame, race: pd.Series) -> List[Dict]:
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
        "NORWAY", "GERMANY", "FRANCE", "SWEDEN", "ITALY", 
        "RUSSIA", "CZECH REPUBLIC", "AUSTRIA", "SWITZERLAND", "UKRAINE", 
        "SLOVENIA", "FINLAND", "CANADA", "USA", "BELARUS"
    ]
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'
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
            'Race_Type': 'Relay',
            'Is_Present': False  # This team is not in the actual startlist
        }
        
        # Initialize all Elo sums and averages as empty strings
        for col in elo_columns:
            team_record[f'Total_{col}'] = ''
            team_record[f'Avg_{col}'] = ''
        
        # Initialize empty member fields
        for i in range(1, 5):  # Relay typically has 4 legs
            team_record[f'Member_{i}'] = ''
            team_record[f'Member_{i}_ID'] = ''
            
            # Set all member Elo values to empty strings
            for col in elo_columns:
                team_record[f'Member_{i}_{col}'] = ''
        
        # Add team record
        team_data.append(team_record)
    
    return team_data

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Process weekend relay races
    process_weekend_relay_races(races_file)
#    check_and_run_weekly_picks()