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

def process_weekend_mixed_relay_races(races_file: str = None) -> None:
    """
    Process mixed relay races for weekend, creating team CSV files
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing weekend mixed relay races")
    
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
            
            # Filter to only mixed relay races
            races_df = races_df[races_df['RaceType'] == 'Mixed Relay']
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
    
    # Process mixed relay races - no gender separation for mixed events
    if not races_df.empty:
        process_mixed_relay_races(races_df)
    else:
        print("No mixed relay races to process")

def process_mixed_relay_races(races_df: pd.DataFrame) -> None:
    """Process mixed relay races, creating team CSV"""
    print("\nProcessing mixed relay races")
    
    # Get path for output file
    team_output_path = "~/ski/elo/python/biathlon/polars/relay/excel365/startlist_mixed_relay_weekend_teams.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.expanduser(team_output_path)), exist_ok=True)
    
    # Get the ELO scores for both genders
    men_elo_path = "~/ski/elo/python/biathlon/polars/relay/excel365/men_chrono_pred.csv"
    women_elo_path = "~/ski/elo/python/biathlon/polars/relay/excel365/ladies_chrono_pred.csv"
    
    men_elo_scores = get_latest_elo_scores(men_elo_path)
    women_elo_scores = get_latest_elo_scores(women_elo_path)
    
    # Process each mixed relay race
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
        
        print(f"Processing mixed relay race {idx+1}: {race['City']} ({race['Date']})")
        
        # Get teams from the biathlon startlist
        teams = get_biathlon_relay_teams(startlist_url)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams
            fallback_teams = generate_fallback_data(men_elo_scores, women_elo_scores, race)
            
            all_teams_data.extend(fallback_teams)
        else:
            # Process the teams to calculate ELO scores
            teams_data = process_mixed_relay_teams(teams, race, men_elo_scores, women_elo_scores)
            
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
        fallback_teams = generate_fallback_data(men_elo_scores, women_elo_scores, race_info)
        
        all_teams_data = fallback_teams  # Replace any existing teams with fallback data
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(os.path.expanduser(team_output_path), index=False)
        print(f"Saved {len(team_df)} mixed relay teams to {team_output_path}")
    else:
        print(f"No team data generated")

def process_mixed_relay_teams(teams: List[Dict], race: pd.Series, men_elo_scores: pd.DataFrame, women_elo_scores: pd.DataFrame) -> List[Dict]:
    """
    Process mixed relay teams and calculate team ELO scores with improved gender detection
    
    Args:
        teams: List of teams with members
        race: Race information
        men_elo_scores: DataFrame with men's ELO scores
        women_elo_scores: DataFrame with women's ELO scores
        
    Returns:
        list: team_data with ELO calculations
    """
    team_data = []
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'
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
            'Race_Type': 'Mixed Relay',
            'Is_Present': True  # This team is in the actual startlist
        }
        
        # Initialize all Elo sums and counters to 0
        for col in elo_columns:
            team_info[f'Total_{col}'] = 0
            team_info[f'Avg_{col}'] = 0
        
        valid_member_count = 0
        team_members = []
        
        # Process each member
        for member in team['members']:
            member_name = member['name']
            team_members.append(member_name)
            
            # Get leg position (for fallback and reference)
            leg_position = int(member.get('leg', valid_member_count + 1))
            
            # IMPROVEMENT: Try to match with both men's and women's ELO scores 
            # to determine gender instead of relying on leg position
            men_elo_match = None
            women_elo_match = None
            men_match_score = 0
            women_match_score = 0
            
            # Try exact match first in men's database
            if member_name in men_elo_scores['Skier'].values:
                men_elo_match = member_name
                men_match_score = 100  # Perfect match
                print(f"Found exact men's ELO match for: {member_name}")
            else:
                # Try fuzzy matching if no exact match
                from thefuzz import fuzz
                best_match = fuzzy_match_name(member_name, men_elo_scores['Skier'].tolist())
                if best_match:
                    men_elo_match = best_match
                    # Calculate fuzzy match score for comparison
                    men_match_score = fuzz.ratio(normalize_name(member_name), normalize_name(best_match))
                    print(f"Found fuzzy men's ELO match: {member_name} -> {best_match} (score: {men_match_score})")
            
            # Try exact match in women's database
            if member_name in women_elo_scores['Skier'].values:
                women_elo_match = member_name
                women_match_score = 100  # Perfect match
                print(f"Found exact women's ELO match for: {member_name}")
            else:
                # Try fuzzy matching if no exact match
                best_match = fuzzy_match_name(member_name, women_elo_scores['Skier'].tolist())
                if best_match:
                    women_elo_match = best_match
                    # Calculate fuzzy match score for comparison
                    women_match_score = fuzz.ratio(normalize_name(member_name), normalize_name(best_match))
                    print(f"Found fuzzy women's ELO match: {member_name} -> {best_match} (score: {women_match_score})")
            
            # Determine gender based on best match score
            # If matches in both databases, use the one with the higher score
            if men_elo_match and women_elo_match:
                if men_match_score >= women_match_score:
                    is_male = True
                    elo_match = men_elo_match
                    elo_scores = men_elo_scores
                    quartiles = men_quartiles
                    print(f"Using men's match for {member_name} based on higher score")
                else:
                    is_male = False
                    elo_match = women_elo_match
                    elo_scores = women_elo_scores
                    quartiles = women_quartiles
                    print(f"Using women's match for {member_name} based on higher score")
            elif men_elo_match:
                is_male = True
                elo_match = men_elo_match
                elo_scores = men_elo_scores
                quartiles = men_quartiles
                print(f"Using men's match for {member_name} (only match found)")
            elif women_elo_match:
                is_male = False
                elo_match = women_elo_match
                elo_scores = women_elo_scores
                quartiles = women_quartiles
                print(f"Using women's match for {member_name} (only match found)")
            else:
                # Fallback to leg position assumption
                is_male = leg_position % 2 == 1  # Odd positions assumed to be male
                elo_scores = men_elo_scores if is_male else women_elo_scores
                quartiles = men_quartiles if is_male else women_quartiles
                elo_match = None
                print(f"No ELO match found, falling back to leg position assumption: {member_name} is {'male' if is_male else 'female'}")
            
            gender = 'men' if is_male else 'ladies'
            
            # Process the athlete now that gender is determined
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                
                # Add member info to team record
                team_info[f'Member_{leg_position}'] = elo_match
                team_info[f'Member_{leg_position}_ID'] = elo_data.get('ID', member.get('ibu_id', ''))
                team_info[f'Member_{leg_position}_Sex'] = 'M' if is_male else 'F'
                
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
                
                # Add placeholder details
                team_info[f'Member_{leg_position}'] = member_name
                team_info[f'Member_{leg_position}_ID'] = member.get('ibu_id', '')
                team_info[f'Member_{leg_position}_Sex'] = 'M' if is_male else 'F'
                
                # Use quartile values for all ELO columns
                for col in elo_columns:
                    team_info[f'Member_{leg_position}_{col}'] = quartiles[col]
                    team_info[f'Total_{col}'] += quartiles[col]
                
                valid_member_count += 1
        
        # Ensure all required positions are filled (for mixed relay, typically 4 positions)
        for i in range(1, 5):
            if f'Member_{i}' not in team_info:
                is_male = i % 2 == 1  # Odd positions for men
                quartiles = men_quartiles if is_male else women_quartiles
                
                team_info[f'Member_{i}'] = f"Unknown {nation} {'Male' if is_male else 'Female'}"
                team_info[f'Member_{i}_ID'] = ''
                team_info[f'Member_{i}_Sex'] = 'M' if is_male else 'F'
                
                # Use quartile values for missing members
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

def generate_fallback_data(men_elo_scores: pd.DataFrame, women_elo_scores: pd.DataFrame, race: pd.Series) -> List[Dict]:
    """
    Generate fallback team data when startlist is empty
    
    Args:
        men_elo_scores: DataFrame with men's ELO scores
        women_elo_scores: DataFrame with women's ELO scores
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
            'Race_Type': 'Mixed Relay',
            'Is_Present': False  # This team is not in the actual startlist
        }
        
        # Initialize all Elo sums and averages as empty strings
        for col in elo_columns:
            team_record[f'Total_{col}'] = ''
            team_record[f'Avg_{col}'] = ''
        
        # Initialize empty member fields for 4 legs of mixed relay
        for i in range(1, 5):
            is_male = i % 2 == 1  # Odd positions for men
            
            team_record[f'Member_{i}'] = ''
            team_record[f'Member_{i}_ID'] = ''
            team_record[f'Member_{i}_Sex'] = 'M' if is_male else 'F'
            
            # Set all member Elo values to empty strings
            for col in elo_columns:
                team_record[f'Member_{i}_{col}'] = ''
        
        # Add team record
        team_data.append(team_record)
    
    return team_data

if __name__ == "__main__":
    # Check if a specific races file was provided
    races_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Process weekend mixed relay races
    process_weekend_mixed_relay_races(races_file)
#    check_and_run_weekly_picks()