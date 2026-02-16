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

def process_races_mixed_team(races_file: str = None) -> None:
    """
    Process mixed team races from races.csv, creating team CSV files
    
    Args:
        races_file: Optional path to a CSV containing specific races to process
    """
    print("Processing Ski Jumping mixed team races")
    
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
            
            # Filter to only TODAY'S mixed team races
            today_races = races_df[races_df['Date'] == today_date]
            print(f"Filtered to {len(today_races)} races for today")
            
            mixed_team_races = today_races[(today_races['RaceType'].str.contains("Team", na=False)) |
                                         (today_races['Sex'] == 'Mixed')]
            races_df = mixed_team_races
            print(f"Filtered to {len(races_df)} mixed team races for today")
            
            # Add deduplication step
            if not races_df.empty:
                # Deduplicate races based on Date, City, and RaceType
                races_df = races_df.drop_duplicates(subset=['Date', 'City', 'RaceType'])
                print(f"After deduplication: {len(races_df)} mixed team races for today")            
            
        except Exception as e:
            print(f"Error loading races from {races_path}: {e}")
            traceback.print_exc()
            return
    
    # Process mixed team races
    if not races_df.empty:
        process_mixed_team_races(races_df)
    else:
        print("No mixed team races to process")

def process_mixed_team_races(races_df: pd.DataFrame) -> None:
    """Process mixed team races, creating team CSV"""
    print("\nProcessing mixed team races")
    
    # Get path for output file
    team_output_path = "~/ski/elo/python/skijump/polars/relay/excel365/startlist_mixed_team_races_teams.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.expanduser(team_output_path)), exist_ok=True)
    
    # Get the ELO scores for both genders (chrono_pred files are in the main excel365 directory, not relay)
    men_elo_path = "~/ski/elo/python/skijump/polars/excel365/men_chrono_pred.csv"
    women_elo_path = "~/ski/elo/python/skijump/polars/excel365/ladies_chrono_pred.csv"
    
    men_elo_scores = get_latest_elo_scores(men_elo_path)
    women_elo_scores = get_latest_elo_scores(women_elo_path)
    
    # Process each mixed team race
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
        
        print(f"Processing mixed team race {idx+1}: {race['City']} ({race['Date']}) with ID: {race_id}")
        
        # Get teams data from FIS website
        teams, race_meta = get_fis_race_data(race_id)
        
        if not teams or len(teams) == 0:
            print(f"No teams found for race {idx+1}, using fallback method")
            
            # Use fallback method to generate teams
            fallback_teams = generate_fallback_data_mixed_team(men_elo_scores, women_elo_scores, race)
            
            all_teams_data.extend(fallback_teams)
        else:
            # Process the teams to calculate ELO scores
            teams_data = process_mixed_teams(teams, race, men_elo_scores, women_elo_scores)
            
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
                'RaceType': 'Mixed Team'
            })
        
        # Generate fallback data
        fallback_teams = generate_fallback_data_mixed_team(men_elo_scores, women_elo_scores, race_info)
        
        all_teams_data = fallback_teams  # Replace any existing teams with fallback data
    
    # Save team data
    if all_teams_data:
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(os.path.expanduser(team_output_path), index=False)
        print(f"Saved {len(team_df)} mixed team entries to {team_output_path}")
    else:
        print(f"No team data generated")

def process_mixed_teams(teams: List[Dict], race: pd.Series, men_elo_scores: pd.DataFrame, women_elo_scores: pd.DataFrame) -> List[Dict]:
    """
    Process mixed teams and calculate team ELO scores with improved gender detection
    
    Args:
        teams: List of teams with members from extract_team_results
        race: Race information
        men_elo_scores: DataFrame with men's ELO scores
        women_elo_scores: DataFrame with women's ELO scores
        
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
        
        # Initialize all Elo sums and counters to 0
        for col in elo_columns:
            team_info[f'Total_{col}'] = 0
            team_info[f'Avg_{col}'] = 0
        
        valid_member_count = 0
        
        # Process each member with gender detection
        team_members = team.get('Members', [])
        print(f"Found {len(team_members)} team members")
        
        for i, member in enumerate(team_members):
            member_name = member.get('Name', '')
            position = i + 1  # 1-indexed position
            
            # Extract all available member data
            member_id = member.get('ID', '')
            fis_code = member.get('FisCode', '')
            length1 = member.get('Length1', '')
            length2 = member.get('Length2', '')
            member_points = member.get('Points', '')
            year = member.get('Year', '')
            
            print(f"Processing member {position}: {member_name}")
            
            # For mixed team ski jumping, typically alternates male/female
            # Position 1,3 = male, Position 2,4 = female (common pattern)
            expected_is_male = position in [1, 3]
            
            # Try to match with both ELO databases to determine actual gender
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
                best_match = fuzzy_match_name(member_name, men_elo_scores['Skier'].tolist())
                if best_match:
                    men_elo_match = best_match
                    from thefuzz import fuzz
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
                    from thefuzz import fuzz
                    women_match_score = fuzz.ratio(normalize_name(member_name), normalize_name(best_match))
                    print(f"Found fuzzy women's ELO match: {member_name} -> {best_match} (score: {women_match_score})")
            
            # Determine final gender based on database matches and expected position
            if men_elo_match and women_elo_match:
                # If athlete exists in both databases, use the one with the higher match score
                # But if scores are close, defer to our position-based assumption
                if abs(men_match_score - women_match_score) < 10:
                    # Scores are close, use position-based assumption
                    final_is_male = expected_is_male
                    elo_match = men_elo_match if final_is_male else women_elo_match
                    print(f"Match scores close, using position-based gender for {member_name}: {'male' if final_is_male else 'female'}")
                elif men_match_score > women_match_score:
                    final_is_male = True
                    elo_match = men_elo_match
                    print(f"Using men's match for {member_name} based on higher score")
                else:
                    final_is_male = False
                    elo_match = women_elo_match
                    print(f"Using women's match for {member_name} based on higher score")
            elif men_elo_match:
                final_is_male = True
                elo_match = men_elo_match
                print(f"Using men's match for {member_name} (only match found)")
            elif women_elo_match:
                final_is_male = False
                elo_match = women_elo_match
                print(f"Using women's match for {member_name} (only match found)")
            else:
                # No database matches, rely on position-based assumption
                final_is_male = expected_is_male
                elo_match = None
                print(f"No ELO match found, using position-based gender for {member_name}: {'male' if final_is_male else 'female'}")
            
            # Select appropriate ELO scores and quartiles based on final gender determination
            elo_scores_to_use = men_elo_scores if final_is_male else women_elo_scores
            quartiles_to_use = men_quartiles if final_is_male else women_quartiles
            
            # Process the athlete now that gender is determined
            if elo_match:
                elo_data = elo_scores_to_use[elo_scores_to_use['Skier'] == elo_match].iloc[0].to_dict()
                
                # Add member info to team record
                team_info[f'Member_{position}'] = elo_match
                team_info[f'Member_{position}_ID'] = elo_data.get('ID', member_id)
                team_info[f'Member_{position}_FisCode'] = fis_code
                team_info[f'Member_{position}_Sex'] = 'M' if final_is_male else 'F'
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
                            team_info[f'Member_{position}_{col}'] = quartiles_to_use[col]
                            team_info[f'Total_{col}'] += quartiles_to_use[col]
                    else:
                        # Use quartile value if Elo is missing
                        team_info[f'Member_{position}_{col}'] = quartiles_to_use[col]
                        team_info[f'Total_{col}'] += quartiles_to_use[col]
                
                valid_member_count += 1
            else:
                print(f"No ELO match found for: {member_name}")
                
                # Add placeholder details
                team_info[f'Member_{position}'] = member_name
                team_info[f'Member_{position}_ID'] = member_id
                team_info[f'Member_{position}_FisCode'] = fis_code
                team_info[f'Member_{position}_Sex'] = 'M' if final_is_male else 'F'
                team_info[f'Member_{position}_Year'] = year
                team_info[f'Member_{position}_Length1'] = length1
                team_info[f'Member_{position}_Length2'] = length2
                team_info[f'Member_{position}_Points'] = member_points
                
                # Use quartile values for all ELO columns
                for col in elo_columns:
                    team_info[f'Member_{position}_{col}'] = quartiles_to_use[col]
                    team_info[f'Total_{col}'] += quartiles_to_use[col]
                
                valid_member_count += 1
        
        # Ensure all positions are filled (typically 4 members per mixed team in ski jumping)
        max_members = 4  # Standard team size for ski jumping mixed team
        for i in range(valid_member_count + 1, max_members + 1):
            # For mixed team, positions 1 and 3 are typically male, 2 and 4 are female
            is_male = i in [1, 3]
            quartiles_to_use = men_quartiles if is_male else women_quartiles
            
            team_info[f'Member_{i}'] = f"Unknown {nation} {'Male' if is_male else 'Female'} {i}"
            team_info[f'Member_{i}_ID'] = ''
            team_info[f'Member_{i}_FisCode'] = ''
            team_info[f'Member_{i}_Sex'] = 'M' if is_male else 'F'
            team_info[f'Member_{i}_Year'] = ''
            team_info[f'Member_{i}_Length1'] = ''
            team_info[f'Member_{i}_Length2'] = ''
            team_info[f'Member_{i}_Points'] = ''
            
            # Use quartile values for all ELO columns
            for col in elo_columns:
                team_info[f'Member_{i}_{col}'] = quartiles_to_use[col]
                team_info[f'Total_{col}'] += quartiles_to_use[col]
            
            valid_member_count += 1
        
        # Calculate average ELO values
        if valid_member_count > 0:
            for col in elo_columns:
                team_info[f'Avg_{col}'] = team_info[f'Total_{col}'] / valid_member_count
        
        # Add team record
        team_data.append(team_info)
    
    return team_data

def generate_fallback_data_mixed_team(men_elo_scores: pd.DataFrame, women_elo_scores: pd.DataFrame, race: pd.Series) -> List[Dict]:
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
    
    # Define possible nations based on common participating countries in ski jumping
    common_nations = [
        "NORWAY", "GERMANY", "AUSTRIA", "POLAND", "SLOVENIA",
        "JAPAN", "SWITZERLAND", "FINLAND", "CZECH REPUBLIC", "ITALY",
        "FRANCE", "USA", "CANADA", "RUSSIA", "UKRAINE"
    ]
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Small_Elo', 'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'
    ]
    
    # Determine hill size
    hill_size = race.get('HillSize', determine_hill_size_from_race_type(race.get('RaceType', '')))
    
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
            'Hill_Size': hill_size,
            'Is_Present': False,  # This team is not in the actual startlist
            'Race1_Prob': 0.0     # 0% probability since not in startlist
        }
        
        # Add team-level fields that may be empty
        team_record['Team_Points'] = ''
        
        # Initialize all Elo sums and averages as empty strings
        for col in elo_columns:
            team_record[f'Total_{col}'] = ''
            team_record[f'Avg_{col}'] = ''
        
        # Initialize empty member fields for mixed team (4 positions)
        for i in range(1, 5):
            # In mixed team, positions 1 and 3 are male, 2 and 4 are female
            is_male = i in [1, 3]
            
            team_record[f'Member_{i}'] = ''
            team_record[f'Member_{i}_ID'] = ''
            team_record[f'Member_{i}_FisCode'] = ''
            team_record[f'Member_{i}_Sex'] = 'M' if is_male else 'F'
            team_record[f'Member_{i}_Year'] = ''
            team_record[f'Member_{i}_Length1'] = ''
            team_record[f'Member_{i}_Length2'] = ''
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
    
    # Process mixed team races
    process_races_mixed_team(races_file)
    
    # Only run race-picks.R if environment variable is not set
    if not os.environ.get('SKIP_RACE_PICKS'):
        print("\nChecking if races are today and need to run race-picks.R...")
        check_and_run_weekly_picks()