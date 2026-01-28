#!/usr/bin/env python3
"""
Championships Prediction Startlist Generator for Ski Jumping

Creates startlists for Championships competitions including:
- Individual competitions (men/ladies)
- Team competitions (men/ladies/mixed)

Race probabilities will be calculated in R like weekly-picks2.R does.
"""

import pandas as pd
import sys
import os
from typing import List, Dict, Tuple, Optional
import warnings
from datetime import datetime, timezone
import traceback
import numpy as np
warnings.filterwarnings('ignore')

# Import common utility functions
from startlist_common import *

# Import Championships config functions
from config import get_champs_athletes

def process_championships() -> None:
    """Main function to process Championships races"""
    print("=== Ski Jumping Championships Startlist Generator ===")
    
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/skijump/polars/excel365/weekends.csv')
        print(f"Successfully read weekends.csv with {len(weekends_df)} rows")
    except Exception as e:
        print(f"Error reading weekends.csv: {e}")
        traceback.print_exc()
        return
    
    # Filter for Championships races only
    champs_races = weekends_df[weekends_df['Championship'] == 1]
    print(f"Found {len(champs_races)} Championships races")
    
    if champs_races.empty:
        print("No Championships races found in weekends.csv (Championship == 1)")
        return
    
    print("Championships races found:")
    for _, race in champs_races.iterrows():
        print(f"  - {race['Sex']} {race['RaceType']} in {race['City']}, {race['Country']}")
    
    # Separate race types (following weekend pattern)
    individual_races = champs_races[~champs_races['RaceType'].str.contains("Team", na=False)]
    
    regular_team_races = champs_races[
        (champs_races['RaceType'].str.contains("Team", na=False)) & 
        (champs_races['Sex'] != 'Mixed')
    ]
    
    mixed_team_races = champs_races[
        (champs_races['RaceType'].str.contains("Team", na=False)) &
        (champs_races['Sex'] == 'Mixed')
    ]
    
    print(f"Found {len(individual_races)} individual races")
    print(f"Found {len(regular_team_races)} regular team races") 
    print(f"Found {len(mixed_team_races)} mixed team races")
    
    # Process each type separately
    if not individual_races.empty:
        print("\n=== Processing Individual Championships ===")
        process_individual_championships(individual_races)
    
    if not regular_team_races.empty:
        print("\n=== Processing Team Championships ===")
        process_team_championships(regular_team_races)
        
    if not mixed_team_races.empty:
        print("\n=== Processing Mixed Team Championships ===")
        process_mixed_team_championships(mixed_team_races)
    
    print("\n=== Championships startlist generation complete ===")

def process_individual_championships(races_df: pd.DataFrame) -> None:
    """Process individual Championships races"""
    
    # Process men's individual races
    men_races = races_df[races_df['Sex'] == 'M']
    if not men_races.empty:
        try:
            print("Creating men's individual Championships startlist")
            create_individual_championships_startlist('men', men_races)
        except Exception as e:
            print(f"Error processing men's individual championships: {e}")
            traceback.print_exc()

    # Process ladies' individual races  
    ladies_races = races_df[races_df['Sex'] == 'L']
    if not ladies_races.empty:
        try:
            print("Creating ladies' individual Championships startlist")
            create_individual_championships_startlist('ladies', ladies_races)
        except Exception as e:
            print(f"Error processing ladies' individual championships: {e}")
            traceback.print_exc()

def create_individual_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create individual Championships startlist without race probabilities (like weekend scraper)"""
    
    print(f"Creating individual startlist for {gender}")
    
    # Get ELO data path (using individual elevation data)
    elo_path = f"~/ski/elo/python/skijump/polars/excel365/{gender}_chrono_pred.csv"
    
    try:
        print("Loading ELO scores...")
        elo_scores = get_latest_elo_scores(elo_path)
        if elo_scores is None or elo_scores.empty:
            print(f"No ELO scores found for {gender}")
            return
        
        # Build startlist from configured athletes
        data = []
        processed_athletes = set()
        
        # Get all nations with Championships athletes
        from config import CHAMPS_ATHLETES_MEN, CHAMPS_ATHLETES_LADIES
        
        if gender == 'men':
            all_nations = set(CHAMPS_ATHLETES_MEN.keys())
        else:
            all_nations = set(CHAMPS_ATHLETES_LADIES.keys())
        
        print(f"Found {len(all_nations)} nations with {gender} athletes configured")
        
        # Process each nation's athletes
        for nation in sorted(all_nations):
            athletes = get_champs_athletes(nation, gender)
            
            if not athletes:  # Skip nations with no athletes
                continue
                
            print(f"Processing {nation}: {len(athletes)} athletes")
            
            for athlete_name in athletes:
                if athlete_name in processed_athletes:
                    print(f"Skipping {athlete_name} - already processed")
                    continue
                
                print(f"Processing athlete: {athlete_name}")
                
                # Match with ELO scores
                elo_match = None
                if athlete_name in elo_scores['Skier'].values:
                    elo_match = athlete_name
                else:
                    # Try fuzzy matching
                    elo_match = fuzzy_match_name(athlete_name, elo_scores['Skier'].tolist())
                
                # Get ELO data
                if elo_match:
                    elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                    print(f"Found ELO match: {athlete_name} -> {elo_match}")
                else:
                    # Use quartile imputation for missing athletes
                    print(f"No ELO match found for {athlete_name}, using quartile imputation")
                    elo_columns = [col for col in elo_scores.columns if 'Elo' in col]
                    elo_data = {}
                    for col in elo_columns:
                        q1_value = elo_scores[col].quantile(0.25)
                        elo_data[col] = q1_value
                    elo_data['Skier'] = athlete_name
                
                # Build row data (simple version - no race probabilities)
                row_data = {
                    'Skier': athlete_name,
                    'Nation': nation,
                    'Price': 0.0,  # Ski jumping doesn't use fantasy prices
                    **{k: v for k, v in elo_data.items() if k != 'Skier'}
                }
                
                data.append(row_data)
                processed_athletes.add(athlete_name)
        
        if not data:
            print("No athlete data generated")
            return
        
        result_df = pd.DataFrame(data)
        print(f"Created startlist with {len(result_df)} athletes from {len([n for n in all_nations if get_champs_athletes(n, gender)])} nations")
        
        # Save to CSV
        output_file = f"excel365/startlist_champs_{gender}.csv"
        result_df.to_csv(output_file, index=False)
        print(f"✓ Saved {gender} Championships startlist: {output_file} ({len(result_df)} athletes)")
        
    except Exception as e:
        print(f"Error creating individual Championships startlist: {e}")
        traceback.print_exc()

def process_team_championships(races_df: pd.DataFrame) -> None:
    """Process regular team Championships races"""
    
    # Process men's team races
    men_teams = races_df[races_df['Sex'] == 'M']
    if not men_teams.empty:
        try:
            print("Creating men's team Championships startlist")
            create_team_championships_startlist('men', men_teams)
        except Exception as e:
            print(f"Error processing men's team championships: {e}")
            traceback.print_exc()

    # Process ladies' team races  
    ladies_teams = races_df[races_df['Sex'] == 'L']
    if not ladies_teams.empty:
        try:
            print("Creating ladies' team Championships startlist")
            create_team_championships_startlist('ladies', ladies_teams)
        except Exception as e:
            print(f"Error processing ladies' team championships: {e}")
            traceback.print_exc()

def create_team_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create team Championships startlist using config athletes"""
    
    print(f"Creating team startlist for {gender}")
    
    # Use individual ELO data (like weekend scraper does)
    elo_path = f"~/ski/elo/python/skijump/polars/excel365/{gender}_chrono_pred.csv"
    
    try:
        print("Loading ELO scores...")
        elo_scores = get_latest_elo_scores(elo_path)
        if elo_scores is None or elo_scores.empty:
            print(f"No ELO scores found for {gender}")
            return
        
        team_data = []
        
        # Get qualifying nations from config (2+ athletes for team competitions - Olympics uses 2-person teams)
        from config import CHAMPS_ATHLETES_MEN, CHAMPS_ATHLETES_LADIES

        if gender == 'men':
            all_nations = set(CHAMPS_ATHLETES_MEN.keys())
        else:
            all_nations = set(CHAMPS_ATHLETES_LADIES.keys())

        qualifying_nations = []
        for nation in all_nations:
            athletes = get_champs_athletes(nation, gender)
            if len(athletes) >= 2:  # Minimum team size requirement (Olympics uses 2-person teams)
                qualifying_nations.append(nation)

        print(f"Found {len(qualifying_nations)} qualifying nations with 2+ {gender} athletes")
        
        for nation in sorted(qualifying_nations):
            athletes = get_champs_athletes(nation, gender)
            
            # Sort athletes by highest ELO to select the best performers
            athlete_elos = []
            for athlete_name in athletes:
                # Match with ELO scores
                elo_match = None
                if athlete_name in elo_scores['Skier'].values:
                    elo_match = athlete_name
                else:
                    elo_match = fuzzy_match_name(athlete_name, elo_scores['Skier'].tolist())
                
                if elo_match:
                    elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                    # Use overall Elo as primary ranking, fallback to specific hill types
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Normal_Elo', 0) or elo_data.get('Large_Elo', 0) or 0
                    athlete_elos.append((athlete_name, main_elo))
                else:
                    # No ELO match - assign low value
                    athlete_elos.append((athlete_name, 0))
            
            # Sort by ELO (highest first) and select top 2 athletes (Olympics uses 2-person teams)
            athlete_elos.sort(key=lambda x: x[1], reverse=True)
            team_athletes = [athlete for athlete, elo in athlete_elos[:2]]

            print(f"Creating team for {nation} with top 2 athletes by ELO: {team_athletes}")
            
            # Create team record using highest ELO athletes
            team_record = create_team_record_from_config(
                nation, team_athletes, elo_scores, races_df.iloc[0]
            )
            team_data.append(team_record)
        
        if not team_data:
            print("No team data generated")
            return
        
        # Save to CSV in relay directory
        os.makedirs("relay/excel365", exist_ok=True)
        output_file = f"relay/excel365/startlist_champs_{gender}_team.csv"
        
        team_df = pd.DataFrame(team_data)
        team_df.to_csv(output_file, index=False)
        print(f"✓ Saved {gender} team Championships startlist: {output_file} ({len(team_df)} teams)")
        
    except Exception as e:
        print(f"Error creating team Championships startlist: {e}")
        traceback.print_exc()

def process_mixed_team_championships(races_df: pd.DataFrame) -> None:
    """Process mixed team Championships races"""
    
    try:
        print("Creating mixed team Championships startlist")
        create_mixed_team_championships_startlist(races_df)
    except Exception as e:
        print(f"Error processing mixed team championships: {e}")
        traceback.print_exc()

def create_mixed_team_championships_startlist(races_df: pd.DataFrame) -> None:
    """Create mixed team Championships startlist (2 men + 2 ladies)"""
    
    print("Creating mixed team startlist")
    
    # Load both gender ELO data
    try:
        print("Loading men's ELO scores...")
        men_elo = get_latest_elo_scores("~/ski/elo/python/skijump/polars/excel365/men_chrono_pred.csv")
        
        print("Loading ladies' ELO scores...")
        ladies_elo = get_latest_elo_scores("~/ski/elo/python/skijump/polars/excel365/ladies_chrono_pred.csv")
        
        if men_elo is None or men_elo.empty or ladies_elo is None or ladies_elo.empty:
            print("Missing ELO data for mixed teams")
            return
        
        mixed_data = []
        
        # Get all nations
        from config import CHAMPS_ATHLETES_MEN, CHAMPS_ATHLETES_LADIES
        all_nations = set(CHAMPS_ATHLETES_MEN.keys()) | set(CHAMPS_ATHLETES_LADIES.keys())
        
        qualifying_nations = []
        for nation in all_nations:
            men_athletes = get_champs_athletes(nation, 'men')
            ladies_athletes = get_champs_athletes(nation, 'ladies')
            
            if len(men_athletes) >= 2 and len(ladies_athletes) >= 2:
                qualifying_nations.append(nation)
        
        print(f"Found {len(qualifying_nations)} qualifying nations with 2+ men and 2+ ladies")
        
        for nation in sorted(qualifying_nations):
            # Get top 2 men by ELO
            men_athletes_all = get_champs_athletes(nation, 'men')
            men_elos = []
            for athlete_name in men_athletes_all:
                if athlete_name in men_elo['Skier'].values:
                    elo_data = men_elo[men_elo['Skier'] == athlete_name].iloc[0].to_dict()
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Normal_Elo', 0) or elo_data.get('Large_Elo', 0) or 0
                    men_elos.append((athlete_name, main_elo))
                else:
                    elo_match = fuzzy_match_name(athlete_name, men_elo['Skier'].tolist())
                    if elo_match:
                        elo_data = men_elo[men_elo['Skier'] == elo_match].iloc[0].to_dict()
                        main_elo = elo_data.get('Elo', 0) or elo_data.get('Normal_Elo', 0) or elo_data.get('Large_Elo', 0) or 0
                        men_elos.append((athlete_name, main_elo))
                    else:
                        men_elos.append((athlete_name, 0))
            
            men_elos.sort(key=lambda x: x[1], reverse=True)
            men_athletes = [athlete for athlete, elo in men_elos[:2]]  # Top 2 men by ELO
            
            # Get top 2 ladies by ELO
            ladies_athletes_all = get_champs_athletes(nation, 'ladies')
            ladies_elos = []
            for athlete_name in ladies_athletes_all:
                if athlete_name in ladies_elo['Skier'].values:
                    elo_data = ladies_elo[ladies_elo['Skier'] == athlete_name].iloc[0].to_dict()
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Normal_Elo', 0) or elo_data.get('Large_Elo', 0) or 0
                    ladies_elos.append((athlete_name, main_elo))
                else:
                    elo_match = fuzzy_match_name(athlete_name, ladies_elo['Skier'].tolist())
                    if elo_match:
                        elo_data = ladies_elo[ladies_elo['Skier'] == elo_match].iloc[0].to_dict()
                        main_elo = elo_data.get('Elo', 0) or elo_data.get('Normal_Elo', 0) or elo_data.get('Large_Elo', 0) or 0
                        ladies_elos.append((athlete_name, main_elo))
                    else:
                        ladies_elos.append((athlete_name, 0))
            
            ladies_elos.sort(key=lambda x: x[1], reverse=True)
            ladies_athletes = [athlete for athlete, elo in ladies_elos[:2]]  # Top 2 ladies by ELO
            
            print(f"Creating mixed team for {nation}: Men (top 2 by ELO): {men_athletes}, Ladies (top 2 by ELO): {ladies_athletes}")
            
            # Create mixed team record
            mixed_record = create_mixed_team_record(
                nation, 
                men_athletes,      # Top 2 men by ELO
                ladies_athletes,   # Top 2 ladies by ELO
                men_elo, 
                ladies_elo, 
                races_df.iloc[0]
            )
            mixed_data.append(mixed_record)
        
        if not mixed_data:
            print("No mixed team data generated")
            return
        
        # Save to CSV in relay directory
        os.makedirs("relay/excel365", exist_ok=True)
        output_file = "relay/excel365/startlist_champs_mixed_team.csv"
        
        mixed_df = pd.DataFrame(mixed_data)
        mixed_df.to_csv(output_file, index=False)
        print(f"✓ Saved mixed team Championships startlist: {output_file} ({len(mixed_df)} teams)")
        
    except Exception as e:
        print(f"Error creating mixed team Championships startlist: {e}")
        traceback.print_exc()

def create_team_record_from_config(nation: str, athletes: List[str], elo_scores: pd.DataFrame, race: pd.Series) -> Dict:
    """Create team record using config athletes (based on fallback method structure)"""
    
    # Define Elo columns to work with for ski jumping
    elo_columns = [
        'Elo', 'Small_Elo', 'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'
    ]
    
    # Create team record with basic info
    team_record = {
        'Team_Name': nation,
        'Nation': nation,
        'Team_Rank': 0,  # No rank for Championships
        'Race_Date': race['Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Race_Type': race['RaceType'],
        'Hill_Size': race.get('HillSize', 'Normal'),
        'Is_Present': True,  # This team will participate
        'Race1_Prob': 1.0    # 100% probability for Championships
    }
    
    # Add team-level fields
    team_record['Team_Points'] = ''
    team_record['TeamMembers'] = ','.join(athletes)  # Comma-separated athlete names

    # Initialize all Elo sums and averages
    for col in elo_columns:
        team_record[f'Total_{col}'] = 0
        team_record[f'Avg_{col}'] = 0
    
    valid_member_count = 0

    # Process team members (up to 2 for Olympics)
    for i in range(2):
        member_index = i + 1
        
        if i < len(athletes):
            athlete_name = athletes[i]
            
            # Match with ELO scores
            elo_match = None
            if athlete_name in elo_scores['Skier'].values:
                elo_match = athlete_name
            else:
                elo_match = fuzzy_match_name(athlete_name, elo_scores['Skier'].tolist())
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                valid_member_count += 1
                
                # Add to team totals
                for col in elo_columns:
                    if col in elo_data and not pd.isna(elo_data[col]):
                        team_record[f'Total_{col}'] += elo_data[col]
                        team_record[f'Member_{member_index}_{col}'] = elo_data[col]
                    else:
                        team_record[f'Member_{member_index}_{col}'] = ''
                
                # Set member info
                team_record[f'Member_{member_index}'] = athlete_name
                team_record[f'Member_{member_index}_ID'] = elo_data.get('ID', '')
            else:
                # No ELO match found
                team_record[f'Member_{member_index}'] = athlete_name
                team_record[f'Member_{member_index}_ID'] = ''
                for col in elo_columns:
                    team_record[f'Member_{member_index}_{col}'] = ''
        else:
            # Empty member slot
            team_record[f'Member_{member_index}'] = ''
            team_record[f'Member_{member_index}_ID'] = ''
            for col in elo_columns:
                team_record[f'Member_{member_index}_{col}'] = ''
        
        # Skip member detail fields (FisCode, Year, Length1, Length2, Points)
        # These are not needed for Championships predictions and cause issues
    
    # Calculate team averages
    if valid_member_count > 0:
        for col in elo_columns:
            if team_record[f'Total_{col}'] > 0:
                team_record[f'Avg_{col}'] = team_record[f'Total_{col}'] / valid_member_count
    
    return team_record

def create_mixed_team_record(nation: str, men_athletes: List[str], ladies_athletes: List[str], 
                           men_elo: pd.DataFrame, ladies_elo: pd.DataFrame, race: pd.Series) -> Dict:
    """Create mixed team record (2 men + 2 ladies)"""
    
    elo_columns = [
        'Elo', 'Small_Elo', 'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'
    ]
    
    # Create team record
    team_record = {
        'Team_Name': nation,
        'Nation': nation,
        'Team_Rank': 0,
        'Race_Date': race['Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Race_Type': race['RaceType'],
        'Hill_Size': race.get('HillSize', 'Normal'),
        'Is_Present': True,
        'Race1_Prob': 1.0
    }
    
    team_record['Team_Points'] = ''

    # Process 2 men + 2 ladies (4 total members)
    all_athletes = men_athletes + ladies_athletes
    team_record['TeamMembers'] = ','.join(all_athletes)  # Comma-separated athlete names

    # Initialize totals
    for col in elo_columns:
        team_record[f'Total_{col}'] = 0
        team_record[f'Avg_{col}'] = 0

    valid_member_count = 0
    all_elos = [men_elo, men_elo, ladies_elo, ladies_elo]  # Match genders
    
    for i in range(4):
        member_index = i + 1
        
        if i < len(all_athletes):
            athlete_name = all_athletes[i]
            elo_data_source = all_elos[i]
            
            # Match with appropriate ELO scores
            elo_match = None
            if athlete_name in elo_data_source['Skier'].values:
                elo_match = athlete_name
            else:
                elo_match = fuzzy_match_name(athlete_name, elo_data_source['Skier'].tolist())
            
            if elo_match:
                elo_data = elo_data_source[elo_data_source['Skier'] == elo_match].iloc[0].to_dict()
                valid_member_count += 1
                
                # Add to team totals
                for col in elo_columns:
                    if col in elo_data and not pd.isna(elo_data[col]):
                        team_record[f'Total_{col}'] += elo_data[col]
                        team_record[f'Member_{member_index}_{col}'] = elo_data[col]
                    else:
                        team_record[f'Member_{member_index}_{col}'] = ''
                
                team_record[f'Member_{member_index}'] = athlete_name
                team_record[f'Member_{member_index}_ID'] = elo_data.get('ID', '')
            else:
                team_record[f'Member_{member_index}'] = athlete_name
                team_record[f'Member_{member_index}_ID'] = ''
                for col in elo_columns:
                    team_record[f'Member_{member_index}_{col}'] = ''
        else:
            team_record[f'Member_{member_index}'] = ''
            team_record[f'Member_{member_index}_ID'] = ''
            for col in elo_columns:
                team_record[f'Member_{member_index}_{col}'] = ''
        
        # Skip member detail fields (FisCode, Year, Length1, Length2, Points)
        # These are not needed for Championships predictions and cause issues
    
    # Calculate averages
    if valid_member_count > 0:
        for col in elo_columns:
            if team_record[f'Total_{col}'] > 0:
                team_record[f'Avg_{col}'] = team_record[f'Total_{col}'] / valid_member_count
    
    return team_record

if __name__ == "__main__":
    process_championships()