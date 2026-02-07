#!/usr/bin/env python3
"""
Championships Prediction Startlist Generator for Cross-Country

Creates startlists for Championships competitions including:
- Individual distance races (men/ladies)
- Sprint races (men/ladies) 
- Team Sprint events (men/ladies)
- Relay events (men/ladies)
- Mixed Relay events

Race probabilities will be calculated in R like race-picks.R does.
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
from config import get_champs_athletes_xc

def process_championships() -> None:
    """Main entry point - reads weekends.csv, filters Championship==1, routes by race type"""
    print("=== Cross-Country Championships Startlist Generator ===")
    
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/ski/polars/excel365/weekends.csv')
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
        print(f"  - {race['Sex']} {race['Distance']} {race['Technique']} in {race['City']}, {race['Country']}")
    
    # Classify races by type
    individual_distance_races = champs_races[
        (champs_races['Distance'].isin(['10', '15', '20', '30', '50'])) &
        (champs_races['Sex'].isin(['M', 'L']))
    ]
    
    sprint_races = champs_races[
        (champs_races['Distance'] == 'Sprint') &
        (champs_races['Sex'].isin(['M', 'L']))
    ]
    
    team_sprint_races = champs_races[
        (champs_races['Distance'] == 'Ts') &
        (champs_races['Sex'].isin(['M', 'L']))
    ]
    
    relay_races = champs_races[
        (champs_races['Distance'] == 'Rel') &
        (champs_races['Sex'].isin(['M', 'L']))
    ]
    
    mixed_relay_races = champs_races[
        (champs_races['Distance'] == 'Mix') &
        (champs_races['Sex'] == 'Mixed')
    ]
    
    print(f"Race classification:")
    print(f"  - Individual Distance: {len(individual_distance_races)} races")
    print(f"  - Sprint: {len(sprint_races)} races") 
    print(f"  - Team Sprint: {len(team_sprint_races)} races")
    print(f"  - Relay: {len(relay_races)} races")
    print(f"  - Mixed Relay: {len(mixed_relay_races)} races")
    
    # Process individual races (both distance and sprint) consolidated by gender
    for gender in ['M', 'L']:
        # Combine distance and sprint races for this gender
        gender_individual_races = pd.concat([
            individual_distance_races[individual_distance_races['Sex'] == gender],
            sprint_races[sprint_races['Sex'] == gender]
        ], ignore_index=True)
        
        if not gender_individual_races.empty:
            gender_name = 'men' if gender == 'M' else 'ladies'
            try:
                print(f"\\n=== Processing {gender_name.title()} Individual Championships (All Races) ===")
                print(f"Found {len(gender_individual_races)} individual races for {gender_name}")
                create_consolidated_individual_championships_startlist(gender_name, gender_individual_races)
            except Exception as e:
                print(f"Error processing {gender_name} individual championships: {e}")
                traceback.print_exc()
    
    # Process team sprint races
    for gender in ['M', 'L']:
        gender_races = team_sprint_races[team_sprint_races['Sex'] == gender]
        if not gender_races.empty:
            gender_name = 'men' if gender == 'M' else 'ladies'
            try:
                print(f"\\n=== Processing {gender_name.title()} Team Sprint Championships ===")
                create_team_sprint_championships_startlist(gender_name, gender_races)
            except Exception as e:
                print(f"Error processing {gender_name} team sprint championships: {e}")
                traceback.print_exc()
    
    # Process relay races
    for gender in ['M', 'L']:
        gender_races = relay_races[relay_races['Sex'] == gender]
        if not gender_races.empty:
            gender_name = 'men' if gender == 'M' else 'ladies'
            try:
                print(f"\\n=== Processing {gender_name.title()} Relay Championships ===")
                create_relay_championships_startlist(gender_name, gender_races)
            except Exception as e:
                print(f"Error processing {gender_name} relay championships: {e}")
                traceback.print_exc()
    
    # Process mixed relay races
    if not mixed_relay_races.empty:
        try:
            print("\\n=== Processing Mixed Relay Championships ===")
            create_mixed_relay_championships_startlist(mixed_relay_races)
        except Exception as e:
            print(f"Error processing mixed relay championships: {e}")
            traceback.print_exc()
    
    print("\\n=== Cross-Country Championships startlist generation complete ===")

def get_race_specific_elo_priority(distance: str, technique: str) -> List[str]:
    """Return prioritized ELO column list based on race characteristics"""
    
    if distance == 'Sprint':
        if technique == 'C':
            return ['Sprint_C_Elo', 'Sprint_Elo', 'Classic_Elo', 'Elo']
        elif technique == 'F':
            return ['Sprint_F_Elo', 'Sprint_Elo', 'Freestyle_Elo', 'Elo']
        else:
            return ['Sprint_Elo', 'Elo']
    
    elif distance in ['10', '15', '20', '30', '50']:
        if technique == 'C':
            return ['Distance_C_Elo', 'Distance_Elo', 'Classic_Elo', 'Elo']
        elif technique == 'F':
            return ['Distance_F_Elo', 'Distance_Elo', 'Freestyle_Elo', 'Elo']
        elif technique == 'P':  # Pursuit
            return ['Distance_Elo', 'Elo']  # technique-neutral
        else:
            return ['Distance_Elo', 'Elo']
    
    elif distance in ['Ts', 'Rel', 'Mix']:  # Team events
        if technique == 'C':
            return ['Classic_Elo', 'Distance_C_Elo', 'Sprint_C_Elo', 'Elo']
        elif technique == 'F':
            return ['Freestyle_Elo', 'Distance_F_Elo', 'Sprint_F_Elo', 'Elo']
        else:
            return ['Elo', 'Distance_Elo', 'Sprint_Elo']
    
    # Default fallback
    return ['Elo']

# NOTE: 4-person quota selection moved to R script for sophisticated probability handling
# Following biathlon pattern: Python creates full startlist, R applies quota constraints
# 
# def apply_4_person_quota_selection(...) - REMOVED
# This function has been removed because quota constraints are now handled in the R script
# using historical participation data and fractional probabilities that sum to 4 per nation

def create_consolidated_individual_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create consolidated individual championships startlist (mimics weekend scraper pattern)"""
    
    print(f"Creating consolidated individual startlist for {gender}")
    
    # Get ELO data path (use elevation file like weekend scraper)
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    
    try:
        print("Loading ELO scores...")
        elo_scores = get_latest_elo_scores(elo_path)
        if elo_scores is None or elo_scores.empty:
            print(f"No ELO scores found for {gender}")
            return
        
        # Sort races by date to ensure consistent Race1, Race2, etc. ordering
        races_df = races_df.sort_values(['Race_Date', 'Distance', 'Technique'])
        num_races = len(races_df)
        
        # Create probability column names like weekend scraper
        if num_races == 1:
            prob_columns = ['Race1_Prob']
        else:
            prob_columns = [f'Race{i+1}_Prob' for i in range(num_races)]
        
        print(f"Processing {num_races} races with probability columns: {prob_columns}")
        
        # Get all nations with Championships athletes
        from config import CHAMPS_ATHLETES_MEN_XC, CHAMPS_ATHLETES_LADIES_XC
        
        if gender == 'men':
            all_nations = set(CHAMPS_ATHLETES_MEN_XC.keys())
        else:
            all_nations = set(CHAMPS_ATHLETES_LADIES_XC.keys())
        
        print(f"Found {len(all_nations)} nations with {gender} athletes configured")
        
        # Build consolidated athlete list
        all_athletes_data = []
        processed_athletes = set()
        
        # Get all unique athletes across all races (championship pool)
        for nation in sorted(all_nations):
            nation_athletes = get_champs_athletes_xc(nation, gender)
            
            for athlete_name in nation_athletes:
                if athlete_name in processed_athletes:
                    continue
                
                # Match with ELO scores
                elo_match = None
                if athlete_name in elo_scores['Skier'].values:
                    elo_match = athlete_name
                else:
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
                    elo_data['Nation'] = nation
                
                # Build base athlete record
                athlete_data = {
                    'Skier': athlete_name,
                    'Nation': nation,
                    'Price': 0.0,  # Championships don't use fantasy prices
                    **{k: v for k, v in elo_data.items() if k not in ['Skier', 'Nation']}
                }
                
                # Initialize all race probability columns to 0
                for prob_col in prob_columns:
                    athlete_data[prob_col] = 0.0
                
                all_athletes_data.append(athlete_data)
                processed_athletes.add(athlete_name)
        
        if not all_athletes_data:
            print("No athlete data generated")
            return
        
        # Create consolidated dataframe with all athletes (no quota selection in Python)
        consolidated_df = pd.DataFrame(all_athletes_data)
        
        print(f"\\nAll championship athletes included with 0.0 probabilities - R script will calculate sophisticated probabilities and apply 4-person quota constraints")
        
        # Show race information for R script processing
        for race_idx, (_, race) in enumerate(races_df.iterrows()):
            race_distance = race['Distance']
            race_technique = race['Technique']
            prob_col = prob_columns[race_idx]
            
            print(f"Race {race_idx+1} ({prob_col}): {race_distance} {race_technique} - R script will calculate participation probabilities")
        
        print(f"\\nCreated consolidated startlist with {len(consolidated_df)} athletes")
        
        # Save consolidated CSV (matching weekend scraper pattern)
        os.makedirs("excel365", exist_ok=True)
        output_file = f"excel365/startlist_champs_{gender}.csv"
        consolidated_df.to_csv(output_file, index=False)
        print(f"✓ Saved {gender} Championships startlist: {output_file} ({len(consolidated_df)} athletes, {num_races} races)")
        
        # Show probability column summary
        for prob_col in prob_columns:
            num_athletes = len(consolidated_df)
            print(f"  - {prob_col}: All {num_athletes} athletes included with 0.0 probability (R script will calculate)")
        
    except Exception as e:
        print(f"Error creating consolidated Championships startlist: {e}")
        traceback.print_exc()

def create_team_sprint_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create 2-person team startlists for team sprint events"""
    
    print(f"Creating team sprint startlist for {gender}")
    
    # Use individual ELO data for team sprint selection (pred file for predictions)
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    
    try:
        print("Loading ELO scores...")
        elo_scores = get_latest_elo_scores(elo_path)
        if elo_scores is None or elo_scores.empty:
            print(f"No ELO scores found for {gender}")
            return
        
        team_data = []
        
        # Get qualifying nations from config (2+ athletes for team sprint)
        from config import CHAMPS_ATHLETES_MEN_XC, CHAMPS_ATHLETES_LADIES_XC
        
        if gender == 'men':
            all_nations = set(CHAMPS_ATHLETES_MEN_XC.keys())
        else:
            all_nations = set(CHAMPS_ATHLETES_LADIES_XC.keys())
        
        qualifying_nations = []
        for nation in all_nations:
            athletes = get_champs_athletes_xc(nation, gender)
            if len(athletes) >= 2:  # Team sprint requires 2 athletes minimum
                qualifying_nations.append(nation)
        
        print(f"Found {len(qualifying_nations)} qualifying nations with 2+ {gender} athletes")
        
        # Get race technique for ELO prioritization
        race_technique = races_df.iloc[0]['Technique'] if not races_df.empty else 'F'
        
        for nation in sorted(qualifying_nations):
            athletes = get_champs_athletes_xc(nation, gender)
            
            # Sort athletes by sprint-relevant ELO 
            athlete_elos = []
            elo_priority = get_race_specific_elo_priority('Ts', race_technique)
            
            for athlete_name in athletes:
                best_elo = 0
                
                # Match with ELO scores
                elo_match = None
                if athlete_name in elo_scores['Skier'].values:
                    elo_match = athlete_name
                else:
                    elo_match = fuzzy_match_name(athlete_name, elo_scores['Skier'].tolist())
                
                if elo_match:
                    athlete_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0]
                    # Use team sprint relevant ELO (prioritize sprint performance)
                    for elo_col in elo_priority:
                        if elo_col in athlete_data and not pd.isna(athlete_data[elo_col]):
                            best_elo = athlete_data[elo_col]
                            break
                
                athlete_elos.append((athlete_name, best_elo))
            
            # Sort by ELO (highest first) and select top 2 athletes
            athlete_elos.sort(key=lambda x: x[1], reverse=True)
            team_athletes = [athlete for athlete, elo in athlete_elos[:2]]
            
            print(f"Creating team sprint for {nation} with top 2 athletes by sprint ELO: {team_athletes}")
            
            # Create team record
            team_record = create_team_record_from_config(
                nation, team_athletes, elo_scores, races_df.iloc[0], 'team_sprint'
            )
            team_data.append(team_record)
        
        if not team_data:
            print("No team sprint data generated")
            return
        
        # Save team and individual data (matching weekend scraper pattern)
        os.makedirs("relay/excel365", exist_ok=True)
        
        # Create both team and individual outputs
        all_teams_data = team_data
        all_individuals_data = []
        
        # Extract individual data from team records (simplified for team sprint)
        for team in team_data:
            team_members = team.get('TeamMembers', '').split(',')
            for i, member_name in enumerate(team_members):
                if member_name.strip():
                    individual_record = {
                        'FIS_Name': member_name.strip(),
                        'Skier': member_name.strip(),
                        'Nation': team['Nation'],
                        'Team_Name': team['Team_Name'],
                        'Team_Position': i + 1,
                        'Race_Type': 'Team Sprint'
                    }
                    all_individuals_data.append(individual_record)
        
        # Save team data
        team_output_file = f"relay/excel365/startlist_champs_team_sprint_teams_{gender}.csv"
        team_df = pd.DataFrame(all_teams_data)
        team_df.to_csv(team_output_file, index=False)
        print(f"✓ Saved {gender} team sprint teams: {team_output_file} ({len(team_df)} teams)")
        
        # Save individual data
        if all_individuals_data:
            individual_output_file = f"relay/excel365/startlist_champs_team_sprint_individuals_{gender}.csv"
            individual_df = pd.DataFrame(all_individuals_data)
            individual_df.to_csv(individual_output_file, index=False)
            print(f"✓ Saved {gender} team sprint individuals: {individual_output_file} ({len(individual_df)} athletes)")
        
    except Exception as e:
        print(f"Error creating team sprint Championships startlist: {e}")
        traceback.print_exc()

def create_relay_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create relay team startlists using config athletes (mimics weekend scraper fallback pattern)"""
    
    print(f"Creating relay startlist for {gender}")
    
    # Get paths matching existing relay scraper structure
    team_output_path = f"relay/excel365/startlist_champs_relay_teams_{gender}.csv"
    individual_output_path = f"relay/excel365/startlist_champs_relay_individuals_{gender}.csv"
    
    # Create output directories
    os.makedirs("relay/excel365", exist_ok=True)
    
    # Get ELO scores (use elevation file like weekend scraper)
    elo_path = f"~/ski/elo/python/ski/polars/excel365/{gender}_chrono_pred.csv"
    
    try:
        print("Loading ELO scores...")
        elo_scores = get_latest_elo_scores(elo_path)
        if elo_scores is None or elo_scores.empty:
            print(f"No ELO scores found for {gender}")
            return
        
        # Get qualifying nations from config (4+ athletes for relay competitions)
        from config import CHAMPS_ATHLETES_MEN_XC, CHAMPS_ATHLETES_LADIES_XC
        
        if gender == 'men':
            all_nations = set(CHAMPS_ATHLETES_MEN_XC.keys())
        else:
            all_nations = set(CHAMPS_ATHLETES_LADIES_XC.keys())
        
        qualifying_nations = []
        for nation in all_nations:
            athletes = get_champs_athletes_xc(nation, gender)
            if len(athletes) >= 4:  # Relay requires 4 athletes
                qualifying_nations.append(nation)
        
        print(f"Found {len(qualifying_nations)} qualifying nations with 4+ {gender} athletes")
        
        # Get race info
        race = races_df.iloc[0] if not races_df.empty else pd.Series({
            'Date': '2026-02-01',
            'City': 'Championships',
            'Country': 'TBD',
            'Technique': 'F'
        })
        
        # Process each qualifying nation to create teams
        all_teams_data = []
        all_individuals_data = []
        
        for nation in sorted(qualifying_nations):
            athletes = get_champs_athletes_xc(nation, gender)
            
            # Sort athletes by relay-relevant ELO (similar pattern to existing scrapers)
            elo_priority = get_race_specific_elo_priority('Rel', race['Technique'])
            athlete_elos = []
            
            for athlete_name in athletes:
                best_elo = 0
                
                # Match with ELO scores
                elo_match = None
                if athlete_name in elo_scores['Skier'].values:
                    elo_match = athlete_name
                else:
                    elo_match = fuzzy_match_name(athlete_name, elo_scores['Skier'].tolist())
                
                if elo_match:
                    athlete_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0]
                    # Use technique-specific ELO prioritization
                    for elo_col in elo_priority:
                        if elo_col in athlete_data and not pd.isna(athlete_data[elo_col]):
                            best_elo = athlete_data[elo_col]
                            break
                
                athlete_elos.append((athlete_name, best_elo))
            
            # Sort by ELO and select top 4 athletes
            athlete_elos.sort(key=lambda x: x[1], reverse=True)
            relay_athletes = [athlete for athlete, elo in athlete_elos[:4]]
            
            print(f"Creating relay team for {nation}: {relay_athletes}")
            
            # Create team data (matching weekend scraper structure)
            team_data, individual_data = create_championship_relay_team(
                nation, relay_athletes, elo_scores, race, gender
            )
            
            all_teams_data.extend(team_data)
            all_individuals_data.extend(individual_data)
        
        # Save team data (matching weekend scraper pattern)
        if all_teams_data:
            team_df = pd.DataFrame(all_teams_data)
            team_df.to_csv(team_output_path, index=False)
            print(f"✓ Saved {len(team_df)} {gender} relay teams to {team_output_path}")
        else:
            print(f"No team data generated for {gender}")
        
        # Save individual data (matching weekend scraper pattern)
        if all_individuals_data:
            individual_df = pd.DataFrame(all_individuals_data)
            individual_df.to_csv(individual_output_path, index=False)
            print(f"✓ Saved {len(individual_df)} {gender} relay individuals to {individual_output_path}")
        else:
            print(f"No individual data generated for {gender}")
        
    except Exception as e:
        print(f"Error creating relay Championships startlist: {e}")
        traceback.print_exc()

def create_mixed_relay_championships_startlist(races_df: pd.DataFrame) -> None:
    """Create mixed gender relay teams (2M+2L) using combined ELO data"""
    
    print("Creating mixed relay startlist")
    
    try:
        # Load both men's and ladies' ELO data (pred file for predictions)
        men_elo = get_latest_elo_scores("~/ski/elo/python/ski/polars/excel365/men_chrono_pred.csv")
        ladies_elo = get_latest_elo_scores("~/ski/elo/python/ski/polars/excel365/ladies_chrono_pred.csv")
        
        if men_elo is None or ladies_elo is None or men_elo.empty or ladies_elo.empty:
            print("Could not load ELO data for mixed relays")
            return
        
        mixed_data = []
        
        # Get nations that have both men and ladies athletes
        from config import CHAMPS_ATHLETES_MEN_XC, CHAMPS_ATHLETES_LADIES_XC
        
        men_nations = set(CHAMPS_ATHLETES_MEN_XC.keys())
        ladies_nations = set(CHAMPS_ATHLETES_LADIES_XC.keys())
        qualifying_nations = men_nations.intersection(ladies_nations)
        
        # Filter for nations with sufficient athletes (at least 2 men and 2 ladies)
        final_nations = []
        for nation in qualifying_nations:
            men_athletes = get_champs_athletes_xc(nation, 'men')
            ladies_athletes = get_champs_athletes_xc(nation, 'ladies')
            if len(men_athletes) >= 2 and len(ladies_athletes) >= 2:
                final_nations.append(nation)
        
        print(f"Found {len(final_nations)} qualifying nations for mixed relays")
        
        # Get race technique for ELO prioritization
        race_technique = races_df.iloc[0]['Technique'] if not races_df.empty else 'F'
        elo_priority = get_race_specific_elo_priority('Mix', race_technique)
        
        for nation in sorted(final_nations):
            # Get top 2 men by ELO
            men_athletes_all = get_champs_athletes_xc(nation, 'men')
            men_elos = []
            for athlete_name in men_athletes_all:
                best_elo = 0
                elo_match = None
                if athlete_name in men_elo['Skier'].values:
                    elo_match = athlete_name
                else:
                    elo_match = fuzzy_match_name(athlete_name, men_elo['Skier'].tolist())
                
                if elo_match:
                    athlete_data = men_elo[men_elo['Skier'] == elo_match].iloc[0]
                    for elo_col in elo_priority:
                        if elo_col in athlete_data and not pd.isna(athlete_data[elo_col]):
                            best_elo = athlete_data[elo_col]
                            break
                
                men_elos.append((athlete_name, best_elo))
            
            men_elos.sort(key=lambda x: x[1], reverse=True)
            men_athletes = [athlete for athlete, elo in men_elos[:2]]  # Top 2 men by ELO
            
            # Get top 2 ladies by ELO
            ladies_athletes_all = get_champs_athletes_xc(nation, 'ladies')
            ladies_elos = []
            for athlete_name in ladies_athletes_all:
                best_elo = 0
                elo_match = None
                if athlete_name in ladies_elo['Skier'].values:
                    elo_match = athlete_name
                else:
                    elo_match = fuzzy_match_name(athlete_name, ladies_elo['Skier'].tolist())
                
                if elo_match:
                    athlete_data = ladies_elo[ladies_elo['Skier'] == elo_match].iloc[0]
                    for elo_col in elo_priority:
                        if elo_col in athlete_data and not pd.isna(athlete_data[elo_col]):
                            best_elo = athlete_data[elo_col]
                            break
                
                ladies_elos.append((athlete_name, best_elo))
            
            ladies_elos.sort(key=lambda x: x[1], reverse=True)
            ladies_athletes = [athlete for athlete, elo in ladies_elos[:2]]  # Top 2 ladies by ELO
            
            print(f"Creating mixed relay for {nation}: Men (top 2 by ELO): {men_athletes}, Ladies (top 2 by ELO): {ladies_athletes}")
            
            # Create mixed relay record
            relay_record = create_mixed_team_record(
                nation, men_athletes, ladies_athletes, men_elo, ladies_elo, races_df.iloc[0]
            )
            mixed_data.append(relay_record)
        
        if not mixed_data:
            print("No mixed relay data generated")
            return
        
        # Save team and individual data (matching weekend scraper pattern)
        os.makedirs("relay/excel365", exist_ok=True)
        
        # Create individual data from mixed relay teams
        all_individuals_data = []
        for team in mixed_data:
            team_members = team.get('TeamMembers', '').split(',')
            for i, member_name in enumerate(team_members):
                if member_name.strip():
                    individual_record = {
                        'FIS_Name': member_name.strip(),
                        'Skier': member_name.strip(),
                        'Nation': team['Nation'],
                        'Team_Name': team['Team_Name'],
                        'Team_Position': i + 1,
                        'Race_Type': 'Mixed Relay'
                    }
                    all_individuals_data.append(individual_record)
        
        # Save team data
        team_output_file = "relay/excel365/startlist_champs_mixed_relay_teams.csv"
        mixed_df = pd.DataFrame(mixed_data)
        mixed_df.to_csv(team_output_file, index=False)
        print(f"✓ Saved mixed relay teams: {team_output_file} ({len(mixed_df)} teams)")
        
        # Save individual data
        if all_individuals_data:
            individual_output_file = "relay/excel365/startlist_champs_mixed_relay_individuals.csv"
            individual_df = pd.DataFrame(all_individuals_data)
            individual_df.to_csv(individual_output_file, index=False)
            print(f"✓ Saved mixed relay individuals: {individual_output_file} ({len(individual_df)} athletes)")
        
    except Exception as e:
        print(f"Error creating mixed relay Championships startlist: {e}")
        traceback.print_exc()

def create_team_record_from_config(nation: str, athletes: List[str], elo_scores: pd.DataFrame, 
                                  race: pd.Series, team_type: str) -> Dict:
    """Create team record using config athletes (adapted for cross-country 9-column system)"""
    
    # Define Elo columns for cross-country (9 total)
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_F_Elo', 'Distance_C_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Create team record with basic info
    team_record = {
        'Team_Name': nation,
        'Nation': nation,
        'Team_Rank': 0,  # No rank for Championships
        'Race_Date': race['Race_Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Distance': race['Distance'],
        'Technique': race['Technique'],
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
    member_ids = []
    
    # Process team members
    for i in range(len(athletes)):
        member_index = i + 1
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
            member_ids.append(str(elo_data.get('ID', '')))
        else:
            # No ELO match found
            team_record[f'Member_{member_index}'] = athlete_name
            team_record[f'Member_{member_index}_ID'] = ''
            member_ids.append('')
            for col in elo_columns:
                team_record[f'Member_{member_index}_{col}'] = ''
    
    # Add member IDs as comma-separated string
    team_record['MemberIDs'] = ','.join(member_ids)
    
    # Calculate team averages
    if valid_member_count > 0:
        for col in elo_columns:
            if team_record[f'Total_{col}'] > 0:
                team_record[f'Avg_{col}'] = team_record[f'Total_{col}'] / valid_member_count
    
    return team_record

def create_mixed_team_record(nation: str, men_athletes: List[str], ladies_athletes: List[str], 
                           men_elo: pd.DataFrame, ladies_elo: pd.DataFrame, race: pd.Series) -> Dict:
    """Create mixed relay record (2 men + 2 ladies for cross-country)"""
    
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_F_Elo', 'Distance_C_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Create team record
    team_record = {
        'Team_Name': nation,
        'Nation': nation,
        'Team_Rank': 0,
        'Race_Date': race['Race_Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Distance': race['Distance'],
        'Technique': race['Technique'],
        'Is_Present': True,
        'Race1_Prob': 1.0
    }
    
    # Initialize totals
    for col in elo_columns:
        team_record[f'Total_{col}'] = 0
        team_record[f'Avg_{col}'] = 0
    
    all_athletes = men_athletes + ladies_athletes
    team_record['TeamMembers'] = ','.join(all_athletes)
    
    valid_member_count = 0
    member_ids = []
    
    # Process men first, then ladies
    member_index = 1
    for athletes, elo_data in [(men_athletes, men_elo), (ladies_athletes, ladies_elo)]:
        for athlete_name in athletes:
            # Match with appropriate ELO data
            elo_match = None
            if athlete_name in elo_data['Skier'].values:
                elo_match = athlete_name
            else:
                elo_match = fuzzy_match_name(athlete_name, elo_data['Skier'].tolist())
            
            if elo_match:
                athlete_data = elo_data[elo_data['Skier'] == elo_match].iloc[0].to_dict()
                valid_member_count += 1
                
                # Add to team totals
                for col in elo_columns:
                    if col in athlete_data and not pd.isna(athlete_data[col]):
                        team_record[f'Total_{col}'] += athlete_data[col]
                        team_record[f'Member_{member_index}_{col}'] = athlete_data[col]
                    else:
                        team_record[f'Member_{member_index}_{col}'] = ''
                
                team_record[f'Member_{member_index}'] = athlete_name
                team_record[f'Member_{member_index}_ID'] = athlete_data.get('ID', '')
                member_ids.append(str(athlete_data.get('ID', '')))
            else:
                team_record[f'Member_{member_index}'] = athlete_name
                team_record[f'Member_{member_index}_ID'] = ''
                member_ids.append('')
                for col in elo_columns:
                    team_record[f'Member_{member_index}_{col}'] = ''
            
            member_index += 1
    
    team_record['MemberIDs'] = ','.join(member_ids)
    
    # Calculate averages
    if valid_member_count > 0:
        for col in elo_columns:
            if team_record[f'Total_{col}'] > 0:
                team_record[f'Avg_{col}'] = team_record[f'Total_{col}'] / valid_member_count
    
    return team_record

def create_championship_relay_team(nation: str, athletes: List[str], elo_scores: pd.DataFrame, 
                                  race: pd.Series, gender: str) -> Tuple[List[Dict], List[Dict]]:
    """Create team and individual data for championship relay (matching weekend scraper structure)"""
    
    # Define Elo columns for cross-country (9 total)
    elo_columns = [
        'Elo', 'Distance_Elo', 'Distance_F_Elo', 'Distance_C_Elo', 
        'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 'Freestyle_Elo'
    ]
    
    # Create team name in standard format (like existing scrapers)
    team_name = f"{nation} I"
    
    # Initialize team data (matching weekend scraper structure)
    team_data = {
        'Team_Name': team_name,
        'Nation': nation,
        'Team_Rank': 1,  # Championships - all teams start equal
        'Race_Date': race['Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Price': 0,  # Championships don't use fantasy prices
        'Is_Present': True,
        'Race_Type': 'Relay'
    }
    
    # Initialize all Elo sums 
    for col in elo_columns:
        team_data[col] = 0
    
    # Track individuals data
    individuals_data = []
    valid_member_count = 0
    
    # Process each team member (up to 4 for relay)
    for position, athlete_name in enumerate(athletes[:4], 1):
        # Match with ELO scores
        elo_match = None
        if athlete_name in elo_scores['Skier'].values:
            elo_match = athlete_name
        else:
            elo_match = fuzzy_match_name(athlete_name, elo_scores['Skier'].tolist())
        
        if elo_match:
            athlete_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
            valid_member_count += 1
            
            # Add to team totals
            for col in elo_columns:
                if col in athlete_data and not pd.isna(athlete_data[col]):
                    team_data[col] += athlete_data[col]
            
            # Create individual data record (matching weekend scraper structure)
            individual_record = {
                'FIS_Name': athlete_name,
                'Skier': elo_match,
                'Nation': nation,
                'In_FIS_List': True,
                'Price': 0,
                'Team_Name': team_name,
                'Team_Rank': 1,
                'Team_Time': '',
                'Team_Position': position,
                'Race_Type': 'Relay',
                'Race_Date': race['Date'],
                'City': race['City'],
                'Country': race['Country']
            }
            
            # Add all ELO columns to individual record
            for col in elo_columns:
                individual_record[col] = athlete_data.get(col, '')
            
            # Add other athlete data
            individual_record['ID'] = athlete_data.get('ID', '')
            individual_record['Age'] = athlete_data.get('Age', '')
            individual_record['Exp'] = athlete_data.get('Exp', '')
            
            individuals_data.append(individual_record)
        else:
            # No ELO match - create minimal record
            individual_record = {
                'FIS_Name': athlete_name,
                'Skier': athlete_name,
                'Nation': nation,
                'In_FIS_List': True,
                'Price': 0,
                'Team_Name': team_name,
                'Team_Rank': 1,
                'Team_Time': '',
                'Team_Position': position,
                'Race_Type': 'Relay',
                'Race_Date': race['Date'],
                'City': race['City'],
                'Country': race['Country']
            }
            
            # Add empty ELO columns
            for col in elo_columns:
                individual_record[col] = ''
            
            individuals_data.append(individual_record)
    
    return [team_data], individuals_data

if __name__ == "__main__":
    process_championships()