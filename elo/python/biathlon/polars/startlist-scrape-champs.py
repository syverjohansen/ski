#!/usr/bin/env python3
"""
Championships Prediction Startlist Generator for Biathlon

Creates startlists for Championships competitions including:
- Individual competitions (men/ladies) 
- Relay competitions (men/ladies/mixed/single mixed)

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
    print("=== Biathlon Championships Startlist Generator ===")
    
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/biathlon/polars/excel365/weekends.csv')
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
    
    # Separate individual and relay races
    individual_races = champs_races[~champs_races['RaceType'].str.contains('Relay', case=False, na=False)]
    relay_races = champs_races[champs_races['RaceType'].str.contains('Relay', case=False, na=False)]
    
    print(f"Found {len(individual_races)} individual races and {len(relay_races)} relay races")
    
    # Process individual races by gender
    men_individual = individual_races[individual_races['Sex'] == 'M']
    ladies_individual = individual_races[individual_races['Sex'] == 'L']
    
    # Process relay races by type
    men_relays = relay_races[relay_races['Sex'] == 'M']
    ladies_relays = relay_races[relay_races['Sex'] == 'L']
    mixed_relays = relay_races[relay_races['Sex'] == 'Mixed']
    single_mixed_relays = relay_races[relay_races['RaceType'].str.contains('Single Mixed', case=False, na=False)]
    
    print(f"Individual races: {len(men_individual)} men, {len(ladies_individual)} ladies")
    print(f"Relay races: {len(men_relays)} men, {len(ladies_relays)} ladies")
    print(f"Mixed relays: {len(mixed_relays)} mixed, {len(single_mixed_relays)} single mixed")
    
    # Process individual startlists
    if not men_individual.empty:
        try:
            print("\\n=== Processing Men's Individual Championships ===")
            create_simple_championships_startlist('men', men_individual)
        except Exception as e:
            print(f"Error processing men's individual championships: {e}")
            traceback.print_exc()

    if not ladies_individual.empty:
        try:
            print("\\n=== Processing Ladies' Individual Championships ===")
            create_simple_championships_startlist('ladies', ladies_individual)
        except Exception as e:
            print(f"Error processing ladies' individual championships: {e}")
            traceback.print_exc()
    
    # Process relay startlists (if any relay races exist)
    if not relay_races.empty:
        try:
            print("\\n=== Processing Relay Championships ===")
            
            # Process men's relays
            if not men_relays.empty:
                print("Processing men's relay races...")
                create_relay_championships_startlist('men', men_relays)
            
            # Process ladies' relays  
            if not ladies_relays.empty:
                print("Processing ladies' relay races...")
                create_relay_championships_startlist('ladies', ladies_relays)
                
            # Process mixed relays
            if not mixed_relays.empty:
                print("Processing mixed relay races...")
                create_mixed_relay_championships_startlist(mixed_relays)
                
            # Process single mixed relays
            if not single_mixed_relays.empty:
                print("Processing single mixed relay races...")
                create_single_mixed_relay_championships_startlist(single_mixed_relays)
                
        except Exception as e:
            print(f"Error processing relay championships: {e}")
            traceback.print_exc()
    
    print("\\n=== Championships startlist generation complete ===")

def create_simple_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create simple Championships startlist without race probabilities (like weekend scraper)"""
    
    print(f"Creating simple startlist for {gender}")
    
    # Get ELO data path
    elo_path = f"~/ski/elo/python/biathlon/polars/excel365/{gender}_chrono_pred.csv"
    
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
            
            print(f"\\nProcessing {nation}: {len(athletes)} athletes")
            
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
                    'Price': 0.0,  # Biathlon doesn't use fantasy prices
                    **{k: v for k, v in elo_data.items() if k != 'Skier'}
                }
                
                data.append(row_data)
                processed_athletes.add(athlete_name)
        
        if not data:
            print("No athlete data generated")
            return
        
        result_df = pd.DataFrame(data)
        print(f"Created startlist with {len(result_df)} athletes from {len(all_nations)} nations")
        
        # Save to CSV
        output_file = f"excel365/startlist_champs_{gender}.csv"
        result_df.to_csv(output_file, index=False)
        print(f"✓ Saved {gender} Championships startlist: {output_file} ({len(result_df)} athletes)")
        
    except Exception as e:
        print(f"Error creating Championships startlist: {e}")
        traceback.print_exc()

def create_relay_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create relay Championships startlist using config athletes (based on ski jumping approach)"""
    
    print(f"Creating relay startlist for {gender}")
    
    # Use individual ELO data (like ski jumping does)
    elo_path = f"~/ski/elo/python/biathlon/polars/excel365/{gender}_chrono_pred.csv"
    
    try:
        print("Loading ELO scores...")
        elo_scores = get_latest_elo_scores(elo_path)
        if elo_scores is None or elo_scores.empty:
            print(f"No ELO scores found for {gender}")
            return
        
        relay_data = []
        
        # Get qualifying nations from config (4+ athletes for relay competitions in biathlon)
        from config import CHAMPS_ATHLETES_MEN, CHAMPS_ATHLETES_LADIES
        
        if gender == 'men':
            all_nations = set(CHAMPS_ATHLETES_MEN.keys())
        else:
            all_nations = set(CHAMPS_ATHLETES_LADIES.keys())
        
        qualifying_nations = []
        for nation in all_nations:
            athletes = get_champs_athletes(nation, gender)
            if len(athletes) >= 4:  # Biathlon uses 4-person minimum for relays
                qualifying_nations.append(nation)
        
        print(f"Found {len(qualifying_nations)} qualifying nations with 4+ {gender} athletes")
        
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
                    # Use overall Elo as primary ranking, fallback to specific disciplines
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                    athlete_elos.append((athlete_name, main_elo))
                else:
                    # No ELO match - assign low value
                    athlete_elos.append((athlete_name, 0))
            
            # Sort by ELO (highest first) and select top 4 athletes
            athlete_elos.sort(key=lambda x: x[1], reverse=True)
            relay_athletes = [athlete for athlete, elo in athlete_elos[:4]]
            
            print(f"Creating relay for {nation} with top 4 athletes by ELO: {relay_athletes}")
            
            # Create relay record
            relay_record = create_relay_record_from_config(
                nation, relay_athletes, elo_scores, races_df.iloc[0]
            )
            relay_data.append(relay_record)
        
        if not relay_data:
            print("No relay data generated")
            return
        
        # Save to CSV in relay directory
        os.makedirs("relay/excel365", exist_ok=True)
        output_file = f"relay/excel365/startlist_champs_{gender}_relay.csv"
        
        relay_df = pd.DataFrame(relay_data)
        relay_df.to_csv(output_file, index=False)
        print(f"✓ Saved {gender} relay Championships startlist: {output_file} ({len(relay_df)} teams)")
        
    except Exception as e:
        print(f"Error creating relay Championships startlist: {e}")
        traceback.print_exc()

def create_mixed_relay_championships_startlist(races_df: pd.DataFrame) -> None:
    """Create mixed relay Championships startlist (2 men + 2 ladies)"""
    
    print("Creating mixed relay startlist")
    
    try:
        # Load both men's and ladies' ELO data
        men_elo = get_latest_elo_scores("~/ski/elo/python/biathlon/polars/excel365/men_chrono_pred.csv")
        ladies_elo = get_latest_elo_scores("~/ski/elo/python/biathlon/polars/excel365/ladies_chrono_pred.csv")
        
        if men_elo is None or ladies_elo is None or men_elo.empty or ladies_elo.empty:
            print("Could not load ELO data for mixed relays")
            return
        
        mixed_data = []
        
        # Get nations that have both men and ladies athletes
        from config import CHAMPS_ATHLETES_MEN, CHAMPS_ATHLETES_LADIES
        
        men_nations = set(CHAMPS_ATHLETES_MEN.keys())
        ladies_nations = set(CHAMPS_ATHLETES_LADIES.keys())
        qualifying_nations = men_nations.intersection(ladies_nations)
        
        # Filter for nations with sufficient athletes (at least 2 men and 2 ladies)
        final_nations = []
        for nation in qualifying_nations:
            men_athletes = get_champs_athletes(nation, 'men')
            ladies_athletes = get_champs_athletes(nation, 'ladies')
            if len(men_athletes) >= 2 and len(ladies_athletes) >= 2:
                final_nations.append(nation)
        
        print(f"Found {len(final_nations)} qualifying nations for mixed relays")
        
        for nation in sorted(final_nations):
            # Get top 2 men by ELO
            men_athletes_all = get_champs_athletes(nation, 'men')
            men_elos = []
            for athlete_name in men_athletes_all:
                if athlete_name in men_elo['Skier'].values:
                    elo_data = men_elo[men_elo['Skier'] == athlete_name].iloc[0].to_dict()
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                    men_elos.append((athlete_name, main_elo))
                else:
                    elo_match = fuzzy_match_name(athlete_name, men_elo['Skier'].tolist())
                    if elo_match:
                        elo_data = men_elo[men_elo['Skier'] == elo_match].iloc[0].to_dict()
                        main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
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
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                    ladies_elos.append((athlete_name, main_elo))
                else:
                    elo_match = fuzzy_match_name(athlete_name, ladies_elo['Skier'].tolist())
                    if elo_match:
                        elo_data = ladies_elo[ladies_elo['Skier'] == elo_match].iloc[0].to_dict()
                        main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                        ladies_elos.append((athlete_name, main_elo))
                    else:
                        ladies_elos.append((athlete_name, 0))
            
            ladies_elos.sort(key=lambda x: x[1], reverse=True)
            ladies_athletes = [athlete for athlete, elo in ladies_elos[:2]]  # Top 2 ladies by ELO
            
            print(f"Creating mixed relay for {nation}: Men (top 2 by ELO): {men_athletes}, Ladies (top 2 by ELO): {ladies_athletes}")
            
            # Create mixed relay record
            relay_record = create_mixed_relay_record(
                nation, men_athletes, ladies_athletes, men_elo, ladies_elo, races_df.iloc[0]
            )
            mixed_data.append(relay_record)
        
        if not mixed_data:
            print("No mixed relay data generated")
            return
        
        # Save to CSV
        os.makedirs("relay/excel365", exist_ok=True)
        output_file = "relay/excel365/startlist_champs_mixed_relay.csv"
        
        mixed_df = pd.DataFrame(mixed_data)
        mixed_df.to_csv(output_file, index=False)
        print(f"✓ Saved mixed relay Championships startlist: {output_file} ({len(mixed_df)} teams)")
        
    except Exception as e:
        print(f"Error creating mixed relay Championships startlist: {e}")
        traceback.print_exc()

def create_single_mixed_relay_championships_startlist(races_df: pd.DataFrame) -> None:
    """Create single mixed relay Championships startlist (1 man + 1 lady)"""
    
    print("Creating single mixed relay startlist")
    
    try:
        # Load both men's and ladies' ELO data
        men_elo = get_latest_elo_scores("~/ski/elo/python/biathlon/polars/excel365/men_chrono_pred.csv")
        ladies_elo = get_latest_elo_scores("~/ski/elo/python/biathlon/polars/excel365/ladies_chrono_pred.csv")
        
        if men_elo is None or ladies_elo is None or men_elo.empty or ladies_elo.empty:
            print("Could not load ELO data for single mixed relays")
            return
        
        single_mixed_data = []
        
        # Get nations that have both men and ladies athletes
        from config import CHAMPS_ATHLETES_MEN, CHAMPS_ATHLETES_LADIES
        
        men_nations = set(CHAMPS_ATHLETES_MEN.keys())
        ladies_nations = set(CHAMPS_ATHLETES_LADIES.keys())
        qualifying_nations = men_nations.intersection(ladies_nations)
        
        # Filter for nations with at least 1 man and 1 lady
        final_nations = []
        for nation in qualifying_nations:
            men_athletes = get_champs_athletes(nation, 'men')
            ladies_athletes = get_champs_athletes(nation, 'ladies')
            if len(men_athletes) >= 1 and len(ladies_athletes) >= 1:
                final_nations.append(nation)
        
        print(f"Found {len(final_nations)} qualifying nations for single mixed relays")
        
        for nation in sorted(final_nations):
            # Get top 1 man by ELO
            men_athletes_all = get_champs_athletes(nation, 'men')
            men_elos = []
            for athlete_name in men_athletes_all:
                if athlete_name in men_elo['Skier'].values:
                    elo_data = men_elo[men_elo['Skier'] == athlete_name].iloc[0].to_dict()
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                    men_elos.append((athlete_name, main_elo))
                else:
                    elo_match = fuzzy_match_name(athlete_name, men_elo['Skier'].tolist())
                    if elo_match:
                        elo_data = men_elo[men_elo['Skier'] == elo_match].iloc[0].to_dict()
                        main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                        men_elos.append((athlete_name, main_elo))
                    else:
                        men_elos.append((athlete_name, 0))
            
            men_elos.sort(key=lambda x: x[1], reverse=True)
            men_athletes = [athlete for athlete, elo in men_elos[:1]]  # Top 1 man by ELO
            
            # Get top 1 lady by ELO
            ladies_athletes_all = get_champs_athletes(nation, 'ladies')
            ladies_elos = []
            for athlete_name in ladies_athletes_all:
                if athlete_name in ladies_elo['Skier'].values:
                    elo_data = ladies_elo[ladies_elo['Skier'] == athlete_name].iloc[0].to_dict()
                    main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                    ladies_elos.append((athlete_name, main_elo))
                else:
                    elo_match = fuzzy_match_name(athlete_name, ladies_elo['Skier'].tolist())
                    if elo_match:
                        elo_data = ladies_elo[ladies_elo['Skier'] == elo_match].iloc[0].to_dict()
                        main_elo = elo_data.get('Elo', 0) or elo_data.get('Individual_Elo', 0) or elo_data.get('Sprint_Elo', 0) or 0
                        ladies_elos.append((athlete_name, main_elo))
                    else:
                        ladies_elos.append((athlete_name, 0))
            
            ladies_elos.sort(key=lambda x: x[1], reverse=True)
            ladies_athletes = [athlete for athlete, elo in ladies_elos[:1]]  # Top 1 lady by ELO
            
            print(f"Creating single mixed relay for {nation}: Man (top by ELO): {men_athletes}, Lady (top by ELO): {ladies_athletes}")
            
            # Create single mixed relay record
            relay_record = create_single_mixed_relay_record(
                nation, men_athletes, ladies_athletes, men_elo, ladies_elo, races_df.iloc[0]
            )
            single_mixed_data.append(relay_record)
        
        if not single_mixed_data:
            print("No single mixed relay data generated")
            return
        
        # Save to CSV
        os.makedirs("relay/excel365", exist_ok=True)
        output_file = "relay/excel365/startlist_champs_single_mixed_relay.csv"
        
        single_mixed_df = pd.DataFrame(single_mixed_data)
        single_mixed_df.to_csv(output_file, index=False)
        print(f"✓ Saved single mixed relay Championships startlist: {output_file} ({len(single_mixed_df)} teams)")
        
    except Exception as e:
        print(f"Error creating single mixed relay Championships startlist: {e}")
        traceback.print_exc()

def create_relay_record_from_config(nation: str, athletes: List[str], elo_scores: pd.DataFrame, race: pd.Series) -> Dict:
    """Create relay record using config athletes (adapted from ski jumping for biathlon)"""
    
    # Define Elo columns for biathlon
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'
    ]
    
    # Create relay record with basic info
    relay_record = {
        'Team_Name': nation,
        'Nation': nation,
        'Team_Rank': 0,  # No rank for Championships
        'Race_Date': race['Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Race_Type': race['RaceType'],
        'Distance': race.get('Distance', 'N/A'),
        'Is_Present': True,  # This team will participate
        'Race1_Prob': 1.0    # 100% probability for Championships
    }
    
    # Add team-level fields
    relay_record['Team_Points'] = ''
    relay_record['TeamMembers'] = ','.join(athletes)  # Comma-separated athlete names
    
    # Initialize all Elo sums and averages
    for col in elo_columns:
        relay_record[f'Total_{col}'] = 0
        relay_record[f'Avg_{col}'] = 0
    
    valid_member_count = 0
    member_ids = []
    
    # Process relay members
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
                    relay_record[f'Total_{col}'] += elo_data[col]
                    relay_record[f'Member_{member_index}_{col}'] = elo_data[col]
                else:
                    relay_record[f'Member_{member_index}_{col}'] = ''
            
            # Set member info
            relay_record[f'Member_{member_index}'] = athlete_name
            relay_record[f'Member_{member_index}_ID'] = elo_data.get('ID', '')
            member_ids.append(str(elo_data.get('ID', '')))
        else:
            # No ELO match found
            relay_record[f'Member_{member_index}'] = athlete_name
            relay_record[f'Member_{member_index}_ID'] = ''
            member_ids.append('')
            for col in elo_columns:
                relay_record[f'Member_{member_index}_{col}'] = ''
    
    # Add member IDs as comma-separated string
    relay_record['MemberIDs'] = ','.join(member_ids)
    
    # Calculate team averages
    if valid_member_count > 0:
        for col in elo_columns:
            if relay_record[f'Total_{col}'] > 0:
                relay_record[f'Avg_{col}'] = relay_record[f'Total_{col}'] / valid_member_count
    
    return relay_record

def create_mixed_relay_record(nation: str, men_athletes: List[str], ladies_athletes: List[str], 
                             men_elo: pd.DataFrame, ladies_elo: pd.DataFrame, race: pd.Series) -> Dict:
    """Create mixed relay record (2 men + 2 ladies for biathlon)"""
    
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'
    ]
    
    # Create relay record
    relay_record = {
        'Team_Name': nation,
        'Nation': nation,
        'Team_Rank': 0,
        'Race_Date': race['Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Race_Type': race['RaceType'],
        'Distance': race.get('Distance', 'N/A'),
        'Is_Present': True,
        'Race1_Prob': 1.0
    }
    
    # Initialize totals
    for col in elo_columns:
        relay_record[f'Total_{col}'] = 0
        relay_record[f'Avg_{col}'] = 0
    
    all_athletes = men_athletes + ladies_athletes
    relay_record['TeamMembers'] = ','.join(all_athletes)
    
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
                        relay_record[f'Total_{col}'] += athlete_data[col]
                        relay_record[f'Member_{member_index}_{col}'] = athlete_data[col]
                    else:
                        relay_record[f'Member_{member_index}_{col}'] = ''
                
                relay_record[f'Member_{member_index}'] = athlete_name
                relay_record[f'Member_{member_index}_ID'] = athlete_data.get('ID', '')
                member_ids.append(str(athlete_data.get('ID', '')))
            else:
                relay_record[f'Member_{member_index}'] = athlete_name
                relay_record[f'Member_{member_index}_ID'] = ''
                member_ids.append('')
                for col in elo_columns:
                    relay_record[f'Member_{member_index}_{col}'] = ''
            
            member_index += 1
    
    relay_record['MemberIDs'] = ','.join(member_ids)
    
    # Calculate averages
    if valid_member_count > 0:
        for col in elo_columns:
            if relay_record[f'Total_{col}'] > 0:
                relay_record[f'Avg_{col}'] = relay_record[f'Total_{col}'] / valid_member_count
    
    return relay_record

def create_single_mixed_relay_record(nation: str, men_athletes: List[str], ladies_athletes: List[str], 
                                    men_elo: pd.DataFrame, ladies_elo: pd.DataFrame, race: pd.Series) -> Dict:
    """Create single mixed relay record (1 man + 1 lady for biathlon)"""
    
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'
    ]
    
    # Create relay record
    relay_record = {
        'Team_Name': nation,
        'Nation': nation,
        'Team_Rank': 0,
        'Race_Date': race['Date'],
        'City': race['City'],
        'Country': race['Country'],
        'Race_Type': race['RaceType'],
        'Distance': race.get('Distance', 'N/A'),
        'Is_Present': True,
        'Race1_Prob': 1.0
    }
    
    # Initialize totals
    for col in elo_columns:
        relay_record[f'Total_{col}'] = 0
        relay_record[f'Avg_{col}'] = 0
    
    all_athletes = men_athletes + ladies_athletes
    relay_record['TeamMembers'] = ','.join(all_athletes)
    
    valid_member_count = 0
    member_ids = []
    
    # Process 1 man + 1 lady (2 total members)
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
                        relay_record[f'Total_{col}'] += athlete_data[col]
                        relay_record[f'Member_{member_index}_{col}'] = athlete_data[col]
                    else:
                        relay_record[f'Member_{member_index}_{col}'] = ''
                
                relay_record[f'Member_{member_index}'] = athlete_name
                relay_record[f'Member_{member_index}_ID'] = athlete_data.get('ID', '')
                member_ids.append(str(athlete_data.get('ID', '')))
            else:
                relay_record[f'Member_{member_index}'] = athlete_name
                relay_record[f'Member_{member_index}_ID'] = ''
                member_ids.append('')
                for col in elo_columns:
                    relay_record[f'Member_{member_index}_{col}'] = ''
            
            member_index += 1
    
    relay_record['MemberIDs'] = ','.join(member_ids)
    
    # Calculate averages
    if valid_member_count > 0:
        for col in elo_columns:
            if relay_record[f'Total_{col}'] > 0:
                relay_record[f'Avg_{col}'] = relay_record[f'Total_{col}'] / valid_member_count
    
    return relay_record

if __name__ == "__main__":
    process_championships()