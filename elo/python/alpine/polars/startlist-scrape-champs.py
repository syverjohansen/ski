#!/usr/bin/env python3
"""
Championships Prediction Startlist Generator for Alpine Skiing

Simple version that just creates basic startlists like weekend scraper.
Race probabilities will be calculated in R like weekly-picks2.R does.
"""

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

# Import Championships config functions
from config import get_champs_athletes

def process_championships() -> None:
    """Main function to process Championships races"""
    print("=== Alpine Championships Startlist Generator ===")
    
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/alpine/polars/excel365/weekends.csv')
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
        print(f"  - {race['Sex']} {race['Distance']} in {race['City']}, {race['Country']}")
    
    # Process each gender separately
    men_races = champs_races[champs_races['Sex'] == 'M']
    ladies_races = champs_races[champs_races['Sex'] == 'L']
    
    print(f"Found {len(men_races)} men's races and {len(ladies_races)} ladies' races")
    
    # Process men's races
    if not men_races.empty:
        try:
            print("\n=== Processing Men's Championships ===")
            create_simple_championships_startlist('men', men_races)
        except Exception as e:
            print(f"Error processing men's championships: {e}")
            traceback.print_exc()

    # Process ladies' races  
    if not ladies_races.empty:
        try:
            print("\n=== Processing Ladies' Championships ===")
            create_simple_championships_startlist('ladies', ladies_races)
        except Exception as e:
            print(f"Error processing ladies' championships: {e}")
            traceback.print_exc()
    
    print("\n=== Championships startlist generation complete ===")

def create_simple_championships_startlist(gender: str, races_df: pd.DataFrame) -> None:
    """Create simple Championships startlist without race probabilities (like weekend scraper)"""
    
    print(f"Creating simple startlist for {gender}")
    
    # Get ELO data path
    elo_path = f"~/ski/elo/python/alpine/polars/excel365/{gender}_chrono_pred.csv"
    
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
            
            print(f"\nProcessing {nation}: {len(athletes)} athletes")
            
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
                    'Price': 0.0,  # Alpine doesn't use fantasy prices
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

if __name__ == "__main__":
    process_championships()