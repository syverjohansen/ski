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

def process_weekend_races() -> None:
    """Main function to process weekend races"""
    # Read weekends CSV
    print("Reading weekends.csv...")
    try:
        weekends_df = pd.read_csv('~/ski/elo/python/biathlon/polars/excel365/weekends.csv')
        print(f"Successfully read weekends.csv with {len(weekends_df)} rows")
    except Exception as e:
        print(f"Error reading weekends.csv: {e}")
        traceback.print_exc()
        return
    
    # Find next race date
    next_date = find_next_race_date(weekends_df)
    print(f"Next race date is {next_date}")
    
    # Filter races for the next date
    next_races = filter_races_by_date(weekends_df, next_date)
    print(f"Found {len(next_races)} races on {next_date}")
    
    if next_races.empty:
        print("No races found for the next date")
        return
    
    # Check for team/relay events
    relay_races = next_races[next_races['RaceType'].isin(['Relay', 'Mixed Relay', 'Single Mixed Relay'])]
    print(f"Found {len(relay_races)} relay/team events on {next_date}")
    
    # Filter for individual races
    individual_races = next_races[~next_races['RaceType'].isin(['Relay', 'Mixed Relay', 'Single Mixed Relay'])]
    print(f"Found {len(individual_races)} individual races on {next_date}")
    
    # Process individual races if available
    if not individual_races.empty:
        process_individual_races(individual_races)
    
    # Process relay races if available
    if not relay_races.empty:
        process_relay_and_team_races(relay_races)
    
    # Check if there are races today, and if so, run the R script once
    today_utc = datetime.now(timezone.utc).strftime('%m/%d/%Y')
    today_races = weekends_df[weekends_df['Date'] == today_utc]
    
    if not today_races.empty:
        print("\n=== All startlists have been scraped. Running weekly-picks2.R ===")
        r_script_path = os.path.expanduser('~/blog/daehl-e/content/post/biathlon/drafts/weekly-picks2.R')
        
        try:
            # Run the R script
            result = subprocess.run(
                ["Rscript", r_script_path],
                check=True,
                capture_output=True,
                text=True
            )
            print("R script executed successfully")
            print("Output:")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error running R script: {e}")
            print("Error output:")
            print(e.stderr)
    else:
        print("\nNo races found for today. Not running the R script.")

def process_individual_races(valid_races: pd.DataFrame) -> None:
    """Process individual races"""
    try:
        # Sort races by Race_Date (earliest first)
        valid_races['Race_Date'] = pd.to_datetime(valid_races['Race_Date'])
        sorted_races = valid_races.sort_values('Race_Date')
        print(f"Sorted races by Race_Date, first race date: {sorted_races['Race_Date'].iloc[0]}")
        
        # Determine host nation from the Country column
        host_nation = sorted_races.iloc[0]['Country']
        print(f"Host nation for this weekend: {host_nation}")
        
        # Select up to two races to scrape (prioritizing earliest race dates)
        races_to_process = sorted_races.head(2)
        print(f"Processing {len(races_to_process)} individual races with earliest dates")
        
        # Process each gender separately
        men_races = races_to_process[races_to_process['Sex'] == 'M']
        ladies_races = races_to_process[races_to_process['Sex'] == 'L']
        
        print(f"Found {len(men_races)} men's races and {len(ladies_races)} ladies' races to process")
        
        # Process men's races
        if not men_races.empty:
            try:
                process_gender_races(men_races, 'men', host_nation)
            except Exception as e:
                print(f"Error processing men's races: {e}")
                traceback.print_exc()

        # Process ladies' races
        if not ladies_races.empty:
            try:
                process_gender_races(ladies_races, 'ladies', host_nation)
            except Exception as e:
                print(f"Error processing ladies' races: {e}")
                traceback.print_exc()
    except Exception as e:
        print(f"Error processing individual races: {e}")
        traceback.print_exc()

def process_relay_and_team_races(relay_races: pd.DataFrame) -> None:
    """Process relay and team races by calling appropriate scripts"""
    # Group by race type
    race_types = relay_races['RaceType'].unique()
    relay_dir = os.path.expanduser("~/ski/elo/python/biathlon/polars/relay")
    
    for race_type in race_types:
        races_of_type = relay_races[relay_races['RaceType'] == race_type]
        
        if race_type == 'Relay':
            # Process relay races
            print(f"Processing {len(races_of_type)} relay races")
            
            # Save to temporary file
            temp_file = "/tmp/weekend_relay_races.csv"
            races_of_type.to_csv(temp_file, index=False)
            
            # Call the relay script with environment variable to skip running weekly picks
            try:
                env = os.environ.copy()
                env["SKIP_WEEKLY_PICKS"] = "1"
                
                subprocess.run(
                    [sys.executable, os.path.join(relay_dir, "startlist_scrape_weekend_relay.py"), temp_file], 
                    check=True,
                    env=env
                )
                print("Successfully processed relay races")
            except subprocess.CalledProcessError as e:
                print(f"Error calling relay script: {e}")
                
        elif race_type == 'Mixed Relay':
            # Process mixed relay races
            print(f"Processing {len(races_of_type)} mixed relay races")
            
            # Save to temporary file
            temp_file = "/tmp/weekend_mixed_relay_races.csv"
            races_of_type.to_csv(temp_file, index=False)
            
            # Call the mixed relay script with environment variable to skip running weekly picks
            try:
                env = os.environ.copy()
                env["SKIP_WEEKLY_PICKS"] = "1"
                
                subprocess.run(
                    [sys.executable, os.path.join(relay_dir, "startlist_scrape_weekend_mixed_relay.py"), temp_file], 
                    check=True,
                    env=env
                )
                print("Successfully processed mixed relay races")
            except subprocess.CalledProcessError as e:
                print(f"Error calling mixed relay script: {e}")
                
        elif race_type == 'Single Mixed Relay':
            # Process single mixed relay races
            print(f"Processing {len(races_of_type)} single mixed relay races")
            
            # Save to temporary file
            temp_file = "/tmp/weekend_single_mixed_relay_races.csv"
            races_of_type.to_csv(temp_file, index=False)
            
            # Call the single mixed relay script with environment variable to skip running weekly picks
            try:
                env = os.environ.copy()
                env["SKIP_WEEKLY_PICKS"] = "1"
                
                subprocess.run(
                    [sys.executable, os.path.join(relay_dir, "startlist_scrape_weekend_single_mixed_relay.py"), temp_file], 
                    check=True,
                    env=env
                )
                print("Successfully processed single mixed relay races")
            except subprocess.CalledProcessError as e:
                print(f"Error calling single mixed relay script: {e}")

def create_season_startlist(elo_path: str, race_info: pd.Series, gender: str, 
                           host_nation: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with all skiers from current season when no startlist is available"""
    try:
        # Get race information
        city = race_info['City']
        country = race_info['Country']
        race_type = race_info['RaceType']
        
        print(f"Creating season startlist for {race_type} in {city}, {country}")
        
        # Get chronological data to find current season skiers
        chrono_path = f"~/ski/elo/python/biathlon/polars/excel365/{gender}_chrono_pred.csv"
        try:
            # First try to read chronological data
            chrono_df = pd.read_csv(chrono_path)
            
            # Get current season
            current_season = chrono_df['Season'].max()
            print(f"Using most recent season {current_season} for all skiers list")
            
            # Filter to current season
            current_season_df = chrono_df[chrono_df['Season'] == current_season]
            
            # Get unique skiers from current season
            current_skiers_df = current_season_df[['Skier', 'ID', 'Nation']].drop_duplicates()
            
            print(f"Found {len(current_skiers_df)} unique skiers from current season {current_season}")
            
        except Exception as e:
            print(f"Error reading chronological data: {e}")
            # Fallback to ELO scores if chrono data fails
            current_skiers_df = None
        
        # Get ELO scores
        elo_scores = get_latest_elo_scores(elo_path)
        
        # If we couldn't get current season skiers, use all from ELO
        if current_skiers_df is None or len(current_skiers_df) == 0:
            print("Using all skiers from ELO scores")
            current_skiers_df = elo_scores[['Skier', 'ID', 'Nation']].drop_duplicates()
        
        # Create data for DataFrame
        data = []
        
        # Process each skier
        for _, skier_row in current_skiers_df.iterrows():
            skier_name = skier_row['Skier']
            skier_id = skier_row['ID']
            nation = skier_row['Nation']
            
            # Get ELO data if available
            elo_data = {}
            if skier_name in elo_scores['Skier'].values:
                elo_data = elo_scores[elo_scores['Skier'] == skier_name].iloc[0].to_dict()
            
            # Check if this is the host nation
            is_host = (nation == host_nation)
            
            # Build base row data
            row_data = {
                'IBUName': skier_name,
                'Skier': skier_name,
                'ID': skier_id,
                'Nation': nation,
                'StartOrder': 999,  # Default high start order
                'Bib': 999,         # Default high bib number
                'Is_Host_Nation': is_host,
                'In_Startlist': False  # Not in actual startlist
            }
            
            # Add ELO columns
            for col in ['Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo']:
                row_data[col] = elo_data.get(col, None)
            
            # Add race-specific ELO
            row_data['Race_Elo'] = get_race_specific_elo(elo_data, race_type)
            
            # Set race probability to 0 since not in startlist
            row_data[prob_column] = 0.0
            
            data.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Add race info to all rows
        race_info_dict = {
            'City': city,
            'Country': country,
            'Race_Type': race_type,
            'Race_Date': race_info['Date']
        }
        
        for key, value in race_info_dict.items():
            df[key] = value
            
        # Sort by race-specific ELO (highest first)
        if 'Race_Elo' in df.columns and len(df) > 0:
            df = df.sort_values('Race_Elo', ascending=False, na_position='last')
        
        print(f"Created season startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating season startlist: {e}")
        traceback.print_exc()
        return None

def create_weekend_startlist(url: str, elo_path: str, race_info: pd.Series, gender: str, 
                           host_nation: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with startlist and ELO scores for weekend races"""
    try:
        # Get race information
        city = race_info['City']
        country = race_info['Country']
        race_type = race_info['RaceType']
        
        print(f"Race details: {race_type} in {city}, {country}")
        
        # Get data from all sources
        athletes = get_biathlon_startlist(url)
        print(f"Found {len(athletes)} athletes in biathlon startlist")
        
        if not athletes:
            print("Biathlon startlist is empty, skipping")
            return None
        
        elo_scores = get_latest_elo_scores(elo_path)
        
        # Create data for DataFrame
        data = []
        
        # Process each athlete from the startlist
        for athlete in athletes:
            # Basic athlete information
            name = athlete['Name']
            nation = athlete['Nation']
            
            print(f"\nProcessing athlete: {name} ({nation})")
            
            # Match with ELO scores
            elo_match = None
            if name in elo_scores['Skier'].values:
                elo_match = name
                print(f"Found exact ELO match for: {name}")
            else:
                # Try fuzzy matching if no exact match
                elo_match = fuzzy_match_name(name, elo_scores['Skier'].tolist())
                if elo_match:
                    print(f"Found fuzzy ELO match: {name} -> {elo_match}")
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                original_name = elo_match
                skier_id = elo_data.get('ID', athlete['IBUId'])
            else:
                print(f"No ELO match found for: {name}")
                elo_data = {}
                original_name = name
                skier_id = athlete['IBUId']
            
            # Check if this is the host nation
            is_host = (nation == host_nation)
            
            # Determine which ELO columns to prioritize based on race type
            try:
                elo_priority = get_elo_priority(race_type)
            except Exception as e:
                print(f"Error determining ELO priority: {e}")
                elo_priority = ['Elo']
            
            # Build base row data
            row_data = {
                'IBUName': name,
                'Skier': original_name,
                'ID': skier_id,
                'Nation': nation,
                'StartOrder': athlete['StartOrder'],
                'Bib': athlete['Bib'],
                'Is_Host_Nation': is_host,
                'In_Startlist': True
            }
            
            # Add ELO columns if available
            for col in ['Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo']:
                row_data[col] = elo_data.get(col, None)
            
            # Add race-specific ELO
            row_data['Race_Elo'] = get_race_specific_elo(elo_data, race_type)
            
            # Set race probability
            if prob_column:
                row_data[prob_column] = 1.0  # In startlist = 100% for this race
            
            data.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Add race info to all rows
        race_info_dict = {
            'City': city,
            'Country': country,
            'Race_Type': race_type,
            'Race_Date': race_info['Date']
        }
        
        for key, value in race_info_dict.items():
            df[key] = value
            
        # Sort by race-specific ELO (highest first)
        if 'Race_Elo' in df.columns:
            df = df.sort_values('Race_Elo', ascending=False)
        
        print(f"Processed startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating weekend startlist: {e}")
        traceback.print_exc()
        return None

def merge_race_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, prob_column: str) -> pd.DataFrame:
    """Merge two race dataframes, preserving unique athletes and combining probability columns"""
    # Create a copy of df1 to avoid modifying the original
    result = df1.copy()
    
    # Create a set of existing skiers in df1
    existing_skiers = set(df1['Skier'])
    
    # For skiers that exist in both dataframes, update the probability column
    common_skiers = set(df2['Skier']) & existing_skiers
    for skier in common_skiers:
        # Find the row index in result for this skier
        idx = result[result['Skier'] == skier].index[0]
        
        # Get probability from df2
        prob_value = df2[df2['Skier'] == skier][prob_column].values[0]
        
        # Update the probability in result
        result.loc[idx, prob_column] = prob_value
    
    # For skiers that only exist in df2, add them to result
    new_skiers = set(df2['Skier']) - existing_skiers
    new_rows = df2[df2['Skier'].isin(new_skiers)]
    
    # Append the new rows to result
    result = pd.concat([result, new_rows], ignore_index=True)
    
    # Sort the result by ELO
    result = result.sort_values('Race_Elo', ascending=False)
    
    return result

def process_gender_races(races_df: pd.DataFrame, gender: str, host_nation: str) -> None:
    """Process races for a specific gender"""
    print(f"\nProcessing {gender} races...")
    
    # Get total number of races for this gender on the next date
    total_gender_races = len(races_df)
    print(f"Total {gender} races: {total_gender_races}")
    
    # Create enough probability columns for all races
    all_prob_columns = [f'Race{i+1}_Prob' for i in range(total_gender_races)]
    print(f"All probability columns: {all_prob_columns}")
    
    # Get the ELO path
    elo_path = f"~/ski/elo/python/biathlon/polars/excel365/{gender}_chrono_pred.csv"
    
    # Initialize a consolidated dataframe
    consolidated_df = None
    
    # Process each race (up to 2)
    races_to_process = races_df.head(2)  # Only process up to 2 races
    print(f"Processing {len(races_to_process)} {gender} races")
    
    for i, (_, race) in enumerate(races_to_process.iterrows()):
        url = race['Startlist']
        prob_column = all_prob_columns[i]
        
        print(f"Processing race {i+1} with probability column: {prob_column}")
        
        if not url or pd.isna(url):
            print(f"No startlist URL for race {i+1}, using all skiers from current season")
            
            # Create startlist with all skiers from current season
            race_df = create_season_startlist(
                elo_path=elo_path,
                race_info=race,
                gender=gender,
                host_nation=host_nation,
                prob_column=prob_column
            )
        else:
            print(f"Processing race {i+1} from URL: {url}")
            
            # Create startlist for this race from URL
            race_df = create_weekend_startlist(
                url=url,
                elo_path=elo_path,
                race_info=race,
                gender=gender,
                host_nation=host_nation,
                prob_column=prob_column
            )
        
        if race_df is None:
            print(f"Failed to create startlist for race {i+1}")
            continue
        
        # If this is the first race, initialize the consolidated dataframe
        if consolidated_df is None:
            consolidated_df = race_df
        else:
            # Merge with existing data (ensuring we don't lose any athletes)
            consolidated_df = merge_race_dataframes(consolidated_df, race_df, prob_column)
    
    # If we successfully processed at least one race
    if consolidated_df is not None:
        # Ensure all race probability columns exist in the final dataframe
        for i, col in enumerate(all_prob_columns):
            if col not in consolidated_df.columns:
                print(f"Adding missing column {col}")
                consolidated_df[col] = 0.0
                
                # For unprocessed races, also add race info
                if i >= len(races_to_process):
                    print(f"Adding race info for unprocessed race {i+1}")
                    race_info = races_df.iloc[i]
        
        # Create a comprehensive season startlist to ensure all skiers are included
        print("Creating comprehensive season startlist to ensure all skiers are included")
        all_season_skiers_df = create_season_startlist(
            elo_path=elo_path,
            race_info=races_df.iloc[0],  # Use first race for race info
            gender=gender,
            host_nation=host_nation,
            prob_column="temp_prob"  # Temporary column
        )
        
        if all_season_skiers_df is not None:
            # Get skiers already in the consolidated dataframe
            existing_skiers = set(consolidated_df['Skier'])
            
            # Find skiers from the season who aren't in the consolidated dataframe
            missing_skiers_df = all_season_skiers_df[~all_season_skiers_df['Skier'].isin(existing_skiers)]
            print(f"Found {len(missing_skiers_df)} skiers from current season not already in the startlist")
            
            # Drop the temp_prob column
            missing_skiers_df = missing_skiers_df.drop(columns=['temp_prob'])
            
            # Add all race probability columns with 0
            for col in all_prob_columns:
                missing_skiers_df[col] = 0.0
            
            # Combine with the consolidated dataframe
            if len(missing_skiers_df) > 0:
                consolidated_df = pd.concat([consolidated_df, missing_skiers_df], ignore_index=True)
                print(f"Added {len(missing_skiers_df)} skiers from current season to the startlist")
        
        # Make sure In_Startlist is FALSE for skiers not on any startlist
        # If a skier has 0 probability for all races, they weren't on any startlist
        # Check row by row if all probability values are 0
        consolidated_df.loc[consolidated_df[all_prob_columns].sum(axis=1) == 0, 'In_Startlist'] = False
        
        # Save the consolidated dataframe
        output_path = f"~/ski/elo/python/biathlon/polars/excel365/startlist_weekend_{gender}.csv"
        print(f"Saving consolidated {gender} startlist to {output_path}")
        consolidated_df.to_csv(output_path, index=False)
        print(f"Saved {len(consolidated_df)} {gender} athletes")
    else:
        print(f"No startlist data was generated for {gender}")

# The remaining functions stay the same...

if __name__ == "__main__":
    process_weekend_races()
    # Note: check_and_run_weekly_picks() is NOT called here anymore
    # Instead, the R script is run directly in process_weekend_races() after all processing is complete