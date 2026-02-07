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

def process_races() -> None:
    """Main function to process races from races.csv"""
    # Read races CSV
    print("Reading races.csv...")
    try:
        races_df = pd.read_csv('~/ski/elo/python/alpine/polars/excel365/races.csv')
        print(f"Successfully read races.csv with {len(races_df)} rows")
    except Exception as e:
        print(f"Error reading races.csv: {e}")
        traceback.print_exc()
        return
    
    # Check if dataframe is empty
    if races_df.empty:
        print("No races found in races.csv")
        return
    
    # Get today's date in UTC 
    today_utc = datetime.now(timezone.utc)
    today_str = today_utc.strftime('%m/%d/%Y')
    
    print(f"Today's date (UTC): {today_str}")
    
    # First, explicitly print all unique dates in the Date column to debug
    unique_dates = races_df['Date'].unique()
    print(f"Unique dates in races.csv: {unique_dates}")
    
    # Filter races for today
    today_races = races_df[races_df['Date'] == today_str]
    
    # If no results with full year format, try with 2-digit year format
    if today_races.empty:
        today_str_short = today_utc.strftime('%m/%d/%y')
        print(f"Trying with short year format: {today_str_short}")
        today_races = races_df[races_df['Date'] == today_str_short]
    
    if today_races.empty:
        print("No races found for today's date. Trying to find the closest date...")
        
        # Try to parse dates and find closest
        try:
            # Convert date strings to datetime objects
            parsed_dates = []
            for date_str in unique_dates:
                try:
                    # Try full year format first
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                except ValueError:
                    try:
                        # Try 2-digit year format
                        date_obj = datetime.strptime(date_str, '%m/%d/%y')
                    except ValueError:
                        print(f"Could not parse date: {date_str}")
                        continue
                parsed_dates.append((date_str, date_obj))
            
            if parsed_dates:
                # Find the closest date to today
                closest_date = min(parsed_dates, key=lambda x: abs((x[1] - today_utc).total_seconds()))
                print(f"Closest date found: {closest_date[0]} ({closest_date[1]})")
                
                # Use the closest date
                today_races = races_df[races_df['Date'] == closest_date[0]]
            else:
                print("No valid dates found in races.csv")
                return
        except Exception as e:
            print(f"Error finding closest date: {e}")
            return
    
    print(f"Found {len(today_races)} races for processing")
    
    if today_races.empty:
        print("No races found for processing")
        return
    
    # Use the filtered races for processing
    races_df = today_races
    
    # Print specific date for processing
    date_for_processing = races_df.iloc[0]['Date']
    print(f"Processing races for date: {date_for_processing}")
    
    # Filter races by type: individual races only for alpine (no team events)
    individual_races = races_df[
        (races_df['Sex'].isin(['M', 'L']))  # Individual men's or ladies' races
    ]
    
    print(f"Found {len(individual_races)} individual races")
    
    # Process individual races if available
    if not individual_races.empty:
        print("\n=== Processing Individual Races ===")
        process_individual_races(individual_races)
    
    # Run the R script once at the end
    print("\n=== All startlists have been scraped. Running weekly-picks-alpine.R ===")
    r_script_path = os.path.expanduser('~/blog/daehl-e/content/post/alpine/drafts/race-picks.R')
    
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

def process_individual_races(valid_races: pd.DataFrame) -> None:
    """Process individual races"""
    try:
        # Reset the Date parsing - don't convert to datetime as it causes issues
        # valid_races['Date'] keeps the original string format
        
        # Determine host nation from the Country column
        host_nation = valid_races.iloc[0]['Country']
        print(f"Host nation for these races: {host_nation}")
        
        # Process each gender separately
        men_races = valid_races[valid_races['Sex'] == 'M']
        ladies_races = valid_races[valid_races['Sex'] == 'L']
        
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

def create_season_startlist(elo_path: str, race_info: pd.Series, gender: str, 
                           host_nation: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with all skiers from current season when no startlist is available"""
    try:
        # Get race information
        city = race_info['City']
        country = race_info['Country']
        # Construct race_type from Sex and Distance columns
        sex_full = "Men's" if race_info['Sex'] == 'M' else "Ladies'"
        distance = race_info.get('Distance', 'Giant Slalom')
        race_type = f"{sex_full} {distance}"
        discipline = determine_discipline_from_race_type(distance)
        
        print(f"Creating season startlist for {race_type} in {city}, {country} (Discipline: {discipline})")
        
        # Get chronological data to find current season skiers
        chrono_path = f"~/ski/elo/python/alpine/polars/excel365/{gender}_chrono_pred.csv"
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
                'FISName': skier_name,
                'Skier': skier_name,
                'ID': skier_id,
                'Nation': nation,
                'StartOrder': 999,  # Default high start order
                'Bib': 999,         # Default high bib number
                'Is_Host_Nation': is_host,
                'In_Startlist': False,  # Not in actual startlist
                'Discipline': discipline
            }
            
            # Add ELO columns for alpine skiing (using correct column names with dots)
            for col in ['Elo', 'Downhill_Elo', 'Super G_Elo', 'Giant Slalom_Elo', 'Slalom_Elo', 'Combined_Elo', 'Tech_Elo', 'Speed_Elo']:
                if col in elo_data:
                    row_data[col] = elo_data.get(col)
            
            # Add race-specific ELO based on discipline
            row_data['Race_Elo'] = get_race_specific_elo(elo_data, discipline)
            
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
            'Race_Date': race_info['Date'],
            'Discipline': discipline
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

def create_race_startlist(race_id: str, elo_path: str, race_info: pd.Series, gender: str, 
                           host_nation: str, prob_column: str) -> Optional[pd.DataFrame]:
    """Creates DataFrame with startlist and ELO scores for races"""
    try:
        # Get race information
        city = race_info['City']
        country = race_info['Country']
        # Construct race_type from Sex and Distance columns  
        sex_full = "Men's" if race_info['Sex'] == 'M' else "Ladies'"
        distance = race_info.get('Distance', 'Giant Slalom')
        race_type = f"{sex_full} {distance}"
        
        print(f"Race details: {race_type} in {city}, {country}")
        
        # Get data from FIS website
        athletes, race_meta = get_fis_race_data(race_id)
        print(f"Found {len(athletes)} athletes in FIS startlist")
        
        if not athletes:
            print("FIS startlist is empty, skipping")
            return None
        
        # Determine discipline from race metadata or race type
        discipline = classify_discipline_from_metadata(race_meta)
        if discipline == 'Giant Slalom':  # If still default, try from race_info
            discipline = race_info.get('Distance', determine_discipline_from_race_type(race_type))
        
        print(f"Determined discipline: {discipline}")
        
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
                skier_id = elo_data.get('ID', athlete.get('FisCode', athlete.get('ID', '')))
            else:
                print(f"No ELO match found for: {name}")
                elo_data = {}
                original_name = name
                skier_id = athlete.get('FisCode', athlete.get('ID', ''))
            
            # Check if this is the host nation
            is_host = (nation == host_nation)
            
            # Build base row data
            row_data = {
                'FISName': name,
                'Skier': original_name,
                'ID': skier_id,
                'Nation': nation,
                'StartOrder': athlete.get('StartOrder', ''),
                'Bib': athlete.get('Bib', ''),
                'Is_Host_Nation': is_host,
                'In_Startlist': True,
                'Year': athlete.get('Year', ''),
                'Run1': athlete.get('Run1', ''),
                'Run2': athlete.get('Run2', ''),
                'Time': athlete.get('Time', ''),
                'Points': athlete.get('Points', ''),
                'Discipline': discipline
            }
            
            # Add ELO columns if available (using correct column names with dots)
            for col in ['Elo', 'Downhill_Elo', 'Super G_Elo', 'Giant Slalom_Elo', 'Slalom_Elo', 'Combined_Elo', 'Tech_Elo', 'Speed_Elo']:
                if col in elo_data:
                    row_data[col] = elo_data.get(col)
            
            # Add race-specific ELO based on discipline
            row_data['Race_Elo'] = get_race_specific_elo(elo_data, discipline)
            
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
            'Race_Date': race_info['Date'],
            'Discipline': discipline
        }
        
        for key, value in race_info_dict.items():
            df[key] = value
            
        # Sort by race-specific ELO (highest first)
        if 'Race_Elo' in df.columns:
            df = df.sort_values('Race_Elo', ascending=False)
        
        print(f"Processed startlist with {len(df)} athletes")
        return df
        
    except Exception as e:
        print(f"Error creating race startlist: {e}")
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
    
    # Get total number of races for this gender on the race date
    total_gender_races = len(races_df)
    print(f"Total {gender} races: {total_gender_races}")
    
    # Create enough probability columns for all races
    all_prob_columns = [f'Race{i+1}_Prob' for i in range(total_gender_races)]
    print(f"All probability columns: {all_prob_columns}")
    
    # Get the ELO path
    elo_path = f"~/ski/elo/python/alpine/polars/excel365/{gender}_chrono_pred.csv"
    
    # Initialize a consolidated dataframe
    consolidated_df = None
    
    # Process each race
    for i, (_, race) in enumerate(races_df.iterrows()):
        race_id = None
        if 'Startlist' in race and not pd.isna(race['Startlist']):
            race_id = extract_race_id_from_url(race['Startlist'])
            
        prob_column = all_prob_columns[i]
        
        print(f"Processing race {i+1} with probability column: {prob_column}")
        
        if not race_id:
            print(f"No race ID for race {i+1}, using all skiers from current season")
            
            # Create startlist with all skiers from current season
            race_df = create_season_startlist(
                elo_path=elo_path,
                race_info=race,
                gender=gender,
                host_nation=host_nation,
                prob_column=prob_column
            )
        else:
            print(f"Processing race {i+1} with ID: {race_id}")
            
            # Create startlist for this race from FIS website
            race_df = create_race_startlist(
                race_id=race_id,
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
        output_path = f"~/ski/elo/python/alpine/polars/excel365/startlist_races_{gender}.csv"
        print(f"Saving consolidated {gender} startlist to {output_path}")
        consolidated_df.to_csv(os.path.expanduser(output_path), index=False)
        print(f"Saved {len(consolidated_df)} {gender} athletes")
    else:
        print(f"No startlist data was generated for {gender}")

if __name__ == "__main__":
    process_races()