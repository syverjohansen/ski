import requests
from bs4 import BeautifulSoup
import pandas as pd
import polars as pl
from typing import Dict, List, Tuple, Optional
import warnings
from datetime import datetime, timezone
import re
import json
import traceback
warnings.filterwarnings('ignore')

def check_and_run_weekly_picks():
    """
    Check if there are races with today's date in weekends.csv and run the weekly-picks2.R script if true.
    
    Returns:
        bool: True if the R script was run successfully, False otherwise
    """
    import os
    import subprocess
    from datetime import datetime, timezone
    
    # Get today's date in UTC
    today_utc = datetime.now(timezone.utc).strftime('%m/%d/%Y')
    print(f"Checking for races on today's date (UTC): {today_utc}")
    
    # Path to weekends.csv
    weekends_csv_path = os.path.expanduser('~/ski/elo/python/biathlon/polars/excel365/weekends.csv')
    
    # Path to R script
    r_script_path = os.path.expanduser('~/blog/daehl-e/content/post/biathlon/drafts/weekly-picks2.R')
    
    try:
        # Check if weekends.csv exists
        if not os.path.exists(weekends_csv_path):
            print(f"Error: weekends.csv not found at {weekends_csv_path}")
            return False
            
        # Check if R script exists
        if not os.path.exists(r_script_path):
            print(f"Error: weekly-picks2.R not found at {r_script_path}")
            return False
            
        # Read weekends.csv
        print(f"Reading {weekends_csv_path}...")
        weekends_df = pd.read_csv(weekends_csv_path)
        print(f"Successfully read weekends.csv with {len(weekends_df)} rows")
        
        # Check for races with today's date
        if 'Date' in weekends_df.columns:
            today_races = weekends_df[weekends_df['Date'] == today_utc]
            print(f"Found {len(today_races)} races scheduled for today ({today_utc})")
            
            if not today_races.empty:
                print("Races found for today! Running weekly-picks2.R...")
                
                # Run the R script
                try:
                    result = subprocess.run(
                        ["Rscript", r_script_path],
                        check=True,
                        capture_output=True,
                        text=True
                    )
                    print("R script executed successfully")
                    print("Output:")
                    print(result.stdout)
                    return True
                except subprocess.CalledProcessError as e:
                    print(f"Error running R script: {e}")
                    print("Error output:")
                    print(e.stderr)
                    return False
            else:
                print("No races found for today. Not running the R script.")
                return False
        else:
            print("Error: 'Date' column not found in weekends.csv")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        return False



def get_biathlon_startlist(url: str) -> List[Dict]:
    """Gets athlete data from Biathlon World website"""
    try:
        # Make request to the URL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract the JSON data that contains athlete information
        # This is typically found in a script tag containing the initial state
        athletes = []
        
        # Look for the script tag with JSON data
        scripts = soup.find_all('script')
        json_data = None
        
        for script in scripts:
            if script.string and '"Results":[' in script.string:
                # Extract JSON string from the script
                match = re.search(r'"Results":\[(.*?)\]', script.string, re.DOTALL)
                if match:
                    json_str = '{"Results":[' + match.group(1) + ']}'
                    try:
                        json_data = json.loads(json_str)
                        break
                    except json.JSONDecodeError:
                        # Try to fix truncated JSON by finding the last complete object
                        json_str = '{"Results":[' + match.group(1).rsplit('},', 1)[0] + '}]}'
                        json_data = json.loads(json_str)
                        break
        
        if not json_data:
            print("Could not find athlete data in the page")
            return []
        
        # Process athlete data
        for athlete in json_data.get('Results', []):
            if not athlete.get('IsTeam', False):  # Skip team entries in the results
                athlete_data = {
                    'Name': athlete.get('Name', ''),
                    'FamilyName': athlete.get('FamilyName', ''),
                    'GivenName': athlete.get('GivenName', ''),
                    'Nation': athlete.get('Nat', ''),
                    'IBUId': athlete.get('IBUId', ''),
                    'Bib': athlete.get('Bib', ''),
                    'StartOrder': athlete.get('StartOrder', '')
                }
                athletes.append(athlete_data)
                print(f"Added athlete: {athlete_data['Name']} ({athlete_data['Nation']})")
        
        print(f"Processed {len(athletes)} athletes from biathlon startlist")
        return athletes
    except Exception as e:
        print(f"Error fetching biathlon data: {e}")
        traceback.print_exc()
        return []

def get_biathlon_relay_teams(url: str) -> List[Dict]:
    """Gets relay team data from Biathlon World website"""
    try:
        # Make request to the URL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract the JSON data that contains team and athlete information
        # This is typically found in a script tag containing the initial state
        teams = {}
        
        # Look for the script tag with JSON data
        scripts = soup.find_all('script')
        json_data = None
        
        for script in scripts:
            if script.string and '"Results":[' in script.string:
                # Extract JSON string from the script
                match = re.search(r'"Results":\[(.*?)\]', script.string, re.DOTALL)
                if match:
                    json_str = '{"Results":[' + match.group(1) + ']}'
                    try:
                        json_data = json.loads(json_str)
                        break
                    except json.JSONDecodeError:
                        # Try to fix truncated JSON by finding the last complete object
                        json_str = '{"Results":[' + match.group(1).rsplit('},', 1)[0] + '}]}'
                        json_data = json.loads(json_str)
                        break
        
        if not json_data:
            print("Could not find team data in the page")
            return []
        
        # First pass to collect team entries
        team_entries = {}
        for entry in json_data.get('Results', []):
            if entry.get('IsTeam', False):  # This is a team entry
                team_nat = entry.get('Nat', '')
                team_entries[team_nat] = {
                    'team_name': entry.get('Name', ''),
                    'nation': team_nat,
                    'team_rank': entry.get('Rank', ''),
                    'team_time': entry.get('TotalTime', ''),
                    'members': []
                }
        
        # Second pass to collect individual athletes
        for entry in json_data.get('Results', []):
            if not entry.get('IsTeam', False) and entry.get('Leg') is not None:  # This is a leg athlete
                team_nat = entry.get('Nat', '')
                if team_nat in team_entries:
                    team_entries[team_nat]['members'].append({
                        'name': entry.get('Name', ''),
                        'family_name': entry.get('FamilyName', ''),
                        'given_name': entry.get('GivenName', ''),
                        'nation': team_nat,
                        'bib': entry.get('Bib', ''),
                        'leg': entry.get('Leg', ''),
                        'ibu_id': entry.get('IBUId', '')
                    })
        
        # Convert to list and sort by rank
        team_list = list(team_entries.values())
        team_list.sort(key=lambda x: int(x['team_rank']) if x['team_rank'].isdigit() else float('inf'))
        
        print(f"Processed {len(team_list)} teams with {sum(len(team['members']) for team in team_list)} athletes")
        return team_list
    
    except Exception as e:
        print(f"Error fetching biathlon relay data: {e}")
        traceback.print_exc()
        return []

def get_latest_elo_scores(file_path: str) -> pd.DataFrame:
    """Gets most recent ELO scores for each athlete with quartile imputation"""
    try:
        # Read file using polars with schema inference settings
        print(f"Reading ELO scores from: {file_path}")
        
        # First try with more robust pandas approach
        try:
            df = pd.read_csv(file_path, low_memory=False)
            print(f"Successfully read ELO file with pandas: {len(df)} rows")
        except Exception as pd_e:
            print(f"Error reading with pandas: {pd_e}")
            
            # Fallback to polars with explicit schema handling
            try:
                df = pl.scan_csv(file_path, 
                              infer_schema_length=10000,
                              ignore_errors=True,
                              null_values=["", "NA", "NULL"],
                             ).collect()
                
                # Convert to pandas
                df = df.to_pandas()
                print(f"Successfully read ELO file with polars+schema_override: {len(df)} rows")
            except Exception as pl_e:
                print(f"Error with polars fallback: {pl_e}")
                # Try with pandas again, but with explicit dtype specification
                df = pd.read_csv(file_path, dtype=str)
                # Convert numeric columns
                numeric_cols = ['Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo', 
                               'Pelo', 'Individual_Pelo', 'Sprint_Pelo', 'Pursuit_Pelo', 'MassStart_Pelo']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        
                print(f"Successfully read ELO file with pandas+dtype=str fallback: {len(df)} rows")
        
        # Ensure we have the required columns
        if 'Skier' not in df.columns:
            if 'Name' in df.columns:
                df['Skier'] = df['Name']
                print("Using 'Name' column as 'Skier'")
            else:
                # Check if there's a column name that might be the skier name
                potential_name_cols = [col for col in df.columns if 'name' in col.lower() or 'skier' in col.lower()]
                if potential_name_cols:
                    df['Skier'] = df[potential_name_cols[0]]
                    print(f"Using '{potential_name_cols[0]}' column as 'Skier'")
                else:
                    print("ERROR: No 'Skier' column found in ELO data!")
                    print("Available columns:", df.columns.tolist())
                    # Create a minimal dataframe with required columns
                    return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Individual_Elo', 
                                              'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'])
        
        # Sort by date if available
        if 'Date' in df.columns:
            df = df.sort_values('Date')
            # Remove future dates (any date after today)
            today = datetime.now().strftime('%Y-%m-%d')
            if 'Date' in df.columns:
                # Convert to datetime for comparison
                df['DateObj'] = pd.to_datetime(df['Date'], errors='coerce')
                today_dt = pd.to_datetime(today)
                # Keep only dates up to and including today
                df = df[df['DateObj'] <= today_dt]
                # Drop the temporary DateObj column
                df = df.drop('DateObj', axis=1)
        
        # Get most recent scores for each athlete
        if 'Skier' in df.columns:
            try:
                latest_scores = df.groupby('Skier').last().reset_index()
            except Exception as group_e:
                print(f"Error grouping by Skier: {group_e}")
                latest_scores = df  # Use full dataset if grouping fails
        else:
            latest_scores = df
        
        # Define ELO columns
        elo_columns = [col for col in ['Elo', 'Individual_Elo', 'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'] 
                      if col in latest_scores.columns]
        
        # Calculate first quartile for each ELO column
        q1_values = {}
        for col in elo_columns:
            if col in latest_scores.columns:
                try:
                    q1_values[col] = latest_scores[col].astype(float).quantile(0.25)
                    print(f"First quartile value for {col}: {q1_values[col]}")
                except Exception as q1_e:
                    print(f"Error calculating quartile for {col}: {q1_e}")
                    q1_values[col] = 1000  # Default value if calculation fails
        
        # Replace NAs with first quartile values
        for col in elo_columns:
            if col in latest_scores.columns:
                latest_scores[col] = latest_scores[col].fillna(q1_values.get(col, 1000))
        
        # Ensure we have Nation column
        if 'Nation' not in latest_scores.columns and 'Country' in latest_scores.columns:
            latest_scores['Nation'] = latest_scores['Country']
            print("Using 'Country' column as 'Nation'")
        elif 'Nation' not in latest_scores.columns:
            latest_scores['Nation'] = 'Unknown'
            print("No 'Nation' column found, using 'Unknown'")
        
        # Ensure we have ID column
        if 'ID' not in latest_scores.columns and 'IBUId' in latest_scores.columns:
            latest_scores['ID'] = latest_scores['IBUId']
            print("Using 'IBUId' column as 'ID'")
        elif 'ID' not in latest_scores.columns:
            latest_scores['ID'] = range(1, len(latest_scores) + 1)
            print("No 'ID' column found, using generated IDs")
        
        # Select all relevant columns
        all_columns = []
        for col in ['ID', 'Skier', 'Nation'] + elo_columns:
            if col in latest_scores.columns:
                all_columns.append(col)
            else:
                print(f"WARNING: '{col}' column not found in ELO data!")
        
        result_df = latest_scores[all_columns]
        print(f"Returning ELO data with {len(result_df)} rows and columns: {result_df.columns.tolist()}")
        return result_df
        
    except Exception as e:
        print(f"Error getting ELO scores: {e}")
        traceback.print_exc()
        # Return an empty dataframe with expected columns
        return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Individual_Elo', 
                                  'Sprint_Elo', 'Pursuit_Elo', 'MassStart_Elo'])

def normalize_name(name: str) -> str:
    """Normalizes name for better fuzzy matching"""
    normalized = name.lower()
    char_map = {
        'ø': 'oe', 'ö': 'oe', 'ó': 'o',
        'ä': 'ae', 'á': 'a', 'å': 'aa',
        'é': 'e', 'è': 'e',
        'ü': 'ue',
        'ý': 'y',
        'æ': 'ae'
    }
    for char, replacement in char_map.items():
        normalized = normalized.replace(char, replacement)
    return normalized

def fuzzy_match_name(name: str, name_list: List[str], threshold: int = 85) -> str:
    """Finds best matching name using normalized comparison"""
    from thefuzz import fuzz
    
    best_score = 0
    best_match = ''
    normalized_name = normalize_name(name)
    
    # Try matching full name
    for candidate in name_list:
        normalized_candidate = normalize_name(candidate)
        score = fuzz.ratio(normalized_name, normalized_candidate)
        
        # Also try matching tokens in any order
        token_score = fuzz.token_sort_ratio(normalized_name, normalized_candidate)
        score = max(score, token_score)
        
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate
    
    return best_match

def get_race_specific_elo(elo_data: Dict, race_type: str) -> float:
    """Get the most relevant ELO score based on race type"""
    # Define priority order for each race type
    priority_elo = {
        'Individual': ['Individual_Elo', 'Elo'],
        'Sprint': ['Sprint_Elo', 'Elo'],
        'Pursuit': ['Pursuit_Elo', 'Sprint_Elo', 'Elo'],
        'Mass Start': ['MassStart_Elo', 'Pursuit_Elo', 'Elo']
    }
    
    # Get the priority list for this race type (default to just 'Elo' if race type not found)
    priority_cols = priority_elo.get(race_type, ['Elo'])
    
    # Try each column in priority order
    for col in priority_cols:
        if col in elo_data and elo_data[col] is not None:
            try:
                return float(elo_data[col])
            except (TypeError, ValueError):
                continue
    
    return 0.0  # Default if no matching ELO found

def find_next_race_date(df: pd.DataFrame) -> str:
    """Find the next race date from today (inclusive) in the dataframe using UTC timezone"""
    
    # Get current date in UTC timezone
    today_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Convert dates to datetime objects for comparison
    df['DateObj'] = pd.to_datetime(df['Date'], errors='coerce')
    today_dt = pd.to_datetime(today_utc)
    
    # Sort the dataframe by date
    df = df.sort_values('DateObj')
    
    # Filter races starting from today (inclusive)
    current_and_future_races = df[df['DateObj'] >= today_dt]
    
    if current_and_future_races.empty:
        print("No current or future races found, using the latest race date")
        return df['Date'].iloc[-1]
    
    # Get the closest current or future date
    next_date = current_and_future_races.iloc[0]['Date']
    print(f"Next race date: {next_date}")
    
    return next_date

def filter_races_by_date(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """Filter races that occur on the specified date"""
    return df[df['Date'] == date]

def get_elo_priority(race_type: str) -> List[str]:
    """Determine which ELO columns to prioritize based on race type"""
    priority_elo = {
        'Individual': ['Individual_Elo', 'Elo'],
        'Sprint': ['Sprint_Elo', 'Elo'],
        'Pursuit': ['Pursuit_Elo', 'Sprint_Elo', 'Elo'],
        'Mass Start': ['MassStart_Elo', 'Pursuit_Elo', 'Elo']
    }
    
    return priority_elo.get(race_type, ['Elo'])