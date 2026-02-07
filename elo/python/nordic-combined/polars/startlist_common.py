import requests
from bs4 import BeautifulSoup
import pandas as pd
import polars as pl
from typing import Dict, List, Tuple, Optional
import warnings
from datetime import datetime, timezone
import re
import traceback
import os
import subprocess
warnings.filterwarnings('ignore')

def check_and_run_weekly_picks():
    """
    Check if there are races with today's date in weekends.csv and run the weekly-picks2.R script if true.
    
    Returns:
        bool: True if the R script was run successfully, False otherwise
    """
    # Get today's date in UTC
    today_utc = datetime.now(timezone.utc).strftime('%m/%d/%Y')
    print(f"Checking for races on today's date (UTC): {today_utc}")
    
    # Path to weekends.csv
    weekends_csv_path = os.path.expanduser('~/ski/elo/python/nordic-combined/polars/excel365/weekends.csv')
    
    # Path to R script
    r_script_path = os.path.expanduser('~/blog/daehl-e/content/post/nordic-combined/drafts/weekly-picks2.R')
    
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

def get_fis_race_data(race_id: str, sector_code: str = 'NK') -> Tuple[List[Dict], Dict]:
    """
    Extract race information from FIS website
    
    Args:
        race_id: FIS race ID
        sector_code: Sport code (NK = Nordic Combined)
        
    Returns:
        Tuple containing (list of athletes/teams, race metadata)
    """
    url = f"https://www.fis-ski.com/DB/general/results.html?sectorcode={sector_code}&raceid={race_id}"
    
    try:
        # Make request to the URL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check if this is a team event
        is_team_event = False
        is_team_sprint = False
        
        # Extract event type and metadata
        event_info = {}
        
        # Try to find the event title
        event_title_elem = soup.select_one('.event-header__name')
        if event_title_elem:
            event_title = event_title_elem.text.strip()
            event_info['Event'] = event_title
            
            # Check if this is a team event based on the title
            if 'Team' in event_title:
                if 'Sprint' in event_title:
                    is_team_sprint = True
                else:
                    is_team_event = True
        
        # Try to find city/country
        location_elem = soup.select_one('.event-header__locality')
        if location_elem:
            location = location_elem.text.strip()
            # Split into city and country if possible
            if ',' in location:
                city, country = location.split(',', 1)
                event_info['City'] = city.strip()
                event_info['Country'] = country.strip()
            else:
                event_info['City'] = location
                event_info['Country'] = ""
        
        # Try to find date
        date_elem = soup.select_one('.event-header__date')
        if date_elem:
            date_text = date_elem.text.strip()
            # Convert to standard date format
            try:
                # Format: "Saturday 21 Jan 2023"
                date_obj = datetime.strptime(date_text, '%A %d %b %Y')
                event_info['Date'] = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                event_info['Date'] = date_text
        
        # Try to find race type (Individual, Sprint, etc.)
        race_type_elem = soup.select_one('.event-header__subtitle')
        if race_type_elem:
            race_type = race_type_elem.text.strip()
            event_info['RaceType'] = race_type
            
            # Refine race type detection
            if 'Team' in race_type and 'Sprint' in race_type:
                is_team_sprint = True
                is_team_event = True
            elif 'Team' in race_type:
                is_team_event = True
        
        # Store event type flags
        event_info['IsTeamEvent'] = is_team_event
        event_info['IsTeamSprint'] = is_team_sprint
        
        athletes = []
        
        # Process based on event type
        if is_team_event:
            # Process team results
            teams = extract_team_results(soup)
            return teams, event_info
        else:
            # Process individual results
            athletes = extract_individual_results(soup)
            return athletes, event_info
            
    except Exception as e:
        print(f"Error fetching race data: {e}")
        traceback.print_exc()
        return [], {}

def extract_individual_results(soup: BeautifulSoup) -> List[Dict]:
    """Extract individual athlete results from FIS race page"""
    athletes = []
    
    try:
        # Find all athlete rows
        athlete_rows = soup.select('.table-row')
        
        for row in athlete_rows:
            try:
                # Extract athlete data
                
                # Get rank
                rank_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.pr-1.bold')
                if not rank_elem:
                    continue  # Skip if no rank (might be header or footer)
                
                rank = rank_elem.text.strip()
                
                # Get bib
                bib_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.justify-center.hidden-sm-down')
                bib = bib_elem.text.strip() if bib_elem else ""
                
                # Special handling for bib spans (colored bibs)
                bib_span = bib_elem.find("span", class_="bib") if bib_elem else None
                if bib_span:
                    bib = bib_span.text.strip()
                
                # Get FIS code
                fis_code_elem = row.select_one('.g-lg-1.g-md-2.g-sm-2.hidden-xs.justify-right.gray.pr-1, .g-lg-2.g-md-2.g-sm-3.hidden-xs.justify-right.gray.pr-1')
                fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
                
                # Get name
                name_elem = row.select_one('.g-lg.g-md.g-sm.g-xs.justify-left.bold, .g-lg-8.g-md-8.g-sm-5.g-xs-9.justify-left.bold')
                name = name_elem.text.strip() if name_elem else ""
                
                # Get birth year
                year_elem = row.select_one('.g-lg-2.g-md-2.g-sm-2.g-xs-3.hidden-md.justify-left')
                year = year_elem.text.strip() if year_elem else ""
                
                # Get nation
                nation_elem = row.select_one('.country__name-short')
                nation = nation_elem.text.strip() if nation_elem else ""
                
                # Get jump distance
                jump_elem = row.select_one('.g-lg.g-md.g-sm.justify-right.bold.hidden-xs')
                jump = jump_elem.text.strip() if jump_elem else ""
                
                # Get jump points
                points_elem = None
                if jump_elem:
                    # Find the next sibling with points
                    next_elem = jump_elem.find_next('.g-lg.g-md.g-sm.justify-right.bold.hidden-xs')
                    if next_elem:
                        points_elem = next_elem
                
                points = points_elem.text.strip() if points_elem else ""
                
                # Get jump rank
                rank_jump_elem = None
                if points_elem:
                    # Find the next sibling with jump rank
                    next_elem = points_elem.find_next('.g-lg.g-md.g-sm.justify-right.bold.hidden-xs.gray')
                    if next_elem:
                        rank_jump_elem = next_elem
                
                rank_jump = rank_jump_elem.text.strip() if rank_jump_elem else ""
                
                # Get time
                time_elem = row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs, .hidden-md-up.blue')
                time = time_elem.text.strip() if time_elem else ""
                
                # Get time diff
                time_diff_elem = row.select_one('.g-lg-2.g-md-2.g-sm-3.g-xs-6.justify-right.bold')
                time_diff = time_diff_elem.text.strip() if time_diff_elem else ""
                
                # Clean time_diff if it contains the time (for first place)
                if 'blue' in str(time_diff_elem):
                    time_diff = ""  # First place has no diff
                
                # Extract athlete ID from the link URL
                athlete_id = ""
                href = row.get('href')
                if href:
                    id_match = re.search(r'competitorid=(\d+)', href)
                    if id_match:
                        athlete_id = id_match.group(1)
                
                athlete_data = {
                    'Rank': rank,
                    'Bib': bib,
                    'FisCode': fis_code,
                    'Name': name,
                    'Year': year,
                    'Nation': nation,
                    'Jump': jump,
                    'Points': points,
                    'JumpRank': rank_jump,
                    'Time': time,
                    'TimeDiff': time_diff,
                    'ID': athlete_id
                }
                
                athletes.append(athlete_data)
                
            except Exception as row_e:
                print(f"Error processing athlete row: {row_e}")
                continue
        
        return athletes
        
    except Exception as e:
        print(f"Error extracting individual results: {e}")
        traceback.print_exc()
        return []

def extract_team_results(soup: BeautifulSoup) -> List[Dict]:
    """Extract team results from FIS race page"""
    teams = []
    
    try:
        # Find all team rows (main rows, not the team members)
        team_rows = soup.select('.table-row_theme_main')
        
        current_team = None
        
        for team_row in team_rows:
            try:
                # Extract team data
                
                # Get rank
                rank_elem = team_row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold.pr-1')
                if not rank_elem:
                    continue  # Skip if no rank (might be header or footer)
                
                rank = rank_elem.text.strip()
                
                # Get team name
                name_elem = team_row.select_one('.g-lg-13.g-md-13.g-sm-10.g-xs-10.justify-left.bold, .g-lg-8.g-md-8.g-sm-5.g-xs-9.justify-left.bold')
                team_name = name_elem.text.strip() if name_elem else ""
                
                # Get nation
                nation_elem = team_row.select_one('.country__name-short')
                nation = nation_elem.text.strip() if nation_elem else ""
                
                # Get points
                points_elem = team_row.select_one('.g-lg-2.g-md-2.g-sm-3.hidden-xs.justify-right')
                points = points_elem.text.strip() if points_elem else ""
                
                # Get time
                time_elem = team_row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs, .hidden-md-up.blue')
                time = time_elem.text.strip() if time_elem else ""
                
                # Get time diff
                time_diff_elem = team_row.select_one('.g-lg-2.g-md-2.g-sm-3.g-xs-6.justify-right.bold')
                time_diff = time_diff_elem.text.strip() if time_diff_elem else ""
                
                # Clean time_diff if it contains the time (for first place)
                if '<span class="hidden-md-up blue">' in str(time_diff_elem):
                    # Extract just the time
                    match = re.search(r'<span class="hidden-md-up blue">(.*?)</span>', str(time_diff_elem))
                    if match:
                        time = match.group(1)
                        time_diff = ""  # First place has no diff
                
                # Extract team ID from the link URL
                team_id = ""
                href = team_row.get('href')
                if href:
                    id_match = re.search(r'competitorid=(\d+)', href)
                    if id_match:
                        team_id = id_match.group(1)
                
                # Create team data dictionary
                team_data = {
                    'Rank': rank,
                    'TeamName': team_name,
                    'Nation': nation,
                    'Points': points,
                    'Time': time,
                    'TimeDiff': time_diff,
                    'ID': team_id,
                    'Members': []
                }
                
                # Find team members (look for rows following this team row)
                member_rows = []
                next_elem = team_row.next_sibling
                
                while next_elem and 'table-row_theme_additional' in getattr(next_elem, 'attrs', {}).get('class', []):
                    member_rows.append(next_elem)
                    next_elem = next_elem.next_sibling
                
                # Extract member data
                for member_row in member_rows:
                    try:
                        # Get FIS code
                        fis_code_elem = member_row.select_one('.g-lg-2.g-md-2.g-sm-3.hidden-xs.justify-right.gray.pr-1')
                        fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
                        
                        # Get name
                        name_elem = member_row.select_one('.g-lg-13.g-md-13.g-sm-10.g-xs-10.justify-left.bold, .g-lg-8.g-md-8.g-sm-5.g-xs-9.justify-left.bold')
                        name = name_elem.text.strip() if name_elem else ""
                        
                        # Get birth year
                        year_elem = member_row.select_one('.g-lg-1.g-md-1.g-sm-2.g-xs-3.justify-left')
                        year = year_elem.text.strip() if year_elem else ""
                        
                        # Get leg number (for team relay)
                        leg = ""
                        bib_elem = member_row.select_one('.g-lg-1.g-md-1.g-sm-1.justify-center.hidden-sm-down')
                        if bib_elem:
                            bib_span = bib_elem.find("span", class_="bib")
                            if bib_span:
                                bib_text = bib_span.text.strip()
                                # Extract leg number from formats like "1-1", "1-2", etc.
                                leg_match = re.search(r'(\d+)-(\d+)', bib_text)
                                if leg_match:
                                    leg = leg_match.group(2)  # Get the second number (leg)
                        
                        # Get jump distance and points (if available)
                        jump = ""
                        points = ""
                        jump_elem = member_row.select_one('.g-lg-2.g-md-2.g-sm-2.justify-right.bold.hidden-xs')
                        if jump_elem:
                            jump = jump_elem.text.strip()
                            
                            # Find points element (next similar element)
                            points_elem = jump_elem.find_next('.g-lg-2.g-md-2.g-sm-2.justify-right.bold.hidden-xs')
                            if points_elem:
                                points = points_elem.text.strip()
                        
                        # Extract athlete ID from the link URL
                        athlete_id = ""
                        href = member_row.get('href')
                        if href:
                            id_match = re.search(r'competitorid=(\d+)', href)
                            if id_match:
                                athlete_id = id_match.group(1)
                        
                        member_data = {
                            'Name': name,
                            'Year': year,
                            'FisCode': fis_code,
                            'ID': athlete_id,
                            'Leg': leg,
                            'Jump': jump,
                            'Points': points
                        }
                        
                        team_data['Members'].append(member_data)
                        
                    except Exception as member_e:
                        print(f"Error processing team member: {member_e}")
                        continue
                
                teams.append(team_data)
                
            except Exception as team_e:
                print(f"Error processing team row: {team_e}")
                continue
        
        return teams
        
    except Exception as e:
        print(f"Error extracting team results: {e}")
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
                numeric_cols = ['Elo', 'Individual_Elo', 'Sprint_Elo', 'MassStart_Elo', 
                               'Pelo', 'Individual_Pelo', 'Sprint_Pelo', 'MassStart_Pelo',
                               'IndividualCompact_Elo', 'IndividualCompact_Pelo']
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
                                                'Sprint_Elo', 'MassStart_Elo', 'IndividualCompact_Elo'])
        
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
        elo_columns = [col for col in ['Elo', 'Individual_Elo', 'Sprint_Elo', 'MassStart_Elo', 'IndividualCompact_Elo'] 
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
        if 'ID' not in latest_scores.columns and 'FisCode' in latest_scores.columns:
            latest_scores['ID'] = latest_scores['FisCode']
            print("Using 'FisCode' column as 'ID'")
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
                                    'Sprint_Elo', 'MassStart_Elo', 'IndividualCompact_Elo'])

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

def fuzzy_match_name(name: str, name_list: List[str], threshold: int = 80) -> str:
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
        'Individual Compact': ['IndividualCompact_Elo', 'Individual_Elo', 'Elo'],
        'Sprint': ['Sprint_Elo', 'Elo'],
        'Mass Start': ['MassStart_Elo', 'Elo'],
        'Team': ['Elo'],  # Team events use overall ELO
        'Team Sprint': ['Sprint_Elo', 'Elo']  # Team sprint uses sprint ELO if available
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
        'Individual Compact': ['IndividualCompact_Elo', 'Individual_Elo', 'Elo'],
        'Sprint': ['Sprint_Elo', 'Elo'],
        'Mass Start': ['MassStart_Elo', 'Elo'],
        'Team': ['Elo'],  # Team events use overall ELO
        'Team Sprint': ['Sprint_Elo', 'Elo']  # Team sprint uses sprint ELO if available
    }
    
    return priority_elo.get(race_type, ['Elo'])

def extract_race_id_from_url(url: str) -> str:
    """Extract race ID from a FIS URL"""
    race_id_match = re.search(r'raceid=(\d+)', url)
    if race_id_match:
        return race_id_match.group(1)
    return ""