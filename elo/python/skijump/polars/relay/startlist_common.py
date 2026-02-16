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
    
    # Path to weekends.csv for ski jumping
    weekends_csv_path = os.path.expanduser('~/ski/elo/python/skijump/polars/excel365/weekends.csv')
    
    # Path to R script for ski jumping
    r_script_path = os.path.expanduser('~/blog/daehl-e/content/post/skijump/drafts/weekly-picks2.R')
    
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

def get_fis_race_data(race_id: str, sector_code: str = 'JP') -> Tuple[List[Dict], Dict]:
    """
    Extract race information from FIS website for ski jumping
    
    Args:
        race_id: FIS race ID
        sector_code: Sport code (JP = Ski Jumping)
        
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
        
        # Extract event type and metadata
        event_info = {}
        
        # Try to find the event title
        event_title_elem = soup.select_one('.event-header__name')
        if event_title_elem:
            event_title = event_title_elem.text.strip()
            event_info['Event'] = event_title
            print(f"Event title: {event_title}")
        
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
        
        # Try to find hill size and race type
        race_type_elem = soup.select_one('.event-header__subtitle')
        if race_type_elem:
            race_type = race_type_elem.text.strip()
            event_info['RaceType'] = race_type
            print(f"Race type: {race_type}")
            
            # Extract hill size from race type if present
            hill_size_match = re.search(r'(K\d+|HS\d+)', race_type)
            if hill_size_match:
                event_info['HillSize'] = hill_size_match.group(1)
            else:
                event_info['HillSize'] = 'Unknown'
        
        # Improved team event detection
        is_team_event = False
        
        # Check event title for team indicators
        if event_title_elem:
            event_title = event_title_elem.text.strip().upper()
            if any(keyword in event_title for keyword in ['TEAM', 'MIXED']):
                is_team_event = True
                print(f"Detected team event from title: {event_title}")
        
        # Check race type for team indicators
        if race_type_elem and not is_team_event:
            race_type = race_type_elem.text.strip().upper()
            if any(keyword in race_type for keyword in ['TEAM', 'MIXED']):
                is_team_event = True
                print(f"Detected team event from race type: {race_type}")
        
        # Additional check: Look for team-specific HTML structure
        if not is_team_event:
            # Check if there are team rows in the results
            team_main_rows = soup.select('.table-row_theme_main')
            team_additional_rows = soup.select('.table-row_theme_additional')

            # If we have both main and additional rows, it's a team event
            # Team events have main rows (teams) and additional rows (members)
            if len(team_main_rows) > 0 and len(team_additional_rows) > 0:
                is_team_event = True
                print(f"Detected team event from HTML structure: {len(team_main_rows)} teams, {len(team_additional_rows)} member rows")
        
        # Store event type flags
        event_info['IsTeamEvent'] = is_team_event
        
        # Process based on event type
        if is_team_event:
            print("Processing as team event...")
            # Process team results
            teams = extract_team_results(soup)
            return teams, event_info
        else:
            print("Processing as individual event...")
            # Process individual results
            athletes = extract_individual_results(soup)
            return athletes, event_info
            
    except Exception as e:
        print(f"Error fetching race data: {e}")
        traceback.print_exc()
        return [], {}


def extract_individual_results(soup: BeautifulSoup) -> List[Dict]:
    """Extract individual athlete results from FIS ski jumping race page"""
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
                
                # Get jump distances (1st and 2nd round if applicable)
                jump_elems = row.select('.g-lg.g-md.g-sm.justify-right.bold.hidden-xs')
                length1 = ""
                length2 = ""
                if len(jump_elems) >= 1:
                    length1 = jump_elems[0].text.strip()
                if len(jump_elems) >= 2:
                    length2 = jump_elems[1].text.strip()
                
                # Get total points
                points_elem = row.select_one('.g-lg-2.g-md-2.g-sm-3.g-xs-6.justify-right.bold')
                points = points_elem.text.strip() if points_elem else ""
                
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
                    'Length1': length1,
                    'Length2': length2,
                    'Points': points,
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
    """Extract team results from FIS ski jumping race page with correct team/member parsing"""
    teams = []
    
    try:
        # Find all main team rows - these represent teams, not individual athletes
        team_main_rows = soup.select('.table-row_theme_main')
        
        for team_row in team_main_rows:
            try:
                # Extract team-level data
                
                # Get team rank
                rank_elem = team_row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold.pr-1')
                if not rank_elem:
                    continue  # Skip if no rank
                
                team_rank = rank_elem.text.strip()
                
                # Get team/country name - this should be the country name like "NORWAY"
                team_name_elem = team_row.select_one('.g-lg.g-md.g-sm.g-xs.justify-left.bold')
                team_name = team_name_elem.text.strip() if team_name_elem else ""
                
                # Get nation code (NOR, AUT, GER, etc.)
                nation_elem = team_row.select_one('.country__name-short')
                nation = nation_elem.text.strip() if nation_elem else team_name
                
                # Get team total points (the blue bold number at the end)
                team_points_elem = team_row.select_one('.g-lg-2.g-md-2.g-sm-3.g-xs-5.justify-right.blue.bold')
                team_points = team_points_elem.text.strip() if team_points_elem else ""
                
                # Extract team ID from href if available
                team_id = ""
                href = team_row.get('href')
                if href:
                    id_match = re.search(r'competitorid=(\d+)', href)
                    if id_match:
                        team_id = id_match.group(1)
                
                # Initialize team data
                team_data = {
                    'Rank': team_rank,
                    'TeamName': team_name,
                    'Nation': nation,
                    'Points': team_points,
                    'ID': team_id,
                    'Members': []
                }
                
                print(f"Found team: {team_name} (Rank {team_rank}, Nation: {nation})")
                
                # Now find all team member rows that follow this team row
                # Member rows have class 'table-row_theme_additional'
                current_element = team_row.next_sibling
                
                while current_element:
                    # Skip text nodes and other non-tag elements
                    if not hasattr(current_element, 'get'):
                        current_element = current_element.next_sibling
                        continue
                    
                    element_classes = current_element.get('class', [])
                    
                    # If we hit another main team row, we're done with this team's members
                    if 'table-row_theme_main' in element_classes:
                        break
                    
                    # If this is a member row, process it
                    if 'table-row_theme_additional' in element_classes:
                        member_data = extract_team_member_data(current_element)
                        if member_data:
                            team_data['Members'].append(member_data)
                            print(f"  Added member: {member_data['Name']}")
                    
                    current_element = current_element.next_sibling
                
                teams.append(team_data)
                print(f"Team {team_name} has {len(team_data['Members'])} members")
                
            except Exception as team_e:
                print(f"Error processing team row: {team_e}")
                continue
        
        print(f"Successfully extracted {len(teams)} teams")
        return teams
        
    except Exception as e:
        print(f"Error extracting team results: {e}")
        traceback.print_exc()
        return []

def extract_team_member_data(member_row) -> Dict:
    """Extract individual team member data from a member row"""
    try:
        # Get member bib number (like "11-1", "11-2")
        bib_elem = member_row.select_one('.g-lg-1.g-md-1.g-sm-1.justify-center.hidden-sm-down.gray')
        bib = bib_elem.text.strip() if bib_elem else ""
        
        # Get FIS code
        fis_code_elem = member_row.select_one('.g-lg-2.g-md-2.g-sm-3.hidden-xs.justify-right.gray.pr-1')
        fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
        
        # Get athlete name
        name_elem = member_row.select_one('.g-lg.g-md.g-sm.g-xs.justify-left.bold')
        name = name_elem.text.strip() if name_elem else ""
        
        # Get birth year
        year_elem = member_row.select_one('.g-lg-1.g-md-1.g-sm-2.g-xs-3.justify-left.hidden-xs')
        year = year_elem.text.strip() if year_elem else ""
        
        # Extract jump data from the complex split-row structure
        length1 = ""
        length2 = ""
        member_points = ""
        
        # Find the split-row container
        split_row_container = member_row.select_one('.split-row')
        if split_row_container:
            split_items = split_row_container.select('.split-row__item')
            
            for split_item in split_items:
                # Get the round number
                round_elem = split_item.select_one('.g-lg-4.g-md-4.g-sm-4.justify-right.gray')
                if not round_elem:
                    continue
                
                round_num = round_elem.text.strip()
                
                # Get jump distance and points for this round
                jump_data_elems = split_item.select('.g-lg.g-md.g-sm.justify-right')
                
                if len(jump_data_elems) >= 2:
                    distance = jump_data_elems[0].text.strip()
                    points = jump_data_elems[1].text.strip()
                    
                    if round_num == "1":
                        length1 = distance
                    elif round_num == "2":
                        length2 = distance
                        # Use round 2 points as the member's total points
                        member_points = points
        
        # Extract athlete ID from href
        athlete_id = ""
        href = member_row.get('href')
        if href:
            id_match = re.search(r'competitorid=(\d+)', href)
            if id_match:
                athlete_id = id_match.group(1)
        
        return {
            'Name': name,
            'Year': year,
            'FisCode': fis_code,
            'ID': athlete_id,
            'Bib': bib,
            'Length1': length1,
            'Length2': length2,
            'Points': member_points
        }
        
    except Exception as e:
        print(f"Error extracting team member data: {e}")
        return None        

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
                # Convert numeric columns for ski jumping
                numeric_cols = ['Elo', 'Small_Elo', 'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo',
                               'Pelo', 'Small_Pelo', 'Medium_Pelo', 'Normal_Pelo', 'Large_Pelo', 'Flying_Pelo']
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
                    return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Small_Elo', 
                                                'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'])
        
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
        
        # Define ELO columns for ski jumping
        elo_columns = [col for col in ['Elo', 'Small_Elo', 'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'] 
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
        return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Small_Elo', 
                                    'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'])

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

def get_race_specific_elo(elo_data: Dict, hill_size: str) -> float:
    """Get the most relevant ELO score based on hill size"""
    # Define priority order for each hill size
    priority_elo = {
        'Small': ['Small_Elo', 'Elo'],
        'Medium': ['Medium_Elo', 'Elo'],  
        'Normal': ['Normal_Elo', 'Elo'],
        'Large': ['Large_Elo', 'Elo'],
        'Flying': ['Flying_Elo', 'Large_Elo', 'Elo']
    }
    
    # Get the priority list for this hill size (default to just 'Elo' if hill size not found)
    priority_cols = priority_elo.get(hill_size, ['Elo'])
    
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

def get_elo_priority(hill_size: str) -> List[str]:
    """Determine which ELO columns to prioritize based on hill size"""
    priority_elo = {
        'Small': ['Small_Elo', 'Elo'],
        'Medium': ['Medium_Elo', 'Elo'],  
        'Normal': ['Normal_Elo', 'Elo'],
        'Large': ['Large_Elo', 'Elo'],
        'Flying': ['Flying_Elo', 'Large_Elo', 'Elo']
    }
    
    return priority_elo.get(hill_size, ['Elo'])

def extract_race_id_from_url(url: str) -> str:
    """Extract race ID from a FIS URL"""
    race_id_match = re.search(r'raceid=(\d+)', url)
    if race_id_match:
        return race_id_match.group(1)
    return ""

def determine_hill_size_from_race_type(race_type: str) -> str:
    """Determine hill size category from race type string"""
    race_type_upper = race_type.upper()
    
    # Check for explicit hill size indicators
    if 'FLYING' in race_type_upper or 'SKI FLYING' in race_type_upper:
        return 'Flying'
    elif 'LARGE' in race_type_upper or 'HS140' in race_type_upper or 'K120' in race_type_upper:
        return 'Large'
    elif 'NORMAL' in race_type_upper or 'HS106' in race_type_upper or 'K95' in race_type_upper:
        return 'Normal'
    elif 'MEDIUM' in race_type_upper or 'HS85' in race_type_upper or 'K85' in race_type_upper:
        return 'Medium'
    elif 'SMALL' in race_type_upper or 'HS65' in race_type_upper or 'K65' in race_type_upper:
        return 'Small'
    
    # Extract K-point or HS value for more precise classification
    k_match = re.search(r'K(\d+)', race_type_upper)
    hs_match = re.search(r'HS(\d+)', race_type_upper)
    
    hill_point = None
    if k_match:
        hill_point = int(k_match.group(1))
    elif hs_match:
        hill_point = int(hs_match.group(1))
    
    if hill_point:
        if hill_point >= 185:
            return 'Flying'
        elif hill_point >= 120:
            return 'Large'
        elif hill_point >= 95:
            return 'Normal'
        elif hill_point >= 85:
            return 'Medium'
        else:
            return 'Small'
    
    # Default to Normal if no clear indication
    return 'Normal'

def generate_fallback_data_team(gender: str, elo_scores: pd.DataFrame, race: pd.Series) -> List[Dict]:
    """
    Generate fallback team data when startlist is empty for team events
    
    Args:
        gender: 'men' or 'ladies'
        elo_scores: DataFrame with ELO scores
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
    
    # Define Elo columns to work with for ski jumping
    elo_columns = [
        'Elo', 'Small_Elo', 'Medium_Elo', 'Normal_Elo', 'Large_Elo', 'Flying_Elo'
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
            'Race_Type': race['RaceType'],
            'Hill_Size': race.get('HillSize', 'Normal'),
            'Is_Present': False,  # This team is not in the actual startlist
            'Race1_Prob': 0.0     # 0% probability since not in startlist
        }
        
        # Add team-level fields that may be empty
        team_record['Team_Points'] = ''
        
        # Initialize all Elo sums and averages as empty strings
        for col in elo_columns:
            team_record[f'Total_{col}'] = ''
            team_record[f'Avg_{col}'] = ''
        
        # Initialize empty member fields for team (typically 4 members in ski jumping)
        for i in range(1, 5):  # Team has 4 members
            team_record[f'Member_{i}'] = ''
            team_record[f'Member_{i}_ID'] = ''
            team_record[f'Member_{i}_FisCode'] = ''
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

def classify_hill_size_from_metadata(event_info: Dict) -> str:
    """
    Classify hill size from event metadata
    
    Args:
        event_info: Dictionary with event information
        
    Returns:
        Hill size classification string
    """
    # Check if HillSize is already in event_info
    if 'HillSize' in event_info and event_info['HillSize'] != 'Unknown':
        hill_size_str = event_info['HillSize']
        return determine_hill_size_from_race_type(hill_size_str)
    
    # Check RaceType for hill size information
    if 'RaceType' in event_info:
        return determine_hill_size_from_race_type(event_info['RaceType'])
    
    # Check Event title for hill size information
    if 'Event' in event_info:
        return determine_hill_size_from_race_type(event_info['Event'])
    
    # Default to Normal hill
    return 'Normal'

def process_team_member_with_elo(
    member: Dict, 
    position: int,
    elo_scores: pd.DataFrame,
    elo_columns: List[str],
    quartiles: Dict[str, float],
    hill_size: str
) -> Dict:
    """
    Process a team member with ELO matching and hill-specific ELO selection.
    
    Args:
        member: Member dictionary from team extraction
        position: Position in team (1-indexed)
        elo_scores: ELO scores DataFrame
        elo_columns: List of ELO columns to process
        quartiles: Quartile values for imputation
        hill_size: Hill size for race-specific ELO selection
        
    Returns:
        Dictionary with processed member information including ELO scores
    """
    member_name = member.get('Name', '')
    member_id = member.get('ID', '')
    fis_code = member.get('FisCode', '')
    length1 = member.get('Length1', '')
    length2 = member.get('Length2', '')
    member_points = member.get('Points', '')
    year = member.get('Year', '')
    
    print(f"Processing member {position}: {member_name}")
    
    # Try to match with ELO scores
    elo_match = None
    if member_name in elo_scores['Skier'].values:
        elo_match = member_name
        print(f"Found exact ELO match for: {member_name}")
    else:
        # Try fuzzy matching if no exact match
        elo_match = fuzzy_match_name(member_name, elo_scores['Skier'].tolist())
        if elo_match:
            print(f"Found fuzzy ELO match: {member_name} -> {elo_match}")
    
    # Build member info dictionary
    member_info = {
        f'Member_{position}': elo_match or member_name,
        f'Member_{position}_ID': member_id,
        f'Member_{position}_FisCode': fis_code,
        f'Member_{position}_Year': year,
        f'Member_{position}_Length1': length1,
        f'Member_{position}_Length2': length2,
        f'Member_{position}_Points': member_points
    }
    
    # Add ELO scores
    if elo_match:
        elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
        actual_id = elo_data.get('ID', member_id)
        member_info[f'Member_{position}_ID'] = actual_id
        
        # Process each ELO column
        for col in elo_columns:
            if col in elo_data and elo_data[col] is not None:
                try:
                    member_elo = float(elo_data[col])
                    member_info[f'Member_{position}_{col}'] = member_elo
                except (ValueError, TypeError):
                    member_info[f'Member_{position}_{col}'] = quartiles[col]
            else:
                member_info[f'Member_{position}_{col}'] = quartiles[col]
        
        # Add race-specific ELO based on hill size
        race_specific_elo = get_race_specific_elo(elo_data, hill_size)
        member_info[f'Member_{position}_Race_Elo'] = race_specific_elo
    else:
        # No match found, use quartile values
        print(f"No ELO match found for: {member_name}")
        for col in elo_columns:
            member_info[f'Member_{position}_{col}'] = quartiles[col]
        
        # Use general ELO quartile for race-specific ELO
        member_info[f'Member_{position}_Race_Elo'] = quartiles.get('Elo', 1000)
    
    return member_info

def calculate_team_elo_totals_and_averages(
    team_info: Dict,
    elo_columns: List[str],
    max_members: int = 4
) -> Dict:
    """
    Calculate team ELO totals and averages from member ELO scores.
    
    Args:
        team_info: Team dictionary with member ELO scores
        elo_columns: List of ELO column names
        max_members: Maximum number of team members
        
    Returns:
        Updated team_info dictionary with totals and averages
    """
    # Initialize totals
    for col in elo_columns:
        team_info[f'Total_{col}'] = 0
        team_info[f'Avg_{col}'] = 0
    
    # Also initialize race-specific ELO totals
    team_info['Total_Race_Elo'] = 0
    team_info['Avg_Race_Elo'] = 0
    
    # Sum up member ELO scores
    valid_member_count = 0
    for i in range(1, max_members + 1):
        member_key = f'Member_{i}'
        if member_key in team_info and team_info[member_key]:
            valid_member_count += 1
            for col in elo_columns:
                member_elo_key = f'Member_{i}_{col}'
                if member_elo_key in team_info:
                    try:
                        member_elo = float(team_info[member_elo_key])
                        team_info[f'Total_{col}'] += member_elo
                    except (ValueError, TypeError):
                        pass
            
            # Add race-specific ELO
            race_elo_key = f'Member_{i}_Race_Elo'
            if race_elo_key in team_info:
                try:
                    race_elo = float(team_info[race_elo_key])
                    team_info['Total_Race_Elo'] += race_elo
                except (ValueError, TypeError):
                    pass
    
    # Calculate averages
    if valid_member_count > 0:
        for col in elo_columns:
            team_info[f'Avg_{col}'] = team_info[f'Total_{col}'] / valid_member_count
        team_info['Avg_Race_Elo'] = team_info['Total_Race_Elo'] / valid_member_count
    
    return team_info