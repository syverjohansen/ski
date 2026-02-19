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

def determine_event_type(soup: BeautifulSoup) -> Tuple[bool, bool, bool]:
    """
    Determine the type of Nordic Combined event from the HTML content.
    
    Args:
        soup: BeautifulSoup object of the race page
        
    Returns:
        Tuple of (is_team_event, is_team_sprint, is_mixed_team)
    """
    is_team_event = False
    is_team_sprint = False
    is_mixed_team = False
    
    # Check event title
    event_title_elem = soup.select_one('.event-header__name')
    if event_title_elem:
        event_title = event_title_elem.text.strip().upper()
        
        if 'MIXED' in event_title and 'TEAM' in event_title:
            is_mixed_team = True
            is_team_event = True
            if 'SPRINT' in event_title:
                is_team_sprint = True
        elif 'TEAM' in event_title:
            is_team_event = True
            if 'SPRINT' in event_title:
                is_team_sprint = True
    
    # Also check race type/kind
    race_type_elem = soup.select_one('.event-header__kind')
    if race_type_elem:
        race_type = race_type_elem.text.strip().upper()
        
        if 'MIXED' in race_type and 'TEAM' in race_type:
            is_mixed_team = True
            is_team_event = True
            if 'SPRINT' in race_type:
                is_team_sprint = True
        elif 'TEAM' in race_type:
            is_team_event = True
            if 'SPRINT' in race_type:
                is_team_sprint = True
    
    return is_team_event, is_team_sprint, is_mixed_team

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
        
        # Determine event type
        is_team_event, is_team_sprint, is_mixed_team = determine_event_type(soup)
        
        # Extract event metadata
        event_info = {}
        
        # Try to find the event title
        event_title_elem = soup.select_one('.event-header__name')
        if event_title_elem:
            event_title = event_title_elem.text.strip()
            event_info['Event'] = event_title

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
        
        # Try to find race type from event header kind
        race_type_elem = soup.select_one('.event-header__kind')
        if race_type_elem:
            race_type = race_type_elem.text.strip()
            event_info['RaceType'] = race_type
        else:
            # Fallback: try the subtitle if kind is not found
            race_type_elem = soup.select_one('.event-header__subtitle')
            if race_type_elem:
                race_type = race_type_elem.text.strip()
                event_info['RaceType'] = race_type
        
        # Store event type flags
        event_info['IsTeamEvent'] = is_team_event
        event_info['IsTeamSprint'] = is_team_sprint
        event_info['IsMixedTeam'] = is_mixed_team
        
        athletes = []
        
        # Process based on event type
        if is_team_event:
            # Process team results (includes mixed teams)
            teams = extract_team_results(soup, is_mixed_team)
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

def determine_member_gender_mixed_team(member: Dict, position: int, bib: str = "") -> str:
    """
    Determine gender for a mixed team member based on position and bib information.
    
    Args:
        member: Member dictionary with athlete information
        position: Position in team (1-indexed)
        bib: Bib number string (may contain patterns like "1-1", "1-2")
        
    Returns:
        'M' for male, 'F' for female
    """
    # Check if bib has a pattern like "1-1", "1-2", etc.
    bib_match = re.search(r'(\d+)-(\d+)', bib) if isinstance(bib, str) else None
    
    if bib_match:
        # For bib format like "1-1", "1-2", etc.
        # In mixed team, typically legs 1 and 3 are male, 2 and 4 are female
        leg_num = int(bib_match.group(2))  # Get the second number (leg)
        return 'M' if leg_num in [1, 3] else 'F'
    else:
        # Fallback to position-based assumption if bib format not recognized
        # Positions 1 and 3 are typically male
        return 'M' if position in [1, 3] else 'F'

def extract_team_results(soup: BeautifulSoup, is_mixed_team: bool = False) -> List[Dict]:
    """Extract team results from Nordic Combined FIS race page"""
    teams = []
    
    try:
        # Find all team rows (main rows, not the team members)
        team_rows = soup.select('.table-row_theme_main')
        
        for team_row in team_rows:
            try:
                # Extract team data
                
                # Get rank
                rank_elem = team_row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.bold.pr-1')
                if not rank_elem:
                    continue  # Skip if no rank (might be header or footer)
                
                rank = rank_elem.text.strip()
                
                # Get bib number
                bib_elem = team_row.select_one('.g-lg-1.g-md-1.g-sm-1.justify-center.hidden-sm-down.gray')
                bib = bib_elem.text.strip() if bib_elem else ""
                
                # Get FIS code (team ID)
                fis_code_elem = team_row.select_one('.g-lg-2.g-md-2.g-sm-3.hidden-xs.justify-right.gray.pr-1')
                fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
                
                # Get team name (country name) - check both grid patterns (g-lg-8 and g-lg-16)
                name_elem = team_row.select_one('.g-lg-16.g-md-16.g-sm-12.g-xs-14.justify-left.bold') or \
                            team_row.select_one('.g-lg-8.g-md-8.g-sm-5.g-xs-9.justify-left.bold')
                team_name = name_elem.text.strip() if name_elem else ""
                
                # Get nation code
                nation_elem = team_row.select_one('.country__name-short')
                nation = nation_elem.text.strip() if nation_elem else ""
                
                # Get team points (total points from jumping)
                points_elem = team_row.select_one('.g-lg-2.g-md-2.g-sm-3.hidden-xs.justify-right')
                points = points_elem.text.strip() if points_elem else ""
                
                # Get time (final time)
                time_elem = team_row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs')
                time = time_elem.text.strip() if time_elem else ""
                
                # Get time diff
                time_diff_elem = team_row.select_one('.g-lg-2.g-md-2.g-sm-3.g-xs-6.justify-right.bold')
                time_diff = ""
                if time_diff_elem:
                    time_diff_text = time_diff_elem.text.strip()
                    # Check if it contains the hidden time span (for first place)
                    hidden_time = time_diff_elem.select_one('.hidden-md-up.blue')
                    if hidden_time:
                        # This is first place, extract time from hidden span
                        time = hidden_time.text.strip()
                        time_diff = ""
                    else:
                        # This is not first place, the text is the time difference
                        time_diff = time_diff_text
                
                # Create team data dictionary
                team_data = {
                    'Rank': rank,
                    'Bib': bib,
                    'FisCode': fis_code,
                    'TeamName': team_name,
                    'Nation': nation,
                    'Points': points,
                    'Time': time,
                    'TimeDiff': time_diff,
                    'ID': fis_code,  # Use FIS code as ID
                    'Members': [],
                    'IsMixedTeam': is_mixed_team
                }
                
                # Find team members (look for rows following this team row)
                next_elem = team_row.next_sibling
                
                # Skip text nodes and find next actual element
                while next_elem and not hasattr(next_elem, 'attrs'):
                    next_elem = next_elem.next_sibling
                
                # Extract team members
                member_position = 1
                while next_elem and hasattr(next_elem, 'attrs'):
                    # Check if this is a team member row
                    if 'table-row_theme_additional' in next_elem.get('class', []):
                        try:
                            # Get FIS code
                            fis_code_elem = next_elem.select_one('.g-lg-2.g-md-2.g-sm-3.hidden-xs.justify-right.gray.pr-1')
                            member_fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
                            
                            # Get athlete name - check both grid patterns (g-lg-8 and g-lg-16)
                            name_elem = next_elem.select_one('.g-lg-16.g-md-16.g-sm-12.g-xs-14.justify-left.bold') or \
                                        next_elem.select_one('.g-lg-8.g-md-8.g-sm-5.g-xs-9.justify-left.bold')
                            name = name_elem.text.strip() if name_elem else ""
                            
                            # Get birth year
                            year_elem = next_elem.select_one('.g-lg-1.g-md-1.g-sm-2.g-xs-3.justify-left')
                            year = year_elem.text.strip() if year_elem else ""
                            
                            # Get bib/leg number
                            bib_elem = next_elem.select_one('.g-lg-1.g-md-1.g-sm-1.justify-center.hidden-sm-down.gray')
                            member_bib = ""
                            leg = ""
                            if bib_elem:
                                bib_span = bib_elem.find("span", class_="bib")
                                if bib_span:
                                    member_bib = bib_span.text.strip()
                                    # Extract leg number from formats like "2-1", "2-2", etc.
                                    leg_match = re.search(r'(\d+)-(\d+)', member_bib)
                                    if leg_match:
                                        leg = leg_match.group(2)  # Get the second number (leg)
                                    else:
                                        leg = str(member_position)
                                else:
                                    # If no bib span, use the element text directly
                                    member_bib = bib_elem.text.strip()
                                    leg = str(member_position)
                            else:
                                leg = str(member_position)
                            
                            # Get jump distance and points
                            jump_elems = next_elem.select('.g-lg-2.g-md-2.g-sm-2.justify-right.bold.hidden-xs')
                            jump = ""
                            jump_points = ""
                            
                            if len(jump_elems) >= 1:
                                jump = jump_elems[0].text.strip()
                            if len(jump_elems) >= 2:
                                jump_points = jump_elems[1].text.strip()
                            
                            # Extract athlete ID from the link URL
                            athlete_id = ""
                            href = next_elem.get('href')
                            if href:
                                id_match = re.search(r'competitorid=(\d+)', href)
                                if id_match:
                                    athlete_id = id_match.group(1)
                            
                            # Determine gender for mixed team events
                            gender = None
                            if is_mixed_team:
                                gender = determine_member_gender_mixed_team(
                                    {'Name': name}, member_position, member_bib
                                )
                            
                            # Create member data
                            member_data = {
                                'Name': name,
                                'Year': year,
                                'FisCode': member_fis_code,
                                'ID': athlete_id,
                                'Bib': member_bib,
                                'Leg': leg,
                                'Jump': jump,
                                'Points': jump_points,
                                'Position': member_position
                            }
                            
                            # Add gender for mixed teams
                            if gender:
                                member_data['Gender'] = gender
                            
                            team_data['Members'].append(member_data)
                            member_position += 1
                            
                        except Exception as member_e:
                            print(f"Error processing team member: {member_e}")
                        
                        # Move to next sibling
                        next_elem = next_elem.next_sibling
                        
                        # Skip text nodes
                        while next_elem and not hasattr(next_elem, 'attrs'):
                            next_elem = next_elem.next_sibling
                    else:
                        # Not a team member row, stop processing this team
                        break
                
                teams.append(team_data)
                
            except Exception as team_e:
                print(f"Error processing team row: {team_e}")
                continue
        
        print(f"Extracted {len(teams)} teams")
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

def fuzzy_match_name_with_gender_priority(name: str, men_elo_scores: pd.DataFrame, 
                                        women_elo_scores: pd.DataFrame, 
                                        expected_gender: str = None, 
                                        threshold: int = 80) -> Tuple[Optional[str], str, int]:
    """
    Enhanced fuzzy matching for mixed team events that considers gender.
    
    Args:
        name: Name to match
        men_elo_scores: DataFrame with men's ELO scores
        women_elo_scores: DataFrame with women's ELO scores  
        expected_gender: 'M' or 'F' if we have a guess about gender
        threshold: Minimum score for a match
        
    Returns:
        Tuple of (matched_name, final_gender, match_score)
    """
    from thefuzz import fuzz
    
    # Try exact matches first
    men_exact = name in men_elo_scores['Skier'].values
    women_exact = name in women_elo_scores['Skier'].values
    
    if men_exact and women_exact:
        # Found in both databases
        if expected_gender == 'M':
            return name, 'M', 100
        elif expected_gender == 'F':
            return name, 'F', 100
        else:
            # No gender preference, return first match (could be arbitrary)
            return name, 'M', 100
    elif men_exact:
        return name, 'M', 100
    elif women_exact:
        return name, 'F', 100
    
    # Try fuzzy matching
    men_match = fuzzy_match_name(name, men_elo_scores['Skier'].tolist(), threshold)
    women_match = fuzzy_match_name(name, women_elo_scores['Skier'].tolist(), threshold)
    
    # Calculate scores for comparison
    men_score = 0
    women_score = 0
    
    if men_match:
        men_score = fuzz.ratio(normalize_name(name), normalize_name(men_match))
    
    if women_match:
        women_score = fuzz.ratio(normalize_name(name), normalize_name(women_match))
    
    # Determine best match considering expected gender
    if men_match and women_match:
        # If both found, consider expected gender and scores
        if expected_gender == 'M' and abs(men_score - women_score) <= 10:
            # Prefer men's match if expected gender is male and scores are close
            return men_match, 'M', men_score
        elif expected_gender == 'F' and abs(men_score - women_score) <= 10:
            # Prefer women's match if expected gender is female and scores are close
            return women_match, 'F', women_score
        elif men_score > women_score:
            return men_match, 'M', men_score
        else:
            return women_match, 'F', women_score
    elif men_match:
        return men_match, 'M', men_score
    elif women_match:
        return women_match, 'F', women_score
    else:
        return None, expected_gender or 'M', 0

def get_race_specific_elo(elo_data: Dict, race_type: str) -> float:
    """Get the most relevant ELO score based on race type"""
    # Define priority order for each race type
    priority_elo = {
        'Individual': ['Individual_Elo', 'Elo'],
        'Individual Compact': ['IndividualCompact_Elo', 'Individual_Elo', 'Elo'],
        'Sprint': ['Sprint_Elo', 'Elo'],
        'Mass Start': ['MassStart_Elo', 'Elo'],
        'Team': ['Elo'],  # Team events use overall ELO
        'Team Sprint': ['Sprint_Elo', 'Elo'],  # Team sprint uses sprint ELO if available
        'Mixed Team': ['Elo'],  # Mixed team events use overall ELO
        'Mixed Team Sprint': ['Sprint_Elo', 'Elo']  # Mixed team sprint uses sprint ELO if available
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
        'Team Sprint': ['Sprint_Elo', 'Elo'],  # Team sprint uses sprint ELO if available
        'Mixed Team': ['Elo'],  # Mixed team events use overall ELO
        'Mixed Team Sprint': ['Sprint_Elo', 'Elo']  # Mixed team sprint uses sprint ELO if available
    }
    
    return priority_elo.get(race_type, ['Elo'])

def extract_race_id_from_url(url: str) -> str:
    """Extract race ID from a FIS URL"""
    race_id_match = re.search(r'raceid=(\d+)', url)
    if race_id_match:
        return race_id_match.group(1)
    return ""

def get_gender_specific_elo_scores(gender: str, base_path: str = None) -> pd.DataFrame:
    """
    Get ELO scores for a specific gender, with automatic path construction.
    
    Args:
        gender: 'men', 'ladies', 'M', or 'F'
        base_path: Optional base path override
        
    Returns:
        DataFrame with ELO scores
    """
    # Normalize gender input
    if gender.upper() in ['M', 'MEN']:
        gender_normalized = 'men'
    elif gender.upper() in ['F', 'L', 'LADIES', 'WOMEN']:
        gender_normalized = 'ladies'
    else:
        raise ValueError(f"Unknown gender: {gender}")
    
    # Construct path if not provided
    if base_path is None:
        base_path = f"~/ski/elo/python/nordic-combined/polars/excel365/{gender_normalized}_chrono_pred.csv"
    
    return get_latest_elo_scores(base_path)

def process_mixed_team_member_with_gender_detection(
    member: Dict, 
    position: int,
    men_elo_scores: pd.DataFrame,
    women_elo_scores: pd.DataFrame,
    elo_columns: List[str],
    men_quartiles: Dict[str, float],
    women_quartiles: Dict[str, float]
) -> Dict:
    """
    Process a mixed team member with enhanced gender detection and ELO matching.
    
    Args:
        member: Member dictionary from team extraction
        position: Position in team (1-indexed)
        men_elo_scores: Men's ELO scores DataFrame
        women_elo_scores: Women's ELO scores DataFrame
        elo_columns: List of ELO columns to process
        men_quartiles: Quartile values for men
        women_quartiles: Quartile values for women
        
    Returns:
        Dictionary with processed member information including gender and ELO scores
    """
    member_name = member.get('Name', '')
    member_id = member.get('ID', '')
    fis_code = member.get('FisCode', '')
    bib = member.get('Bib', '')
    jump_distance = member.get('Jump', '')
    jump_points = member.get('Points', '')
    year = member.get('Year', '')
    leg = member.get('Leg', str(position))
    
    # Determine expected gender based on position and bib
    expected_gender = determine_member_gender_mixed_team(member, position, bib)
    
    print(f"Processing member {position}: {member_name} (Expected gender: {expected_gender})")
    
    # Try to match with ELO scores using gender-aware fuzzy matching
    matched_name, final_gender, match_score = fuzzy_match_name_with_gender_priority(
        member_name, men_elo_scores, women_elo_scores, expected_gender
    )
    
    # Select appropriate ELO scores and quartiles based on final gender
    elo_scores_to_use = men_elo_scores if final_gender == 'M' else women_elo_scores
    quartiles_to_use = men_quartiles if final_gender == 'M' else women_quartiles
    
    # Build member info dictionary
    member_info = {
        f'Member_{position}': matched_name or member_name,
        f'Member_{position}_ID': member_id,
        f'Member_{position}_FisCode': fis_code,
        f'Member_{position}_Sex': final_gender,
        f'Member_{position}_Year': year,
        f'Member_{position}_Leg': leg,
        f'Member_{position}_Jump': jump_distance,
        f'Member_{position}_Points': jump_points,
        f'Member_{position}_MatchScore': match_score
    }
    
    # Add ELO scores
    if matched_name:
        elo_data = elo_scores_to_use[elo_scores_to_use['Skier'] == matched_name].iloc[0].to_dict()
        actual_id = elo_data.get('ID', member_id)
        member_info[f'Member_{position}_ID'] = actual_id
        
        # Process each ELO column
        for col in elo_columns:
            if col in elo_data and elo_data[col] is not None:
                try:
                    member_elo = float(elo_data[col])
                    member_info[f'Member_{position}_{col}'] = member_elo
                except (ValueError, TypeError):
                    member_info[f'Member_{position}_{col}'] = quartiles_to_use[col]
            else:
                member_info[f'Member_{position}_{col}'] = quartiles_to_use[col]
    else:
        # No match found, use quartile values
        print(f"No ELO match found for: {member_name}")
        for col in elo_columns:
            member_info[f'Member_{position}_{col}'] = quartiles_to_use[col]
    
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
    
    # Calculate averages
    if valid_member_count > 0:
        for col in elo_columns:
            team_info[f'Avg_{col}'] = team_info[f'Total_{col}'] / valid_member_count
    
    return team_info

# Add this function to startlist_common.py

def generate_fallback_data_team_sprint(gender: str, elo_scores: pd.DataFrame, race: pd.Series) -> List[Dict]:
    """
    Generate fallback team data when startlist is empty for team sprint events
    
    Args:
        gender: 'men' or 'ladies'
        elo_scores: DataFrame with ELO scores
        race: Race information
    
    Returns:
        list: Fallback team data
    """
    team_data = []
    
    # Define possible nations based on common participating countries
    common_nations = [
        "NORWAY", "GERMANY", "AUSTRIA", "JAPAN", "FINLAND",
        "FRANCE", "ITALY", "SLOVENIA", "SWITZERLAND", "CZECH REPUBLIC",
        "POLAND", "ESTONIA", "USA", "CANADA", "KOREA"
    ]
    
    # Define Elo columns to work with
    elo_columns = [
        'Elo', 'Individual_Elo', 'Sprint_Elo', 'MassStart_Elo', 'IndividualCompact_Elo'
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
            'Is_Present': False,  # This team is not in the actual startlist
            'Race1_Prob': 0.0     # 0% probability since not in startlist
        }
        
        # Add team-level fields that may be empty
        team_record['Team_Time'] = ''
        team_record['Team_TimeDiff'] = ''
        team_record['Team_Points'] = ''
        
        # Initialize all Elo sums and averages as empty strings
        for col in elo_columns:
            team_record[f'Total_{col}'] = ''
            team_record[f'Avg_{col}'] = ''
        
        # Initialize empty member fields for team sprint (typically 2 members)
        for i in range(1, 3):  # Team sprint has 2 members
            team_record[f'Member_{i}'] = ''
            team_record[f'Member_{i}_ID'] = ''
            team_record[f'Member_{i}_FisCode'] = ''
            team_record[f'Member_{i}_Year'] = ''
            team_record[f'Member_{i}_Leg'] = ''
            team_record[f'Member_{i}_Jump'] = ''
            team_record[f'Member_{i}_Points'] = ''
            
            # Set all member Elo values to empty strings
            for col in elo_columns:
                team_record[f'Member_{i}_{col}'] = ''
        
        # Add team record
        team_data.append(team_record)
    
    return team_data