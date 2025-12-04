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
    Check if there are races with today's date in weekends.csv and run the weekly-picks-alpine.R script if true.
    
    Returns:
        bool: True if the R script was run successfully, False otherwise
    """
    # Get today's date in UTC
    today_utc = datetime.now(timezone.utc).strftime('%m/%d/%Y')
    print(f"Checking for races on today's date (UTC): {today_utc}")
    
    # Path to weekends.csv for alpine skiing
    weekends_csv_path = os.path.expanduser('~/ski/elo/python/alpine/polars/excel365/weekends.csv')
    
    # Path to R script for alpine skiing
    r_script_path = os.path.expanduser('~/blog/daehl-e/content/post/alpine/drafts/weekly-picks-alpine.R')
    
    try:
        # Check if weekends.csv exists
        if not os.path.exists(weekends_csv_path):
            print(f"Error: weekends.csv not found at {weekends_csv_path}")
            return False
            
        # Check if R script exists
        if not os.path.exists(r_script_path):
            print(f"Error: weekly-picks-alpine.R not found at {r_script_path}")
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
                print("Races found for today! Running weekly-picks-alpine.R...")
                
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

def get_fis_race_data(race_id: str, sector_code: str = 'AL') -> Tuple[List[Dict], Dict]:
    """
    Extract race information from FIS website for alpine skiing
    
    Args:
        race_id: FIS race ID
        sector_code: Sport code (AL = Alpine)
        
    Returns:
        Tuple containing (list of athletes, race metadata)
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
        
        # Try to find discipline and race type
        race_type_elem = soup.select_one('.event-header__subtitle')
        if race_type_elem:
            race_type = race_type_elem.text.strip()
            event_info['RaceType'] = race_type
            print(f"Race type: {race_type}")
            
            # Extract discipline from race type
            discipline = determine_discipline_from_race_type(race_type)
            event_info['Discipline'] = discipline
        
        # Alpine skiing only has individual events
        event_info['IsTeamEvent'] = False
        
        # Process individual results
        print("Processing as individual alpine event...")
        athletes = extract_individual_results(soup)
        return athletes, event_info
            
    except Exception as e:
        print(f"Error fetching race data: {e}")
        traceback.print_exc()
        return [], {}

def extract_individual_results(soup: BeautifulSoup) -> List[Dict]:
    """Extract individual athlete results from FIS alpine skiing race page"""
    athletes = []
    
    try:
        # First try the newer format with .table-row class
        athlete_rows = soup.select('.table-row')
        print(f"Found {len(athlete_rows)} .table-row elements in extract_individual_results")
        
        if not athlete_rows:
            # If no .table-row found, try events-info-results format
            print("No .table-row found, trying events-info-results format...")
            events_results = extract_events_info_results(soup)
            if events_results:
                return events_results
            
            # If that also fails, try alternative format with result cards
            print("No events-info-results found, trying alternative format...")
            return extract_alternative_format_results(soup)
        
        # Debug: if we find .table-row elements but names are empty, let's check the structure
        print("Processing .table-row elements from main extract function...")
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
                fis_code_elem = row.select_one('.pr-1.g-lg-2.g-md-2.g-sm-2.hidden-xs.justify-right.gray, .g-lg-2.g-md-2.g-sm-2.hidden-xs.justify-right.gray.pr-1')
                fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
                
                # Get name (different selectors for different race formats)
                name = ""
                
                # First try the new split-row format with athlete-name class
                athlete_name_elem = row.select_one('.athlete-name')
                if athlete_name_elem and athlete_name_elem.text.strip():
                    name = athlete_name_elem.text.strip()
                    if len(athletes) < 3:  # Debug for first few rows
                        print(f"Main extract found name using .athlete-name selector: '{name}'")
                else:
                    # Fallback to original selectors
                    name_selectors = [
                        '.g-lg-18.g-md-18.g-sm-16.g-xs-16.justify-left.bold',
                        '.g-lg-12.g-md-12.g-sm-11.g-xs-8.justify-left.bold',
                        '.g-lg-8.g-md-8.g-sm-7.g-xs-8.justify-left.bold', 
                        '.g-lg-10.g-md-10.g-sm-9.g-xs-11.justify-left.bold',
                        # Add more selectors for events-info-results format
                        '.g-lg-4.g-md-4.g-sm-3.g-xs-8.justify-left.bold',
                        '.justify-left.bold'
                    ]
                    
                    for selector in name_selectors:
                        name_elem = row.select_one(selector)
                        if name_elem and name_elem.text.strip():
                            name = name_elem.text.strip()
                            if len(athletes) < 3:  # Debug for first few rows
                                print(f"Main extract found name using selector '{selector}': '{name}'")
                            break
                
                # If still no name found, debug what elements are available
                if not name and len(athletes) < 3:
                    print("Main extract: No name found, checking all bold elements in row:")
                    bold_elems = row.select('.bold')
                    for i, elem in enumerate(bold_elems):
                        print(f"  Bold element {i}: '{elem.text.strip()}' with classes: {elem.get('class', [])}")
                    print("Main extract: All div elements with any class:")
                    div_elems = row.select('div[class]')
                    for i, elem in enumerate(div_elems):
                        if elem.text.strip():  # Only show divs with text
                            print(f"  Div {i}: '{elem.text.strip()}' with classes: {elem.get('class', [])}")
                
                # Get birth year
                year_elem = row.select_one('.g-lg-1.g-md-1.hidden-sm-down.justify-left')
                year = year_elem.text.strip() if year_elem else ""
                
                # Get nation
                nation_elem = row.select_one('.country__name-short')
                nation = nation_elem.text.strip() if nation_elem else ""
                
                # Get run times and total time (format varies by discipline)
                run1 = ""
                run2 = ""
                total_time = ""
                
                # For technical events (GS, SL) - look for Run 1, Run 2, Tot. Time
                run1_elem = row.select_one('.g-lg-2.g-md-2.g-sm-2.justify-right.bold.hidden-xs')
                if run1_elem:
                    run1 = run1_elem.text.strip()
                
                # Look for second run time
                run_elems = row.select('.g-lg-2.g-md-2.g-sm-2.justify-right.bold.hidden-xs')
                if len(run_elems) >= 2:
                    run2 = run_elems[1].text.strip()
                
                # Get total time - could be in different positions
                time_elem = row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs, .g-lg-2.g-md-2.g-sm-3.g-xs-5.justify-right.bold')
                if time_elem:
                    time_text = time_elem.text.strip()
                    # If it starts with +, it's a time difference, look for actual time
                    if time_text.startswith('+'):
                        # Look for the actual time in a different element
                        actual_time_elem = row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs')
                        if actual_time_elem:
                            total_time = actual_time_elem.text.strip()
                        else:
                            total_time = time_text  # Use the difference if no absolute time found
                    else:
                        total_time = time_text
                
                # For speed events (DH, SG) - usually just one time
                if not total_time:
                    speed_time_elem = row.select_one('.g-lg-2.g-md-2.g-sm-3.g-xs-5.justify-right.bold')
                    if speed_time_elem:
                        speed_time_text = speed_time_elem.get_text(strip=True)
                        # Check if this contains the actual time (not a difference)
                        if not speed_time_text.startswith('+') or ':' in speed_time_text:
                            total_time = speed_time_text
                
                # Get FIS points
                points_elem = row.select_one('.g-lg-2.g-md-2.g-sm-2.g-xs-3.justify-right')
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
                    'Run1': run1,
                    'Run2': run2,
                    'Time': total_time,
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

def extract_alternative_format_results(soup: BeautifulSoup) -> List[Dict]:
    """Extract athlete results from alternative HTML format with result cards and table-row links"""
    athletes = []
    
    try:
        # Look for athlete rows that are anchor tags with class table-row
        athlete_rows = soup.select('a.table-row')
        
        print(f"Found {len(athlete_rows)} athlete rows in alternative format")
        
        for row in athlete_rows:
            try:
                # Get rank - first number in bold
                rank_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.pr-1.bold')
                rank = rank_elem.text.strip() if rank_elem else ""
                
                # Get bib number
                bib_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.justify-center.hidden-sm-down.gray')
                bib = bib_elem.text.strip() if bib_elem else ""
                
                # Get FIS code
                fis_code_elem = row.select_one('.pr-1.g-lg-2.g-md-2.g-sm-2.hidden-xs.justify-right.gray')
                fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
                
                # Get athlete name
                name_elem = row.select_one('.g-lg-10.g-md-10.g-sm-9.g-xs-11.justify-left.bold')
                name = name_elem.text.strip() if name_elem else ""
                
                # Get birth year
                year_elem = row.select_one('.g-lg-1.g-md-1.hidden-sm-down.justify-left')
                year = year_elem.text.strip() if year_elem else ""
                
                # Get nation from country code
                nation_elem = row.select_one('.country__name-short')
                nation = nation_elem.text.strip() if nation_elem else ""
                
                # Get run times - there should be 2 for technical events
                run_time_elems = row.select('.g-lg-2.g-md-2.g-sm-2.justify-right.bold.hidden-xs')
                run1 = run_time_elems[0].text.strip() if len(run_time_elems) > 0 else ""
                run2 = run_time_elems[1].text.strip() if len(run_time_elems) > 1 else ""
                
                # Get total time - could be in different places
                total_time = ""
                # Try blue bold time first (absolute time)
                time_elem = row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs')
                if time_elem:
                    total_time = time_elem.text.strip()
                else:
                    # Try other time element
                    time_elem = row.select_one('.g-lg-2.g-md-2.g-sm-3.g-xs-5.justify-right.bold')
                    if time_elem:
                        time_text = time_elem.text.strip()
                        # If it contains visible-sm, get the text inside
                        hidden_elem = time_elem.select_one('.hidden-md-up.visible-sm')
                        if hidden_elem:
                            total_time = hidden_elem.text.strip()
                        else:
                            total_time = time_text
                
                # Extract athlete ID from href
                athlete_id = ""
                href = row.get('href', '')
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
                    'Run1': run1,
                    'Run2': run2,
                    'Time': total_time,
                    'Points': "",  # Not available in this format
                    'ID': athlete_id
                }
                
                athletes.append(athlete_data)
                
            except Exception as row_e:
                print(f"Error processing alternative format athlete row: {row_e}")
                continue
        
        return athletes
        
    except Exception as e:
        print(f"Error extracting alternative format results: {e}")
        traceback.print_exc()
        return []

def extract_events_info_results(soup: BeautifulSoup) -> List[Dict]:
    """Extract athlete results from events-info-results format"""
    athletes = []
    
    try:
        # Look for the events-info-results div
        events_div = soup.find('div', {'id': 'events-info-results', 'class': 'table__body'})
        
        if not events_div:
            print("No events-info-results div found")
            return []
            
        print("Found results format with events-info-results")
        
        # Find all table rows
        table_rows = events_div.find_all('a', class_='table-row')
        print(f"Found {len(table_rows)} athlete rows in events-info-results format")
        
        for row in table_rows:
            try:
                # Debug: Print the HTML structure for the first few rows
                if len(athletes) < 3:
                    print(f"Debug row HTML: {row}")
                
                # Get rank - first bold number
                rank_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.g-xs-2.justify-right.pr-1.bold')
                rank = rank_elem.text.strip() if rank_elem else ""
                
                # Get bib number - second gray number
                bib_elem = row.select_one('.g-lg-1.g-md-1.g-sm-1.justify-center.hidden-sm-down.gray')
                bib = bib_elem.text.strip() if bib_elem else ""
                
                # Get FIS code - gray number in 2-column div
                fis_code_elem = row.select_one('.pr-1.g-lg-2.g-md-2.g-sm-2.hidden-xs.justify-right.gray')
                fis_code = fis_code_elem.text.strip() if fis_code_elem else ""
                
                # Get athlete name - try multiple selector patterns used in main extract function
                name = ""
                
                # First try the new split-row format with athlete-name class
                athlete_name_elem = row.select_one('.athlete-name')
                if athlete_name_elem and athlete_name_elem.text.strip():
                    name = athlete_name_elem.text.strip()
                    if len(athletes) < 3:  # Debug for first few rows
                        print(f"Events-info found name using .athlete-name selector: '{name}'")
                else:
                    # Fallback to original selectors
                    name_selectors = [
                        # From main extraction function - all the name selectors
                        '.g-lg-18.g-md-18.g-sm-16.g-xs-16.justify-left.bold',
                        '.g-lg-12.g-md-12.g-sm-11.g-xs-8.justify-left.bold', 
                        '.g-lg-8.g-md-8.g-sm-7.g-xs-8.justify-left.bold',
                        '.g-lg-10.g-md-10.g-sm-9.g-xs-11.justify-left.bold',
                        # Additional patterns for events-info-results format
                        '.g-lg-4.g-md-4.g-sm-3.g-xs-8.justify-left.bold',
                        '.g-lg-4.justify-left.bold',
                        '.justify-left.bold',
                        # Fallback patterns
                        'div.bold',
                        '.bold'
                    ]
                    
                    for selector in name_selectors:
                        name_elem = row.select_one(selector)
                        if name_elem and name_elem.text.strip():
                            name = name_elem.text.strip()
                            if len(athletes) < 3:  # Debug for first few rows
                                print(f"Found name using selector '{selector}': '{name}'")
                            break
                
                # If still no name found, debug what elements are available
                if not name and len(athletes) < 3:
                    print("No name found, checking all bold elements in row:")
                    bold_elems = row.select('.bold')
                    for i, elem in enumerate(bold_elems):
                        print(f"  Bold element {i}: '{elem.text.strip()}' with classes: {elem.get('class', [])}")
                    print("All div elements with any class:")
                    div_elems = row.select('div[class]')
                    for i, elem in enumerate(div_elems):
                        if elem.text.strip():  # Only show divs with text
                            print(f"  Div {i}: '{elem.text.strip()}' with classes: {elem.get('class', [])}")
                
                # Get birth year
                year_elem = row.select_one('.g-lg-1.g-md-1.hidden-sm-down.justify-left')
                year = year_elem.text.strip() if year_elem else ""
                
                # Get nation from country code
                nation_elem = row.select_one('.country__name-short')
                nation = nation_elem.text.strip() if nation_elem else ""
                
                # Get run times - bold hidden-xs elements
                run_time_elems = row.select('.g-lg-2.g-md-2.g-sm-2.justify-right.bold.hidden-xs')
                run1 = run_time_elems[0].text.strip() if len(run_time_elems) > 0 else ""
                run2 = run_time_elems[1].text.strip() if len(run_time_elems) > 1 else ""
                
                # Get total time - blue bold time
                total_time = ""
                time_elem = row.select_one('.g-lg-2.g-md-2.justify-right.blue.bold.hidden-sm.hidden-xs')
                if time_elem:
                    total_time = time_elem.text.strip()
                
                # Get FIS points - last numeric element
                points_elem = row.select_one('.g-lg-2.g-md-2.g-sm-2.g-xs-3.justify-right')
                points = points_elem.text.strip() if points_elem else ""
                
                # Extract athlete ID from href
                athlete_id = ""
                href = row.get('href', '')
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
                    'Run1': run1,
                    'Run2': run2,
                    'Time': total_time,
                    'Points': points,
                    'ID': athlete_id
                }
                
                athletes.append(athlete_data)
                print(f"Added skier: {rank} - {name} ({nation})")
                
            except Exception as e:
                print(f"Error parsing events-info-results row: {e}")
                continue
        
        return athletes
        
    except Exception as e:
        print(f"Error extracting events-info-results: {e}")
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
                # Convert numeric columns for alpine skiing
                numeric_cols = ['Elo', 'Downhill_Elo', 'Super G_Elo', 'Giant Slalom_Elo', 'Slalom_Elo', 'Combined_Elo', 'Tech_Elo', 'Speed_Elo',
                               'Pelo', 'Downhill_Pelo', 'Super G_Pelo', 'Giant Slalom_Pelo', 'Slalom_Pelo', 'Combined_Pelo', 'Tech_Pelo', 'Speed_Pelo']
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
                    return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Downhill_Elo', 'Super G_Elo', 
                                                'Giant Slalom_Elo', 'Slalom_Elo', 'Combined_Elo', 'Tech_Elo', 'Speed_Elo'])
        
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
        
        # Define ELO columns for alpine skiing
        elo_columns = [col for col in ['Elo', 'Downhill_Elo', 'Super G_Elo', 'Giant Slalom_Elo', 'Slalom_Elo', 'Combined_Elo', 'Tech_Elo', 'Speed_Elo'] 
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
        return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Downhill_Elo', 'Super G_Elo', 
                                    'Giant Slalom_Elo', 'Slalom_Elo', 'Combined_Elo', 'Tech_Elo', 'Speed_Elo'])

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

def get_race_specific_elo(elo_data: Dict, discipline: str) -> float:
    """Get the most relevant ELO score based on discipline"""
    # Define priority order for each discipline
    priority_elo = {
        'Downhill': ['Downhill_Elo', 'Speed_Elo', 'Elo'],
        'Super G': ['Super G_Elo', 'Speed_Elo', 'Elo'],  
        'Giant Slalom': ['Giant Slalom_Elo', 'Tech_Elo', 'Elo'],
        'Slalom': ['Slalom_Elo', 'Tech_Elo', 'Elo'],
        'Combined': ['Combined_Elo', 'Elo'],
        'Alpine Combined': ['Combined_Elo', 'Elo']
    }
    
    # Get the priority list for this discipline (default to just 'Elo' if discipline not found)
    priority_cols = priority_elo.get(discipline, ['Elo'])
    
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

def extract_race_id_from_url(url: str) -> str:
    """Extract race ID from a FIS URL"""
    race_id_match = re.search(r'raceid=(\d+)', url)
    if race_id_match:
        return race_id_match.group(1)
    return ""

def determine_discipline_from_race_type(race_type: str) -> str:
    """Determine discipline from race type string"""
    race_type_upper = race_type.upper()
    
    # Check for explicit discipline indicators
    if 'DOWNHILL' in race_type_upper or race_type_upper == 'DH':
        return 'Downhill'
    elif 'SUPER-G' in race_type_upper or 'SUPER G' in race_type_upper or race_type_upper == 'SG':
        return 'Super G'
    elif 'GIANT SLALOM' in race_type_upper or race_type_upper == 'GS':
        return 'Giant Slalom'
    elif ('SLALOM' in race_type_upper and 'GIANT' not in race_type_upper) or race_type_upper == 'SL':
        return 'Slalom'
    elif 'COMBINED' in race_type_upper or race_type_upper == 'AC':
        return 'Combined'
    
    # Default to Giant Slalom if no clear indication
    return 'Giant Slalom'

def classify_discipline_from_metadata(event_info: Dict) -> str:
    """
    Classify discipline from event metadata
    
    Args:
        event_info: Dictionary with event information
        
    Returns:
        Discipline classification string
    """
    # Check if Discipline is already in event_info
    if 'Discipline' in event_info and event_info['Discipline'] != 'Giant Slalom':
        return event_info['Discipline']
    
    # Check RaceType for discipline information
    if 'RaceType' in event_info:
        return determine_discipline_from_race_type(event_info['RaceType'])
    
    # Check Event title for discipline information
    if 'Event' in event_info:
        return determine_discipline_from_race_type(event_info['Event'])
    
    # Default to Giant Slalom
    return 'Giant Slalom'