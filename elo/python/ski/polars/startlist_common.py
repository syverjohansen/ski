import requests
from bs4 import BeautifulSoup
import pandas as pd
import polars as pl
from thefuzz import fuzz
from typing import Dict, List, Tuple, Optional
import warnings
from datetime import datetime, timezone
import re
warnings.filterwarnings('ignore')

# Add manual name mappings
MANUAL_NAME_MAPPINGS = {
    'Thomas MALONEY WESTGAARD': 'Thomas Hjalmar Westgård',
    'John Steel HAGENBUCH': 'Johnny Hagenbuch',
    'Imanol ROJO GARCIA': 'Imanol Rojo',
    'JC SCHOONMAKER': 'James Clinton Schoonmaker',
    'HAGENBUCH John Steel': 'Johnny Hagenbuch',
    'MALONEY WESTGAARD Thomas': 'Thomas Hjalmar Westgård',
    'Lars Michael Saab BJERTNAES':'Lars Michael Bjertnæs',
    'BJERTNAES Lars Michael Saab':  'Lars Michael Bjertnæs',
    'Amund August KORSAETH': 'Amund Korsæth',
    'KORSAETH Amund August': 'Amund Korsæth',
    'Samantha SMITH': 'Sammy Smith',
    'SMITH Samantha': 'Sammy Smith'
}

def get_fantasy_price(name: str, fantasy_prices: Dict[str, int]) -> int:
    """Gets fantasy price by trying multiple name formats"""
    print(f"\nTrying to find price for: {name}")
    
    # First check if there's a reverse mapping
    reverse_map = {v: k for k, v in MANUAL_NAME_MAPPINGS.items()}
    
    # If this name has a FIS format, try that first
    if name in reverse_map:
        fis_name = reverse_map[name]
        if fis_name in fantasy_prices:
            print(f"Found FIS manual mapping match: {fis_name} -> {fantasy_prices[fis_name]}")
            return fantasy_prices[fis_name]
    
    # Try exact match
    if name in fantasy_prices:
        print(f"Found exact match: {name} -> {fantasy_prices[name]}")
        return fantasy_prices[name]
    
    # Try all possible FIS formats
    fis_formats = convert_to_last_first(name)
    print(f"Trying FIS formats: {fis_formats}")
    
    for fis_format in fis_formats:
        if fis_format in fantasy_prices:
            print(f"Found FIS format match: {fis_format} -> {fantasy_prices[fis_format]}")
            return fantasy_prices[fis_format]
    
    # Try fuzzy matching with original name
    best_match = fuzzy_match_name(name, list(fantasy_prices.keys()))
    if best_match and best_match in fantasy_prices:
        print(f"Found fuzzy match for original name: {name} -> {best_match} -> {fantasy_prices[best_match]}")
        return fantasy_prices[best_match]
    
    # Try fuzzy matching with all FIS formats
    for fis_format in fis_formats:
        best_match = fuzzy_match_name(fis_format, list(fantasy_prices.keys()))
        if best_match and best_match in fantasy_prices:
            print(f"Found fuzzy match for FIS format: {fis_format} -> {best_match} -> {fantasy_prices[best_match]}")
            return fantasy_prices[best_match]
    
    print(f"No price found for: {name}")
    print("Available names in fantasy prices (first 5):", list(fantasy_prices.keys())[:5])
    return 0

def get_fis_startlist(url: str) -> List[Tuple[str, str]]:
    """Gets skier names and nations from FIS website"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        athletes = []
        
        # Different approach to find athlete rows
        # Look for the table rows
        athlete_rows = soup.find_all('a', class_='table-row')
        
        print(f"Found {len(athlete_rows)} athlete rows in HTML")
        
        if not athlete_rows:
            # If no rows found with this selector, try an alternative approach
            print("No rows found with standard selector, trying alternative approach...")
            athlete_divs = soup.find_all('div', class_='athlete-name')
            for div in athlete_divs:
                try:
                    # Try to navigate up to find the containing row and country
                    row = div.find_parent('a', class_='table-row')
                    if row:
                        name = div.get_text(strip=True)
                        country_div = row.find('div', class_='country country_flag')
                        if country_div:
                            country_span = country_div.find('span', class_='country__name-short')
                            if country_span:
                                nation = country_span.get_text(strip=True)
                                athletes.append((name, nation))
                                print(f"Alternative method found: {name} ({nation})")
                except Exception as e:
                    print(f"Error in alternative row processing: {e}")
        else:
            # Process using the standard approach
            for row in athlete_rows:
                try:
                    # First try to get athlete-name directly
                    athlete_name_div = row.select_one('.athlete-name')
                    
                    if athlete_name_div:
                        # Found the athlete name div - clean up extra whitespace
                        name = ' '.join(athlete_name_div.get_text().split())
                    else:
                        # Try to find any div that might contain the name
                        # Look for common patterns in the HTML structure
                        name_divs = [
                            row.select_one('div.g-lg-18.g-md-18.g-sm-16.g-xs-16.justify-left.bold'),
                            row.select_one('div.g-lg-12.g-md-12.g-sm-11.g-xs-8.justify-left.bold')
                        ]
                        
                        name_div = next((div for div in name_divs if div is not None), None)
                        
                        if name_div:
                            name = name_div.get_text(strip=True)
                        else:
                            # Last resort - try to get any text that might be a name
                            # This is risky but better than nothing
                            bold_divs = row.select('div.bold')
                            if bold_divs:
                                name = bold_divs[0].get_text(strip=True)
                            else:
                                print("Could not find name in row")
                                continue
                    
                    # Look for country element
                    country_div = row.select_one('div.country.country_flag')
                    if country_div:
                        nation_span = country_div.select_one('span.country__name-short')
                        if nation_span:
                            nation = nation_span.get_text(strip=True)
                        else:
                            # If we can't find the nation span, try to get any text in the country div
                            nation = country_div.get_text(strip=True)
                            if not nation:
                                print("Could not find nation in country div")
                                continue
                    else:
                        print("Could not find country div in row")
                        continue
                    
                    # Clean up the name (remove extra whitespace, construction name, etc.)
                    name = name.strip()
                    # If name contains constructor information like "Name Constructor", just get the name part
                    if '\n' in name:
                        name = name.split('\n')[0].strip()
                    
                    # Add to our list
                    athletes.append((name, nation))
                    print(f"Added athlete: {name} ({nation})")
                    
                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue
        
        print(f"Processed {len(athletes)} athletes from FIS startlist")
        return athletes
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def get_fantasy_prices() -> Dict[str, int]:
    """Gets athlete prices from Fantasy XC API"""
    try:
        response = requests.get('https://www.fantasyxc.se/api/athletes')
        response.raise_for_status()
        
        athletes = response.json()
        return {athlete['name']: athlete['price'] for athlete in athletes}
        
    except Exception as e:
        print(f"Error getting Fantasy XC prices: {e}")
        return {}

def get_latest_elo_scores(file_path: str) -> pd.DataFrame:
    """Gets most recent ELO scores for each athlete with quartile imputation"""
    try:
        # Read file using polars with schema inference settings
        # Use low_memory=False and explicit null_values for better handling of mixed types
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
                              infer_schema_length=10000,  # Scan more rows for better schema inference
                              ignore_errors=True,        # Skip rows that would cause parsing errors
                              null_values=["", "NA", "NULL", "Sprint"],  # Additional null values to handle
                             ).collect()
                
                # Convert to pandas
                df = df.to_pandas()
                print(f"Successfully read ELO file with polars+schema_override: {len(df)} rows")
            except Exception as pl_e:
                print(f"Error with polars fallback: {pl_e}")
                # Try with pandas again, but with explicit dtype specification
                df = pd.read_csv(file_path, dtype=str)
                # Convert numeric columns
                numeric_cols = ['Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
                              'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 'Freestyle_Elo']
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
                    return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Distance_Elo', 
                                               'Distance_C_Elo', 'Distance_F_Elo', 'Sprint_Elo', 
                                               'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 
                                               'Freestyle_Elo'])
        
        # Sort by Date, Season, Race if available (handles multiple races per day)
        sort_cols = []
        if 'Date' in df.columns:
            sort_cols.append('Date')
        if 'Season' in df.columns:
            sort_cols.append('Season')
        if 'Race' in df.columns:
            sort_cols.append('Race')

        if sort_cols:
            df = df.sort_values(sort_cols)

        # Remove future dates (any date after today)
        if 'Date' in df.columns:
            today = datetime.now().strftime('%Y-%m-%d')
            # Convert to datetime for comparison
            df['DateObj'] = pd.to_datetime(df['Date'], errors='coerce')
            today_dt = pd.to_datetime(today)
            # Keep only dates up to and including today
            df = df[df['DateObj'] <= today_dt]
            # Drop the temporary DateObj column
            df = df.drop('DateObj', axis=1)        
        # Get most recent scores for each athlete (by ID to handle duplicate names)
        if 'ID' in df.columns:
            try:
                latest_scores = df.groupby('ID').last().reset_index()
            except Exception as group_e:
                print(f"Error grouping by ID: {group_e}")
                latest_scores = df  # Use full dataset if grouping fails
        elif 'Skier' in df.columns:
            try:
                latest_scores = df.groupby('Skier').last().reset_index()
            except Exception as group_e:
                print(f"Error grouping by Skier: {group_e}")
                latest_scores = df  # Use full dataset if grouping fails
        else:
            latest_scores = df
        
        # Define ELO columns
        elo_columns = [col for col in ['Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
                                     'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 
                                     'Freestyle_Elo'] if col in latest_scores.columns]
        
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
        if 'ID' not in latest_scores.columns and 'FIS_Code' in latest_scores.columns:
            latest_scores['ID'] = latest_scores['FIS_Code']
            print("Using 'FIS_Code' column as 'ID'")
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
        return pd.DataFrame(columns=['ID', 'Skier', 'Nation', 'Elo', 'Distance_Elo', 
                                   'Distance_C_Elo', 'Distance_F_Elo', 'Sprint_Elo', 
                                   'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 
                                   'Freestyle_Elo'])

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
    # First check manual mappings
    if name in MANUAL_NAME_MAPPINGS:
        mapped_name = MANUAL_NAME_MAPPINGS[name]
        if mapped_name in name_list:
            return mapped_name
    
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

def convert_to_first_last(name: str) -> str:
    """Converts name from 'LAST First' to 'First Last' format"""
    # First check manual mappings
    if name in MANUAL_NAME_MAPPINGS:
        return MANUAL_NAME_MAPPINGS[name]
        
    parts = name.strip().split()
    
    last_name_end = 0
    for i, part in enumerate(parts):
        if not part.isupper():
            last_name_end = i
            break
    
    if last_name_end == 0:
        last_name_end = len(parts) - 1
        
    first_name = ' '.join(parts[last_name_end:])
    last_name = ' '.join(parts[:last_name_end])
    
    return f"{first_name} {last_name}"

def convert_to_last_first(name: str) -> List[str]:
    """
    Converts name from 'First Last' to multiple possible 'LAST First' formats
    Returns a list of possible formats to try
    """
    parts = name.split()
    if len(parts) < 2:
        return [name]
    
    formats = []
    
    # Standard format: LAST First Middle
    last_name = ' '.join(parts[1:]).upper()
    first_name = parts[0]
    formats.append(f"{last_name} {first_name}")
    
    # For names with 3+ parts, try treating last word as surname
    if len(parts) >= 3:
        last_name = parts[-1].upper()
        first_name = ' '.join(parts[:-1])
        formats.append(f"{last_name} {first_name}")
        
        # Also try combining last two parts as surname
        if len(parts) >= 4:
            last_name = ' '.join(parts[-2:]).upper()
            first_name = ' '.join(parts[:-2])
            formats.append(f"{last_name} {first_name}")
    
    return formats

def get_current_season_from_chronos(chronos: pd.DataFrame) -> int:
    """Get the current (maximum) season from chronos data"""
    try:
        if 'Season' in chronos.columns:
            current_season = chronos['Season'].max()
            print(f"Current season determined from chronos data: {current_season}")
            return current_season
        else:
            print("No Season column found in chronos data, defaulting to current year (UTC)")
            return datetime.now(timezone.utc).year
    except Exception as e:
        print(f"Error determining current season: {e}")
        return datetime.now(timezone.utc).year

def get_additional_national_skiers(chronos: pd.DataFrame, elo_scores: pd.DataFrame, 
                                fantasy_prices: Dict[str, int], nation: str, current_season: int = None) -> List[dict]:
    """Get all skiers from a nation who competed in the current season"""
    if current_season is None:
        current_season = get_current_season_from_chronos(chronos)
    
    # Get all skiers from this nation in current season
    nation_skiers = chronos[
        (chronos['Nation'] == nation) & 
        (chronos['Season'] == current_season)
    ]['Skier'].unique()
    
    additional_data = []
    
    for skier in nation_skiers:
        # Match with ELO scores
        if skier in elo_scores['Skier'].values:
            elo_data = elo_scores[elo_scores['Skier'] == skier].iloc[0].to_dict()
            skier_id = elo_data['ID']
        else:
            elo_data = {}
            skier_id = None
            
        # Get fantasy price
        price = get_fantasy_price(skier, fantasy_prices)
        
        row_data = {
            'FIS_Name': skier,
            'Skier': skier,
            'ID': skier_id,
            'Nation': nation,
            'In_FIS_List': False,
            'Config_Nation': False,
            'In_Config': False,
            'Price': price,
            'Elo': elo_data.get('Elo', None),
            'Distance_Elo': elo_data.get('Distance_Elo', None),
            'Distance_C_Elo': elo_data.get('Distance_C_Elo', None),
            'Distance_F_Elo': elo_data.get('Distance_F_Elo', None),
            'Sprint_Elo': elo_data.get('Sprint_Elo', None),
            'Sprint_C_Elo': elo_data.get('Sprint_C_Elo', None),
            'Sprint_F_Elo': elo_data.get('Sprint_F_Elo', None),
            'Classic_Elo': elo_data.get('Classic_Elo', None),
            'Freestyle_Elo': elo_data.get('Freestyle_Elo', None)
        }
        
        additional_data.append(row_data)
        
    return additional_data

def parse_fis_race_id(url: str) -> Optional[int]:
    """Extract the race ID from a FIS URL"""
    if pd.isna(url) or not url:
        return None
    
    match = re.search(r'raceid=(\d+)', url)
    if match:
        return int(match.group(1))
    return None

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

def extract_race_ids(startlist_urls: List[str]) -> List[int]:
    """Extract race IDs from startlist URLs"""
    race_ids = []
    for url in startlist_urls:
        race_id = parse_fis_race_id(url)
        if race_id:
            race_ids.append(race_id)
    return race_ids

def sort_races_by_id(race_df: pd.DataFrame) -> pd.DataFrame:
    """Extract race IDs and sort the dataframe by them"""
    # Extract race IDs from Startlist column
    race_df['RaceID'] = race_df['Startlist'].apply(parse_fis_race_id)
    
    # Sort by RaceID
    sorted_df = race_df.sort_values('RaceID')
    
    return sorted_df