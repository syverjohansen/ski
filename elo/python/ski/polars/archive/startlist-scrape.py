import requests
from bs4 import BeautifulSoup
import pandas as pd
import polars as pl
from thefuzz import fuzz
from typing import Dict, List, Tuple
import warnings
from config import get_additional_skiers, get_nation_quota  # Added get_nation_quota import
warnings.filterwarnings('ignore')


# Add manual name mappings
MANUAL_NAME_MAPPINGS = {
    'Thomas MALONEY WESTGAARD': 'Thomas Hjalmar Westgård',
    'John Steel HAGENBUCH': 'Johnny Hagenbuch',
    'Imanol ROJO GARCIA': 'Imanol Rojo',
    'JC SCHOONMAKER': 'James Clinton Schoonmaker',
    'HAGENBUCH John Steel': 'Johnny Hagenbuch',
    'MALONEY WESTGAARD Thomas': 'Thomas Hjalmar Westgård'
    #'Thomas Hjalmar Westgård': 'Thomas MALONEY WESTGAARD'

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
    if best_match:
        print(f"Found fuzzy match for original name: {name} -> {best_match} -> {fantasy_prices[best_match]}")
        return fantasy_prices[best_match]
    
    # Try fuzzy matching with all FIS formats
    for fis_format in fis_formats:
        best_match = fuzzy_match_name(fis_format, list(fantasy_prices.keys()))
        if best_match:
            print(f"Found fuzzy match for FIS format: {fis_format} -> {best_match} -> {fantasy_prices[best_match]}")
            return fantasy_prices[best_match]
    
    print(f"No price found for: {name}")
    print("Available names in fantasy prices (first 5):", list(fantasy_prices.keys())[:5])
    return 0

# In create_complete_startlist, modify the additional skiers processing:
    # Process additional skiers
    for nation, skiers in ADDITIONAL_SKIERS.items():
        print(f"\nProcessing additional skiers for {nation}")
        for skier_entry in skiers:
            # Handle both dictionary and string entries
            if isinstance(skier_entry, dict):
                name = skier_entry['name']
            else:
                name = skier_entry
            
            print(f"\nProcessing additional skier: {name}")
            
            # Skip if this name has already been processed
            if name in processed_names:
                print(f"Skipping {name} - already processed")
                continue
            
            # Try multiple name formats for price matching
            price = get_fantasy_price(name, fantasy_prices)
            print(f"Final price for {name}: {price}")
            
            # Match with ELO scores
            elo_match = fuzzy_match_name(name, elo_scores['Skier'].tolist())
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                original_name = elo_match
                skier_id = elo_data['ID']
                nation = elo_data['Nation']  # Use nation from ELO data
            else:
                elo_data = {}
                original_name = name
                skier_id = None
                nation = nation  # Use nation from ADDITIONAL_SKIERS if no match
            
            row_data = {
                'FIS_Name': name,
                'Skier': original_name,
                'ID': skier_id,
                'Nation': nation,
                'In_FIS_List': False,
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
            
            data.append(row_data)
            processed_names.add(original_name)

def get_fis_startlist(url: str) -> List[Tuple[str, str]]:
    """Gets skier names and nations from FIS website"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        athletes = []
        
        athlete_rows = soup.find_all('a', class_='table-row')
        '''for row in athlete_rows:
            name_element = row.find('div', class_='g-lg-18 g-md-18 g-sm-16 g-xs-16 justify-left bold')
            nation_element = row.find('div', class_='country country_flag')
            
            if name_element and nation_element:
                name = name_element.text.strip()
                nation = nation_element.find('span', class_='country__name-short').text.strip()
                athletes.append((name, nation))'''

        for row in athlete_rows:
            name_element = row.find('div', class_='g-lg-12 g-md-12 g-sm-11 g-xs-8 justify-left bold')
            nation_element = row.find('div', class_='country country_flag')
            
            if name_element and nation_element:
                name = name_element.text.strip()
                nation = nation_element.find('span', class_='country__name-short').text.strip()
                athletes.append((name, nation))
        
        return athletes
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def create_startlist(fis_url: str, gender: str, host_nation: str) -> pd.DataFrame:
    """
    Creates DataFrame with startlist and configuration data
    
    Args:
        fis_url: URL for FIS startlist
        gender: 'men' or 'ladies'
        host_nation: Nation code of the host country
    """
    # Get FIS startlist
    fis_athletes = get_fis_startlist(fis_url)
    print("These are the fis_athletes:")
    print(fis_athletes)
    
    # Get additional skiers from config
    ADDITIONAL_SKIERS = get_additional_skiers(gender)
    
    # Create data for DataFrame
    data = []
    processed_names = set()
    
    # Process FIS athletes first
    for fis_name, nation in fis_athletes:
        # Convert to First Last for matching
        fis_first_last = convert_to_first_last(fis_name)
        
        row_data = {
            'Name': fis_first_last,
            'Nation': nation,
            'In_FIS_List': True,
            'In_Config': False,
            'Base_Quota': get_nation_quota(nation, gender),  # Base quota without host bonus
            'Quota': get_nation_quota(nation, gender, is_host=(nation == host_nation)),  # Total quota with host bonus
            'Is_Host_Nation': (nation == host_nation),
            'Race1_Prob': 1.0,  # In FIS list = definite for first race
            'Race2_Prob': None  # Will be calculated in R
        }
        
        # Check if in config
        for config_nation, skiers in ADDITIONAL_SKIERS.items():
            skier_found = False
            for skier in skiers:
                if fuzz.ratio(fis_first_last.lower(), skier.lower()) >= 85:
                    row_data['In_Config'] = True
                    skier_found = True
                    break
            if skier_found:
                break
        
        data.append(row_data)
        processed_names.add(fis_first_last)
    
    # Process additional skiers not in FIS list
    for nation, skiers in ADDITIONAL_SKIERS.items():
        for skier in skiers:
            if skier not in processed_names:
                row_data = {
                    'Name': skier,
                    'Nation': nation,
                    'In_FIS_List': False,
                    'In_Config': True,
                    'Base_Quota': get_nation_quota(nation, gender),  # Base quota without host bonus
                    'Quota': get_nation_quota(nation, gender, is_host=(nation == host_nation)),  # Total quota with host bonus
                    'Is_Host_Nation': (nation == host_nation),
                    'Race1_Prob': 0.0,  # Not in FIS list = definitely not in first race
                    'Race2_Prob': None  # Will be calculated in R
                }
                data.append(row_data)
                processed_names.add(skier)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    return df


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
        # Read feather file using polars
        df = pl.read_csv(file_path)
        
        # Convert to pandas and sort by date
        df = df.to_pandas()
        df = df.sort_values('Date')
        
        # Remove future dates
        df = df.loc[df['Date']!="2025-05-01 00:00:00"]
        
        # Get most recent scores for each athlete
        latest_scores = df.groupby('Skier').last().reset_index()
        
        # Define ELO columns
        elo_columns = ['Elo', 'Distance_Elo', 'Distance_C_Elo', 'Distance_F_Elo', 
                      'Sprint_Elo', 'Sprint_C_Elo', 'Sprint_F_Elo', 'Classic_Elo', 'Freestyle_Elo']
        
        # Calculate first quartile for each ELO column
        q1_values = {}
        for col in elo_columns:
            q1_values[col] = latest_scores[col].quantile(0.25)
            
        # Replace NAs with first quartile values
        for col in elo_columns:
            latest_scores[col] = latest_scores[col].fillna(q1_values[col])
            print(f"First quartile value for {col}: {q1_values[col]}")
        
        # Select all relevant columns including ID and Nation
        all_columns = ['ID', 'Skier', 'Nation'] + elo_columns
        
        return latest_scores[all_columns]
        
    except Exception as e:
        print(f"Error getting ELO scores: {e}")
        return pd.DataFrame()

def normalize_name(name: str) -> str:
    """Normalizes name for better fuzzy matching"""
    normalized = name.lower()
    char_map = {
        'ø': 'oe', 'ö': 'oe', 'ó': 'o',
        'ä': 'ae', 'á': 'a', 'å': 'aa',
        'é': 'e', 'è': 'e',
        'ü': 'ue',
        'ý': 'y',
        'æ': 'ae'  # Added æ mapping
    }
    for char, replacement in char_map.items():
        normalized = normalized.replace(char, replacement)
    return normalized

def fuzzy_match_name(name: str, name_list: List[str], threshold: int = 85) -> str:
    """Finds best matching name using normalized comparison"""
    # First check manual mappings
    if name in MANUAL_NAME_MAPPINGS:
        return MANUAL_NAME_MAPPINGS[name]
    
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

def get_additional_national_skiers(chronos: pd.DataFrame, elo_scores: pd.DataFrame, 
                                 fantasy_prices: Dict[str, int], nation: str) -> List[dict]:
    """Get all skiers from a nation who competed in 2025"""
    # Get all skiers from this nation in 2025
    nation_skiers = chronos[
        (chronos['Nation'] == nation) & 
        (chronos['Season'] == 2025)
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
def create_complete_startlist(fis_url: str, elo_path: str, gender: str) -> pd.DataFrame:
    """Creates DataFrame with startlist, prices and ELO scores"""
    # Get data from all sources
    fis_athletes = get_fis_startlist(fis_url)
    print("These are the fis_athletes:")
    print(fis_athletes)
    fantasy_prices = get_fantasy_prices()
    elo_scores = get_latest_elo_scores(elo_path)
    
    # Get list of nations in config at the start
    ADDITIONAL_SKIERS = get_additional_skiers(gender)
    config_nations = list(ADDITIONAL_SKIERS.keys())
    
    # Create data for DataFrame
    data = []
    processed_names = set()
    
    # Process FIS athletes first
    for fis_name, fis_nation_code in fis_athletes:
        print(f"\nProcessing FIS athlete: {fis_name}")
        
        # STEP 1: Name Processing
        # Check manual mappings first
        if fis_name in MANUAL_NAME_MAPPINGS:
            processed_name = MANUAL_NAME_MAPPINGS[fis_name]
            print(f"Found direct manual mapping: {fis_name} -> {processed_name}")
        else:
            # Convert to First Last format
            first_last = convert_to_first_last(fis_name)
            if first_last in MANUAL_NAME_MAPPINGS:
                processed_name = MANUAL_NAME_MAPPINGS[first_last]
                print(f"Found manual mapping after conversion: {first_last} -> {processed_name}")
            else:
                processed_name = first_last
                print(f"Using converted name: {processed_name}")
        
        if processed_name in processed_names:
            print(f"Skipping {processed_name} - already processed")
            continue
        
        # STEP 2: Get Fantasy Price
        # Try with original FIS name first
        price = get_fantasy_price(fis_name, fantasy_prices)
        if price == 0:
            # If no price found, try with processed name
            price = get_fantasy_price(processed_name, fantasy_prices)
        
        # STEP 3: Match with ELO scores
        # Try exact match first
        elo_match = None
        if processed_name in elo_scores['Skier'].values:
            elo_match = processed_name
            print(f"Found exact ELO match for: {processed_name}")
        else:
            # Try fuzzy matching if no exact match
            elo_match = fuzzy_match_name(processed_name, elo_scores['Skier'].tolist())
            if elo_match:
                print(f"Found fuzzy ELO match: {processed_name} -> {elo_match}")
        
        if elo_match:
            elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
            original_name = elo_match
            skier_id = elo_data['ID']
            nation = elo_data['Nation']
        else:
            print(f"No ELO match found for: {processed_name}")
            elo_data = {}
            original_name = processed_name
            skier_id = None
            nation = fis_nation_code
        
        row_data = {
            'FIS_Name': fis_name,
            'Skier': original_name,
            'ID': skier_id,
            'Nation': nation,
            'In_FIS_List': True,
            'Config_Nation': nation in config_nations,
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
        
        data.append(row_data)
        processed_names.add(original_name)
    
    # Process additional skiers from config
    for nation, skiers in ADDITIONAL_SKIERS.items():
        print(f"\nProcessing additional skiers for {nation}")
        for skier_entry in skiers:
            name = skier_entry if isinstance(skier_entry, str) else skier_entry['name']
            
            print(f"\nProcessing additional skier: {name}")
            
            if name in processed_names:
                print(f"Skipping {name} - already processed")
                continue
            
            # Check manual mappings for config skiers too
            if name in MANUAL_NAME_MAPPINGS:
                processed_name = MANUAL_NAME_MAPPINGS[name]
                print(f"Found manual mapping for config skier: {name} -> {processed_name}")
            else:
                processed_name = name
            
            price = get_fantasy_price(processed_name, fantasy_prices)
            print(f"Final price for {processed_name}: {price}")
            
            # Match with ELO scores using processed name
            elo_match = None
            if processed_name in elo_scores['Skier'].values:
                elo_match = processed_name
            else:
                elo_match = fuzzy_match_name(processed_name, elo_scores['Skier'].tolist())
            
            if elo_match:
                elo_data = elo_scores[elo_scores['Skier'] == elo_match].iloc[0].to_dict()
                original_name = elo_match
                skier_id = elo_data['ID']
                skier_nation = elo_data['Nation']
            else:
                elo_data = {}
                original_name = processed_name
                skier_id = None
                skier_nation = nation
            
            row_data = {
                'FIS_Name': name,
                'Skier': original_name,
                'ID': skier_id,
                'Nation': skier_nation,
                'In_FIS_List': False,
                'Price': price,
                'Elo': elo_data.get('Elo', None),
                'Distance_Elo': elo_data.get('Distance_Elo', None),
                'Distance_C_Elo': elo_data.get('Distance_C_Elo', None),
                'Distance_F_Elo': elo_data.get('Distance_F_Elo', None),
                'Sprint_Elo': elo_data.get('Sprint_Elo', None),
                'Sprint_C_Elo': elo_data.get('Sprint_C_Elo', None),
                'Sprint_F_Elo': elo_data.get('Sprint_F_Elo', None),
                'Classic_Elo': elo_data.get('Classic_Elo', None),
                'Freestyle_Elo': elo_data.get('Freestyle_Elo', None),
                'Config_Nation': skier_nation in config_nations,
                'In_Config': True
            }
            
            data.append(row_data)
            processed_names.add(original_name)
    # Get chronos data for finding additional national skiers
    chronos = pl.read_csv(elo_path).to_pandas()
    
    # Get all nations from 2025 season
    all_2025_nations = set(chronos[chronos['Season'] == 2025]['Nation'].unique())
    
    # Find nations that aren't in config
    non_config_nations = {nation for nation in all_2025_nations if nation not in config_nations}
    
    print(f"Found {len(non_config_nations)} non-config nations from 2025")
    
    # Process each non-config nation
    for nation in non_config_nations:
        print(f"Processing additional skiers for non-config nation: {nation}")
        
        # Get current skiers for this nation (if any)
        current_skiers = {row['Skier'] for row in data if row['Nation'] == nation}
        
        # Get additional skiers for this nation
        additional_skiers = get_additional_national_skiers(chronos, elo_scores, fantasy_prices, nation)
        
        # Add only new skiers
        for skier_data in additional_skiers:
            if skier_data['Skier'] not in current_skiers:
                print(f"Adding skier {skier_data['Skier']} from {nation}")
                data.append(skier_data)
    
    # Create DataFrame and sort by price
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=['Skier'], keep='first')
    df = df.sort_values(['Price', 'Elo'], ascending=[False, False])
    
    print("This is the end of the creating the startlist")
    print(df)
    
    return df

if __name__ == "__main__":
    # Example usage with Norway as host nation
    host_nation = "Switzerland"  # Make sure to use correct FIS country code
    
    # Men's race
    url_men = ""
    elo_path_men = "~/ski/elo/python/ski/polars/excel365/men_chrono_elevation.csv"
    
    df_men = create_complete_startlist(
        fis_url=url_men,
        elo_path=elo_path_men, 
        gender='men'
    )
    print("\nMen's Startlist:")
    pd.set_option('display.max_rows', None)
    print(df_men.to_string(index=False))
    
    # Add quota information after creation
    df_men['Base_Quota'] = df_men['Nation'].apply(lambda x: get_nation_quota(x, 'men'))
    df_men['Is_Host_Nation'] = df_men['Nation'] == host_nation
    df_men['Quota'] = df_men.apply(lambda row: get_nation_quota(row['Nation'], 'men', row['Is_Host_Nation']), axis=1)
    df_men['Race1_Prob'] = df_men['In_FIS_List'].apply(lambda x: 1.0 if x else 0.0)
    df_men['Race2_Prob'] = None
    
    # Save men's data
    df_men.to_csv('~/ski/elo/python/ski/polars/excel365/startlist_scraped_men.csv')
    
    # Ladies' race
    url_ladies = "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=46935"
    elo_path_ladies = "~/ski/elo/python/ski/polars/excel365/ladies_chrono_elevation.csv"
    
    df_ladies = create_complete_startlist(
        fis_url=url_ladies,
        elo_path=elo_path_ladies,
        gender='ladies'
    )
    
    # Add quota information after creation
    df_ladies['Base_Quota'] = df_ladies['Nation'].apply(lambda x: get_nation_quota(x, 'ladies'))
    df_ladies['Is_Host_Nation'] = df_ladies['Nation'] == host_nation
    df_ladies['Quota'] = df_ladies.apply(lambda row: get_nation_quota(row['Nation'], 'ladies', row['Is_Host_Nation']), axis=1)
    df_ladies['Race1_Prob'] = df_ladies['In_FIS_List'].apply(lambda x: 1.0 if x else 0.0)
    df_ladies['Race2_Prob'] = None
    
    print("\nLadies' Startlist:")
    print(df_ladies.to_string(index=False))
    
    # Save ladies' data
    df_ladies.to_csv('~/ski/elo/python/ski/polars/excel365/startlist_scraped_ladies.csv')