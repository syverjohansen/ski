import json
import pandas as pd
import requests
import concurrent.futures
from pathlib import Path
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass
from datetime import datetime
from difflib import get_close_matches

def normalize_special_chars(text: str) -> str:
    """
    Normalize special characters in names
    """
    replacements = {
        'ø': 'oe',
        'ä': 'ae',
        'æ': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'å': 'aa',
        'á': 'a',
        'é': 'e',
        'í': 'i',
        'ó': 'o',
        'ú': 'u',
        'ý': 'y'
    }
    
    text = text.lower()
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    return text

@dataclass
class Athlete:
    athlete_id: int
    name: str
    gender: str
    country: str
    
    @property
    def is_country_team(self) -> bool:
        """Check if this is a country/relay team entry"""
        parts = self.name.split()
        # Check if last part is a number (I, II, etc) or if all parts are uppercase
        return (len(parts) >= 2 and parts[-1] in {'I', 'II', 'III', 'IV'}) or all(part.isupper() for part in parts)
    
    @property
    def standardized_name(self) -> str:
        """
        Converts name from "LASTNAME Firstname" to "Firstname Lastname" format
        Handles multi-part last names and normalizes special characters
        """
        if self.is_country_team:
            return self.name
            
        parts = self.name.split()
        
        # Find the split between uppercase (last name) and regular case (first name)
        lastname_parts = []
        firstname_parts = []
        
        for part in parts:
            if part.isupper():
                lastname_parts.append(part.title())
            else:
                firstname_parts.append(part)
        
        if not firstname_parts or not lastname_parts:  # If no clear split found
            return self.name
            
        lastname = ' '.join(lastname_parts)
        firstname = ' '.join(firstname_parts)
        
        normalized_name = f"{firstname} {lastname}"
        return normalize_special_chars(normalized_name)
    
    @property
    def name_key(self) -> str:
        """
        Creates a standardized key for name matching
        """
        firstname, lastname = standardize_name(self.name)
        return f"{lastname}_{firstname}"

def fetch_fantasy_data() -> List[Athlete]:
    """
    Fetches athlete data from the FantasyXC API.
    """
    url = "https://www.fantasyxc.se/api/athletes"
    response = requests.get(url)
    response.raise_for_status()
    
    athletes = []
    country_teams = 0
    for athlete_data in response.json():
        athlete = Athlete(
            athlete_id=athlete_data['athlete_id'],
            name=athlete_data['name'],
            gender=athlete_data['gender'],
            country=athlete_data.get('country', '')
        )
        # Filter out country/relay teams
        if athlete.is_country_team:
            country_teams += 1
        else:
            athletes.append(athlete)
    
    print(f"Filtered out {country_teams} country/relay teams")
    return athletes

def standardize_name(name: str) -> tuple:
    """
    Standardizes a name to firstname, lastname format.
    Handles multi-part last names and normalizes special characters.
    """
    parts = name.strip().split()
    
    # Split based on case (uppercase = lastname parts)
    lastname_parts = []
    firstname_parts = []
    
    for part in parts:
        if part.isupper():
            lastname_parts.append(part.title())
        else:
            firstname_parts.append(part)
    
    if not firstname_parts or not lastname_parts:  # If no clear split found
        firstname = ' '.join(parts[:-1])
        lastname = parts[-1]
    else:
        firstname = ' '.join(firstname_parts)
        lastname = ' '.join(lastname_parts)
    
    # Normalize special characters
    firstname = normalize_special_chars(firstname)
    lastname = normalize_special_chars(lastname)
    
    return firstname, lastname

def create_name_index(chrono_df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Create dictionaries mapping standardized names to IDs for faster lookups
    Returns both the index and a full name lookup for finding close matches
    """
    name_to_id = {}
    full_names = {}  # For fuzzy matching
    duplicates = set()
    
    for _, row in chrono_df.iterrows():
        # Normalize the name from chrono data
        normalized_name = normalize_special_chars(row['Skier'])
        firstname, lastname = standardize_name(normalized_name)
        
        key = f"{lastname}_{firstname}"
        full_name = f"{firstname} {lastname}"
        
        if key in name_to_id and name_to_id[key] != row['ID']:
            duplicates.add(key)
        name_to_id[key] = row['ID']
        full_names[full_name] = row['ID']
    
    # Remove any duplicates to avoid incorrect matches
    for key in duplicates:
        del name_to_id[key]
    
    print(f"Created name index with {len(name_to_id)} unique entries")
    if duplicates:
        print(f"Removed {len(duplicates)} duplicate entries to avoid incorrect matches")
    
    return name_to_id, full_names

def find_closest_match(name: str, full_names: Dict[str, str]) -> Optional[Tuple[str, float]]:
    """
    Finds the closest matching name from the chrono database
    Returns tuple of (closest_match, similarity_ratio) or None if no good matches
    """
    names_list = list(full_names.keys())
    normalized_name = normalize_special_chars(name.lower())
    matches = get_close_matches(normalized_name, [n.lower() for n in names_list], n=1, cutoff=0.6)
    
    if matches:
        # Find the original case version of the match
        original_case = next(n for n in names_list if n.lower() == matches[0])
        # Calculate similarity ratio
        similarity = sum(a == b for a, b in zip(normalized_name, matches[0])) / max(len(normalized_name), len(matches[0]))
        return (original_case, similarity)
    return None

def process_athlete(athlete: Athlete, name_index: Dict[str, str], full_names: Dict[str, str]) -> tuple:
    """
    Processes a single athlete for matching using the name index.
    """
    internal_id = name_index.get(athlete.name_key)
    closest_match = None
    if not internal_id:
        closest_match = find_closest_match(athlete.standardized_name, full_names)
    return athlete, internal_id, closest_match

def create_id_mappings(chrono_path: str, gender: str = 'm', min_season: int = 2017) -> Tuple[Dict[int, str], Dict[str, int]]:
    """
    Creates bidirectional mappings between FIS athlete IDs and internal IDs using multithreading.
    """
    chrono_path = Path(chrono_path).expanduser()
    
    print(f"\nStarting processing at {datetime.now().strftime('%H:%M:%S')}")
    print(f"Reading data from: {chrono_path}")
    
    # Fetch and filter athletes
    try:
        all_athletes = fetch_fantasy_data()
        athletes = [a for a in all_athletes if a.gender == gender]
        print(f"Found {len(athletes)} {gender} athletes in FantasyXC API (excluding country teams)")
        
        # Print sample of name conversions
        print("\nSample name conversions:")
        for athlete in athletes[:5]:
            print(f"Original: {athlete.name:<30} -> Standardized: {athlete.standardized_name}")
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from FantasyXC API: {e}")
        raise
    
    # Read and filter the chrono data
    try:
        chrono_df = pd.read_feather(chrono_path)
        original_size = len(chrono_df)
        
        # Filter for recent seasons
        chrono_df = chrono_df[chrono_df['Season'] >= min_season]
        print(f"\nFiltered chrono data from {original_size} to {len(chrono_df)} entries (Season >= {min_season})")
        
        # Create name indices
        name_index, full_names = create_name_index(chrono_df)
        
    except Exception as e:
        print(f"Error processing chrono data from {chrono_path}: {e}")
        raise
    
    # Initialize the mapping dictionaries
    fis_to_internal = {}
    internal_to_fis = {}
    unmatched_athletes = []
    
    # Process athletes in parallel
    print("\nMatching athletes...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        future_to_athlete = {
            executor.submit(process_athlete, athlete, name_index, full_names): athlete 
            for athlete in athletes
        }
        
        for future in concurrent.futures.as_completed(future_to_athlete):
            athlete = future_to_athlete[future]
            try:
                _, internal_id, closest_match = future.result()
                if internal_id:
                    fis_to_internal[athlete.athlete_id] = internal_id
                    internal_to_fis[internal_id] = athlete.athlete_id
                else:
                    unmatched_athletes.append((athlete, closest_match))
            except Exception as e:
                print(f"Error processing {athlete.name}: {e}")
    
    # Print summary statistics
    print(f"\nMatching Summary (completed at {datetime.now().strftime('%H:%M:%S')}):")
    print(f"Total athletes in FantasyXC ({gender}): {len(athletes)}")
    print(f"Successfully matched: {len(fis_to_internal)}")
    print(f"Unmatched: {len(unmatched_athletes)}")
    
    if unmatched_athletes:
        print("\nUnmatched athletes with closest matches:")
        print(f"{'Original Name':<30} {'Standardized Name':<30} {'Country':<6} {'FIS ID':<10} {'Closest Match':<30} {'Similarity':<8}")
        print("-" * 114)
        for athlete, closest in sorted(unmatched_athletes, key=lambda x: x[0].name):
            closest_str = f"{closest[0]} ({closest[1]:.2%})" if closest else "No close match found"
            print(f"{athlete.name:<30} {athlete.standardized_name:<30} {athlete.country:<6} "
                  f"{athlete.athlete_id:<10} {closest_str:<30}")
    
    return fis_to_internal, internal_to_fis

def save_mappings(mappings: Tuple[Dict[int, str], Dict[str, int]], output_path: str):
    """
    Saves the ID mappings to a JSON file.
    """
    fis_to_internal, internal_to_fis = mappings
    
    mappings_dict = {
        'fis_to_internal': {str(k): v for k, v in fis_to_internal.items()},
        'internal_to_fis': {k: str(v) for k, v in internal_to_fis.items()},
        'timestamp': datetime.now().isoformat()
    }
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(mappings_dict, f, indent=2)
    print(f"\nMappings saved to: {output_path}")

if __name__ == "__main__":
    men_chrono_path = "~/ski/elo/python/ski/polars/excel365/men_chrono.feather"
    women_chrono_path = "~/ski/elo/python/ski/polars/excel365/ladies_chrono.feather"
    
    try:
        # Process men's data
        print("\nProcessing men's data...")
        men_mappings = create_id_mappings(men_chrono_path, gender='m', min_season=2017)
        save_mappings(men_mappings, "men_id_mappings.json")
        
        # Process women's data
        print("\nProcessing women's data...")
        women_mappings = create_id_mappings(women_chrono_path, gender='f', min_season=2017)
        save_mappings(women_mappings, "women_id_mappings.json")
        
    except Exception as e:
        print(f"An error occurred: {e}")