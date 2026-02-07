import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re
import time
import pandas as pd
from fuzzywuzzy import fuzz, process

def create_country_mapping():
    """Create mapping of country codes to full names"""
    return {
        'FIN': 'Finland',
        'NOR': 'Norway', 
        'SWE': 'Sweden',
        'GER': 'Germany',
        'SUI': 'Switzerland',
        'ITA': 'Italy',
        'FRA': 'France',
        'AUT': 'Austria',
        'CAN': 'Canada',
        'USA': 'USA',
        'RUS': 'Russia',
        'POL': 'Poland',
        'CZE': 'Czechia',
        'SVK': 'Slovakia',
        'SLO': 'Slovenia',
        'EST': 'Estonia',
        'CHN': 'China',
        'JPN': 'Japan',
        'KOR': 'South Korea',
        'BUL': 'Bulgaria',
        'ESP': 'Spain',
        'CRO': 'Croatia',
        'AND': 'Andorra'
    }

def load_elevation_data():
    """Load elevation data for fuzzy matching"""
    try:
        elevation_df = pd.read_csv('excel365/elevation.csv')
        return elevation_df
    except FileNotFoundError:
        print("elevation.csv not found")
        return pd.DataFrame()

def fuzzy_match_elevation(city, elevation_df):
    """Fuzzy match city to elevation data with custom mappings"""
    if elevation_df.empty:
        return ""
    
    # Custom city mappings for biathlon
    city_mappings = {
        'Östersund': 'Ostersund',
        'Annecy-Le Grand Bornand': 'Le Grand Bornand',
        'Antholz-Anterselva': 'Antholz',
        'Antholz': 'Antholz',
        'Mesto': 'Nove Mesto'
    }
    
    # Check for exact custom mapping first
    if city in city_mappings:
        mapped_city = city_mappings[city]
        if mapped_city in elevation_df['City'].values:
            elevation = elevation_df[elevation_df['City'] == mapped_city]['Elevation'].iloc[0]
            return str(elevation)
    
    # Fall back to fuzzy matching
    cities = elevation_df['City'].tolist()
    match = process.extractOne(city, cities, scorer=fuzz.ratio)
    
    if match and match[1] >= 70:  # 70% similarity threshold
        matched_city = match[0]
        elevation = elevation_df[elevation_df['City'] == matched_city]['Elevation'].iloc[0]
        return str(elevation)
    return ""

def map_race_type(race_type_text):
    """Map race type from HTML to standardized format"""
    if not race_type_text:
        return ""
    
    race_type_text = race_type_text.strip()
    
    # Direct mappings
    if race_type_text == "Sprint":
        return "Sprint"
    elif race_type_text == "Mass Start":
        return "Mass Start"
    elif race_type_text == "Individual":
        return "Individual"
    elif race_type_text == "Pursuit":
        return "Pursuit"
    elif race_type_text == "Relay":
        return "Relay"
    elif race_type_text == "Single Mixed":
        return "Single Mixed Relay"
    elif race_type_text == "Mixed":
        return "Mixed Relay"
    
    return race_type_text

def get_distance_by_race_type(race_type, sex):
    """Get distance based on race type and gender"""
    if race_type in ["Relay", "Single Mixed Relay", "Mixed Relay"]:
        return "N/A"
    elif race_type == "Sprint":
        return "10" if sex == "M" else "7.5"
    elif race_type == "Individual":
        return "20" if sex == "M" else "15"
    elif race_type == "Mass Start":
        return "15" if sex == "M" else "12.5"
    elif race_type == "Pursuit":
        return "12.5" if sex == "M" else "10"
    
    return ""

def assign_periods_by_race_count(races):
    """Assign periods based on equal race counts with Olympic structure"""
    # Find Olympic end date
    olympic_end_date = None
    for race in races:
        if race['championship'] == "1":
            try:
                race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                if olympic_end_date is None or race_date > olympic_end_date:
                    olympic_end_date = race_date
            except:
                continue
    
    # Separate races by gender
    mens_races = [r for r in races if r['sex'] == 'M']
    ladies_races = [r for r in races if r['sex'] == 'L']
    mixed_races = [r for r in races if r['sex'] == 'Mixed']
    
    # Process each gender group with same Olympic structure
    for gender_races in [mens_races, ladies_races]:
        olympic_races = []
        non_olympic_races = []
        post_olympic_races = []
        
        # Categorize races
        for race in gender_races:
            if race['championship'] == "1":
                olympic_races.append(race)
                race['period'] = "4"
            else:
                try:
                    race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                    if olympic_end_date and race_date > olympic_end_date:
                        post_olympic_races.append(race)
                        race['period'] = "5"
                    else:
                        non_olympic_races.append(race)
                except:
                    non_olympic_races.append(race)
        
        # Sort non-Olympic races by date and divide into 3 equal groups
        non_olympic_races.sort(key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y'))
        total_races = len(non_olympic_races)
        if total_races > 0:
            races_per_period = total_races // 3
            remainder = total_races % 3
            
            current_index = 0
            # Period 1
            period1_count = races_per_period + (1 if remainder > 0 else 0)
            for i in range(current_index, current_index + period1_count):
                non_olympic_races[i]['period'] = "1"
            current_index += period1_count
            
            # Period 2
            period2_count = races_per_period + (1 if remainder > 1 else 0)
            for i in range(current_index, current_index + period2_count):
                non_olympic_races[i]['period'] = "2"
            current_index += period2_count
            
            # Period 3
            for i in range(current_index, total_races):
                non_olympic_races[i]['period'] = "3"
    
    # Mixed races follow men's logic - match periods with men's races at same time
    for race in mixed_races:
        if race['championship'] == "1":
            race['period'] = "4"
        else:
            try:
                race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                if olympic_end_date and race_date > olympic_end_date:
                    race['period'] = "5"
                else:
                    # Find the closest men's race on the same date or nearby dates to match period
                    matching_period = "1"  # default
                    
                    # First try same date
                    for mens_race in mens_races:
                        if mens_race['date'] == race['date'] and mens_race['period']:
                            matching_period = mens_race['period']
                            break
                    
                    # If no same-date match, find closest men's race by date
                    if matching_period == "1":
                        closest_diff = float('inf')
                        for mens_race in mens_races:
                            if mens_race['period']:
                                try:
                                    mens_date = datetime.strptime(mens_race['date'], '%m/%d/%Y')
                                    diff = abs((race_date - mens_date).days)
                                    if diff < closest_diff:
                                        closest_diff = diff
                                        matching_period = mens_race['period']
                                except:
                                    continue
                    
                    race['period'] = matching_period
            except:
                race['period'] = "1"
    
    # Combine all races
    all_races = mens_races + ladies_races + mixed_races
    return all_races

def scrape_biathlon():
    """Scrape biathlon race data from firstskisport.com"""
    
    # URLs for men's and ladies' calendars
    mens_url = "https://firstskisport.com/biathlon/calendar.php?y=2026"
    ladies_url = "https://firstskisport.com/biathlon/calendar.php?y=2026&g=w"
    
    country_mapping = create_country_mapping()
    elevation_df = load_elevation_data()
    all_races = []
    
    for url, gender in [(mens_url, "M"), (ladies_url, "L")]:
        print(f"Scraping {gender} calendar...")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the table with race data
            tbody = soup.find('tbody')
            if not tbody:
                print(f"No tbody found for {gender}")
                continue
            
            rows = tbody.find_all('tr')
            print(f"Found {len(rows)} races for {gender}")
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue
                
                # Extract date from hidden cell
                date_cell = cells[0]
                if date_cell.get('style') != 'display:none;':
                    continue
                
                date_text = date_cell.get_text().strip()
                try:
                    date_obj = datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
                    race_date = date_obj.strftime('%m/%d/%Y')
                except:
                    continue
                
                # Extract championship status (OWG = Olympics)
                championship = "0"
                championship_cell = cells[2]
                if championship_cell and 'OWG' in championship_cell.get_text():
                    championship = "1"
                
                # Extract city and country
                location_cell = cells[3]
                if not location_cell:
                    continue
                
                # Get country from flag image
                flag_img = location_cell.find('img', class_='flag')
                country_code = ""
                country = ""
                if flag_img and flag_img.get('title'):
                    country_code = flag_img.get('title')
                    country = country_mapping.get(country_code, "")
                
                # Get city from text after flag
                city_text = location_cell.get_text().strip()
                city = city_text.split()[-1] if city_text else ""
                
                # Extract race type
                race_type_cell = cells[4]
                if not race_type_cell:
                    continue
                
                race_type_raw = race_type_cell.get_text().strip()
                race_type = map_race_type(race_type_raw)
                
                # Determine sex for mixed events
                sex = gender
                if race_type in ["Single Mixed Relay", "Mixed Relay"]:
                    sex = "Mixed"
                
                # Get distance based on race type and gender
                distance = get_distance_by_race_type(race_type, sex)
                
                # Get elevation
                elevation = fuzzy_match_elevation(city, elevation_df)
                
                # Create race entry (period will be assigned later)
                race = {
                    'date': race_date,
                    'startlist_url': "",  # Will be filled manually
                    'sex': sex,
                    'city': city,
                    'country': country,
                    'distance': distance,
                    'race_type': race_type,
                    'period': "",
                    'elevation': elevation,
                    'championship': championship
                }
                
                all_races.append(race)
                print(f"Found race: {race_date} ({sex}) {city} - {race_type}")
                
        except Exception as e:
            print(f"Error scraping {gender}: {str(e)}")
        
        time.sleep(1)  # Be polite to server
    
    return all_races

def assign_race_numbers(races):
    """Assign race numbers within each sex, starting from 1"""
    # Add race numbering by gender
    race_counters = {'M': 0, 'L': 0, 'Mixed': 0}
    
    for race in races:
        race_counters[race['sex']] += 1
        race['race'] = race_counters[race['sex']]
    
    return races

def save_biathlon_races(races, output_file):
    """Save biathlon race data to CSV"""
    
    # Remove duplicate Mixed and Single Mixed Relay races (appear on both men's and ladies' pages)
    unique_races = []
    seen_mixed_races = set()
    
    for race in races:
        if race['race_type'] in ['Mixed Relay', 'Single Mixed Relay']:
            # Create unique identifier for mixed races
            race_key = (race['date'], race['city'], race['race_type'])
            if race_key not in seen_mixed_races:
                seen_mixed_races.add(race_key)
                unique_races.append(race)
        else:
            unique_races.append(race)
    
    print(f"Removed {len(races) - len(unique_races)} duplicate mixed relay races")
    
    # Assign periods based on race count
    races_with_periods = assign_periods_by_race_count(unique_races)
    
    # Assign race numbers within each sex
    races_with_numbers = assign_race_numbers(races_with_periods)
    
    # Sort by date
    race_objects = []
    for race in races_with_numbers:
        try:
            date_obj = datetime.strptime(race['date'], '%m/%d/%Y')
            race_objects.append((date_obj, race))
        except ValueError:
            continue
    
    race_objects.sort(key=lambda x: x[0])
    sorted_races = [race for _, race in race_objects]
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Header
        writer.writerow(['Date', 'Startlist', 'Sex', 'City', 'Country', 'Distance', 'RaceType', 'Period', 'Elevation', 'Championship', 'Race'])
        
        # Data rows
        for race in sorted_races:
            row = [
                race['date'],
                race['startlist_url'],
                race['sex'],
                race['city'],
                race['country'],
                race['distance'],
                race['race_type'],
                race['period'],
                race['elevation'],
                race['championship'],
                race['race']
            ]
            writer.writerow(row)
    
    print(f"Saved {len(sorted_races)} biathlon races to {output_file}")

if __name__ == "__main__":
    print("Starting biathlon race scraping...")
    
    races = scrape_biathlon()
    
    if races:
        output_file = "excel365/races2026.csv"
        save_biathlon_races(races, output_file)
        print(f"Scraping complete! Found {len(races)} total biathlon races.")
    else:
        print("No biathlon races found.")