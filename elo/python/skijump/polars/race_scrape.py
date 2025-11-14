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
    
    # Custom city mappings
    city_mappings = {
        'Toblach': 'Toblach / Dobbiaco',
        'Milano Cortina': 'Predazzo',
        'Oslo': 'Holmenkollen',
        'Kulm': 'Bad Mitterndorf',
        'Ruka': 'Kuusamo',
        'Garmisch-Partenkirchen': 'Garmisch',
        'Zhangjiakou': 'Taizicheng'
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

def parse_hill_size_and_racetype(race_text):
    """Parse hill size and race type from race description"""
    if not race_text:
        return "", ""
    
    hill_size = ""
    race_type = ""
    
    # Extract hill size from HS pattern (e.g., "HS140", "HS106")
    hill_match = re.search(r'HS(\d+)', race_text)
    if hill_match:
        hill_size = hill_match.group(1)
    
    # Determine race type
    if "Team" in race_text:
        if "Large" in race_text or "Flying" in race_text:
            race_type = "Team Large"
        elif "Normal" in race_text:
            race_type = "Team Normal"
        else:
            race_type = "Team"
    elif "Flying" in race_text:
        race_type = "Flying"
    elif "Large" in race_text:
        race_type = "Large"
    elif "Normal" in race_text:
        race_type = "Normal"
    
    return hill_size, race_type

def assign_periods_by_race_count(races):
    """Assign periods based on equal race counts - both men and ladies follow Olympic structure"""
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

def scrape_skijump():
    """Scrape ski jumping race data"""
    
    # URLs for World Cup and Olympic races
    wc_url = "https://www.fis-ski.com/DB/ski-jumping/calendar-results.html?eventselection=&place=&sectorcode=JP&seasoncode=2026&categorycode=WC&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    olympic_url = "https://www.fis-ski.com/DB/ski-jumping/calendar-results.html?eventselection=&place=&sectorcode=JP&seasoncode=2026&categorycode=OWG&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    
    country_mapping = create_country_mapping()
    elevation_df = load_elevation_data()
    all_races = []
    
    for url, category in [(wc_url, "World Cup"), (olympic_url, "Olympics")]:
        print(f"Scraping {category} calendar...")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            calendar_div = soup.find('div', {'id': 'calendardata'})
            if calendar_div:
                event_rows = calendar_div.find_all('div', {'class': 'table-row'})
                print(f"Found {len(event_rows)} {category} events")
                
                for row in event_rows:
                    event_links = row.find_all('a', href=lambda x: x and 'event-details.html' in x)
                    for link in event_links:
                        event_url = link.get('href')
                        if event_url and not event_url.startswith('http'):
                            event_url = "https://www.fis-ski.com" + event_url
                        
                        if event_url:
                            print(f"Processing {category} event: {event_url}")
                            races = scrape_skijump_event(event_url, category, country_mapping, elevation_df)
                            all_races.extend(races)
                            time.sleep(1)
                            break
        except Exception as e:
            print(f"Error scraping {category}: {str(e)}")
    
    return all_races

def scrape_skijump_event(event_url, source_category, country_mapping, elevation_df):
    """Scrape ski jumping races from an event page"""
    races = []
    
    try:
        response = requests.get(event_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Get city and country from page header
        header = soup.find('h1', class_=lambda x: x and 'event-header__name' in x)
        city = ""
        country = ""
        
        if header:
            header_text = header.get_text().strip()
            if '(' in header_text and ')' in header_text:
                city = header_text.split('(')[0].strip()
                country_code = header_text.split('(')[1].split(')')[0].strip()
                country = country_mapping.get(country_code, "")
        
        # Find race content
        content_div = soup.find('div', {'id': 'eventdetailscontent'})
        if not content_div:
            print(f"No event details content found for {event_url}")
            return races
        
        race_rows = content_div.find_all('div', {'class': 'table-row'})
        
        for row in race_rows:
            # Extract date
            date_div = row.find('div', {'class': 'timezone-date'})
            if not date_div or not date_div.get('data-date'):
                continue
            
            try:
                date_obj = datetime.strptime(date_div.get('data-date'), '%Y-%m-%d')
                race_date = date_obj.strftime('%m/%d/%Y')
            except:
                continue
            
            # Check for qualification vs competition vs Olympics
            has_qualification = False
            has_competition = False
            has_olympics = False
            split_rows = row.find_all('div', {'class': 'split-row__item'})
            for split_row in split_rows:
                text_div = split_row.find('div', class_='g-xs-12 justify-left')
                if text_div:
                    text = text_div.get_text().strip()
                    if 'QUA' in text:
                        has_qualification = True
                    elif 'WC' in text:
                        has_competition = True
                    elif 'OWG' in text:
                        has_olympics = True
            
            # Extract startlist URL
            result_links = row.find_all('a', href=lambda x: x and 'results.html' in x and 'raceid=' in x)
            if not result_links:
                continue
            
            startlist_url = result_links[0].get('href')
            if not startlist_url.startswith('http'):
                startlist_url = "https://www.fis-ski.com" + startlist_url
            
            # Extract sex
            sex = ""
            gender_items = row.find_all('div', class_=lambda x: x and 'gender__item' in x and x != 'gender__item')
            
            for item in gender_items:
                classes = item.get('class', [])
                if 'gender__item_m' in classes:
                    sex = "M"
                    break
                elif 'gender__item_l' in classes:
                    sex = "L"
                    break
                elif 'gender__item_a' in classes:
                    sex = "Mixed"
                    break
            
            if not sex:
                continue
            
            # Extract race description
            race_text = ""
            clip_divs = row.find_all('div', {'class': 'clip'})
            for div in clip_divs:
                text = div.get_text().strip()
                if any(keyword in text for keyword in ['HS', 'Hill', 'Team', 'Large', 'Normal', 'Flying']):
                    race_text = text
                    break
            
            # Parse hill size and race type
            hill_size, race_type = parse_hill_size_and_racetype(race_text)
            
            # For individual events: keep QUA and OWG, skip WC. For team events: keep all
            if "Team" not in race_type:
                # Individual event - keep qualification rounds or Olympic rounds
                if has_competition and not has_qualification and not has_olympics:
                    print(f"Skipping individual competition round (no startlist): {race_date} - {race_text}")
                    continue
            # Team events don't have qualification, so we keep them all
            
            # Determine championship
            championship = "1" if source_category == "Olympics" else "0"
            
            # Get elevation
            elevation = fuzzy_match_elevation(city, elevation_df)
            
            # Create race entry (period will be assigned later)
            race = {
                'date': race_date,
                'startlist_url': startlist_url,
                'sex': sex,
                'city': city,
                'country': country,
                'hill_size': hill_size,
                'race_type': race_type,
                'period': "",
                'elevation': elevation,
                'championship': championship
            }
            
            races.append(race)
            print(f"Found race: {race_date} ({sex}) {city} - {race_text[:50]}...")
    
    except Exception as e:
        print(f"Error processing event {event_url}: {str(e)}")
    
    return races

def assign_race_numbers(races):
    """Assign race numbers within each sex, starting from 1"""
    # Add race numbering by gender
    race_counters = {'M': 0, 'L': 0, 'Mixed': 0}
    
    for race in races:
        race_counters[race['sex']] += 1
        race['race'] = race_counters[race['sex']]
    
    return races

def save_skijump_races(races, output_file):
    """Save ski jumping race data to CSV"""
    
    # Assign periods based on race count
    races_with_periods = assign_periods_by_race_count(races)
    
    # Sort by date
    race_objects = []
    for race in races_with_periods:
        try:
            date_obj = datetime.strptime(race['date'], '%m/%d/%Y')
            race_objects.append((date_obj, race))
        except ValueError:
            continue
    
    race_objects.sort(key=lambda x: x[0])
    sorted_races = [race for _, race in race_objects]
    
    # Assign race numbers
    sorted_races = assign_race_numbers(sorted_races)
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Header
        writer.writerow(['Date', 'Startlist', 'Sex', 'City', 'Country', 'HillSize', 'RaceType', 'Period', 'Elevation', 'Championship', 'Race'])
        
        # Data rows
        for race in sorted_races:
            row = [
                race['date'],
                race['startlist_url'],
                race['sex'],
                race['city'],
                race['country'],
                race['hill_size'],
                race['race_type'],
                race['period'],
                race['elevation'],
                race['championship'],
                race['race']
            ]
            writer.writerow(row)
    
    print(f"Saved {len(sorted_races)} ski jumping races to {output_file}")

if __name__ == "__main__":
    print("Starting ski jumping race scraping...")
    
    races = scrape_skijump()
    
    if races:
        output_file = "excel365/races2026.csv"
        save_skijump_races(races, output_file)
        print(f"Scraping complete! Found {len(races)} total ski jumping races.")
    else:
        print("No ski jumping races found.")