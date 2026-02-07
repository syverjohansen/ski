import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re
import time
import random
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
        'Milano Cortina': 'Val di Fiemme',
        'Oslo': 'Holmenkollen',
        'Kulm': 'Bad Mitterndorf',
        'Ruka': 'Kuusamo'
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

def parse_distance_and_racetype(race_text):
    """Parse distance and race type from race description"""
    if not race_text:
        return "", ""
    
    # Skip Provisional Rounds
    if "Provisional Round" in race_text:
        return "SKIP", ""
    
    distance = ""
    race_type = ""
    
    # Extract distance from after "/" 
    if "/" in race_text:
        after_slash = race_text.split("/")[-1]
        # Look for distance pattern like "10,0 Km" or "10 Km"
        distance_match = re.search(r'(\d+(?:,\d+)?)\s*[Kk]m', after_slash)
        if distance_match:
            distance = distance_match.group(1).replace(',', '.')
    
    # Determine race type
    if "Individual Compact" in race_text:
        race_type = "IndividualCompact"
    elif "Mass Start" in race_text:
        race_type = "Mass Start"
    elif "Individual" in race_text:
        race_type = "Individual"
    elif "Team Sprint" in race_text:
        race_type = "Team Sprint"
        distance = "N/A"
    elif "Team" in race_text:
        race_type = "Team"
        distance = "N/A"
    
    return distance, race_type

def assign_periods_by_race_count(races):
    """Assign periods based on equal race counts"""
    # Separate men's and ladies' races for different period logic
    mens_races = [r for r in races if r['sex'] == 'M']
    ladies_races = [r for r in races if r['sex'] == 'L']
    mixed_races = [r for r in races if r['sex'] == 'Mixed']
    
    # For men: periods 1,2,3 equal, 4 Olympics, 5 post-Olympics
    mens_olympic_races = []
    mens_non_olympic_races = []
    mens_post_olympic_races = []
    
    # Find Olympic end date for men
    olympic_end_date = None
    for race in mens_races:
        if race['championship'] == "1":
            try:
                race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                if olympic_end_date is None or race_date > olympic_end_date:
                    olympic_end_date = race_date
            except:
                continue
    
    # Categorize men's races
    for race in mens_races:
        if race['championship'] == "1":
            mens_olympic_races.append(race)
            race['period'] = "4"
        else:
            try:
                race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                if olympic_end_date and race_date > olympic_end_date:
                    mens_post_olympic_races.append(race)
                    race['period'] = "5"
                else:
                    mens_non_olympic_races.append(race)
            except:
                mens_non_olympic_races.append(race)
    
    # Sort men's non-Olympic races by date and divide into 3 equal groups
    mens_non_olympic_races.sort(key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y'))
    total_mens = len(mens_non_olympic_races)
    if total_mens > 0:
        races_per_period = total_mens // 3
        remainder = total_mens % 3
        
        current_index = 0
        # Period 1
        period1_count = races_per_period + (1 if remainder > 0 else 0)
        for i in range(current_index, current_index + period1_count):
            mens_non_olympic_races[i]['period'] = "1"
        current_index += period1_count
        
        # Period 2
        period2_count = races_per_period + (1 if remainder > 1 else 0)
        for i in range(current_index, current_index + period2_count):
            mens_non_olympic_races[i]['period'] = "2"
        current_index += period2_count
        
        # Period 3
        for i in range(current_index, total_mens):
            mens_non_olympic_races[i]['period'] = "3"
    
    # For ladies: all 5 periods equal (no Olympics)
    ladies_races.sort(key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y'))
    total_ladies = len(ladies_races)
    if total_ladies > 0:
        races_per_period = total_ladies // 5
        remainder = total_ladies % 5
        
        current_index = 0
        for period in range(1, 6):
            period_count = races_per_period + (1 if remainder >= period else 0)
            for i in range(current_index, current_index + period_count):
                if i < total_ladies:
                    ladies_races[i]['period'] = str(period)
            current_index += period_count
    
    # Mixed races follow men's logic
    for race in mixed_races:
        if race['championship'] == "1":
            race['period'] = "4"
        else:
            try:
                race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                if olympic_end_date and race_date > olympic_end_date:
                    race['period'] = "5"
                else:
                    # Assign to period 1, 2, or 3 based on date relative to men's races
                    # For simplicity, we'll assign based on the date position
                    if race_date.month <= 11:
                        race['period'] = "1"
                    elif race_date.month == 12 or (race_date.month == 1 and race_date.day <= 15):
                        race['period'] = "2"
                    else:
                        race['period'] = "3"
            except:
                race['period'] = "1"
    
    # Combine all races
    all_races = mens_non_olympic_races + mens_olympic_races + mens_post_olympic_races + ladies_races + mixed_races
    return all_races

def scrape_nordic_combined():
    """Scrape nordic combined race data"""
    
    # URLs for World Cup and Olympic races
    wc_url = "https://www.fis-ski.com/DB/alpine-skiing/calendar-results.html?eventselection=&place=&sectorcode=NK&seasoncode=2026&categorycode=WC&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    olympic_url = "https://www.fis-ski.com/DB/general/event-details.html?sectorcode=NK&eventid=58249&seasoncode=2026"
    
    country_mapping = create_country_mapping()
    elevation_df = load_elevation_data()
    all_races = []
    
    # Scrape World Cup races
    print("Scraping World Cup calendar...")
    try:
        # Add headers to avoid bot detection
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        response = make_request_with_retry(wc_url, headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        calendar_div = soup.find('div', {'id': 'calendardata'})
        if calendar_div:
            event_rows = calendar_div.find_all('div', {'class': 'table-row'})
            print(f"Found {len(event_rows)} World Cup events")
            
            for row in event_rows:
                event_links = row.find_all('a', href=lambda x: x and 'event-details.html' in x)
                for link in event_links:
                    event_url = link.get('href')
                    if event_url and not event_url.startswith('http'):
                        event_url = "https://www.fis-ski.com" + event_url
                    
                    if event_url:
                        print(f"Processing WC event: {event_url}")
                        races = scrape_nordic_event(event_url, "World Cup", country_mapping, elevation_df)
                        all_races.extend(races)
                        time.sleep(3)  # Be more polite to server
                        break
    except Exception as e:
        print(f"Error scraping World Cup: {str(e)}")
    
    # Scrape Olympic races
    print("Scraping Olympic event...")
    try:
        races = scrape_nordic_event(olympic_url, "Olympics", country_mapping, elevation_df)
        all_races.extend(races)
    except Exception as e:
        print(f"Error scraping Olympics: {str(e)}")
    
    return all_races

def make_request_with_retry(url, headers, max_retries=3):
    """Make HTTP request with retry logic and exponential backoff"""
    for attempt in range(max_retries):
        try:
            # Add random delay between requests to avoid rate limiting
            time.sleep(random.uniform(2, 5))
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 2)
                print(f"Request failed (attempt {attempt + 1}), retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Request failed after {max_retries} attempts: {e}")
                raise e

def scrape_nordic_event(event_url, source_category, country_mapping, elevation_df):
    """Scrape nordic combined races from an event page"""
    races = []
    
    try:
        # Add headers to avoid bot detection
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        response = make_request_with_retry(event_url, headers)
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
            # Check if race is cancelled first
            cancelled_span = row.find('span', {'class': 'status__item status__item_cancelled'})
            if cancelled_span and cancelled_span.get('title') == 'Cancelled':
                print(f"Skipping cancelled race")
                continue
            
            # Extract date
            date_div = row.find('div', {'class': 'timezone-date'})
            if not date_div or not date_div.get('data-date'):
                continue
            
            try:
                date_obj = datetime.strptime(date_div.get('data-date'), '%Y-%m-%d')
                race_date = date_obj.strftime('%m/%d/%Y')
            except:
                continue
            
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
                if any(keyword in text for keyword in ['Individual', 'Team', 'Mass Start', '/', 'Km', 'Provisional']):
                    race_text = text
                    break
            
            # Skip races with empty race descriptions (likely provisional rounds or invalid entries)
            if not race_text or race_text.strip() == "":
                print(f"Skipping race with empty description for race ID: {startlist_url}")
                continue
                
            # Check for Provisional Round first - skip entire race
            if "Provisional Round" in race_text:
                print(f"Skipping Provisional Round: {race_text}")
                continue
            
            # Parse distance and race type
            distance, race_type = parse_distance_and_racetype(race_text)
            
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
                'distance': distance,
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

def save_nordic_races(races, output_file):
    """Save nordic combined race data to CSV"""
    
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
    
    print(f"Saved {len(sorted_races)} nordic combined races to {output_file}")

if __name__ == "__main__":
    print("Starting nordic combined race scraping...")
    
    races = scrape_nordic_combined()
    
    if races:
        output_file = "excel365/races.csv"
        save_nordic_races(races, output_file)
        print(f"Scraping complete! Found {len(races)} total nordic combined races.")
    else:
        print("No nordic combined races found.")