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
        'ESP': 'Spain'
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
        'Oslo': 'Holmenkollen'
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

def determine_period(date_str):
    """Determine period based on date"""
    try:
        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        
        # Calculate current season based on current UTC date
        from datetime import timezone
        current_utc_date = datetime.now(timezone.utc)
        current_utc_year = current_utc_date.year
        
        # Determine the current ski season (starts in late fall of previous year)
        # If we're in Jan-September, the season started last year
        # If we're in Oct-December, the season started this year
        if current_utc_date.month <= 9:
            season_start_year = current_utc_year - 1
        else:
            season_start_year = current_utc_year
        
        # Period definitions for the current season:
        # Period 1: Season start through Dec 24
        # Period 2: Dec 25 through Jan 7 (Tour de Ski)  
        # Period 3: Jan 8 through Jan 31
        # Period 4: February (Olympics when applicable)
        # Period 5: March onwards
        
        # Before Christmas (period 1) - season start year dates before Dec 25
        if year == season_start_year and (month < 12 or (month == 12 and day < 25)):
            return "1"
        # Christmas to January 7th (period 2) - Dec 25 season start year to Jan 7 next year  
        elif (year == season_start_year and month == 12 and day >= 25) or (year == season_start_year + 1 and month == 1 and day <= 7):
            return "2"
        # After January 7th to before Olympics (period 3) - Jan 8 to Jan 31 next year
        elif year == season_start_year + 1 and month == 1 and day > 7:
            return "3"
        # Olympics (period 4) - February next year
        elif year == season_start_year + 1 and month == 2:
            return "4"
        # After Olympics (period 5) - March next year and later
        elif year == season_start_year + 1 and month >= 3:
            return "5"
        else:
            return "1"  # Default fallback
    except:
        return ""

def parse_distance(distance_text, has_tspq=False):
    """Parse distance from race description"""
    if not distance_text:
        return ""
    
    distance_text = distance_text.strip()
    
    # Check for Team Sprint Qualification using TSPQ indicator
    if has_tspq or "Team Sprint Qualification" in distance_text:
        return "Ts"
    
    # Check for deletion cases first
    if "Sprint" in distance_text and "Qualification" not in distance_text:
        return "DELETE"
    if "Team Sprint" in distance_text and "Qualification" not in distance_text:
        return "DELETE"
    
    # Check specific patterns
    if distance_text.startswith("4x"):
        return "Rel"
    if distance_text.startswith("Sprint Qualification"):
        return "Sprint"
    
    # Extract kilometers
    words = distance_text.split()
    for word in words:
        if "km" in word:
            try:
                return word.replace("km", "")
            except:
                continue
    
    # Use "10" as fallback if no distance can be parsed
    return "10"

def parse_technique(distance_text, distance):
    """Parse technique from race description"""
    if not distance_text:
        return ""
    
    if distance == "Rel":
        return "N/A"
    if "Skiathlon" in distance_text:
        return "P"
    if "Classic" in distance_text:
        return "C"
    if "Free" in distance_text:
        return "F"
    
    return ""

def determine_ms(distance_text):
    """Determine if Mass Start"""
    if not distance_text:
        return "0"
    if "Mass Start" in distance_text or "Skiathlon" in distance_text:
        return "1"
    return "0"

def determine_pursuit(distance_text):
    """Determine if Pursuit"""
    if not distance_text:
        return "0"
    if "Pursuit" in distance_text:
        return "1"
    return "0"

def determine_stage(period):
    """Determine stage based on period"""
    return "1" if period == "2" else "0"

def determine_final_climb(city, stage, ms, technique, distance):
    """Determine Final_Climb based on specific criteria"""
    try:
        # Handle case variations of Val di Fiemme
        city_lower = city.lower() if city else ""
        if (("val di fiemme" in city_lower or "val di fiemme" == city_lower) and 
            stage == "1" and ms == "1" and technique == "F"):
            return "1"
    except:
        pass
    return "0"

def determine_championship(source_category, city, distance_text):
    """Determine if championship race"""
    if source_category == "Olympics":
        return "1"
    if "World Championships" in str(distance_text):
        return "1"
    return "0"

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

def scrape_fis_comprehensive():
    """Comprehensive FIS race scraper with all 17 data points"""
    
    # URLs for World Cup and Olympic calendars
    wc_url = "https://www.fis-ski.com/DB/cross-country/calendar-results.html?eventselection=&place=&sectorcode=CC&seasoncode=2026&categorycode=WC&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    olympic_url = "https://www.fis-ski.com/DB/cross-country/calendar-results.html?eventselection=&place=&sectorcode=CC&seasoncode=2026&categorycode=OWG&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    
    country_mapping = create_country_mapping()
    elevation_df = load_elevation_data()
    all_races = []
    
    for url, category in [(wc_url, "World Cup"), (olympic_url, "Olympics")]:
        print(f"Scraping {category} calendar...")
        
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
            
            response = make_request_with_retry(url, headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all event weekend rows
            calendar_div = soup.find('div', {'id': 'calendardata'})
            if not calendar_div:
                print(f"No calendar data found for {category}")
                continue
            
            event_rows = calendar_div.find_all('div', {'class': 'table-row'})
            print(f"Found {len(event_rows)} events for {category}")
            
            for row in event_rows:
                # Get event detail links
                event_links = row.find_all('a', href=lambda x: x and 'event-details.html' in x)
                
                for link in event_links:
                    event_url = link.get('href')
                    if event_url and not event_url.startswith('http'):
                        event_url = "https://www.fis-ski.com" + event_url
                    
                    if event_url:
                        print(f"Processing event: {event_url}")
                        races = scrape_event_comprehensive(event_url, category, country_mapping, elevation_df)
                        all_races.extend(races)
                        time.sleep(3)  # Be more polite to server
                        break
        
        except Exception as e:
            print(f"Error scraping {category}: {str(e)}")
    
    return all_races

def scrape_event_comprehensive(event_url, source_category, country_mapping, elevation_df):
    """Scrape all race data from an event page"""
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
            # Extract city and country code from "Ruka (FIN)" format
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
            
            # 1. Extract date
            date_div = row.find('div', {'class': 'timezone-date'})
            if not date_div or not date_div.get('data-date'):
                continue
            
            try:
                date_obj = datetime.strptime(date_div.get('data-date'), '%Y-%m-%d')
                race_date = date_obj.strftime('%m/%d/%Y')
            except:
                continue
            
            # 2. Extract startlist URL
            result_links = row.find_all('a', href=lambda x: x and 'results.html' in x and 'raceid=' in x)
            if not result_links:
                continue
            
            startlist_url = result_links[0].get('href')
            if not startlist_url.startswith('http'):
                startlist_url = "https://www.fis-ski.com" + startlist_url
            
            # 3. Extract sex
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
            
            # 8. Extract distance from race description
            distance_text = ""
            clip_divs = row.find_all('div', {'class': 'clip'})
            for div in clip_divs:
                text = div.get_text().strip()
                if any(keyword in text for keyword in ['km', 'Sprint', 'Rel', 'Skiathlon', 'Classic', 'Free']):
                    distance_text = text
                    break
            
            # Also check for TSPQ text to catch Team Sprint Qualification
            has_tspq = False
            all_text_divs = row.find_all('div')
            for div in all_text_divs:
                if 'TSPQ' in div.get_text():
                    has_tspq = True
                    break
            
            distance = parse_distance(distance_text, has_tspq)
            if distance == "DELETE":
                continue  # Skip this race
            
            # 9. Technique
            technique = parse_technique(distance_text, distance)
            
            # 10. MS (Mass Start)
            ms = determine_ms(distance_text)
            
            # 11. Period
            period = determine_period(race_date)
            
            # 12. Pursuit
            pursuit = determine_pursuit(distance_text)
            
            # 13. Stage
            stage = determine_stage(period)
            
            # 14. Final_Climb
            final_climb = determine_final_climb(city, stage, ms, technique, distance)
            
            # 15. Championship
            championship = determine_championship(source_category, city, distance_text)
            
            # 16. Elevation (fuzzy match)
            elevation = fuzzy_match_elevation(city, elevation_df)
            
            # Skip Tour de Ski races
            if city == "Tour de Ski":
                print(f"Skipping Tour de Ski race: {race_date} ({sex}) {city}")
                continue
            
            # Create race entry
            race = {
                'date': race_date,
                'startlist_url': startlist_url,
                'sex': sex,
                'city': city,
                'country': country,
                'distance': distance,
                'technique': technique,
                'ms': ms,
                'period': period,
                'pursuit': pursuit,
                'stage': stage,
                'final_climb': final_climb,
                'elevation': elevation,
                'championship': championship
            }
            
            races.append(race)
            print(f"Found race: {race_date} ({sex}) {city} - {distance_text[:30]}...")
    
    except Exception as e:
        print(f"Error processing event {event_url}: {str(e)}")
    
    return races

def save_comprehensive_races(races, output_file):
    """Save race data with all fields and race numbering"""
    
    # Sort by date
    race_objects = []
    for race in races:
        try:
            date_obj = datetime.strptime(race['date'], '%m/%d/%Y')
            race_objects.append((date_obj, race))
        except ValueError:
            continue
    
    race_objects.sort(key=lambda x: x[0])
    sorted_races = [race for _, race in race_objects]
    
    # Add race numbering by gender
    race_counters = {'M': 0, 'L': 0, 'Mixed': 0}
    
    for race in sorted_races:
        race_counters[race['sex']] += 1
        race['race'] = race_counters[race['sex']]
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Header
        writer.writerow(['Date', 'Startlist', 'Sex', 'City', 'Country', 'Distance', 'Technique', 
                        'MS', 'Period', 'Pursuit', 'Stage', 'Final_Climb', 'Elevation', 'Race', 'Championship'])
        
        # Data rows
        for race in sorted_races:
            row = [
                race['date'],
                race['startlist_url'],
                race['sex'],
                race['city'],
                race['country'],
                race['distance'],
                race['technique'],
                race['ms'],
                race['period'],
                race['pursuit'],
                race['stage'],
                race['final_climb'],
                race['elevation'],
                race['race'],
                race['championship']
            ]
            writer.writerow(row)
    
    print(f"Saved {len(sorted_races)} races to {output_file}")

if __name__ == "__main__":
    print("Starting comprehensive FIS race scraping...")
    
    races = scrape_fis_comprehensive()
    
    if races:
        output_file = "excel365/races.csv"
        save_comprehensive_races(races, output_file)
        print(f"Scraping complete! Found {len(races)} total races.")
    else:
        print("No races found.")