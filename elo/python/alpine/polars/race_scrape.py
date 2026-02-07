import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re
import time
import random

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

def assign_periods_by_race_count(races):
    """Assign periods 1, 2, 3 based on equal race counts, not time"""
    # First, find when Olympics end
    olympic_end_date = None
    for race in races:
        if race['championship'] == "1":  # Olympics
            try:
                race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                if olympic_end_date is None or race_date > olympic_end_date:
                    olympic_end_date = race_date
            except:
                continue
    
    # Separate races into categories
    olympic_races = []
    non_olympic_races = []
    post_olympic_races = []
    
    for race in races:
        if race['championship'] == "1":  # Olympics
            olympic_races.append(race)
            race['period'] = "4"
        else:
            # Check if post-Olympics (after last Olympic race)
            try:
                race_date = datetime.strptime(race['date'], '%m/%d/%Y')
                if olympic_end_date and race_date > olympic_end_date:
                    post_olympic_races.append(race)
                    race['period'] = "5"
                else:
                    non_olympic_races.append(race)
            except:
                non_olympic_races.append(race)
    
    # Sort non-Olympic races by date
    non_olympic_races.sort(key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y'))
    
    # Divide into 3 equal groups
    total_races = len(non_olympic_races)
    if total_races > 0:
        races_per_period = total_races // 3
        remainder = total_races % 3
        
        # Assign periods
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
        
        # Period 3 (remaining races)
        for i in range(current_index, total_races):
            non_olympic_races[i]['period'] = "3"
    
    # Combine all races back together
    all_races = non_olympic_races + olympic_races + post_olympic_races
    return all_races

def determine_championship(source_category):
    """Determine if championship race"""
    if source_category == "Olympics":
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

def scrape_alpine_races():
    """Scrape alpine race data from FIS World Cup and Olympic calendars"""
    
    # URLs for World Cup and Olympic calendars
    wc_url = "https://www.fis-ski.com/DB/alpine-skiing/calendar-results.html?eventselection=&place=&sectorcode=AL&seasoncode=2026&categorycode=WC&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    olympic_url = "https://www.fis-ski.com/DB/alpine-skiing/calendar-results.html?eventselection=&place=&sectorcode=AL&seasoncode=2026&categorycode=OWG&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    
    country_mapping = create_country_mapping()
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
                        races = scrape_alpine_event(event_url, category, country_mapping)
                        all_races.extend(races)
                        time.sleep(3)  # Be more polite to server
                        break
        
        except Exception as e:
            print(f"Error scraping {category}: {str(e)}")
    
    return all_races

def scrape_alpine_event(event_url, source_category, country_mapping):
    """Scrape all race data from an alpine event page"""
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
            # Extract city and country code from "Saalbach (AUT)" format
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
            
            # Extract distance (discipline) from race description
            distance = ""
            clip_divs = row.find_all('div', {'class': 'clip'})
            for div in clip_divs:
                text = div.get_text().strip()
                # Skip training sessions
                if "Training" in text:
                    continue
                # Look for alpine disciplines
                if any(keyword in text for keyword in ['Slalom', 'Giant Slalom', 'Super G', 'Downhill', 'Combined', 'Alpine Combined']):
                    distance = text
                    break
            
            # Skip if no valid distance found, if it's training, or if it's a team event
            if not distance or "Training" in distance or "Team" in distance:
                continue
            
            # Determine championship
            championship = determine_championship(source_category)
            
            # Create race entry (period will be assigned later)
            race = {
                'date': race_date,
                'startlist_url': startlist_url,
                'sex': sex,
                'city': city,
                'country': country,
                'distance': distance,
                'period': "",  # Will be assigned by race count
                'championship': championship
            }
            
            races.append(race)
            print(f"Found race: {race_date} ({sex}) {city} - {distance}")
    
    except Exception as e:
        print(f"Error processing event {event_url}: {str(e)}")
    
    return races

def assign_race_numbers(races):
    """Assign race numbers within each sex, starting from 1"""
    # Add race numbering by gender
    race_counters = {'M': 0, 'L': 0}
    
    for race in races:
        race_counters[race['sex']] += 1
        race['race'] = race_counters[race['sex']]
    
    return races

def save_alpine_races(races, output_file):
    """Save alpine race data to CSV"""
    
    # First assign periods based on race count
    races_with_periods = assign_periods_by_race_count(races)
    
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
        writer.writerow(['Date', 'Startlist', 'Sex', 'City', 'Country', 'Distance', 'Period', 'Championship', 'Race'])
        
        # Data rows
        for race in sorted_races:
            row = [
                race['date'],
                race['startlist_url'],
                race['sex'],
                race['city'],
                race['country'],
                race['distance'],
                race['period'],
                race['championship'],
                race['race']
            ]
            writer.writerow(row)
    
    print(f"Saved {len(sorted_races)} alpine races to {output_file}")

if __name__ == "__main__":
    print("Starting alpine race scraping...")
    
    races = scrape_alpine_races()
    
    if races:
        output_file = "excel365/races.csv"
        save_alpine_races(races, output_file)
        print(f"Scraping complete! Found {len(races)} total alpine races.")
    else:
        print("No alpine races found.")