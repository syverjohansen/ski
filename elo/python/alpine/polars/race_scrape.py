import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time
import random

from race_schedule_output import save_alpine_races
from schedule_sources import (
    calendar_sources,
    championship_value,
    prediction_discipline,
)

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

def determine_championship(source_category):
    """Determine if championship race"""
    return championship_value(source_category)

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

def scrape_alpine_races(
    season=2026,
    *,
    include_championships=True,
    request_fn=make_request_with_retry,
    sleep_fn=time.sleep,
):
    """Scrape alpine race data from FIS World Cup and Olympic calendars"""
    
    country_mapping = create_country_mapping()
    all_races = []
    
    for url, category in calendar_sources(season, include_championships):
        print(f"Scraping {category} calendar...")
        category_start_count = len(all_races)
        
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
            
            response = request_fn(url, headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all event weekend rows
            calendar_div = soup.find('div', {'id': 'calendardata'})
            if not calendar_div:
                raise RuntimeError(f"No calendar data found for {category}")
            
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
                        races = scrape_alpine_event(
                            event_url,
                            category,
                            country_mapping,
                            request_fn=request_fn,
                            strict=True,
                        )
                        all_races.extend(races)
                        sleep_fn(3)  # Be more polite to server
                        break

            if len(all_races) == category_start_count:
                raise RuntimeError(f"No usable races found for {category}")
        
        except Exception as e:
            raise RuntimeError(f"Error scraping {category}: {e}") from e
    
    return all_races

def scrape_alpine_event(
    event_url,
    source_category,
    country_mapping,
    *,
    request_fn=make_request_with_retry,
    strict=False,
):
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
        
        response = request_fn(event_url, headers)
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
            raise RuntimeError(f"No event details content found for {event_url}")
        
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
                distance = prediction_discipline(text)
                if distance:
                    break
            
            # Skip training, qualification, team, and unrelated event rows.
            if not distance:
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
        if strict:
            raise
        print(f"Error processing event {event_url}: {str(e)}")
    
    return races

if __name__ == "__main__":
    print("Starting alpine race scraping...")
    
    races = scrape_alpine_races()
    
    if races:
        output_file = "excel365/races.csv"
        save_alpine_races(races, output_file)
        print(f"Scraping complete! Found {len(races)} total alpine races.")
    else:
        print("No alpine races found.")
