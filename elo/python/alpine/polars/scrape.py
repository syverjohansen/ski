import logging
import ssl
import re
from urllib.request import urlopen
from urllib.error import URLError
from http.client import HTTPConnection, HTTPSConnection
from bs4 import BeautifulSoup
import time
import polars as pl
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import warnings
import traceback
from collections import defaultdict
import random
import platform
import sys
import os
from typing import List, Dict, Any, Optional, Tuple
import asyncio
import aiohttp
import logging
import multiprocessing
from pathlib import Path

# Create output directory if it doesn't exist
output_dir = os.path.expanduser("~/ski/elo/python/alpine/polars/excel365")
os.makedirs(output_dir, exist_ok=True)

def check_environment():
    """Check and log system environment information"""
    try:
        logging.info(f"Python version: {sys.version}")
        logging.info(f"Platform: {platform.platform()}")
        logging.info(f"Polars version: {pl.__version__}")
        logging.info(f"NumPy version: {np.__version__}")
        
        # Test Polars functionality
        test_df = pl.DataFrame({"a": [1, 2, 3]})
        logging.info("Polars basic functionality test passed")
    except Exception as e:
        logging.error(f"Environment check failed: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)

# Suppress warnings
warnings.filterwarnings('ignore')

# Set up SSL context and logging
ssl._create_default_https_context = ssl._create_unverified_context
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables
start_time = time.time()
BIRTHDAY_CACHE = {}
SKIER_INFO_CACHE = {}  # Cache for skier name standardization and other info

def setup_cache_structure():
    """Initialize the caching structure for skier data"""
    global SKIER_INFO_CACHE
    SKIER_INFO_CACHE = {
        'birthdays': {},      # (id, sex) -> birthday
        'name_variants': {},  # variant -> standardized name
        'ids': {},           # (standardized_name, nation) -> id
        'active_years': defaultdict(set)  # id -> set of years active
    }

class RateLimit:
    """Simple rate limiter for API calls"""
    def __init__(self, max_per_second=2):
        self.delay = 1.0 / max_per_second
        self.last_call = 0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_call = time.time()

# Initialize rate limiter
rate_limiter = RateLimit(max_per_second=5)

def fetch_with_retry(url, max_retries=3, timeout=10):
    """Fetch URL with retry logic and rate limiting"""
    rate_limiter.wait()  # Respect rate limiting
    
    for attempt in range(max_retries):
        try:
            response = urlopen(url, timeout=timeout)
            return response.read().decode('utf-8')
        except (URLError, TimeoutError) as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to fetch {url} after {max_retries} attempts: {e}")
                raise
            wait_time = (attempt + 1) * 2  # Progressive backoff
            logging.warning(f"Attempt {attempt + 1} failed, waiting {wait_time}s...")
            time.sleep(wait_time)
    return None

def fetch_season_links(year, sex='M'):
    """Fetch all race links for a given season"""
    base_url = f"https://firstskisport.com/alpine/calendar.php?y={year}"
    if sex == 'L':
        base_url += "&g=w"
    
    try:
        html_content = fetch_with_retry(base_url)
        if not html_content:
            logging.warning(f"No content found for {year} ({sex})")
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        
        # Find results table
        results_table = soup.find('table', {'class': 'sortTabell tablesorter'})
        if not results_table:
            logging.warning(f"No results table found for {year} ({sex})")
            return []
        
        # Get all rows skipping the header
        rows = results_table.find_all('tr')[1:]  # Skip header
        
        race_num = 0
        processed_ids = set()  # To avoid duplicates
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 5:  # Need at least 5 cells for a valid row
                continue
            
            # Find all links with "results.php?id=" in the href attribute
            result_links = []
            for cell in cells:
                links_in_cell = cell.find_all('a', href=lambda href: href and 'results.php?id=' in href)
                result_links.extend(links_in_cell)
            
            if not result_links:
                continue
                
            # Extract unique race IDs (only process each race once)
            for link in result_links:
                href = link['href']
                match = re.search(r'id=(\d+)', href)
                if match:
                    race_id = match.group(1)
                    if race_id not in processed_ids:
                        processed_ids.add(race_id)
                        full_url = 'https://firstskisport.com/alpine/' + href
                        race_num += 1
                        links.append([full_url, year, race_num])
                        # Once we've found one link for this race, break to avoid duplicates
                        break
                
        logging.info(f"Found {len(links)} races for {year} ({sex})")
        return links
        
    except Exception as e:
        logging.error(f"Error fetching season {year} ({sex}): {e}")
        logging.error(traceback.format_exc())
        return []

def standardize_name(name):
    """Standardize skier names for consistent matching"""
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Handle special characters
    replacements = {
        'æ': 'ae', 'Æ': 'Ae',
        'ø': 'o', 'Ø': 'O',
        'å': 'a', 'Å': 'A',
        'ä': 'a', 'Ä': 'A',
        'ö': 'o', 'Ö': 'O',
        'ü': 'u', 'Ü': 'U'
    }
    
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    return name

def parse_birthday(text):
    """Parse birthday from various text formats"""
    try:
        # Common patterns for birthday formats - order matters!
        patterns = [
            # Primary format: "13.Mar 1995 (30)" - extract the full date part
            r"(\d{1,2})\.(\w{3}) (\d{4})",
            # Alternative format: "15/01/1990"
            r"(\d{1,2})/(\d{1,2})/(\d{4})",
            # Format: "15-01-1990"
            r"(\d{1,2})-(\d{1,2})-(\d{4})",
            # Year with age format: "1995 (30)" - but only use if no day/month found
            r"(\d{4})\s*\((\d{1,2})\)",
            # Just age format: "(30)"
            r"\((\d{1,2})\)"
        ]

        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }

        # Try each pattern
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text)
            if match:
                if len(match.groups()) == 3:  # Full date
                    if i == 0 and match.group(2) in month_map:  # Day.Month Year format
                        day = int(match.group(1))
                        month = month_map[match.group(2)]
                        year = int(match.group(3))
                        return datetime(year, month, day)
                    elif i > 0:  # Numeric formats
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = int(match.group(3))
                        return datetime(year, month, day)
                elif len(match.groups()) == 2 and i == 3:  # Year and age format
                    # Year with age: "1995 (30)" - use the year, default to January 1st
                    year = int(match.group(1))
                    return datetime(year, 1, 1)
                elif len(match.groups()) == 1 and i == 4:  # Just age
                    # Age only: "(30)" - calculate birth year and default to January 1st
                    age = int(match.group(1))
                    current_year = datetime.now().year
                    birth_year = current_year - age
                    return datetime(birth_year, 1, 1)

        return None

    except Exception as e:
        logging.warning(f"Error parsing birthday '{text}': {e}")
        return None

def extract_athlete_info(soup, athlete_id, sex):
    """Extract comprehensive athlete information from their page"""
    try:
        info = {
            'id': athlete_id,
            'sex': sex,
            'birthday': None,
            'nation': None,
            'names': set(),
            'active_years': set()
        }

        # Extract birthday
        h2_text = soup.body.find('h2').text if soup.body.find('h2') else ""
        info_parts = h2_text.split(",")
        
        if len(info_parts) >= 3:
            # The birthday should be in the third part after splitting by comma
            # Format: "13.Mar 1995 (30)" or similar
            birthday_text = info_parts[2].strip()
            birthday = parse_birthday(birthday_text)
            if birthday:
                info['birthday'] = birthday

        # Extract nation
        if len(info_parts) >= 4:
            info['nation'] = info_parts[3].strip()

        # Extract name variants
        name_elements = soup.find_all('h1')
        for elem in name_elements:
            name = elem.text.strip()
            if name:
                info['names'].add(standardize_name(name))

        # Extract active years from results table
        results_table = soup.find('table', {'class': 'tablesorter sortTabell'})
        if results_table:
            for row in results_table.find_all('tr'):
                cells = row.find_all('td')
                if cells and len(cells) > 2:
                    date_text = cells[0].text.strip()
                    try:
                        year = int(date_text.split('.')[-1])
                        info['active_years'].add(year)
                    except (ValueError, IndexError):
                        continue

        return info

    except Exception as e:
        logging.error(f"Error extracting athlete info for ID {athlete_id}: {e}")
        return None

def get_or_fetch_athlete_info(athlete_id, sex):
    """Get athlete info from cache or fetch from website"""
    cache_key = (athlete_id, sex)
    
    # Check cache first
    if cache_key in SKIER_INFO_CACHE['birthdays']:
        return SKIER_INFO_CACHE['birthdays'][cache_key]

    # Fetch from website
    try:
        url = f"https://firstskisport.com/alpine/athlete.php?id={athlete_id}"
        if sex == 'L':
            url += "&g=w"
            
        html_content = fetch_with_retry(url)
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        info = extract_athlete_info(soup, athlete_id, sex)
        
        if info:
            # Update various caches
            SKIER_INFO_CACHE['birthdays'][cache_key] = info['birthday']
            for name in info['names']:
                SKIER_INFO_CACHE['name_variants'][name] = list(info['names'])[0]  # Use first name as standard
            SKIER_INFO_CACHE['active_years'][athlete_id].update(info['active_years'])
            
            logging.info(f"Cached info for athlete {athlete_id} ({sex})")
            return info['birthday']
            
    except Exception as e:
        logging.error(f"Error fetching athlete {athlete_id}: {e}")
        
    return None

def parse_race_type(race_text):
    """Parse race type from text, handling specific categorizations"""
    race_text = race_text.strip()
    
    # Define categories
    if "Downhill" in race_text:
        return "Downhill"
    elif "Super G" in race_text or "Super-G" in race_text:
        return "Super G"
    elif "Giant Slalom" in race_text:
        return "Giant Slalom"
    elif "Slalom" in race_text and not any(x in race_text for x in ["Parallel", "Giant", "Team", "City Event"]):
        return "Slalom"
    elif any(x in race_text for x in ["Parallel", "City Event", "Team"]):
        return "Slalom"  # All parallel events classified as Slalom as requested
    elif "Combined" in race_text:
        return "Combined"
    else:
        logging.warning(f"Unknown race type: {race_text}")
        return race_text  # Return as-is if unknown

def get_race_data(link):
    """Extract race information from race page, handling different formats"""
    try:
        season = link[1]
        race_num = link[2]
        url = link[0]
        
        logging.info(f"Processing race data from: {url}")
        
        html_content = fetch_with_retry(url)
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Get race header - contains race name and city
        race_header = soup.find('h1')
        if not race_header:
            logging.warning(f"No race header found for {url}")
            return None
            
        race_city = race_header.text.strip()
        
        # Extract city and race type - handle cases with and without " - " separator
        if ' - ' in race_city:
            parts = race_city.split(' - ')
            race_type = parts[0].strip()
            city = parts[1].strip()
        else:
            # If no separator, try to infer race type from text
            city = race_city.strip()
            # Default to basic discipline types
            if "Downhill" in race_city:
                race_type = "Downhill"
            elif "Super" in race_city:
                race_type = "Super G"
            elif "Giant" in race_city:
                race_type = "Giant Slalom"
            elif "Slalom" in race_city and "Giant" not in race_city:
                race_type = "Slalom"
            elif "Combined" in race_city:
                race_type = "Combined"
            else:
                race_type = "Unknown"
                logging.warning(f"Could not determine race type from: {race_city}")
            
        # Parse race type to standardized format
        distance = parse_race_type(race_type)
        
        # Get date, event, country from h2 tag
        date_header = soup.find('h2')
        if not date_header:
            logging.warning(f"No date header found for {url}")
            return None
            
        date_country_line = date_header.text
        
        # Split into parts using comma as separator
        date_country_parts = [part.strip() for part in date_country_line.split(',')]
        
        # Make sure we have enough parts to extract date, event, country
        if len(date_country_parts) < 4:
            logging.warning(f"Invalid date/country format: {date_country_line}")
            # Try alternative parsing for older formats
            # Some old formats have "Alpine, WSC, Saturday 6.Feb 1932, Italy"
            # Extract what we can
            event = date_country_parts[1] if len(date_country_parts) > 1 else "Unknown"
            country = date_country_parts[-1] if date_country_parts else "Unknown"
            
            # Try to find a date pattern in any of the parts
            date = None
            for part in date_country_parts:
                if re.search(r'\d{1,2}\.\w{3}\s+\d{4}', part):  # Pattern like "6.Feb 1932"
                    date_match = re.search(r'(\d{1,2})\.(\w{3})\s+(\d{4})', part)
                    if date_match:
                        day = date_match.group(1)
                        month = date_match.group(2)
                        year = date_match.group(3)
                        try:
                            date_obj = datetime.strptime(f"{day}.{month} {year}", "%d.%b %Y")
                            date = date_obj.strftime("%Y%m%d")
                            break
                        except ValueError:
                            pass
            
            if not date:
                # If we still couldn't find a date, try a more general approach
                for part in date_country_parts:
                    if re.search(r'\d{4}', part):  # Look for a year
                        year_match = re.search(r'(\d{4})', part)
                        if year_match:
                            year = year_match.group(1)
                            # Assume January 1st if no specific date
                            date = f"{year}0101"
                            break
            
            if not date:
                # Last resort - use season as the year
                date = f"{season}0101"
                
        else:
            # Standard format parsing
            # Get event (e.g., World Cup)
            event = date_country_parts[1].strip()
            
            # Get country
            country = date_country_parts[3].strip()
            
            # Parse date
            date_str = date_country_parts[2].strip()
            try:
                # First try format: "Sunday 18.Nov 2018"
                date_parts = date_str.split()
                
                # There must be at least a day, month, and year
                if len(date_parts) >= 3:
                    # Look for the part with day.month format
                    day_month_part = None
                    year_part = None
                    
                    for part in date_parts:
                        if '.' in part:  # This might be a day.month format
                            day_month_part = part
                        if re.match(r'\d{4}$', part):  # This looks like a year
                            year_part = part
                    
                    if day_month_part and year_part:
                        # Format the date string for parsing
                        try:
                            day = day_month_part.split('.')[0]
                            month = day_month_part.split('.')[1]
                            year = year_part
                            
                            date_obj = datetime.strptime(f"{day}.{month} {year}", "%d.%b %Y")
                            date = date_obj.strftime("%Y%m%d")
                        except (ValueError, IndexError):
                            # Fallback - use season as year and assume January 1
                            date = f"{season}0101"
                    else:
                        # Fallback - use season as year and assume January 1
                        date = f"{season}0101"
                else:
                    # Fallback - use season as year and assume January 1
                    date = f"{season}0101"
                    
            except Exception as e:
                logging.error(f"Error parsing date '{date_str}': {e}")
                # Fallback - use season as year and assume January 1
                date = f"{season}0101"
        
        result = [date, city, country, event, distance, 0, "N/A", season, race_num]
        logging.info(f"Extracted race data: {result}")
        return result
        
    except Exception as e:
        logging.error(f"Error processing race {url}: {e}")
        logging.error(traceback.format_exc())
        return None

async def fetch_athlete_info_batch(session: aiohttp.ClientSession, athlete_ids: List[str], sex: str):
    """Fetch athlete information in parallel batches"""
    async def fetch_single_athlete(athlete_id: str):
        cache_key = (athlete_id, sex)
        
        # Check cache first
        if cache_key in SKIER_INFO_CACHE['birthdays']:
            return athlete_id, SKIER_INFO_CACHE['birthdays'][cache_key]
        
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Respect rate limiting
        await asyncio.sleep(0.2)  # 5 requests per second rate limit
        
        url = f"https://firstskisport.com/alpine/athlete.php?id={athlete_id}"
        if sex == 'L':
            url += "&g=w"
            
        try:
            async with session.get(url, ssl=ssl_context) as response:
                if response.status != 200:
                    return athlete_id, None
                    
                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                info = extract_athlete_info(soup, athlete_id, sex)
                
                if info and info['birthday']:
                    SKIER_INFO_CACHE['birthdays'][cache_key] = info['birthday']
                    return athlete_id, info['birthday']
                    
        except Exception as e:
            logging.error(f"Error fetching athlete {athlete_id}: {e}")
            
        return athlete_id, None

    # Create tasks for all uncached athletes
    tasks = []
    for athlete_id in athlete_ids:
        if (athlete_id, sex) not in SKIER_INFO_CACHE['birthdays']:
            tasks.append(fetch_single_athlete(athlete_id))
    
    # Execute all tasks in parallel
    if tasks:
        results = await asyncio.gather(*tasks)
        return dict(results)
    return {}

def get_race_results(link: List[Any], sex: str) -> List[Dict]:
    """Extract results from race page, handling different formats and table structures"""
    try:
        url = link[0]
        logging.info(f"Processing race results from: {url}")
        
        html_content = fetch_with_retry(url)
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find results table
        results_table = soup.find('table', {'class': 'tablesorter sortTabell'})
        if not results_table:
            logging.warning(f"No results table found for {url}")
            return []
            
        # Find table headers to determine the format
        headers = results_table.find_all('th')
        header_texts = [h.text.strip() for h in headers]
        
        # Find all rows and skip the header
        rows = results_table.find_all('tr')[1:]  # Skip header row
        
        # Prepare data structure for results
        athletes_data = []
        
        for row in rows:
            cells = row.find_all('td')
            if not cells or len(cells) < 3:  # Need at least position, name, nation
                continue
                
            # Get position
            place_cell = cells[0].text.strip()
            
            # Skip rows without a valid position or DNF/DSQ from first run
            if not place_cell or place_cell in ["DNS", "DNQ"]:
                continue
                
            # Handle DNF, DSQ in second run, and OOT (out of time) - assign next position
            if place_cell.startswith("DNF") or place_cell.startswith("DSQ") or place_cell == "OOT":
                # These skiers started but didn't finish, so they should be included with the next position
                if "(2)" in place_cell or place_cell == "OOT":  # Only count DNF from second run or OOT
                    place = len(athletes_data) + 1  # Assign next position
                else:
                    continue  # Skip DNF from first run
            else:
                try:
                    place = int(place_cell)
                except ValueError:
                    logging.warning(f"Could not parse place: {place_cell}")
                    continue
            
            # Find the name cell - this is consistent across formats (usually index 2)
            name_cell = cells[2] if len(cells) > 2 else None
            
            # Find birth year cell - usually index 3 but may not exist in older formats
            birth_cell = cells[3] if len(cells) > 3 else None
            
            # Find nation cell - usually index 4
            nation_cell = cells[4] if len(cells) > 4 else None
            
            # Make sure we have at least the name cell
            if not name_cell:
                continue
                
            # Get skier link
            skier_link = name_cell.find('a')
            if not skier_link or 'href' not in skier_link.attrs:
                continue
                
            # Extract skier ID
            href = skier_link['href']
            match = re.search(r'id=(\d+)', href)
            if not match:
                continue
                
            ski_id = match.group(1)
            
            # Extract name - handle different format possibilities
            last_name_span = name_cell.find('span', {'style': lambda s: s and 'text-transform:uppercase' in s})
            if last_name_span:
                last_name = last_name_span.text.strip()
                # Remove the last name from the full text to get the first name
                name_text = name_cell.text.strip()
                first_name = name_text.replace(last_name, '').strip()
                skier_name = f"{first_name} {last_name}".strip()
            else:
                # Fallback - just use the text from the cell
                skier_name = name_cell.text.strip()
            
            # Get nation
            nation = ""
            if nation_cell:
                # Try to find nation link
                nation_link = nation_cell.find('a')
                if nation_link:
                    nation = nation_link.text.strip()
                else:
                    # Get text directly from cell
                    nation = nation_cell.text.strip()
            
            # Get birthday
            birthday = None
            
            # First try from the birth cell if it exists
            if birth_cell:
                birth_text = birth_cell.text.strip()
                if birth_text and birth_text != "-":
                    try:
                        # If it's a simple year, skip it and fetch from athlete page instead
                        if re.match(r'^\d{4}$', birth_text):
                            birthday = None  # Force fetch from athlete page
                        else:
                            # Parse birthday from birth cell text
                            birthday = parse_birthday(birth_text)
                    except (ValueError, TypeError):
                        birthday = None
            
            # If we still don't have a birthday, fetch from athlete page
            if not birthday:
                # Check cache first
                cache_key = (ski_id, sex)
                if cache_key in SKIER_INFO_CACHE['birthdays']:
                    birthday = SKIER_INFO_CACHE['birthdays'][cache_key]
                else:
                    # Fetch from athlete page
                    athlete_url = f"https://firstskisport.com/alpine/athlete.php?id={ski_id}"
                    if sex == 'L':
                        athlete_url += "&g=w"
                    
                    try:
                        athlete_html = fetch_with_retry(athlete_url)
                        if athlete_html:
                            athlete_soup = BeautifulSoup(athlete_html, 'html.parser')
                            h2_text = athlete_soup.find('h2').text if athlete_soup.find('h2') else ""
                            info_parts = h2_text.split(",")
                            
                            if len(info_parts) >= 3:
                                birthday = parse_birthday(info_parts[2].strip())
                                if birthday:
                                    # Store in cache
                                    SKIER_INFO_CACHE['birthdays'][cache_key] = birthday
                    except Exception as e:
                        logging.warning(f"Error fetching birthday for athlete {ski_id}: {e}")
            
            # Include all athletes, even those without birthdays
            athletes_data.append({
                'Place': place,
                'Skier': skier_name,
                'Nation': nation,
                'ID': ski_id,
                'Birthday': birthday  # Can be None
            })
            
            if not birthday:
                logging.warning(f"Could not determine birthday for {skier_name} (ID: {ski_id})")
        
        logging.info(f"Processed {len(athletes_data)} valid results for race {url}")
        return athletes_data
        
    except Exception as e:
        logging.error(f"Error processing race results for {url}: {e}")
        logging.error(traceback.format_exc())
        return []

def construct_historical_df(tables, results_data, sex):
    """Construct DataFrame from historical race data"""
    try:
        logging.info(f"Constructing DataFrame for {sex}")
        
        # Filter out None values and empty results
        valid_data = [(table, results) 
                     for table, results in zip(tables, results_data) 
                     if table is not None and results]
        
        if not valid_data:
            logging.error("No valid data to process")
            return None
            
        tables, results_data = zip(*valid_data)
        
        # Create base race information DataFrame
        table_df = pl.DataFrame(
            data=tables,
            schema=["Date", "City", "Country", "Event", "Distance", "MS", 
                   "Technique", "Season", "Race"],
            orient="row"
        )
        
        # Add sex column
        table_df = table_df.with_columns(pl.lit(sex).alias("Sex"))
        
        # Create results DataFrame
        all_results = []
        for idx, race_results in enumerate(results_data):
            race_table = tables[idx]
            for result in race_results:
                row = {
                    'Date': race_table[0],
                    'City': race_table[1],
                    'Country': race_table[2],
                    'Event': race_table[3],
                    'Distance': race_table[4],
                    'MS': race_table[5],
                    'Technique': race_table[6],
                    'Season': race_table[7],
                    'Race': race_table[8],
                    'Sex': sex,
                    'Place': result['Place'],
                    'Skier': result['Skier'],
                    'Nation': result['Nation'],
                    'ID': result['ID'],
                    'Birthday': result['Birthday']
                }
                all_results.append(row)
        
        # Create DataFrame from all results
        df = pl.DataFrame(all_results)
        
        # Convert types - handle None birthdays properly
        df = df.with_columns([
            pl.col('Date').str.strptime(pl.Date, format='%Y%m%d'),
            pl.col('Birthday').cast(pl.Datetime),  # Polars handles None values automatically
            pl.col('Place').cast(pl.Int64),
            pl.col('MS').cast(pl.Int64),
            pl.col('Season').cast(pl.Int64),
            pl.col('Race').cast(pl.Int64)
        ])
        
        # Handle athletes with missing birthdays - estimate birthday based on first race
        athletes_without_birthdays = df.filter(pl.col('Birthday').is_null()).select('ID').unique()
        
        if len(athletes_without_birthdays) > 0:
            logging.info(f"Estimating birthdays for {len(athletes_without_birthdays)} athletes based on first race (assuming age 22 at debut)")
            
            # For each athlete without birthday, calculate estimated birthday
            for athlete_row in athletes_without_birthdays.iter_rows():
                athlete_id = athlete_row[0]
                
                # Find their earliest race date
                athlete_races = df.filter(pl.col('ID') == athlete_id).sort('Date')
                if len(athlete_races) > 0:
                    first_race_date = athlete_races['Date'][0]
                    
                    # Calculate birthday assuming they were 22 years old at first race
                    # Subtract 22 years from first race date
                    try:
                        estimated_birthday = datetime(first_race_date.year - 22, first_race_date.month, first_race_date.day)
                    except ValueError:
                        # Handle leap year issue - if Feb 29 doesn't exist in birth year, use Feb 28
                        if first_race_date.month == 2 and first_race_date.day == 29:
                            estimated_birthday = datetime(first_race_date.year - 22, 2, 28)
                        else:
                            raise  # Re-raise if it's a different date issue
                    
                    # Update all records for this athlete with the estimated birthday
                    df = df.with_columns(
                        pl.when(pl.col('ID') == athlete_id)
                        .then(pl.lit(estimated_birthday))
                        .otherwise(pl.col('Birthday'))
                        .alias('Birthday')
                    )
                    
                    logging.info(f"Set estimated birthday for athlete {athlete_id}: {estimated_birthday.strftime('%Y-%m-%d')} (22 years before first race on {first_race_date})")
        
        # Calculate age - now all athletes should have birthdays (actual or estimated)
        df = df.with_columns(
            ((pl.col('Date').cast(pl.Datetime) - pl.col('Birthday')).dt.total_days() / 365.25)
            .cast(pl.Float64)
            .alias('Age')
        )
        
        # Calculate experience (races participated in)
        df = df.sort(['ID', 'Date'])
        df = df.with_columns([
            pl.col('ID')
            .cum_count()
            .over(['ID'])
            .cast(pl.Int32)
            .alias('Exp')
        ])
        
        logging.info(f"Created DataFrame with shape {df.shape}")
        return df
        
    except Exception as e:
        logging.error(f"Error constructing DataFrame: {e}")
        logging.error(traceback.format_exc())
        return None

def save_dataframes(men_df, ladies_df, base_path="~/ski/elo/python/alpine/polars/excel365"):
    """Save DataFrames to both feather and CSV formats"""
    try:
        base_path = os.path.expanduser(base_path)
        os.makedirs(base_path, exist_ok=True)
        
        if men_df is not None:
            men_df.write_csv(f"{base_path}/men_scrape.csv")
            logging.info(f"Saved men's historical data with {len(men_df)} rows")
            
        if ladies_df is not None:
            ladies_df.write_csv(f"{base_path}/ladies_scrape.csv")
            logging.info(f"Saved ladies' historical data with {len(ladies_df)} rows")
            
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        logging.error(traceback.format_exc())

def process_year_range(start_year, end_year, sex):
    """Process a range of years for given sex using parallel processing"""
    all_tables = []
    all_results = []
    
    def process_race(link):
        """Process a single race"""
        try:
            # Get race data
            print(link)
            table_data = get_race_data(link)
            if table_data is None:
                return None
                
            # Get results
            results = get_race_results(link, sex)
            if not results:
                return None
                
            return (table_data, results)
            
        except Exception as e:
            logging.error(f"Error processing race {link}: {e}")
            return None

    for year in range(start_year, end_year + 1):
        logging.info(f"Processing year {year} for {sex}")
        
        # Get links for the season
        season_links = fetch_season_links(year, sex)
        if not season_links:
            logging.warning(f"No races found for {year} ({sex})")
            continue

        max_workers = min(32, multiprocessing.cpu_count() * 4)    
        # Process races in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            race_results = list(executor.map(process_race, season_links))
            
        # Filter out None results and append valid ones
        for result in race_results:
            if result is not None:
                table_data, race_results = result
                all_tables.append(table_data)
                all_results.append(race_results)
                
        # Delay between seasons
        time.sleep(random.uniform(1, 2))
    
    return all_tables, all_results

def main():
    """Main execution function"""
    check_environment()
    setup_cache_structure()
    
    # Define year range - can be adjusted as needed
    start_year = 1924  # First World Cup season
    #start_year = 2025
    current_year = datetime.now().year
    end_year = current_year    # Current season
    
    # Process men's data
    logging.info("Processing men's historical data")
    men_tables, men_results = process_year_range(start_year, end_year, 'M')
    men_df = construct_historical_df(men_tables, men_results, 'M')
    
    # Process ladies' data
    logging.info("Processing ladies' historical data")
    ladies_tables, ladies_results = process_year_range(start_year, end_year, 'L')
    ladies_df = construct_historical_df(ladies_tables, ladies_results, 'L')
    
    # Save the data
    save_dataframes(men_df, ladies_df)
    
    # Log execution time
    execution_time = time.time() - start_time
    logging.info(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == '__main__':
    main()