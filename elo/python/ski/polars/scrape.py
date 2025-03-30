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
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import warnings
import traceback
from collections import defaultdict
import random
import platform
import sys
from typing import List, Dict, Any, Optional
import asyncio
import aiohttp
import logging
import multiprocessing

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
    base_url = f"https://firstskisport.com/cross-country/calendar.php?y={year}"
    if sex == 'L':
        base_url += "&g=w"
    
    try:
        html_content = fetch_with_retry(base_url)
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        
        # Find all results links
        result_links = soup.find_all('a', {'title': 'Results'}, href=True)
        
        # Process every second link (to avoid duplicates)
        for i in range(0, len(result_links), 2):
            link = result_links[i]
            race_num = int(i/2 + 1)
            full_url = 'https://firstskisport.com/cross-country/' + link['href']
            links.append([full_url, year, race_num])
            
        logging.info(f"Found {len(links)} races for {year} ({sex})")
        return links
        
    except Exception as e:
        logging.error(f"Error fetching season {year} ({sex}): {e}")
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
        # Common patterns for birthday formats
        patterns = [
            # Standard format: "15.Jan 1990"
            r"(\d{1,2})\.(\w{3}) (\d{4})",
            # Alternative format: "15/01/1990"
            r"(\d{1,2})/(\d{1,2})/(\d{4})",
            # Year with age format: "1990 (32)"
            r"(\d{4})\s*\((\d{1,2})\)",
            # Just age format: "(32)"
            r"\((\d{1,2})\)"
        ]

        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }

        # Try each pattern
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                if len(match.groups()) == 3:  # Full date
                    if match.group(2) in month_map:  # Text month
                        day = int(match.group(1))
                        month = month_map[match.group(2)]
                        year = int(match.group(3))
                    else:  # Numeric month
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = int(match.group(3))
                    return datetime(year, month, day)
                elif len(match.groups()) == 2 and pattern.endswith(r"(\d{4})\s*\((\d{1,2})\)"):
                    # Year and age format
                    year = int(match.group(1))
                    return datetime(year, 1, 1)  # Default to January 1st
                elif len(match.groups()) == 1:  # Just age
                    age = int(match.group(1))
                    current_year = datetime.now().year
                    return datetime(current_year - age, 1, 1)  # Approximate

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
            birthday = parse_birthday(info_parts[2])
            if birthday:
                info['birthday'] = birthday

        # Extract nation
        if len(info_parts) >= 4:
            info['nation'] = info_parts[3].strip()

        # Extract name variants
        name_elements = soup.find_all('h1')
        for elem in name_elements:
            name = elem.text.strip()
            #if name:
            #    info['names'].add(standardize_name(name))

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
        url = f"https://firstskisport.com/cross-country/athlete.php?id={athlete_id}"
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

def parse_race_info(race_text, year):
    """Parse race information handling historical formats"""
    distance = 0
    technique = "N/A"
    mass_start = 0
    
    # Clean the text
    race_text = race_text.strip()
    
    # Special cases first
    if "Duathlon" in race_text:
        return "Duathlon", 1, "P"
    if any(x in race_text for x in ["4x", "3x", "Relay"]):
        return "Rel", 0, "N/A"
    if "Team" in race_text:
        return "Ts", 0, "N/A"
        
    # Extract distance
    distance_match = re.search(r'^(\d+)', race_text)
    if distance_match:
        distance = distance_match.group(1)
    elif "Sprint" in race_text:
        distance = "Sprint"
    
    # Determine mass start
    mass_start = 1 if "Mass Start" in race_text else 0
    
    # Extract technique
    # For races before 1985, we don't specify technique
    if int(year) > 1985 and "Sprint" not in race_text:
        if any(word in race_text for word in ["Classical", "C ", " C"]):
            technique = "C"
        elif any(word in race_text for word in ["Freestyle", "F ", " F"]):
            technique = "F"
    elif "Sprint" in race_text:
        # For sprints, technique is usually the word after "Sprint"
        sprint_parts = race_text.split("Sprint")
        if len(sprint_parts) > 1 and sprint_parts[1].strip():
            technique = sprint_parts[1].strip()[0]
    
    return distance, mass_start, technique

def get_race_data(link):
    """Extract race information from race page"""
    try:
        season = link[1]
        race_num = link[2]
        url = link[0]
        
        html_content = fetch_with_retry(url)
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Get race header
        race_city = soup.body.find('h1').text.strip()
        date_country = soup.body.find('h2').text.split(", ")
        
        if len(date_country) < 4:
            logging.error(f"Invalid date/country format: {date_country}")
            return None
        
        # Parse date
        date_parts = date_country[2].strip().split()
        if len(date_parts) < 3:  # Need at least day, month abbreviation, and year
            logging.error(f"Invalid date format: {date_country[2]}")
            return None
            
        try:
            day = date_parts[-2].split('.')[0]
            month = date_parts[-2].split('.')[1]
            year = date_parts[-1]
            date_str = f"{day}.{month} {year}"
            date_obj = datetime.strptime(date_str, "%d.%b %Y")
            date = date_obj.strftime("%Y%m%d")
        except Exception as e:
            logging.error(f"Error parsing date {date_parts}: {e}")
            return None
        
        # Parse location and race info
        race_parts = race_city.split(" - ")
        if len(race_parts) < 2:
            logging.error(f"Invalid race city format: {race_city}")
            return None
            
        race = race_parts[0].strip()
        city = race_parts[1].strip()
        
        # Get event and country
        event = date_country[1].strip()
        country = date_country[3].strip()
        
        # Parse race details
        distance, ms, technique = parse_race_info(race, date_obj.year)
        
        result = [date, city, country, event, distance, ms, technique, season, race_num]
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
        await asyncio.sleep(0.2)  # 2 requests per second rate limit
        
        url = f"https://firstskisport.com/cross-country/athlete.php?id={athlete_id}"
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

async def get_race_results_async(link: List[Any], sex: str) -> List[Dict]:
    """Extract results from race page with batch processing of athlete info"""
    try:
        html_content = fetch_with_retry(link[0])
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find results table
        tables = soup.find_all('table', {'class': 'tablesorter sortTabell'})
        if not tables:
            logging.error(f"No results table found for {link[0]}")
            return []
            
        # Find all rows
        rows = tables[0].find_all('td')
        
        # First pass: collect all athlete IDs and basic info
        athletes_data = []
        athlete_ids = set()
        
        for a in range(0, len(rows), 7):
            try:
                place = rows[a].text.strip()
                
                # Skip non-finishing positions
                if place in {"DNF", "DSQ", "DNS", "DNQ", "OOT", ""}:
                    continue
                    
                skier_cell = str(rows[a+2])
                if 'id=' not in skier_cell:
                    continue
                    
                ski_id = skier_cell.split('id=')[1].split('&')[0]
                skier_name = skier_cell.split('title="')[1].split('"><span')[0]
                nation = rows[a+4].text.strip()
                
                # Standardize the name
                #std_name = standardize_name(skier_name)
                
                athletes_data.append({
                    'Place': int(place),
                    'Skier': skier_name,
                    'Nation': nation,
                    'ID': ski_id
                })
                athlete_ids.add(ski_id)
                
            except Exception as e:
                logging.error(f"Error processing result row: {e}")
                continue
        
        # Create SSL context for the session
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Batch fetch athlete info
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            birthdays = await fetch_athlete_info_batch(session, list(athlete_ids), sex)
        
        # Combine results with fetched birthdays
        results = []
        for athlete in athletes_data:
            birthday = birthdays.get(athlete['ID']) or SKIER_INFO_CACHE['birthdays'].get((athlete['ID'], sex))
            if birthday:  # Only include athletes where we have birthday info
                athlete['Birthday'] = birthday
                results.append(athlete)
        
        logging.info(f"Processed {len(results)} results for race {link[0]}")
        return results
        
    except Exception as e:
        logging.error(f"Error processing race results for {link[0]}: {e}")
        return []

def get_race_results(link: List[Any], sex: str) -> List[Dict]:
    """Synchronous wrapper for the async get_race_results function"""
    return asyncio.run(get_race_results_async(link, sex))

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
        
        # Convert types
        df = df.with_columns([
            pl.col('Date').str.strptime(pl.Date, format='%Y%m%d'),
            pl.col('Birthday').cast(pl.Datetime),
            pl.col('Place').cast(pl.Int64),
            pl.col('MS').cast(pl.Int64),
            pl.col('Season').cast(pl.Int64),
            pl.col('Race').cast(pl.Int64)
        ])
        
        # Calculate age
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
            #.add(1)
            .alias('Exp')
        ])
        
        logging.info(f"Created DataFrame with shape {df.shape}")
        return df
        
    except Exception as e:
        logging.error(f"Error constructing DataFrame: {e}")
        logging.error(traceback.format_exc())
        return None


def save_dataframes(men_df, ladies_df, base_path="~/ski/elo/python/ski/polars/excel365"):
    """Save DataFrames to both feather and CSV formats"""
    try:
        if men_df is not None:
            men_df.write_ipc(f"{base_path}/men_scrape.feather")
            men_df.write_csv(f"{base_path}/men_scrape.csv")
            logging.info("Saved men's historical data")
            
        if ladies_df is not None:
            ladies_df.write_ipc(f"{base_path}/ladies_scrape.feather")
            ladies_df.write_csv(f"{base_path}/ladies_scrape.csv")
            logging.info("Saved ladies' historical data")
            
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
    
    # Process men's data
    logging.info("Processing men's historical data")
    men_tables, men_results = process_year_range(1924, 2024, 'M')
    men_df = construct_historical_df(men_tables, men_results, 'M')
    
    # Process ladies' data
    logging.info("Processing ladies' historical data")
    ladies_tables, ladies_results = process_year_range(1924, 2024, 'L')
    ladies_df = construct_historical_df(ladies_tables, ladies_results, 'L')
    
    # Save the data
    save_dataframes(men_df, ladies_df)
    
    # Log execution time
    execution_time = time.time() - start_time
    logging.info(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == '__main__':
    main()