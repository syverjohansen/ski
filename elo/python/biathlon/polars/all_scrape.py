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
from typing import List, Dict, Any, Optional, Tuple, Set
import asyncio
import aiohttp
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
    """Fetch all race links for a given season with improved duplicate detection"""
    base_url = f"https://firstskisport.com/biathlon/calendar.php?y={year}&hva=k"
    if sex == 'L':
        base_url += "&g=w"

    try:
        html_content = fetch_with_retry(base_url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        processed_races = set()  # Track unique race identifiers

        # Find all table rows - this gives us the context for each race
        race_rows = soup.find_all('tr')

        for row in race_rows:
            # Skip rows without enough cells
            cells = row.find_all('td')
            if len(cells) < 5:  # We need at least 5 cells to have discipline info
                continue

            # The race discipline is in the 5th column (index 4)
            # Contains links like <a href="results.php?id=1282" title="Results">Single Mixed</a>
            discipline_cell = cells[4]
            discipline_link = discipline_cell.find('a', {'title': 'Results'})

            if not discipline_link:
                continue

            # Extract race ID from the URL
            match = re.search(r'id=(\d+)', discipline_link['href'])
            if not match:
                continue

            race_id = match.group(1)
            race_type_text = discipline_link.text.strip()


            # Check if this is a mixed relay
            is_mixed_relay = "Mixed" in race_type_text

            # Skip mixed relays for ladies since they'll be processed from men's calendar
            if sex == 'L' and is_mixed_relay:
                continue

            # Construct full URL for this race
            race_url = 'https://firstskisport.com/biathlon/' + discipline_link['href']

            # For mixed relays, never add the gender parameter
            if is_mixed_relay:
                full_url = race_url
            elif sex == 'L':
                # For ladies' races, add g=w if needed
                if "g=w" not in race_url:
                    if "?" in race_url:
                        full_url = race_url + "&g=w"
                    else:
                        full_url = race_url + "?g=w"
                else:
                    full_url = race_url
            else:
                # Men's races - use URL as is
                full_url = race_url

            # Skip if we've already processed this race ID
            if race_id in processed_races:
                #print("Continuing because race was already processed")
                continue

            # Add race ID to processed set
            processed_races.add(race_id)

            # Calculate race number based on order in the table
            race_num = len(links) + 1

            # Add to links
            links.append([full_url, year, race_num, is_mixed_relay])

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
        'ü': 'u', 'Ü': 'U',
        'ë': 'e', 'Ë': 'E',
        'č': 'c', 'Č': 'C',
        'š': 's', 'Š': 'S',
        'ž': 'z', 'Ž': 'Z'
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
        url = f"https://firstskisport.com/biathlon/athlete.php?id={athlete_id}"
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

def parse_race_info(race_text):
    """Parse biathlon race information"""
    # Clean the text
    race_text = race_text.strip()

    # Default values
    distance = "N/A"
    race_type = "Individual"
    mass_start = 0

    # Extract distance if present at the beginning (e.g., "10 km Sprint")
    distance_match = re.search(r'^(\d+(?:\.\d+)?)\s*km', race_text, re.IGNORECASE)
    if distance_match:
        distance = distance_match.group(1)

    # Determine race type
    if "Sprint" in race_text:
        race_type = "Sprint"
    elif "Pursuit" in race_text:
        race_type = "Pursuit"
    elif "Individual" in race_text:
        race_type = "Individual"
    elif "Mass Start" in race_text:
        race_type = "Mass Start"
        mass_start = 1
    elif any(x in race_text for x in ["Mixed Relay", "Mixed"]) and "Single" not in race_text:
        race_type = "Mixed Relay"
    elif "Single Mixed" in race_text:
        race_type = "Single Mixed Relay"
    elif "Relay" in race_text:
        race_type = "Relay"

    return distance, race_type, mass_start

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

        # Extract race header (example: "10 km Sprint - Kontiolahti")
        h1_tag = soup.body.find('h1')
        if not h1_tag:
            logging.error(f"No race title found for {url}")
            return None

        race_title = h1_tag.text.strip()

        # Extract from h2 tag (example: "Biathlon, World Cup, Friday 6.Dec 2024, Finland")
        h2_tag = soup.body.find('h2')
        if not h2_tag:
            logging.error(f"No race details found for {url}")
            return None

        race_details = h2_tag.text.strip()
        details_parts = race_details.split(',')

        if len(details_parts) < 4:
            logging.error(f"Invalid race details format: {race_details}")
            return None

        # Parse date
        date_part = [part for part in details_parts if 'day' in part.lower() or 'day' in part.lower()]
        if not date_part:
            logging.error(f"No date found in {details_parts}")
            return None

        date_text = date_part[0].strip()
        try:
            # Extract day, month, year from text like "Friday 6.Dec 2024"
            date_match = re.search(r'(\d{1,2})\.([A-Za-z]{3})\s+(\d{4})', date_text)
            if not date_match:
                logging.error(f"Failed to parse date from {date_text}")
                return None

            day = date_match.group(1)
            month = date_match.group(2)
            year = date_match.group(3)
            date_obj = datetime.strptime(f"{day}.{month} {year}", "%d.%b %Y")
            date = date_obj.strftime("%Y%m%d")
        except Exception as e:
            logging.error(f"Error parsing date {date_text}: {e}")
            return None

        # Parse location and event
        race_parts = race_title.split(" - ")
        if len(race_parts) < 2:
            logging.error(f"Invalid race title format: {race_title}")
            return None

        city = race_parts[-1].strip()
        race_info = race_parts[0].strip()

        # Parse country from details
        country = details_parts[-1].strip()

        # Extract event type (usually "World Cup" or similar)
        event = details_parts[1].strip()

        # Parse race details
        distance, race_type, mass_start = parse_race_info(race_info)

        result = [date, city, country, event, distance, race_type, mass_start, season, race_num]
        logging.info(f"Extracted race data: {result}")
        return result

    except Exception as e:
        logging.error(f"Error processing race {url}: {e}")
        logging.error(traceback.format_exc())
        return None

async def fetch_athlete_info_batch(session: aiohttp.ClientSession, athlete_ids: List[str], sex_map: Dict[str, str]):
    """Fetch athlete information in parallel batches with gender mapping"""
    async def fetch_single_athlete(athlete_id: str, athlete_sex: str):
        cache_key = (athlete_id, athlete_sex)

        # Check cache first
        if cache_key in SKIER_INFO_CACHE['birthdays']:
            return athlete_id, SKIER_INFO_CACHE['birthdays'][cache_key]

        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Respect rate limiting
        await asyncio.sleep(0.2)  # 5 requests per second rate limit

        url = f"https://firstskisport.com/biathlon/athlete.php?id={athlete_id}"
        if athlete_sex == 'L':
            url += "&g=w"

        try:
            async with session.get(url, ssl=ssl_context) as response:
                if response.status != 200:
                    return athlete_id, None

                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                info = extract_athlete_info(soup, athlete_id, athlete_sex)

                if info and info['birthday']:
                    SKIER_INFO_CACHE['birthdays'][cache_key] = info['birthday']
                    return athlete_id, info['birthday']

        except Exception as e:
            logging.error(f"Error fetching athlete {athlete_id}: {e}")

        return athlete_id, None

    # Create tasks for all uncached athletes
    tasks = []
    for athlete_id in athlete_ids:
        athlete_sex = sex_map.get(athlete_id, 'M')  # Default to M if not found
        if (athlete_id, athlete_sex) not in SKIER_INFO_CACHE['birthdays']:
            tasks.append(fetch_single_athlete(athlete_id, athlete_sex))

    # Execute all tasks in parallel
    if tasks:
        results = await asyncio.gather(*tasks)
        return dict(results)
    return {}

def format_skier_name(name):
    """Convert 'LASTNAME Firstname' to 'Firstname Lastname'"""
    # Extract from HTML structure if possible
    try:
        # Look for the pattern: <span style="text-transform:uppercase;">LASTNAME</span> Firstname
        match = re.search(r'<span style="text-transform:uppercase;">([^<]+)</span>\s*([^<]+)', name)
        if match:
            last_name = match.group(1).strip()
            first_name = match.group(2).strip()
            return f"{first_name} {last_name}"
    except:
        pass

    # Try text-based parsing if HTML parsing didn't work
    name_text = re.sub(r'<[^>]+>', '', name).strip()
    parts = name_text.split()

    if len(parts) >= 2:
        # Find all uppercase parts (likely last name)
        uppercase_indices = [i for i, part in enumerate(parts) if part.isupper()]
        if uppercase_indices:
            last_name = ' '.join(parts[i] for i in uppercase_indices).title()
            first_name = ' '.join(parts[i] for i in range(len(parts)) if i not in uppercase_indices)
            return f"{first_name} {last_name}"

    # If we can't parse properly, return original text without HTML
    return name_text

def determine_athlete_sex(cell_text, default_sex='M'):
    """Determine athlete sex from cell HTML content"""
    if 'female.png' in cell_text or 'g=w' in cell_text:

        return 'L'
    elif 'male.png' in cell_text:
        return 'M'
    else:
        # If we can't determine from the HTML, use the default provided
        return default_sex

async def get_race_results_async(link: List[Any], default_sex: str) -> Tuple[List[Dict], Dict[str, str]]:
    """Extract results from race page with batch processing of athlete info"""
    try:
        html_content = fetch_with_retry(link[0])
        if not html_content:
            return [], {}

        soup = BeautifulSoup(html_content, 'html.parser')

        # Find results table
        tables = soup.find_all('table', {'class': 'tablesorter sortTabell'})
        if not tables:
            logging.error(f"No results table found for {link[0]}")
            return [], {}

        # First pass: collect all athlete IDs, basic info, and determine sex
        athletes_data = []
        athlete_ids = set()
        sex_map = {}  # Map of athlete ID to sex

        # Get race type for processing
        race_data = get_race_data(link)
        is_team_event = False
        if race_data and race_data[5] in ["Relay", "Mixed Relay", "Single Mixed Relay"]:
            is_team_event = True

        # Find all rows (td cells)
        rows = tables[0].find_all('tr')[1:]  # Skip header row

        for row in rows:
            cells = row.find_all('td')

            # Skip if not enough cells
            if len(cells) < 3:
                continue

            # Extract place
            place_cell = cells[0].text.strip()
            if not place_cell.isdigit() or place_cell in {"DNF", "DSQ", "DNS", "DNQ", "OOT", ""}:
                continue

            # Handle leg information for team events
            leg_num = 0
            if is_team_event and len(cells) > 1:
                leg_cell = cells[1].text.strip()
                if "smore" in str(cells[1]):
                    leg_text = cells[1].find("span", {"class": "smore"}).text.strip() if cells[1].find("span", {"class": "smore"}) else ""
                    if "-" in leg_text:
                        try:
                            leg_num = int(leg_text.split("-")[1])
                        except (ValueError, IndexError):
                            leg_num = 0

            # Extract skier name and ID
            skier_cell = cells[2] if is_team_event else cells[2]
            skier_cell_str = str(skier_cell)

            # Determine sex from cell content
            athlete_sex = determine_athlete_sex(skier_cell_str)


            # Extract athlete ID from link
# Extract athlete ID from link
# Extract athlete ID and sex from link
            if 'athlete.php?id=' in skier_cell_str:
                # Extract ID first
                id_match = re.search(r'athlete\.php\?id=(\d+)', skier_cell_str)
                if id_match:
                    athlete_id = id_match.group(1)

                    # Determine the athlete's sex
                    if 'female.png' in skier_cell_str or '&g=w' in skier_cell_str:
                        athlete_sex = 'L'
                    elif 'male.png' in skier_cell_str:
                        athlete_sex = 'M'
                    else:
                        # If we can't determine from HTML indicators, use the default provided
                        athlete_sex = default_sex

                    # Get the name from the title attribute first (for male athletes mostly)
                    title_match = re.search(r'title="([^"]+)"', skier_cell_str)
                    if title_match:
                        skier_name = title_match.group(1)
                    else:
                        # Alternative method: extract from the text pattern with span
                        name_match = re.search(r'<span style="text-transform:uppercase;">([^<]+)</span>\s*([^<]+)', skier_cell_str)
                        if name_match:
                            last_name = name_match.group(1).strip()
                            first_name = name_match.group(2).strip()
                            skier_name = f"{first_name} {last_name}"
                        else:
                            # Last resort: just use the plain text from the cell
                            skier_name = re.sub(r'<[^>]+>', '', skier_cell.text).strip()

                    # Extract nation
                    nation_cell = cells[4] if is_team_event else cells[4]
                    nation = nation_cell.text.strip()

                    # Record sex mapping
                    sex_map[athlete_id] = athlete_sex

                    # Store athlete data
                    athletes_data.append({
                        'Place': int(place_cell),
                        'Skier': skier_name,
                        'Nation': nation,
                        'ID': athlete_id,
                        'Sex': athlete_sex,
                        'Leg': leg_num
                    })
                    athlete_ids.add(athlete_id)

        # Create SSL context for the session
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Batch fetch athlete info with sex mapping
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            birthdays = await fetch_athlete_info_batch(session, list(athlete_ids), sex_map)

        # Combine results with fetched birthdays
# Modified version that keeps all athletes
        results = []
        for athlete in athletes_data:
            birthday = birthdays.get(athlete['ID']) or SKIER_INFO_CACHE['birthdays'].get((athlete['ID'], athlete['Sex']))
            # Include all athletes, with or without birthday info
            athlete['Birthday'] = birthday  # This will be None if no birthday was found
            results.append(athlete)

            if not birthday:
                logging.warning(f"Could not determine birthday for {athlete['Skier']} (ID: {athlete['ID']})")

        logging.info(f"Processed {len(results)} results for race {link[0]}")
        return results, sex_map

    except Exception as e:
        logging.error(f"Error processing race results for {link[0]}: {e}")
        logging.error(traceback.format_exc())
        return [], {}

def get_race_results(link: List[Any], default_sex: str) -> Tuple[List[Dict], Dict[str, str]]:
    """Synchronous wrapper for the async get_race_results function"""
    return asyncio.run(get_race_results_async(link, default_sex))

def construct_historical_df(tables, results_data, sex_maps):
    """Construct DataFrame from historical race data"""
    try:
        logging.info(f"Constructing DataFrame")

        # Filter out None values and empty results
        valid_data = [(table, results, sex_map)
                     for table, results, sex_map in zip(tables, results_data, sex_maps)
                     if table is not None and results]

        if not valid_data:
            logging.error("No valid data to process")
            return None

        tables, results_data, sex_maps = zip(*valid_data)

        # Create base race information DataFrame
        table_df = pl.DataFrame(
            data=tables,
            schema=["Date", "City", "Country", "Event", "Distance", "RaceType",
                   "MassStart", "Season", "Race"],
            orient="row"
        )

        # Create results DataFrame
        all_results = []
        for idx, (race_results, sex_map) in enumerate(zip(results_data, sex_maps)):
            race_table = tables[idx]
            for result in race_results:
                row = {
                    'Date': race_table[0],
                    'City': race_table[1],
                    'Country': race_table[2],
                    'Event': race_table[3],
                    'Distance': race_table[4],
                    'RaceType': race_table[5],
                    'MassStart': race_table[6],
                    'Season': race_table[7],
                    'Race': race_table[8],
                    'Place': result['Place'],
                    'Skier': result['Skier'],
                    'Nation': result['Nation'],
                    'ID': result['ID'],
                    'Sex': result['Sex'],
                    'Birthday': result['Birthday'],
                    'Leg': result.get('Leg', 0)
                }
                all_results.append(row)

        # Create DataFrame from all results
        df = pl.DataFrame(all_results)

        # Convert types - handle None birthdays properly
        df = df.with_columns([
            pl.col('Date').str.strptime(pl.Date, format='%Y%m%d'),
            pl.col('Birthday').cast(pl.Datetime),  # Polars handles None values automatically
            pl.col('Place').cast(pl.Int64),
            pl.col('MassStart').cast(pl.Int64),
            pl.col('Season').cast(pl.Int64),
            pl.col('Race').cast(pl.Int64),
            pl.col('Leg').cast(pl.Int64)
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
            .over(['ID', 'Skier'])
            .cast(pl.Int32)
            .alias('Exp')
        ])

        # Split by sex for proper data organization
        men_df = df.filter(pl.col('Sex') == 'M')
        ladies_df = df.filter(pl.col('Sex') == 'L')

        logging.info(f"Created DataFrames with shapes: Men {men_df.shape}, Ladies {ladies_df.shape}")
        return men_df, ladies_df

    except Exception as e:
        logging.error(f"Error constructing DataFrame: {e}")
        logging.error(traceback.format_exc())
        return None, None

def save_dataframes(men_df, ladies_df, base_path="~/ski/elo/python/biathlon/polars/excel365"):
    """Save DataFrames to both feather and CSV formats"""
    try:
        if men_df is not None:
            men_df.write_csv(f"{base_path}/all_men_scrape.csv")
            logging.info("Saved men's historical data")

        if ladies_df is not None:
            ladies_df.write_csv(f"{base_path}/all_ladies_scrape.csv")
            logging.info("Saved ladies' historical data")

    except Exception as e:
        logging.error(f"Error saving data: {e}")
        logging.error(traceback.format_exc())

def process_year_range(start_year, end_year):
    """Process a range of years for biathlon (handling men and women separately)"""
    all_tables = []
    all_results = []
    all_sex_maps = []

    def process_race(link, default_sex):
        """Process a single race"""
        try:
            # Get race data
            table_data = get_race_data(link)
            if table_data is None:
                return None

            # Get results and sex mapping with specified default sex
            results, sex_map = get_race_results(link, default_sex)
            if not results:
                return None

            return (table_data, results, sex_map)

        except Exception as e:
            logging.error(f"Error processing race {link}: {e}")
            return None

    for year in range(start_year, end_year + 1):
        # Process men's races
        logging.info(f"Processing men's races for year {year}")
        men_season_links = fetch_season_links(year, 'M')
        if men_season_links:
            max_workers = min(32, multiprocessing.cpu_count() * 4)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                race_results = list(executor.map(lambda link: process_race(link, 'M'), men_season_links))

            # Filter out None results and append valid ones
            for result in race_results:
                if result is not None:
                    table_data, results, sex_map = result
                    all_tables.append(table_data)
                    all_results.append(results)
                    all_sex_maps.append(sex_map)

        # Process women's races separately
        logging.info(f"Processing women's races for year {year}")
        women_season_links = fetch_season_links(year, 'L')
        if women_season_links:
            max_workers = min(32, multiprocessing.cpu_count() * 4)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                race_results = list(executor.map(lambda link: process_race(link, 'L'), women_season_links))

            # Filter out None results and append valid ones
            for result in race_results:
                if result is not None:
                    table_data, results, sex_map = result
                    all_tables.append(table_data)
                    all_results.append(results)
                    all_sex_maps.append(sex_map)

        # Delay between years
        time.sleep(random.uniform(1, 2))

    return all_tables, all_results, all_sex_maps

def main():
    """Main execution function"""
    check_environment()
    setup_cache_structure()

    # Process biathlon data for all years (1958-current)
    logging.info("Processing biathlon historical data")
    current_year = datetime.now().year
    tables, results, sex_maps = process_year_range(1958, current_year)
    men_df, ladies_df = construct_historical_df(tables, results, sex_maps)

    # Save the data
    save_dataframes(men_df, ladies_df)

    # Log execution time
    execution_time = time.time() - start_time
    logging.info(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == '__main__':
    main()
