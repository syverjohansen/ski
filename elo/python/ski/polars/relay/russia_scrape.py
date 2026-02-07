"""
russia_scrape.py - Scraper for Russian cross-country skiing results from flgr-results.ru

Since Russia was banned from FIS events in 2022, this scraper collects results from
the Russian Cup and national championships held domestically.
"""

import logging
import ssl
import re
import unicodedata
from urllib.request import urlopen, Request
from urllib.error import URLError
from http.client import RemoteDisconnected, IncompleteRead
from bs4 import BeautifulSoup
import time
import polars as pl
from datetime import datetime, timezone
import warnings
import random
from typing import List, Dict, Any, Optional, Tuple
from thefuzz import fuzz
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Suppress warnings
warnings.filterwarnings('ignore')

# Set up SSL context and logging
ssl._create_default_https_context = ssl._create_unverified_context
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables
start_time = time.time()
BASE_URL = "https://flgr-results.ru"
FIS_BAN_DATE = "2022-03-01"  # Russia banned from FIS starting this date

# ============================================================================
# CYRILLIC TO LATIN TRANSLITERATION (BGN/PCGN Standard)
# ============================================================================

CYRILLIC_TO_LATIN = {
    'А': 'A', 'а': 'a', 'Б': 'B', 'б': 'b', 'В': 'V', 'в': 'v',
    'Г': 'G', 'г': 'g', 'Д': 'D', 'д': 'd', 'Е': 'E', 'е': 'e',
    'Ё': 'Yo', 'ё': 'yo', 'Ж': 'Zh', 'ж': 'zh', 'З': 'Z', 'з': 'z',
    'И': 'I', 'и': 'i', 'Й': 'Y', 'й': 'y', 'К': 'K', 'к': 'k',
    'Л': 'L', 'л': 'l', 'М': 'M', 'м': 'm', 'Н': 'N', 'н': 'n',
    'О': 'O', 'о': 'o', 'П': 'P', 'п': 'p', 'Р': 'R', 'р': 'r',
    'С': 'S', 'с': 's', 'Т': 'T', 'т': 't', 'У': 'U', 'у': 'u',
    'Ф': 'F', 'ф': 'f', 'Х': 'Kh', 'х': 'kh', 'Ц': 'Ts', 'ц': 'ts',
    'Ч': 'Ch', 'ч': 'ch', 'Ш': 'Sh', 'ш': 'sh', 'Щ': 'Shch', 'щ': 'shch',
    'Ъ': '', 'ъ': '', 'Ы': 'Y', 'ы': 'y', 'Ь': '', 'ь': '',
    'Э': 'E', 'э': 'e', 'Ю': 'Yu', 'ю': 'yu', 'Я': 'Ya', 'я': 'ya',
}

_TRANS_TABLE = str.maketrans({k: v for k, v in CYRILLIC_TO_LATIN.items() if len(v) <= 1})
_MULTI_CHAR_MAP = {k: v for k, v in CYRILLIC_TO_LATIN.items() if len(v) > 1}

def romanize(text: str) -> str:
    """Convert Cyrillic text to Latin using BGN/PCGN transliteration."""
    for cyr, lat in _MULTI_CHAR_MAP.items():
        text = text.replace(cyr, lat)
    return text.translate(_TRANS_TABLE)

def convert_name_format(cyrillic_name: str) -> str:
    """Convert 'LASTNAME Firstname' to 'Firstname Lastname' and romanize."""
    parts = cyrillic_name.strip().split()
    if len(parts) >= 2:
        lastname = romanize(parts[0].title())
        firstname = romanize(' '.join(parts[1:]))
        return f"{firstname} {lastname}"
    elif len(parts) == 1:
        return romanize(parts[0].title())
    return romanize(cyrillic_name)

# ============================================================================
# TRANSLATION DICTIONARIES
# ============================================================================

TECHNIQUE_MAP = {
    'СВ': 'F', 'св': 'F',  # Freestyle
    'КЛ': 'C', 'кл': 'C',  # Classic
}

# Race types to skip
RACE_TYPE_SKIP = frozenset({
    'Кв', 'Кв.', 'Квал', 'Квал.', 'Квалификация',  # Qualification
    'Полуфинал',  # Semi-final
    'ПФ',  # Semi-final abbreviation (ПолуФинал)
    'ЛР', 'Лыжероллеры',  # Roller ski
    'Кросс',  # Cross-country running/roller ski variant
})

# Summer months (June-September) indicate roller ski races
SUMMER_MONTHS = {6, 7, 8, 9}

EVENT_MAP = {
    'Чемпионат России': 'Russian Championship',
    'Первенство России': 'Russian Youth Championship',
    'Всероссийские соревнования': 'All-Russian Competition',
}

# Root-based patterns (handles grammatical case variations like Кубок/Кубка/Кубку)
# List of tuples checked in order - more specific patterns first
EVENT_ROOTS = [
    ('первенство федеральн', 'Federal District Youth Championship'),
    ('чемпионат федеральн', 'Federal District Championship'),
    ('спартакиад', 'Spartakiad'),
    ('универсиад', 'Universiade'),
    ('кубк', 'Russian Cup'),  # Кубок, Кубка, Кубку...
]

CITY_MAP = {
    'Москва': 'Moscow',
    'Санкт-Петербург': 'Saint Petersburg',
    'Тюмень': 'Tyumen',
    'Сыктывкар': 'Syktyvkar',
    'Красногорск': 'Krasnogorsk',
    'Малиновка': 'Malinovka',
    'Нижний Новгород': 'Nizhny Novgorod',
    'Ханты-Мансийск': 'Khanty-Mansiysk',
    'Мурманск': 'Murmansk',
    'Архангельск': 'Arkhangelsk',
    'Пересвет': 'Peresvet',
    'Вершина Тёи': 'Vershina Tei',
    'Кировск': 'Kirovsk',
}

def translate_city(city_cyrillic: str) -> str:
    """Translate city name or romanize if not in dictionary."""
    return CITY_MAP.get(city_cyrillic, romanize(city_cyrillic))

def translate_event(event_cyrillic: str) -> str:
    """Translate event name - extract just the event type."""
    # Check for exact matches first
    for rus, eng in EVENT_MAP.items():
        if rus in event_cyrillic:
            return eng
    # Check root-based patterns (case-insensitive for grammatical variations)
    event_lower = event_cyrillic.lower()
    for root, eng in EVENT_ROOTS:
        if root in event_lower:
            return eng
    # If not found, just romanize
    return romanize(event_cyrillic)

# ============================================================================
# PRE-COMPILED REGEX PATTERNS
# ============================================================================

RE_RACE_LINK = re.compile(r'/results/(\d+)/(\d+)')
RE_ATHLETE_LINK = re.compile(r'/athletes/(\d+)/')
# Match distance: number followed by "км" (handles mixed Cyrillic к/Latin k and м/m)
# Also handles various Unicode spaces and optional characters between number and km
RE_DISTANCE = re.compile(r'(\d+(?:[.,]\d+)?)\s*[кk][мm]', re.IGNORECASE)
# Fallback: just find numbers that look like distances (5-70 range, common ski distances)
RE_DISTANCE_FALLBACK = re.compile(r'\b(\d{1,2}(?:[.,]\d)?)\s*(?:[кk][мm]|км)', re.IGNORECASE)
RE_BIRTH_YEAR = re.compile(r'^\d{4}$')
RE_DATE_RANGE = re.compile(r'(\d{1,2})-\d{1,2}\.(\d{1,2})(?:\.(\d{4}))?')
RE_DATE_SINGLE = re.compile(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?')

# ============================================================================
# RATE LIMITING AND FETCHING
# ============================================================================

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

def fetch_with_retry(url: str, max_retries: int = 2, timeout: int = 10) -> Optional[str]:
    """Fetch URL with retry logic."""
    for attempt in range(max_retries):
        try:
            req = Request(url, headers=HEADERS)
            response = urlopen(req, timeout=timeout)
            return response.read().decode('utf-8')
        except (URLError, TimeoutError, RemoteDisconnected, ConnectionResetError, IncompleteRead) as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to fetch {url}: {e}")
                return None
            time.sleep(1)  # Brief retry delay
    return None

# ============================================================================
# RACE PARSING
# ============================================================================

def parse_race_info(race_text: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Parse race information from Russian text.
    Returns: (distance, mass_start, technique) or (None, None, None) to skip.
    """
    # Skip qualification, semi-final, roller ski, cross
    for skip_term in RACE_TYPE_SKIP:
        if skip_term in race_text:
            return None, None, None

    distance = "N/A"
    technique = "N/A"
    mass_start = 0

    # Normalize: replace ALL Unicode whitespace with regular space
    normalized = ''.join(
        ' ' if unicodedata.category(c) in ('Zs', 'Zl', 'Zp') else c
        for c in race_text
    )

    # Also normalize mixed Latin/Cyrillic characters
    normalized = normalized.replace('e', 'е').replace('E', 'Е')  # Latin e -> Cyrillic е
    normalized = normalized.replace('p', 'р').replace('P', 'Р')  # Latin p -> Cyrillic р
    normalized = normalized.replace('o', 'о').replace('O', 'О')  # Latin o -> Cyrillic о
    normalized = normalized.replace('a', 'а').replace('A', 'А')  # Latin a -> Cyrillic а
    normalized = normalized.replace('c', 'с').replace('C', 'С')  # Latin c -> Cyrillic с
    normalized = normalized.replace('x', 'х').replace('X', 'Х')  # Latin x -> Cyrillic х
    normalized_lower = normalized.lower()

    # Check for Overall Standings (Общий зачет, Итог) - set distance to 0
    if 'общий зачет' in normalized_lower or 'общий зачёт' in normalized_lower:
        return "0", 0, "N/A"
    if 'итог' in normalized_lower:
        # ТБВ - Итог, Минитур - Итог, etc.
        return "0", 0, "N/A"

    # Extract distance - try multiple patterns
    dist_match = None

    # Pattern 1: Standard regex on original and normalized text
    for text in [race_text, normalized]:
        if not dist_match:
            dist_match = RE_DISTANCE.search(text)
        if not dist_match:
            dist_match = RE_DISTANCE_FALLBACK.search(text)

    # Pattern 2: More aggressive - any number followed by anything that looks like "km"
    if not dist_match:
        # Try to match number + any 2-char combo that could be "km" in various encodings
        for text in [race_text, normalized]:
            match = re.search(r'(\d{1,2})\s*.{0,2}[кkКK][мmМM]', text)
            if match:
                dist_match = match
                break

    # Pattern 3: Ultimate fallback - just find a reasonable distance number (5-70)
    # in context of skiathlon, pursuit, or standalone
    if not dist_match:
        # Look for numbers that are likely distances
        all_numbers = re.findall(r'\b(\d{1,2})\b', race_text)
        for num in all_numbers:
            n = int(num)
            # Common ski distances: 5, 7.5, 10, 15, 20, 30, 50, 70
            if n in [5, 7, 10, 15, 20, 30, 50, 70]:
                distance = num
                break

    if dist_match and distance == "N/A":
        distance = dist_match.group(1).replace(',', '.')  # Handle comma decimal

    # Extract technique - check for СВ (Freestyle) and КЛ (Classic)
    # Check multiple variations to handle mixed Latin/Cyrillic
    text_upper = race_text.upper()
    normalized_upper = normalized.upper()

    # Freestyle: СВ, CB, св
    if any(x in text_upper for x in ['СВ', 'CB']) or any(x in normalized_upper for x in ['СВ', 'CB']):
        technique = 'F'
    # Classic: КЛ, KЛ, кл
    elif any(x in text_upper for x in ['КЛ', 'KЛ']) or any(x in normalized_upper for x in ['КЛ', 'KЛ']):
        technique = 'C'
    # Also check lowercase
    elif 'св' in normalized_lower or 'cb' in normalized_lower:
        technique = 'F'
    elif 'кл' in normalized_lower or 'kл' in normalized_lower:
        technique = 'C'

    # Check for sprint (overrides distance)
    if 'спр' in normalized_lower or 'спринт' in normalized_lower:
        distance = "Sprint"

    # Check for mass start
    if 'мс' in normalized_lower or 'масстарт' in normalized_lower or 'масс-старт' in normalized_lower:
        mass_start = 1

    # Check for skiathlon - sets technique to P, keeps extracted distance
    if 'скиатлон' in normalized_lower:
        technique = 'P'
        mass_start = 1

    # Check for relay (overrides distance)
    if 'эстафета' in normalized_lower or '4x' in normalized_lower or '3x' in normalized_lower:
        distance = "Rel"

    # Check for team sprint (overrides distance)
    # КС = Командный Спринт (Team Sprint)
    if 'командный спринт' in normalized_lower or 'ком. спр' in normalized_lower or 'кс ' in normalized_lower or normalized_lower.startswith('кс'):
        distance = "Ts"

    # Debug logging for cases where distance is still N/A
    if distance == "N/A":
        logging.warning(f"[parse_race_info] Could not extract distance from: {race_text!r}")

    return distance, mass_start, technique

def parse_date(date_text: str, season_year: int) -> Optional[str]:
    """Parse date from Russian format and return YYYY-MM-DD string."""
    try:
        match = RE_DATE_RANGE.match(date_text)
        if match:
            day, month = int(match.group(1)), int(match.group(2))
            year = int(match.group(3)) if match.group(3) else None
        else:
            match = RE_DATE_SINGLE.match(date_text)
            if not match:
                logging.warning(f"Could not parse date format: '{date_text}'")
                return None
            day, month = int(match.group(1)), int(match.group(2))
            year = int(match.group(3)) if match.group(3) else None

        if year is None:
            year = season_year if month >= 10 else season_year + 1

        return f"{year:04d}-{month:02d}-{day:02d}"
    except Exception as e:
        logging.warning(f"Error parsing date '{date_text}': {e}")
        return None

# ============================================================================
# SCRAPING FUNCTIONS
# ============================================================================

def fetch_season_race_links(year: int, sex: str = 'M') -> List[Dict[str, Any]]:
    """Fetch all race links for a given season and sex from the results page."""
    url = f"{BASE_URL}/results?season={year}"
    sex_label = 'Men' if sex == 'M' else 'Ladies'
    logging.info(f"Fetching season {year} for {sex_label}")

    html_content = fetch_with_retry(url)
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    race_links = []
    seen = set()

    # Men's races in column 5 (index 4), Women's in column 6 (index 5)
    race_col_idx = 4 if sex == 'M' else 5

    for row in soup.find_all('tr', class_='ant-table-row'):
        cells = row.find_all('td')
        if len(cells) <= race_col_idx:
            continue

        race_cell = cells[race_col_idx]

        # Extract date and event from other columns
        date_text = None
        if len(cells) > 1:
            date_div = cells[1].find('div')
            if date_div:
                date_text = date_div.get_text(strip=True)

        event_text = None
        if len(cells) > 2:
            event_link = cells[2].find('a')
            if event_link:
                event_text = event_link.get_text(strip=True)

        # Find all race links in this cell
        for link in race_cell.find_all('a', href=True):
            match = RE_RACE_LINK.match(link['href'])
            if not match:
                continue

            comp_id, race_id = match.group(1), match.group(2)
            key = (comp_id, race_id)
            if key in seen:
                continue
            seen.add(key)

            race_text = link.get_text(strip=True)
            distance, ms, technique = parse_race_info(race_text)
            if distance is None:
                continue

            race_links.append({
                'comp_id': comp_id,
                'race_id': race_id,
                'race_text': race_text,
                'distance': distance,
                'mass_start': ms,
                'technique': technique,
                'date_text': date_text,
                'event_text': event_text,
                'url': f"{BASE_URL}{link['href']}"
            })

    # REVERSE the order - HTML shows most recent first, we want chronological
    race_links.reverse()

    logging.info(f"Found {len(race_links)} races for season {year} ({sex_label})")
    return race_links

def fetch_race_page(race_url: str) -> Optional[BeautifulSoup]:
    """Fetch and parse a race page. Returns BeautifulSoup or None."""
    html_content = fetch_with_retry(race_url)
    if not html_content:
        return None
    return BeautifulSoup(html_content, 'html.parser')

def extract_results_from_soup(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Extract athlete results from a parsed race page."""
    results = []
    place = 0
    in_dnf_section = False

    # Find the main results table, excluding sanctions table
    # The sanctions table is inside div.race-sanctions
    sanctions_div = soup.find('div', class_='race-sanctions')
    sanctions_rows = set()
    if sanctions_div:
        for row in sanctions_div.find_all('tr', class_='ant-table-row'):
            sanctions_rows.add(id(row))

    for row in soup.find_all('tr', class_='ant-table-row'):
        # Skip rows that are in the sanctions table
        if id(row) in sanctions_rows:
            continue
        # Check for separator rows (DNF/DNS sections)
        row_key = row.get('data-row-key', '')
        if 'separator' in str(row_key):
            in_dnf_section = True
            continue

        cells = row.find_all('td')
        if len(cells) < 4:
            continue

        # Skip if place is "-", "-1", "999", or empty (DNF/DNS/DSQ)
        first_cell_text = cells[0].get_text(strip=True)
        if first_cell_text in ('-', '-1', '999', ''):
            continue

        # Skip if we're in DNF/DNS section
        if in_dnf_section:
            # Check if this row has a valid place number (means we're back to normal results)
            try:
                int(first_cell_text)
                in_dnf_section = False
            except ValueError:
                continue

        athlete_link = row.find('a', href=RE_ATHLETE_LINK)
        if not athlete_link:
            continue

        # Get place from first cell
        try:
            place = int(first_cell_text)
        except ValueError:
            continue

        # Extract athlete ID
        id_match = RE_ATHLETE_LINK.search(athlete_link['href'])
        russian_id = int(id_match.group(1)) if id_match else 0

        # Extract and convert name
        name_cyrillic = athlete_link.get_text(strip=True)
        name_latin = convert_name_format(name_cyrillic)

        # Find birth year
        birth_year = None
        for cell in cells:
            cell_text = cell.get_text(strip=True)
            if RE_BIRTH_YEAR.match(cell_text):
                year_val = int(cell_text)
                if 1950 <= year_val <= 2015:
                    birth_year = year_val
                    break

        results.append({
            'Place': place,
            'Skier': name_latin,
            'Russian_ID': russian_id,
            'Birth_Year': birth_year,
        })

    return results

def extract_metadata_from_soup(soup: BeautifulSoup, race_info: Dict[str, Any],
                                season: int) -> Dict[str, Any]:
    """Extract race metadata from a parsed race page."""
    # Try to get event from title tag first (more reliable)
    event = 'Russian Competition'
    distance = race_info['distance']
    technique = race_info['technique']

    title_tag = soup.find('title')
    title_text = ''
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # Title format: "Event - Distance - Sex - City - Date"
        # Extract just the first part (event type)
        for rus_event, eng_event in EVENT_MAP.items():
            if rus_event in title_text:
                event = eng_event
                break

    # If distance is N/A, try to extract from title
    if distance == "N/A" and title_text:
        logging.warning(f"[extract_metadata] Distance N/A, trying title: {title_text!r}")
        title_lower = title_text.lower()

        # Check for Overall Standings in title - set distance to 0
        if 'общий зачет' in title_lower or 'общий зачёт' in title_lower or 'итог' in title_lower:
            distance = "0"
            logging.warning(f"[extract_metadata] Detected overall standings, distance = 0")
        else:
            # Try to find distance in title
            dist_match = RE_DISTANCE.search(title_text)
            if dist_match:
                distance = dist_match.group(1).replace(',', '.')
                logging.warning(f"[extract_metadata] Extracted distance {distance} from title")
            else:
                # Fallback: look for common ski distances in title
                all_numbers = re.findall(r'\b(\d{1,2})\b', title_text)
                for num in all_numbers:
                    n = int(num)
                    if n in [5, 7, 10, 15, 20, 30, 50, 70]:
                        distance = num
                        logging.warning(f"[extract_metadata] Extracted distance {distance} via number fallback")
                        break

        # Still N/A? Log it
        if distance == "N/A":
            logging.warning(f"[extract_metadata] STILL N/A after title. Title: {title_text!r}")

    # If technique is N/A, try to extract from title
    if technique == "N/A" and title_text:
        title_upper = title_text.upper()
        if 'СВ' in title_upper or 'CB' in title_upper:
            technique = 'F'
        elif 'КЛ' in title_upper or 'KЛ' in title_upper:
            technique = 'C'

    # Extract date
    date_str = None
    date_elem = soup.find('div', class_='race-info__date')
    if date_elem:
        date_str = parse_date(date_elem.get_text(strip=True), season)

    # Extract city
    city = 'Unknown'
    city_elem = soup.find('div', class_='race-info__place')
    if city_elem:
        city = translate_city(city_elem.get_text(strip=True))

    return {
        'Date': date_str,
        'City': city,
        'Country': 'Russia',
        'Event': event,
        'Distance': distance,
        'MS': race_info['mass_start'],
        'Technique': technique,
        'Season': season
    }

# ============================================================================
# FUZZY MATCHING WITH FIS DATA
# ============================================================================

def load_fis_reference_data(sex: str, base_path: str) -> List[Dict[str, Any]]:
    """Load FIS reference data for Russian athletes as a list of dicts."""
    filename = 'all_men_scrape.csv' if sex == 'M' else 'all_ladies_scrape.csv'
    filepath = os.path.join(base_path, filename)

    try:
        df = pl.read_csv(filepath)
        russia_df = df.filter(pl.col('Nation') == 'Russia')
        athletes = russia_df.select(['ID', 'Skier', 'Birthday']).unique()
        result = athletes.to_dicts()
        logging.info(f"Loaded {len(result)} Russian athletes from FIS data for {sex}")
        return result
    except Exception as e:
        logging.error(f"Error loading FIS reference data: {e}")
        return []

def build_id_mapping(russian_athletes: List[Dict[str, Any]],
                     fis_athletes: List[Dict[str, Any]],
                     threshold: int = 80) -> Tuple[Dict[int, int], Dict[int, str], Dict[int, Any]]:
    """
    Build mapping from Russian IDs to FIS IDs using fuzzy matching.
    Returns: (id_mapping, name_mapping, birthday_mapping)
    - id_mapping: Dict[russian_id, fis_id]
    - name_mapping: Dict[russian_id, fis_name] (use FIS name when matched)
    - birthday_mapping: Dict[russian_id, fis_birthday] (use FIS birthday when matched)

    Uses greedy matching - best matches first, ensuring 1:1 mapping.
    """
    id_mapping = {}
    name_mapping = {}
    birthday_mapping = {}

    if not fis_athletes:
        for a in russian_athletes:
            rid = a['Russian_ID']
            id_mapping[rid] = -rid
            name_mapping[rid] = a['Skier']
            birthday_mapping[rid] = None
        return id_mapping, name_mapping, birthday_mapping

    # Pre-process FIS athletes
    fis_processed = []
    for fis in fis_athletes:
        fis_year = None
        if fis['Birthday']:
            try:
                bd = fis['Birthday']
                fis_year = int(bd[:4]) if isinstance(bd, str) else bd.year
            except:
                pass
        fis_processed.append({
            'id': fis['ID'],
            'name': fis['Skier'],
            'name_lower': fis['Skier'].lower(),
            'birth_year': fis_year,
            'birthday': fis['Birthday'],
            'matched': False
        })

    # Calculate all match scores
    match_candidates = []
    for athlete in russian_athletes:
        russian_id = athlete['Russian_ID']
        name_lower = athlete['Skier'].lower()
        birth_year = athlete['Birth_Year']

        for fis in fis_processed:
            score = fuzz.token_sort_ratio(name_lower, fis['name_lower'])

            # Boost score if birth years match
            if birth_year and fis['birth_year'] and birth_year == fis['birth_year']:
                score += 15

            if score >= threshold:
                match_candidates.append({
                    'russian_id': russian_id,
                    'russian_name': athlete['Skier'],
                    'russian_birth_year': birth_year,
                    'fis_idx': fis_processed.index(fis),
                    'fis_id': fis['id'],
                    'fis_name': fis['name'],
                    'fis_birthday': fis['birthday'],
                    'score': score
                })

    # Sort by score descending - best matches first
    match_candidates.sort(key=lambda x: x['score'], reverse=True)

    # Greedy matching - assign best matches first
    matched_russian_ids = set()
    matched_fis_ids = set()

    for candidate in match_candidates:
        rid = candidate['russian_id']
        fid = candidate['fis_id']

        # Skip if either already matched
        if rid in matched_russian_ids or fid in matched_fis_ids:
            continue

        # Make the match
        id_mapping[rid] = fid
        name_mapping[rid] = candidate['fis_name']  # Use FIS name
        birthday_mapping[rid] = candidate['fis_birthday']  # Use FIS birthday
        matched_russian_ids.add(rid)
        matched_fis_ids.add(fid)

    # Assign negative IDs and original names to unmatched athletes
    for athlete in russian_athletes:
        rid = athlete['Russian_ID']
        if rid not in id_mapping:
            id_mapping[rid] = -rid
            name_mapping[rid] = athlete['Skier']  # Use romanized name
            birthday_mapping[rid] = None

    matched = len(matched_russian_ids)
    total = len(russian_athletes)
    match_pct = (100 * matched / total) if total > 0 else 0
    logging.info(f"Matched {matched}/{total} athletes ({match_pct:.1f}%)")
    return id_mapping, name_mapping, birthday_mapping

# ============================================================================
# DATAFRAME CONSTRUCTION
# ============================================================================

def construct_dataframe(all_races: List[Dict[str, Any]],
                        id_mapping: Dict[int, int],
                        name_mapping: Dict[int, str],
                        birthday_mapping: Dict[int, Any],
                        sex: str) -> Optional[pl.DataFrame]:
    """Construct the final DataFrame with all results."""
    if not all_races:
        return None

    all_rows = []
    race_num = 0

    for race in all_races:
        if not race['results']:
            continue

        metadata = race['metadata']

        if not metadata.get('Date'):
            logging.warning(f"Skipping race without date: {metadata.get('Event', 'Unknown')}")
            continue

        if metadata['Date'] < FIS_BAN_DATE:
            logging.info(f"Skipping pre-ban race: {metadata['Date']}")
            continue

        # Skip summer months (June-September) - these are roller ski races
        try:
            race_month = int(metadata['Date'].split('-')[1])
            if race_month in SUMMER_MONTHS:
                logging.info(f"Skipping summer roller ski race: {metadata['Date']}")
                continue
        except (ValueError, IndexError):
            pass  # If date parsing fails, continue with the race

        race_num += 1

        for result in race['results']:
            russian_id = result['Russian_ID']
            fis_id = id_mapping.get(russian_id, -russian_id)

            # Use FIS name if matched, otherwise use romanized name
            skier_name = name_mapping.get(russian_id, result['Skier'])

            # Use FIS birthday if matched, otherwise use birth year from Russian data
            fis_birthday = birthday_mapping.get(russian_id)
            if fis_birthday:
                # Parse FIS birthday
                try:
                    if isinstance(fis_birthday, str):
                        birthday = datetime.strptime(fis_birthday[:10], '%Y-%m-%d')
                    else:
                        birthday = fis_birthday
                except:
                    birthday = datetime(result['Birth_Year'], 1, 1) if result['Birth_Year'] else None
            elif result['Birth_Year']:
                birthday = datetime(result['Birth_Year'], 1, 1)
            else:
                birthday = None

            all_rows.append({
                'Date': metadata['Date'],
                'City': metadata['City'],
                'Country': metadata['Country'],
                'Event': metadata['Event'],
                'Distance': str(metadata['Distance']),
                'MS': metadata['MS'],
                'Technique': metadata['Technique'],
                'Season': metadata['Season'],
                'Race': race_num,
                'Sex': sex,
                'Place': result['Place'],
                'Skier': skier_name,
                'Nation': 'Russia',
                'ID': fis_id,
                'Birthday': birthday
            })

    if not all_rows:
        return None

    df = pl.DataFrame(all_rows)

    df = df.with_columns([
        pl.col('Date').str.strptime(pl.Date, format='%Y-%m-%d'),
        pl.col('Birthday').cast(pl.Datetime),
        pl.col('Place').cast(pl.Int64),
        pl.col('MS').cast(pl.Int64),
        pl.col('Season').cast(pl.Int64),
        pl.col('Race').cast(pl.Int64),
        pl.col('ID').cast(pl.Int64)
    ])

    df = df.with_columns(
        ((pl.col('Date').cast(pl.Datetime) - pl.col('Birthday')).dt.total_days() / 365.25)
        .cast(pl.Float64)
        .alias('Age')
    )

    df = df.sort(['ID', 'Date', 'Race']).with_columns(
        pl.col('ID').cum_count().over('ID').cast(pl.Int32).alias('Exp')
    )

    return df

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_single_race(race_info: Dict[str, Any], year: int) -> Optional[Dict[str, Any]]:
    """Process a single race - for parallel execution."""
    url = race_info['url']

    soup = fetch_race_page(url)
    if not soup:
        return None

    results = extract_results_from_soup(soup)
    if not results:
        return None

    # Get metadata
    date_str = None
    if race_info.get('date_text'):
        date_str = parse_date(race_info['date_text'], year)

    event = 'Russian Competition'
    if race_info.get('event_text'):
        event = translate_event(race_info['event_text'])

    metadata = {
        'Date': date_str,
        'City': 'Russia',
        'Country': 'Russia',
        'Event': event,
        'Distance': race_info['distance'],
        'MS': race_info['mass_start'],
        'Technique': race_info['technique'],
        'Season': year
    }

    # Enhance with data from race page
    page_meta = extract_metadata_from_soup(soup, race_info, year)
    if page_meta.get('Date'):
        metadata['Date'] = page_meta['Date']
    if page_meta.get('City') and page_meta['City'] != 'Unknown':
        metadata['City'] = page_meta['City']
    if page_meta.get('Event') and page_meta['Event'] != 'Russian Competition':
        metadata['Event'] = page_meta['Event']
    # Use Distance from page if link text didn't have it
    if metadata['Distance'] == 'N/A' and page_meta.get('Distance') and page_meta['Distance'] != 'N/A':
        metadata['Distance'] = page_meta['Distance']
    # Use Technique from page if link text didn't have it
    if metadata['Technique'] == 'N/A' and page_meta.get('Technique') and page_meta['Technique'] != 'N/A':
        metadata['Technique'] = page_meta['Technique']

    return {'metadata': metadata, 'results': results}

def process_season(year: int, sex: str) -> List[Dict[str, Any]]:
    """Process all races for a given season and sex using parallel execution."""
    sex_label = 'Men' if sex == 'M' else 'Ladies'
    logging.info(f"Processing season {year} for {sex_label}")

    race_links = fetch_season_race_links(year, sex)
    if not race_links:
        return []

    all_races = []
    max_workers = 50  # High parallelism - no rate limiting

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_race, race_info, year): race_info
            for race_info in race_links
        }

        for future in as_completed(futures):
            race_info = futures[future]
            try:
                result = future.result()
                if result:
                    all_races.append(result)
            except Exception as e:
                logging.error(f"Error processing race {race_info.get('url', 'unknown')}: {e}")

    # Sort by date to maintain chronological order (since parallel execution scrambles order)
    all_races.sort(key=lambda x: x['metadata'].get('Date') or '9999-99-99')

    logging.info(f"Processed {len(all_races)} races for season {year}")
    return all_races

def save_id_mapping(mapping: Dict[int, int], sex: str, base_path: str):
    """Save the Russian ID to FIS ID mapping to CSV."""
    rows = [{'Russian_ID': k, 'FIS_ID': v} for k, v in mapping.items()]
    df = pl.DataFrame(rows)

    sex_name = 'men' if sex == 'M' else 'ladies'
    filepath = os.path.join(base_path, f"russia_{sex_name}_id_mapping.csv")
    df.write_csv(filepath)
    logging.info(f"Saved ID mapping to {filepath}")

def get_last_season_year() -> int:
    """
    Determine the last completed season year based on current UTC date.
    Ski seasons are named for the year they end in:
    - Oct 1 - Dec 31: Year (current is Year+1, so last complete is Year)
    - Jan 1 - Sep 30: Year - 1 (current is Year, so last complete is Year-1)
    """
    now = datetime.now(timezone.utc)
    if now.month >= 10:  # Oct-Dec: new season (Year+1) has started
        return now.year
    else:  # Jan-Sep: in season Year
        return now.year - 1


def main():
    """Main execution function."""
    logging.info("Starting Russia scraper (historical)")

    base_path = os.path.expanduser("~/ski/elo/python/ski/polars/relay/excel365")
    start_year = 2022
    end_year = get_last_season_year()

    logging.info(f"Scraping seasons {start_year} through {end_year}")

    for sex in ['M', 'L']:
        sex_label = 'Men' if sex == 'M' else 'Ladies'
        logging.info(f"\n{'='*50}\nProcessing {sex_label}\n{'='*50}")

        # Collect all races
        all_races = []
        for year in range(start_year, end_year + 1):
            races = process_season(year, sex)
            all_races.extend(races)

        if not all_races:
            logging.warning(f"No races found for {sex_label}")
            continue

        # Collect unique athletes for ID matching
        athlete_map = {}
        for race in all_races:
            for result in race['results']:
                rid = result['Russian_ID']
                if rid not in athlete_map:
                    athlete_map[rid] = {
                        'Russian_ID': rid,
                        'Skier': result['Skier'],
                        'Birth_Year': result['Birth_Year']
                    }
        unique_athletes = list(athlete_map.values())

        # Load FIS reference data and build ID mapping (80% threshold)
        fis_athletes = load_fis_reference_data(sex, base_path)
        id_mapping, name_mapping, birthday_mapping = build_id_mapping(
            unique_athletes, fis_athletes, threshold=90
        )

        # Save ID mapping
        save_id_mapping(id_mapping, sex, base_path)

        # Construct and save final DataFrame
        df = construct_dataframe(all_races, id_mapping, name_mapping, birthday_mapping, sex)
        if df is not None:
            sex_name = 'men' if sex == 'M' else 'ladies'
            filepath = os.path.join(base_path, f"russia_{sex_name}_scrape.csv")
            df.write_csv(filepath)
            logging.info(f"Saved {len(df)} results to {filepath}")

        time.sleep(2)

    logging.info(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")

if __name__ == '__main__':
    main()
