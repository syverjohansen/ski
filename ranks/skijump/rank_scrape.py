import logging
import ssl
import re
from urllib.request import urlopen
from urllib.error import URLError
import pandas as pd
import time
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from typing import List, Dict, Optional
import os
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global settings
ssl._create_default_https_context = ssl._create_unverified_context
BASE_URL = "https://firstskisport.com/ski-jumping/ranking.php"

class RateLimit:
    """Rate limiter for API requests"""
    def __init__(self, max_per_second=2):
        self.delay = 1.0 / max_per_second
        self.last_call = 0

    async def wait(self):
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.delay:
            await asyncio.sleep(self.delay - elapsed)
        self.last_call = time.time()

rate_limiter = RateLimit()

async def fetch_standings_page(session: aiohttp.ClientSession, year: int, gender: str = 'M', tournament: str = '') -> Optional[str]:
    """Fetch standings page with retries"""
    url = f"{BASE_URL}?y={year}"
    if gender == 'L':
        url += "&g=w"
    if tournament:
        url += f"&hva={tournament}"
    
    # Create SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
        
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await rate_limiter.wait()
            async with session.get(url, ssl=ssl_context) as response:
                if response.status == 200:
                    return await response.text()
                logging.warning(f"Failed to fetch {url}, status: {response.status}")
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
        if attempt < max_retries - 1:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return None

def parse_standings(html_content: str, season: int, gender: str, event_type: str = "Standings") -> List[Dict]:
    """Parse the standings table from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the standings table - Ski Jumping uses different table classes
    table = soup.find('table', {'class': 'sortTabell tablesorter'})
    if not table:
        table = soup.find('table', {'class': 'tablesorter'})
    if not table:
        table = soup.find('table', {'class': 'sortTabell'})
    if not table:
        # Try finding any table with sortTabell or tablesorter in the class
        table = soup.find('table', class_=lambda x: x and ('sortTabell' in x or 'tablesorter' in x))
    if not table:
        logging.warning(f"No standings table found for {season} {gender} {event_type}")
        return []

    standings = []
    
    # Find tbody to get data rows
    tbody = table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
    else:
        rows = table.find_all('tr')[1:]  # Skip header if no tbody
    
    if len(rows) == 0:
        logging.warning(f"No data rows found in standings table for {season} {gender} {event_type}")
        return []
    
    for row in rows:
        try:
            cells = row.find_all('td')
            if not cells or len(cells) < 3:
                continue

            # Extract skier ID and name from first cell
            skier_link = cells[0].find('a')
            if not skier_link or 'href' not in skier_link.attrs:
                continue
            
            skier_id_match = re.search(r'id=(\d+)', skier_link['href'])
            if not skier_id_match:
                continue
            skier_id = skier_id_match.group(1)

            # Extract skier name - ski jumping uses span with uppercase surname
            name_span = cells[0].find('span')
            if name_span:
                last_name = name_span.text.strip()
                # Get the full text and extract first name
                full_text = cells[0].get_text()
                first_name = full_text.replace(last_name, '').strip()
                skier_name = f"{first_name} {last_name}".strip()
            else:
                # Fallback to full text
                skier_name = cells[0].get_text(strip=True)
            
            # Extract nation from second cell
            nation_img = cells[1].find('img')
            if nation_img and 'title' in nation_img.attrs:
                nation = nation_img['title']
            else:
                # Fallback to text content if image not found
                nation_text = cells[1].get_text(strip=True)
                # Extract nation code if it exists
                if any(word.isupper() and len(word) == 3 for word in nation_text.split()):
                    nation = next(word for word in nation_text.split() if word.isupper() and len(word) == 3)
                else:
                    nation = "UNK"

            # Extract ranking from third cell
            try:
                place = int(cells[2].text.strip())
            except (ValueError, IndexError):
                logging.warning(f"Could not parse place from: {cells[2].text if len(cells) > 2 else 'N/A'}")
                continue
            
            # Determine the appropriate date and race number based on event type
            if event_type == "4 Hill Tournament":
                date = f"{season}-01-06"  # 4HT typically ends around January 6th
                race = 1  # Special race number for 4HT
            else:
                date = f"{season}-05-01"  # End of season date
                race = 0  # Standings are race 0
            
            # Create standardized record for Ski Jumping
            standing = {
                'Date': date,
                'City': "Offseason" if event_type == "Standings" else "4 Hill Tournament",
                'Country': "Offseason" if event_type == "Standings" else "GER/AUT", 
                'Sex': gender,
                'HillSize': "0",  # Specific to ski jumping
                'RaceType': event_type,
                'TeamEvent': 0,  # Specific to ski jumping
                'Event': event_type,
                'Place': place,
                'Skier': skier_name,
                'Nation': nation,
                'ID': int(skier_id),
                'Season': season,
                'Race': race,
                'Birthday': None,  # Will be filled in during merge
                'Age': None,  # Will be calculated during merge
                'Exp': None,  # Will be calculated during merge
                'Leg': 0,
                'Length1': None,  # Specific to ski jumping
                'Length2': None,  # Specific to ski jumping
                'Points': None   # Specific to ski jumping
            }
            standings.append(standing)

        except Exception as e:
            logging.error(f"Error parsing row in season {season} {event_type}: {e}")
            continue

    logging.info(f"Parsed {len(standings)} standings for {season} {gender} {event_type}")
    return standings

async def fetch_year_standings(year: int, gender: str, session: aiohttp.ClientSession) -> List[Dict]:
    """Fetch standings for a single year - both World Cup and 4 Hill Tournament"""
    all_standings = []
    
    # Fetch World Cup standings
    logging.info(f"Fetching {year} World Cup standings for {gender}")
    wc_html = await fetch_standings_page(session, year, gender)
    if wc_html:
        wc_standings = parse_standings(wc_html, year, gender, "Standings")
        all_standings.extend(wc_standings)
    
    # Fetch 4 Hill Tournament standings (starts in 1977)
    if year >= 1977:
        logging.info(f"Fetching {year} 4 Hill Tournament standings for {gender}")
        fourhill_html = await fetch_standings_page(session, year, gender, "4ht")
        if fourhill_html:
            fourhill_standings = parse_standings(fourhill_html, year, gender, "4 Hill Tournament")
            all_standings.extend(fourhill_standings)
    
    return all_standings

async def fetch_batch_standings(years: List[int], gender: str, session: aiohttp.ClientSession) -> List[Dict]:
    """Fetch standings for a batch of years"""
    tasks = [fetch_year_standings(year, gender, session) for year in years]
    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]  # Flatten results

async def fetch_all_standings(start_year: int, end_year: int, gender: str) -> List[Dict]:
    """Fetch standings for multiple years using batching"""
    # Create SSL context for the session
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Create connector with SSL context
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=10)  # Limit concurrent connections
    
    async with aiohttp.ClientSession(connector=connector) as session:
        all_years = list(range(start_year, end_year + 1))
        batch_size = 5  # Process 5 years at a time
        all_standings = []
        
        # Process years in batches
        for i in range(0, len(all_years), batch_size):
            batch_years = all_years[i:i + batch_size]
            batch_results = await fetch_batch_standings(batch_years, gender, session)
            all_standings.extend(batch_results)
            
            if i + batch_size < len(all_years):
                await asyncio.sleep(1)  # Brief pause between batches
                
        return all_standings

def merge_with_existing_data(standings_df: pd.DataFrame, gender: str) -> pd.DataFrame:
    """Merge new standings with existing race data"""
    try:
        # Read existing race data - Updated path for Ski Jumping with relay support
        base_path = os.path.expanduser("~/ski/elo/python/skijump/polars/relay/excel365")
        existing_file = f"{base_path}/{'ladies' if gender == 'L' else 'men'}_scrape_update.csv"
        
        existing_df = pd.read_csv(existing_file)
        logging.info(f"Read existing data with shape {existing_df.shape}")
        
        # Convert date columns to datetime
        existing_df['Date'] = pd.to_datetime(existing_df['Date'])
        standings_df['Date'] = pd.to_datetime(standings_df['Date'])
        
        # Get Birthday info from existing data
        birthday_map = existing_df.groupby('ID')['Birthday'].first().to_dict()
        
        # Fill in Birthday info in standings data
        standings_df['Birthday'] = standings_df['ID'].map(birthday_map)
        
        # Calculate Age for standings records
        # First ensure Birthday isn't NaN before conversion
        mask = pd.notna(standings_df['Birthday'])
        standings_df.loc[mask, 'Age'] = (
            pd.to_datetime(standings_df.loc[mask, 'Date']) - 
            pd.to_datetime(standings_df.loc[mask, 'Birthday'])
        ).dt.days / 365.25
        
        # Fill NaN ages with previous known age for same ID
        standings_df['Age'] = standings_df.groupby('ID')['Age'].ffill()
        
        # Calculate Experience (number of previous appearances)
        standings_df['Exp'] = standings_df.sort_values('Date').groupby('ID').cumcount()
        
        # Combine existing data with standings
        # First, remove any existing standings records from the race data
        existing_df = existing_df[~existing_df['Event'].isin(['Standings', '4 Hill Tournament'])]
        
        # Concatenate and sort
        combined_df = pd.concat([existing_df, standings_df], ignore_index=True)
        combined_df = combined_df.sort_values(['Season', 'Race', 'Place'])
        
        # Ensure string columns are consistent
        string_columns = ['HillSize', 'RaceType']
        for col in string_columns:
            if col in combined_df.columns:
                combined_df[col] = combined_df[col].astype(str)
        
        # Ensure all required columns are present (Ski Jumping format)
        expected_columns = [
            'Date', 'City', 'Country', 'Sex', 'HillSize', 'RaceType', 'TeamEvent',
            'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season', 'Race', 
            'Birthday', 'Age', 'Exp', 'Leg', 'Length1', 'Length2', 'Points'
        ]
        missing_columns = set(expected_columns) - set(combined_df.columns)
        if missing_columns:
            logging.warning(f"Missing columns in output: {missing_columns}")
        
        logging.info(f"Combined data shape: {combined_df.shape}")
        return combined_df
        
    except Exception as e:
        logging.error(f"Error merging data: {e}")
        logging.error(f"Error details: {str(e)}")
        return standings_df

def save_standings(standings: List[Dict], gender: str):
    """Save standings to CSV format"""
    if not standings:
        logging.warning(f"No standings data to save for {gender}")
        return

    df = pd.DataFrame(standings)
    
    # Ensure output directory exists - Updated path for Ski Jumping
    output_dir = Path(os.path.expanduser("~/ski/ranks/skijump/excel365"))
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert date to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Ensure string columns are proper type
    string_columns = ['HillSize', 'RaceType']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Ensure integer types for Ski Jumping - handle None values
    integer_columns = ['TeamEvent', 'Place', 'Season', 'Race', 'ID', 'Leg']
    for col in integer_columns:
        if col in df.columns:
            df[col] = df[col].astype(int)
    
    # Handle Exp column separately since it can be None initially
    if 'Exp' in df.columns:
        df['Exp'] = df['Exp'].fillna(0).infer_objects(copy=False).astype(int)
    
    # Merge with existing data
    final_df = merge_with_existing_data(df, gender)
    
    # Save files
    base_name = "ladies" if gender == "L" else "men"
    csv_path = output_dir / f"{base_name}_scrape_update.csv"
    
    final_df.to_csv(csv_path, index=False)
    logging.info(f"Saved {len(final_df)} total records for {gender}")

async def process_gender(gender: str):
    """Process standings for a single gender"""
    try:
        # Ski Jumping World Cup started in 1979/80 season
        standings = await fetch_all_standings(1980, 2026, gender)
        save_standings(standings, gender)
    except Exception as e:
        logging.error(f"Error processing {gender} standings: {e}")

async def main():
    """Main execution function with parallel gender processing"""
    start_time = time.time()
    
    # Process both genders concurrently
    await asyncio.gather(
        process_gender('M'),
        process_gender('L')
    )
    
    execution_time = time.time() - start_time
    logging.info(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())