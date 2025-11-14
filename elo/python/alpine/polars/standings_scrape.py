#!/usr/bin/env python3
"""
Alpine skiing standings scraper for FirstSkiSport.
This script scrapes men's and women's World Cup standings and saves them to CSV files.
"""

import os
import csv
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import time
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Updated URLs for World Cup rankings
MEN_URL = "https://firstskisport.com/alpine/ranking.php?y=2026&cup=1"
WOMEN_URL = "https://firstskisport.com/alpine/ranking.php?y=2026&g=w&cup=1"
OUTPUT_DIR = os.path.expanduser("~/ski/elo/python/alpine/polars/excel365")
MEN_OUTPUT = os.path.join(OUTPUT_DIR, "men_standings.csv")
WOMEN_OUTPUT = os.path.join(OUTPUT_DIR, "ladies_standings.csv")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

# Ensure output directory exists
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

def extract_athlete_id(href):
    """Extract athlete ID from href attribute."""
    if not href:
        return None
    match = re.search(r'id=(\d+)', href)
    return match.group(1) if match else None

def extract_points(points_text):
    """Extract numeric points value from text like '(1721 pts)'"""
    if not points_text:
        return None
    match = re.search(r'\((\d+)', points_text)
    return int(match.group(1)) if match else None

def scrape_standings(url, gender):
    """
    Scrape standings data from the given URL.
    
    Args:
        url (str): URL to scrape
        gender (str): 'men' or 'women'
        
    Returns:
        list: List of dictionaries containing standings data
    """
    start_time = time()
    logger.info(f"Starting to scrape {gender}'s World Cup standings from {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {gender}'s standings: {e}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the main table containing standings data
    table = soup.select_one('table.sortTabell')
    if not table:
        logger.error(f"No standings table found on the page for {gender}")
        return []
    
    # Extract rows from the table (skip header row)
    rows = table.select('tbody tr')
    
    standings_data = []
    for row in rows:
        try:
            # Get athlete name and ID
            name_cell = row.select_one('td:nth-child(1) a')
            if not name_cell:
                continue
                
            athlete_id = extract_athlete_id(name_cell.get('href', ''))
            
            # Extract name - handle the format with uppercase last name
            last_name_span = name_cell.select_one('span[style*="text-transform:uppercase"]')
            first_name = name_cell.text.replace(last_name_span.text if last_name_span else "", "").strip()
            last_name = last_name_span.text.strip() if last_name_span else ""
            
            # Combine for full name
            athlete_name = f"{first_name} {last_name}".strip()
            
            # Get nation
            nation_cell = row.select_one('td:nth-child(2)')
            nation_link = nation_cell.select_one('a') if nation_cell else None
            nation = nation_link.text.strip() if nation_link else ""
            
            # Get ranking
            ranking_cell = row.select_one('td:nth-child(3)')
            ranking = int(ranking_cell.text.strip()) if ranking_cell else None
            
            # Get points
            points_cell = row.select_one('td:nth-child(4)')
            points_text = points_cell.text.strip() if points_cell else ""
            points = extract_points(points_text)
            
            # Get wins
            wins_cell = row.select_one('td:nth-child(5)')
            wins = int(wins_cell.text.strip()) if wins_cell and wins_cell.text.strip().isdigit() else 0
            
            standings_data.append({
                'Skier': athlete_name,
                'ID': athlete_id,
                'Nation': nation,
                'Ranking': ranking,
                'Points': points,
                'Wins': wins
            })
            
        except Exception as e:
            logger.warning(f"Error processing row: {e}")
            continue
    
    logger.info(f"Scraped {len(standings_data)} {gender}'s standings in {time() - start_time:.2f} seconds")
    return standings_data

def save_to_csv(data, output_file):
    """Save data to CSV file."""
    if not data:
        logger.warning(f"No data to save to {output_file}")
        return
    
    try:
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(data)} records to {output_file}")
    except Exception as e:
        logger.error(f"Failed to save data to {output_file}: {e}")

def main():
    """Main execution function."""
    start_time = time()
    
    # Scrape men's standings
    logger.info("Scraping men's World Cup standings...")
    men_data = scrape_standings(MEN_URL, 'men')
    save_to_csv(men_data, MEN_OUTPUT)
    
    # Scrape women's standings
    logger.info("Scraping women's World Cup standings...")
    women_data = scrape_standings(WOMEN_URL, 'women')
    save_to_csv(women_data, WOMEN_OUTPUT)
    
    logger.info(f"Completed scraping in {time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()