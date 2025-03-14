import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import queue
from datetime import datetime

class RateLimiter:
    def __init__(self, calls_per_second):
        self.delay = 1.0 / calls_per_second
        self.last_call = 0
        self.lock = Lock()
    
    def wait(self):
        with self.lock:
            current_time = time.time()
            time_passed = current_time - self.last_call
            if time_passed < self.delay:
                time.sleep(self.delay - time_passed)
            self.last_call = time.time()

class FISPDFScraper:
    def __init__(self, output_dir='fis_pdfs', max_workers=4, requests_per_second=2):
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.rate_limiter = RateLimiter(requests_per_second)
        self.session = requests.Session()
        self.print_lock = Lock()
        self.categories = {
            'WC': 'World Cup',
            'WSC': 'World Championships',
            'OWG': 'Olympics'
        }
        
    def safe_print(self, message):
        """Thread-safe printing with timestamp"""
        with self.print_lock:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {message}")

    def is_valid_results_pdf(self, url):
        """
        Check if the PDF is a main results file (ends with RL.pdf but not PRL.pdf or SRL.pdf)
        """
        return url.endswith('RL.pdf') and not any(url.endswith(x) for x in ['PRL.pdf', 'SRL.pdf'])

    def download_pdf(self, pdf_info):
        """
        Download a single PDF file.
        
        Args:
            pdf_info: tuple of (url, save_path, filename, season, category, gender)
        """
        url, save_path, filename, season, category, gender = pdf_info
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()  # Rate limit the requests
                
                response = self.session.get(url, stream=True)
                response.raise_for_status()
                
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                self.safe_print(f"Season {season} ({self.categories[category]}) Gender {gender}: Successfully downloaded: {filename}")
                return True
            
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    self.safe_print(f"Season {season} ({self.categories[category]}) Gender {gender}: Failed to download {filename}: {str(e)}")
                    return False
                time.sleep(2 ** attempt)  # Exponential backoff

    def extract_gender_from_btn(self, btn):
        """Extract gender from button data attributes"""
        gender = btn.get('data-ga-gender', '')
        if gender in ['M', 'W']:
            return gender
        return None

    def get_pdf_links(self, event_url, season, category):
        """
        Get all valid result PDF links from an event page.
        """
        try:
            self.rate_limiter.wait()
            response = self.session.get(event_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            pdf_infos = []
            
            # Find all drop-btn__content divs
            drop_btn_contents = soup.find_all('div', class_='drop-btn__content')
            
            for content_div in drop_btn_contents:
                # Check if this div contains any span with "Qualification" in it
                qual_spans = content_div.find_all('span', string=lambda x: x and 'Qualification' in x)
                if qual_spans:
                    continue
                    
                # Process PDFs in this div
                pdf_buttons = content_div.find_all('a', attrs={
                    'class': 'btn btn_yellow btn_icon-right',
                    'href': lambda x: x and x.endswith('.pdf')
                })
                
                for pdf_button in pdf_buttons:
                    pdf_url = pdf_button['href']
                    if not pdf_url.startswith('http'):
                        pdf_url = urljoin('https://www.fis-ski.com', pdf_url)
                    
                    if not self.is_valid_results_pdf(pdf_url):
                        continue
                        
                    # Extract gender and skip if not M or L
                    gender = self.extract_gender_from_btn(pdf_button)
                    if not gender:
                        self.safe_print(f"Season {season}: Skipping mixed gender or unknown gender race")
                        continue
                    
                    filename = pdf_url.split('/')[-1]
                    gender_dir = os.path.join(self.output_dir, gender)
                    season_dir = os.path.join(gender_dir, str(season))
                    save_path = os.path.join(season_dir, filename)
                    
                    if not os.path.exists(save_path):
                        pdf_infos.append((pdf_url, save_path, filename, season, category, gender))
                    else:
                        self.safe_print(f"Season {season} ({self.categories[category]}) Gender {gender}: Skipping existing file: {filename}")
            
            return pdf_infos
            
        except requests.exceptions.RequestException as e:
            self.safe_print(f"Season {season} ({self.categories[category]}): Error processing event {event_url}: {str(e)}")
            return []

    def scrape_season_category(self, season, category):
        """
        Scrape PDF results for a specific season and category
        """
        season_url = f"https://www.fis-ski.com/DB/cross-country/calendar-results.html?eventselection=&place=&sectorcode=CC&seasoncode={season}&categorycode={category}&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-{season}&saveselection=-1&seasonselection="
        
        try:
            self.rate_limiter.wait()
            response = self.session.get(season_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            event_links = soup.find_all('a', href=lambda x: x and 'event-details' in x)
            
            pdf_infos = []
            for event_link in event_links:
                event_url = urljoin('https://www.fis-ski.com', event_link['href'])
                pdf_infos.extend(self.get_pdf_links(event_url, season, category))
            
            return pdf_infos
            
        except requests.exceptions.RequestException as e:
            self.safe_print(f"Season {season} ({self.categories[category]}): Error accessing season calendar: {str(e)}")
            return []

    def scrape_all_seasons(self, start_year=2017, end_year=2024):
        """
        Scrape PDF results from multiple seasons using multiple threads.
        Includes World Cup (WC), World Championships (WSC), and Olympics (OWG).
        PDFs are organized by gender (M/L) and then by season.
        """
        # Create base directories for each gender
        for gender in ['M', 'L']:
            os.makedirs(os.path.join(self.output_dir, gender), exist_ok=True)
        
        # First, gather all PDF links from all seasons and categories
        all_pdf_infos = []
        for season in range(start_year, end_year + 1):
            self.safe_print(f"Scanning season {season}")
            for category in self.categories.keys():
                self.safe_print(f"Scanning {self.categories[category]} events for season {season}")
                season_pdfs = self.scrape_season_category(season, category)
                all_pdf_infos.extend(season_pdfs)
                self.safe_print(f"Found {len(season_pdfs)} PDFs for season {season} {self.categories[category]}")
        
        total_pdfs = len(all_pdf_infos)
        self.safe_print(f"Found total of {total_pdfs} PDFs across all seasons and categories")
        
        # Download PDFs using thread pool
        successful_downloads = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_pdf = {executor.submit(self.download_pdf, pdf_info): pdf_info 
                           for pdf_info in all_pdf_infos}
            
            for future in as_completed(future_to_pdf):
                pdf_info = future_to_pdf[future]
                try:
                    if future.result():
                        successful_downloads += 1
                except Exception as e:
                    self.safe_print(f"Error downloading {pdf_info[2]} from season {pdf_info[3]} ({self.categories[pdf_info[4]]}) Gender {pdf_info[5]}: {str(e)}")
        
        self.safe_print(f"Download complete. Successfully downloaded {successful_downloads} of {total_pdfs} PDFs")

if __name__ == "__main__":
    scraper = FISPDFScraper(
        output_dir=os.path.expanduser('~/ski/elo/python/elo/polars/excel365/fis_pdfs'),
        max_workers=4,
        requests_per_second=2
    )
    scraper.scrape_all_seasons(start_year=2002, end_year=2024)