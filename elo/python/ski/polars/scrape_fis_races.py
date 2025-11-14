import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re
import time

def scrape_fis_calendar_races():
    """Scrape race data from FIS World Cup and Olympic calendars"""
    
    # URLs for World Cup and Olympic calendars
    wc_url = "https://www.fis-ski.com/DB/general/calendar-results.html?eventselection=&place=&sectorcode=CC&seasoncode=2026&categorycode=WC&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    olympic_url = "https://www.fis-ski.com/DB/general/calendar-results.html?eventselection=&place=&sectorcode=CC&seasoncode=2026&categorycode=OWG&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    
    all_races = []
    
    for url, category in [(wc_url, "World Cup"), (olympic_url, "Olympics")]:
        print(f"Scraping {category} calendar...")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for the calendar data div
            calendar_div = soup.find('div', {'id': 'calendardata'})
            if not calendar_div:
                print(f"No calendar data div found for {category}")
                continue
            
            # Find all event rows
            event_rows = calendar_div.find_all('div', {'class': 'table-row'})
            print(f"Found {len(event_rows)} events for {category}")
            
            for row in event_rows:
                # Extract event ID from the row
                event_id = row.get('id')
                if event_id:
                    # Look for event detail links
                    event_links = row.find_all('a', href=lambda x: x and 'event-details.html' in x)
                    
                    for link in event_links:
                        event_url = link.get('href')
                        if event_url and not event_url.startswith('http'):
                            event_url = "https://www.fis-ski.com" + event_url
                        
                        if event_url:
                            print(f"Processing event {event_id}: {event_url}")
                            
                            # Get individual races from event page
                            races = scrape_event_races(event_url)
                            all_races.extend(races)
                            
                            # Be polite to the server
                            time.sleep(1)
                            break  # Only need one link per event
            
        except Exception as e:
            print(f"Error scraping {category}: {str(e)}")
    
    return all_races

def scrape_event_races(event_url):
    """Scrape individual races from an event page"""
    races = []
    
    try:
        response = requests.get(event_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for the event details content div
        content_div = soup.find('div', {'id': 'eventdetailscontent'})
        if not content_div:
            print(f"No event details content found for {event_url}")
            return races
        
        # Find all race rows in the event
        race_rows = content_div.find_all('div', {'class': 'table-row'})
        
        for row in race_rows:
            # 1) Extract date from data-date attribute
            date_div = row.find('div', {'class': 'timezone-date'})
            race_date = ""
            if date_div and date_div.get('data-date'):
                try:
                    # Convert from YYYY-MM-DD to MM/DD/YYYY
                    date_obj = datetime.strptime(date_div.get('data-date'), '%Y-%m-%d')
                    race_date = date_obj.strftime('%m/%d/%Y')
                except ValueError:
                    continue
            
            if not race_date:
                continue
                
            # 2) Extract startlist URL from results.html links
            startlist_url = ""
            result_links = row.find_all('a', href=lambda x: x and 'results.html' in x and 'raceid=' in x)
            if result_links:
                href = result_links[0].get('href')
                if href:
                    if not href.startswith('http'):
                        startlist_url = "https://www.fis-ski.com" + href
                    else:
                        startlist_url = href
            
            # 3) Extract sex from gender__item classes
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
            
            # Only add race if we have all required data
            if race_date and startlist_url and sex:
                races.append({
                    'date': race_date,
                    'startlist_url': startlist_url,
                    'sex': sex
                })
                print(f"Found race: {race_date} ({sex}) - {startlist_url}")
        
    except Exception as e:
        print(f"Error processing event {event_url}: {str(e)}")
    
    return races

def save_races_to_csv(races, output_file):
    """Save race data to CSV file"""
    
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
    
    # Create CSV with same structure as existing races.csv
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Header matching the original races.csv
        writer.writerow(['Date', 'Startlist', 'Sex', 'City', 'Country', 'Distance', 'Technique', 'MS', 'Period', 'Pursuit', 'Stage', 'Final_Climb', 'Elevation', 'Race', 'Championship'])
        
        # Write each race
        for race in sorted_races:
            row = [race['date'], race['startlist_url'], race['sex']] + [''] * 12  # Date, Startlist URL, Sex, then 12 empty columns
            writer.writerow(row)
    
    print(f"Saved {len(sorted_races)} races to {output_file}")

if __name__ == "__main__":
    print("Starting FIS race scraping...")
    
    # Scrape all races
    races = scrape_fis_calendar_races()
    
    if races:
        # Save to output file
        output_file = "excel365/races2026.csv"
        save_races_to_csv(races, output_file)
        print(f"Scraping complete! Found {len(races)} total races.")
    else:
        print("No races found.")