import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re
import time

def scrape_fis_calendar_dates():
    """Scrape race dates from FIS World Cup and Olympic calendars"""
    
    # URLs for World Cup and Olympic calendars
    wc_url = "https://www.fis-ski.com/DB/general/calendar-results.html?eventselection=&place=&sectorcode=CC&seasoncode=2026&categorycode=WC&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    olympic_url = "https://www.fis-ski.com/DB/general/calendar-results.html?eventselection=&place=&sectorcode=CC&seasoncode=2026&categorycode=OWG&disciplinecode=&gendercode=&racedate=&racecodex=&nationcode=&seasonmonth=X-2026&saveselection=-1&seasonselection="
    
    all_race_dates = []
    
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
                            
                            # Get individual race dates from event page
                            race_dates = scrape_event_races(event_url)
                            all_race_dates.extend(race_dates)
                            
                            # Be polite to the server
                            time.sleep(1)
                            break  # Only need one link per event
            
        except Exception as e:
            print(f"Error scraping {category}: {str(e)}")
    
    return all_race_dates

def scrape_event_races(event_url):
    """Scrape individual race dates from an event page"""
    race_dates = []
    
    try:
        response = requests.get(event_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for the event details content div
        content_div = soup.find('div', {'id': 'eventdetailscontent'})
        if not content_div:
            print(f"No event details content found for {event_url}")
            return race_dates
        
        # Find all race rows in the event
        race_rows = content_div.find_all('div', {'class': 'table-row'})
        
        for row in race_rows:
            # Look for timezone-date divs which contain the race dates
            date_divs = row.find_all('div', {'class': 'timezone-date'})
            
            for date_div in date_divs:
                date_attr = date_div.get('data-date')
                if date_attr:
                    try:
                        # Convert from YYYY-MM-DD to MM/DD/YYYY
                        date_obj = datetime.strptime(date_attr, '%Y-%m-%d')
                        formatted_date = date_obj.strftime('%m/%d/%Y')
                        
                        # Look for startlist link in this row
                        startlist_url = ""
                        result_links = row.find_all('a', href=lambda x: x and 'results.html' in x)
                        for link in result_links:
                            href = link.get('href')
                            if href and 'raceid=' in href:
                                if not href.startswith('http'):
                                    startlist_url = "https://www.fis-ski.com" + href
                                else:
                                    startlist_url = href
                                break
                        
                        # Check for gender information in the row
                        gender_divs = row.find_all('div', {'class': 'gender__item'})
                        genders = []
                        
                        for gender_div in gender_divs:
                            if 'gender__item_m' in gender_div.get('class', []):
                                genders.append('M')
                            elif 'gender__item_l' in gender_div.get('class', []):
                                genders.append('L')
                        
                        # If no specific gender markers found, assume both M and L
                        if not genders:
                            genders = ['M', 'L']
                        
                        # Add date for each gender
                        for gender in genders:
                            date_gender_combo = f"{formatted_date}_{gender}"
                            if date_gender_combo not in [item['date_gender'] for item in race_dates]:
                                race_dates.append({
                                    'date': formatted_date,
                                    'gender': gender,
                                    'date_gender': date_gender_combo,
                                    'startlist_url': startlist_url
                                })
                                print(f"Found race date: {formatted_date} ({gender}) - {startlist_url}")
                        
                    except ValueError:
                        continue
    
    except Exception as e:
        print(f"Error processing event {event_url}: {str(e)}")
    
    return race_dates

def save_dates_to_csv(race_data, output_file):
    """Save race dates to CSV file"""
    
    # Sort by date then by gender
    date_objects = []
    
    for item in race_data:
        try:
            date_obj = datetime.strptime(item['date'], '%m/%d/%Y')
            date_objects.append((date_obj, item['date'], item['gender'], item.get('startlist_url', '')))
        except ValueError:
            continue
    
    date_objects.sort(key=lambda x: (x[0], x[2]))  # Sort by date, then gender
    
    # Create CSV with same structure as existing races.csv
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Header matching the original races.csv
        writer.writerow(['Date', 'Startlist', 'Sex', 'City', 'Country', 'Distance', 'Technique', 'MS', 'Period', 'Pursuit', 'Stage', 'Final_Climb', 'Elevation', 'Race', 'Championship'])
        
        # Write each date/gender combination with startlist URL
        for _, date, gender, startlist_url in date_objects:
            row = [date, startlist_url, gender] + [''] * 12  # Date, Startlist URL, Gender, then 12 empty columns
            writer.writerow(row)
    
    print(f"Saved {len(date_objects)} race date/gender combinations to {output_file}")

if __name__ == "__main__":
    print("Starting FIS calendar scraping...")
    
    # Scrape all race dates
    race_dates = scrape_fis_calendar_dates()
    
    if race_dates:
        # Save to output file
        output_file = "excel365/races2026.csv"
        save_dates_to_csv(race_dates, output_file)
        print(f"Scraping complete! Found {len(race_dates)} total race date/gender combinations.")
    else:
        print("No race dates found.")