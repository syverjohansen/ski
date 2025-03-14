import pandas as pd
import re
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor
import PyPDF2
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_text_from_pdf(pdf_path):
    """Extract text content from a PDF file"""
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ''
            for page in pdf_reader.pages:
                text += page.extract_text()
            return text
    except Exception as e:
        logging.error(f"Error reading PDF {pdf_path}: {str(e)}")
        return None

def parse_date(text):
    """Extract and convert date to YYYYMMDD format"""
    date_match = re.search(r'(\d{1,2}\s+[A-Z]{3}\s+\d{4})', text)
    if date_match:
        date_str = date_match.group(1)
        date_obj = datetime.strptime(date_str, '%d %b %Y')
        return date_obj.strftime('%Y%m%d')
    return None

def extract_location(text):
    """Extract city and country from text"""
    patterns = [
        r'([A-Z][A-Za-z\s-]+)\s+\(([A-Z]{3})\)',  # Standard format: City (COO)
        r'([A-Z][A-Za-z\s-]+)\s+\(([A-Z]{3})',    # Format without closing parenthesis
        r'in\s+([A-Z][A-Za-z\s-]+)\s+\(([A-Z]{3})\)'  # Format with 'in' prefix
    ]
    
    for pattern in patterns:
        location_match = re.search(pattern, text)
        if location_match:
            return location_match.group(1).strip(), location_match.group(2)
    return None, None

def extract_technique(text):
    """Extract technique from race title and text"""
    # First try to find explicit F or C
    simple_pattern = r'\b([F|C])\b'
    match = re.search(simple_pattern, text)
    if match:
        return match.group(1)
    
    # Also check for C/F in relays
    if re.search(r'C/F', text, re.IGNORECASE):
        return 'C/F'
    
    # Then check word patterns
    if re.search(r'\bClassical\b', text, re.IGNORECASE):
        return 'C'
    if re.search(r'\bClassic\b', text, re.IGNORECASE):
        return 'C'
    if re.search(r'\bFree\b', text, re.IGNORECASE):
        return 'F'
    if re.search(r'\bFreestyle\b', text, re.IGNORECASE):
        return 'F'
    
    return None

def extract_course_info(text):
    """Extract course information from the text"""
    patterns = {
        'height_difference': [
            r'Height Difference.*?[:\s]+(\d+)\s*m',
            r'Height Difference \(HD\).*?[:\s]+(\d+)\s*m',
            r'HD[:\s]+(\d+)\s*m'
        ],
        'maximum_climb': [
            r'Maximum Climb.*?[:\s]+(\d+)\s*m',
            r'Maximum Climb \(MC\).*?[:\s]+(\d+)\s*m',
            r'MC[:\s]+(\d+)\s*m'
        ],
        'total_climb': [
            r'Total Climb.*?[:\s]+(\d+)\s*m',
            r'Total Climb \(TC\).*?[:\s]+(\d+)\s*m',
            r'TC[:\s]+(\d+)\s*m'
        ],
        'lap_length': [
            r'Length of [Ll]ap.*?[:\s]+(\d+)\s*m',
            r'Length of lap[:\s]+(\d+)\s*m',
            r'Course.*?:\s*.*?(\d+)\s*m',
            r'L[aä]nge der Runde:\s*(\d+)\s*m'
        ],
        'number_of_laps': [
            r'Number of [Ll]aps.*?[:\s]+(\d+)',
            r'Laps[:\s]+(\d+)',
            r'Number of Laps / Runden:\s*(\d+)',
            r'Runden:\s*(\d+)'
        ]
    }
    
    results = {}
    for field, field_patterns in patterns.items():
        results[field] = None
        for pattern in field_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    results[field] = float(match.group(1))
                    break
                except (ValueError, TypeError):
                    continue
    
    # Handle split course info (e.g., pursuit races)
    if results['total_climb'] is None:
        split_tc_match = re.search(r'Total Climb.*?:\s*(\d+)\s*m\s*[/|+]\s*(\d+)\s*m', text)
        if split_tc_match:
            try:
                tc1 = float(split_tc_match.group(1))
                tc2 = float(split_tc_match.group(2))
                results['total_climb'] = tc1 + tc2
            except (ValueError, TypeError):
                pass
    
    # Default to 1 lap for sprint races
    if results['number_of_laps'] is None and 'sprint' in text.lower():
        results['number_of_laps'] = 1.0
    
    return results

def extract_race_info(text):
    """Extract race information including distance, technique, and type"""
    race_title = _get_race_title(text)
    sex = _extract_sex(text, race_title)
    
    # Identify race type from patterns
    race_formats = [
        # Sprint patterns
        (r'Sprint Qualification', 'Sprint'),
        (r'Sprint\s+(?:Final|Finals|Qualification)', 'Sprint'),
        (r'\d+(?:\.\d+)?\s*km\s+(?:Free|Classical)?\s*Sprint', 'Sprint'),
        
        # Handicap/Pursuit patterns
        (r'(?:Free|Classical)?\s*\'Handicap\'\s*Start', 'Pursuit'),
        (r'Handicap\s+Start', 'Pursuit'),
        
        # Individual patterns
        (r'Individual\s+Start', 'Ind'),
        (r'Individual(?!\s+Sprint)', 'Ind'),
        
        # Mass Start patterns
        (r'Mass\s+Start', 'MS'),
        
        # Stage race patterns
        (r'Stage\s+\d+\s+of\s+\d+', None),  # Will use underlying format
        
        # Tour patterns 
        (r'Tour\s+(?:de\s+)?Ski', None)  # Will use underlying format
    ]
    
    # Check for specific race format
    race_type = None
    for pattern, format_type in race_formats:
        if re.search(pattern, text, re.IGNORECASE):
            race_type = format_type
            break
    
    # If no specific format found yet, check title
    if not race_type:
        if 'SPRINT' in race_title.upper():
            race_type = 'Sprint'
        elif 'PURSUIT' in race_title.upper() or 'HANDICAP' in race_title.upper():
            race_type = 'Pursuit'
        elif 'MASS START' in race_title.upper():
            race_type = 'MS'
        elif 'INDIVIDUAL' in race_title.upper():
            race_type = 'Ind'
            
    # Extract distance and technique
    distance_match = re.search(r'(\d+(?:\.\d+)?)\s*km', text)
    distance = float(distance_match.group(1)) if distance_match else None
    technique = extract_technique(text)
    
    is_sprint = race_type == 'Sprint'
    is_mass_start = race_type == 'MS'
    
    return sex, distance, technique, is_sprint, is_mass_start, race_type

def _get_race_title(text):
    """Extract the race title from the first few lines with better pattern matching"""
    keywords = ['SPRINT', 'PURSUIT', 'WOMEN', 'MEN', 'KM', 'RELAY', 'DOUBLE', 
                'MASS START', 'MASS', 'HANDICAP', 'QUALIFICATION', 'FINAL', 'INDIVIDUAL']
    first_few_lines = text.split('\n')[:10]
    
    # Clean up the text lines
    clean_lines = [re.sub(r'[\x00-\x1F\x7F-\xFF]', ' ', line).strip() for line in first_few_lines]
    title = ' '.join(clean_lines)

    for line in clean_lines:
        # Look for specific race formats
        if any(keyword in line.upper() for keyword in keywords):
            return line.strip()
    return ""

def _extract_sex(text, race_title=""):
    """Extract the sex from the race information and team sprint results"""
    # Debug logging to see what we're working with
    logging.debug("Race title: %s", race_title)
    
    # First clean up garbled text
    clean_text = re.sub(r'[\x00-\x1F\x7F-\xFF]', ' ', text)
    clean_text = re.sub(r'\s+', ' ', clean_text)
    
    # Try to extract FIS codes which can indicate sex
    fis_codes = re.findall(r'\b[13]\d{6}\b', clean_text)
    if fis_codes:
        logging.debug("Found FIS codes: %s", fis_codes)
        # Women's FIS codes often start with 1 or 3
        women_codes = [code for code in fis_codes if code.startswith(('1', '3'))]
        if len(women_codes) > len(fis_codes) / 2:
            return 'Women'
        else:
            return 'Men'
    
    # List of known women skiers' names that appear in the text
    women_names = [
        'BJOERGEN', 'KOVALTSJUK', 'KOWALCZYK', 'FESSEL', 'RANDALL',
        'JOHAUG', 'KALLA', 'STEIRA', 'KRISTOFFERSEN', 'SAARINEN',
        'LAHTEENMAKI', 'NISKANEN', 'BRODIN', 'ZELLER', 'ROPONEN'
    ]
    
    # List of known men skiers' names that appear in the text
    men_names = [
        'NORTHUG', 'COLOGNA', 'HARVEY', 'HELLNER', 'KERSHAW',
        'JOENSSON', 'PETERSON', 'HATTESTAD', 'PANZHINSKIY', 'KRIUKOV',
        'PENTSINEN', 'BRANDSDAL', 'HAMILTON', 'BABIKOV', 'CLARA'
    ]
    
    # Count appearances of known names
    women_count = sum(1 for name in women_names if name in clean_text.upper())
    men_count = sum(1 for name in men_names if name in clean_text.upper())
    
    logging.debug(f"Women names found: {women_count}, Men names found: {men_count}")
    
    if women_count > men_count:
        return 'Women'
    elif men_count > women_count:
        return 'Men'
    
    # Look for relay teams which often indicate sex
    teams_list = re.findall(r'(?:\d{1,2}\s*\.\s*)?([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\s*\-[12]', clean_text)
    logging.debug("Found team entries: %s", teams_list)
    
    # Check team names against additional lists of names
    additional_women = {
        'MARIT', 'THERESE', 'JUSTYNA', 'KIKKAN', 'CHARLOTTE',
        'KRISTA', 'AINO', 'HEIDI', 'INGVILD', 'ASTRID'
    }
    
    additional_men = {
        'PETTER', 'DARIO', 'MARCUS', 'DEVON', 'ALEXANDER',
        'LUKAS', 'EMIL', 'SERGEY', 'MAXIM', 'IVAN'
    }
    
    women_team_count = sum(1 for team in teams_list if any(name in team.upper() for name in additional_women))
    men_team_count = sum(1 for team in teams_list if any(name in team.upper() for name in additional_men))
    
    if women_team_count > men_team_count:
        return 'Women'
    elif men_team_count > women_team_count:
        return 'Men'
    
    # Look for explicit gender markers in title or headers
    if any(marker in clean_text.upper() for marker in ['LADIES', 'WOMEN', "WOMEN'S"]):
        return 'Women'
    if any(marker in clean_text.upper() for marker in ['MEN', "MEN'S"]) and 'WOMEN' not in clean_text.upper():
        return 'Men'
    
    # Look for mixed relay/team sprint indicators
    if 'MIXED' in clean_text.upper() or ('Mixed' in clean_text and 'Relay' in clean_text):
        return 'Mixed'
        
    # Look at FIS code patterns as last resort
    # Try to find any 7-digit numbers (FIS codes)
    all_numbers = re.findall(r'\b\d{7}\b', clean_text)
    if all_numbers:
        # Women's codes often start with 3 followed by 4
        women_pattern_matches = sum(1 for num in all_numbers if num.startswith('34'))
        # Men's codes often start with 3 followed by 5
        men_pattern_matches = sum(1 for num in all_numbers if num.startswith('35'))
        
        if women_pattern_matches > men_pattern_matches:
            return 'Women'
        elif men_pattern_matches > women_pattern_matches:
            return 'Men'
    
    return None

def _check_team_sprint(text, sex):
    """
    Improved check for team sprint events with better pattern matching
    """
    # For team sprints, look for patterns in the results format
    # Team sprints typically show two athletes per team with -1 and -2 suffixes
    team_sprint_indicators = [
        r'-[12]\b',  # Common suffix for team sprint results
        r'Team Sprint',
        r'Sprint\s+Teams',
        r'Teams\s+Sprint',
        r'\d+\s*x\s*\d+\s*[Ss]print'  # e.g., 6x1.2 sprint
    ]
    
    is_team_sprint = any(re.search(pattern, text, re.IGNORECASE) for pattern in team_sprint_indicators)
    
    if is_team_sprint:
        # Look for distance indicators
        distance_patterns = [
            r'(\d+(?:\.\d+)?)\s*km',  # e.g., 1.6 km
            r'(\d+(?:\.\d+)?)\s*x\s*\d+',  # e.g., 1.6 x 3
            r'Length of Lap:\s*(\d+)m'  # Course information
        ]
        
        for pattern in distance_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    # For team sprints, typically the distance is per leg
                    distance = float(match.group(1))
                    if 'km' not in pattern and 'm' in pattern:
                        distance = distance / 1000  # Convert meters to km
                    technique = extract_technique(text)
                    if not technique:
                        technique = 'F'  # Team sprints are typically freestyle
                    return sex, distance, technique, True, False, 'TS'
                except (ValueError, IndexError):
                    pass
        
        # If we can't find a specific distance but we're sure it's a team sprint
        # Use default distance of 1.6km (common team sprint distance)
        return sex, 1.6, extract_technique(text) or 'F', True, False, 'TS'
    
    return None

def _check_relay(text, sex=None):
    """Check if race is a relay and extract relevant information"""
    relay_patterns = [
        # Mixed relay patterns
        r'Mixed\s+(?:Team\s+)?Relay(?:\s+(?:C/F|[CF]))?',  # Basic mixed relay
        r'Mixed\s+(?:\d+\s*x\s*\d+(?:\.\d)?)\s*km\s+Relay',  # Mixed relay with distance
        r'Mixed\s+(?:\d+\s*x\s*\d+(?:\.\d)?(?:\s*\/\s*\d+\s*x\s*\d+(?:\.\d)?)?)\s*km\s+Relay',  # Complex mixed
        
        # Standard relay patterns
        r'(?:^|\s)(\d+)\s*x\s*(\d+(?:\.\d)?)\s*km(?:\s+Relay)?(?:\s+(?:C/F|[CF]))?',  # Basic relay format
        r'(?:MEN|WOMEN|LADIES|MIXED)\s+(?:TEAM\s+)?(?:(\d+)\s*x\s*(\d+(?:\.\d)?)\s*km)(?:\s+Relay)?',  # With gender
        r'Relay\s+(?:C/F|[CF])',  # Simple relay with technique
        r'(?:^|\s)Relay\s+(?:\d+\s*x\s*\d+(?:\.\d)?)\s*km',  # Relay with distance
        r'(\d+)\s*x\s*(\d+(?:\.\d)?)\s*km\s+(?:Free|Classical)\s+Relay'  # With technique spelled out
    ]
    
    # First handle mixed relays
    if any(pattern in text.upper() for pattern in ['MIXED RELAY', 'MIXED TEAM RELAY']):
        technique = extract_technique(text)
        if not technique:
            if 'C/F' in text:
                technique = 'C/F'
            else:
                if re.search(r'Classical\s+Relay', text, re.IGNORECASE):
                    technique = 'C'
                elif re.search(r'Free\s+Relay', text, re.IGNORECASE):
                    technique = 'F'
        
        # Try to get the actual relay distance
        distance = _extract_relay_distance(text)
        return 'Mixed', distance or -1, technique, False, False, 'Rel'
    
    # Then check standard relay formats
    for pattern in relay_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Determine sex if not provided
            if sex is None:
                if 'WOMEN' in text.upper() or 'LADIES' in text.upper():
                    sex = 'Women'
                elif 'MEN' in text.upper() and 'WOMEN' not in text.upper():
                    sex = 'Men'
                else:
                    sex = 'Mixed'  # Default to Mixed if unclear
            
            technique = extract_technique(text)
            if not technique and 'C/F' in text:
                technique = 'C/F'
            
            # Try to get the actual relay distance
            distance = _extract_relay_distance(text)
            return sex, distance or -1, technique, False, False, 'Rel'
    
    return None

def _check_sprint(text, sex):
    """Check if race is a sprint"""
    sprint_patterns = [
        r'SPRINT\s+([F|C])',
        r'(\d+(?:\.\d+)?)\s*km\s+Sprint\s+([F|C])',
    ]
    
    for pattern in sprint_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            technique = match.group(1) if len(match.groups()) == 1 else match.group(2)
            return sex, 1.5, technique, True, False, None
    return None

def _check_pursuit(text, sex):
    """Check if race is a pursuit/skiathlon"""
    pursuit_patterns = [
        r'(?:Double\s+)?Pursuit\s+(?:C\s+)?(\d+(?:\.\d+)?)\s*km\s*(?:\+|and)\s*(?:F\s+)?(\d+(?:\.\d+)?)\s*km',
        r'Skiathlon\s+(?:C\s+)?(\d+(?:\.\d+)?)\s*km\s*(?:\+|and)\s*(?:F\s+)?(\d+(?:\.\d+)?)\s*km',
        r'(?:Ladies\'|Men\'s)\s+Skiathlon\s+(\d+(?:\.\d+)?)\s*km\s*(?:Classic|C)\s*\+\s*(\d+(?:\.\d+)?)\s*km\s*(?:Free|F)',
        r'(\d+(?:\.\d+)?)\s*km\s*(?:Classic|C)\s*\+\s*(\d+(?:\.\d+)?)\s*km\s*(?:Free|F)',
        r'Skiathlon.*?(\d+)(?:\.\d+)?\s*km.*?\+.*?(\d+)(?:\.\d+)?\s*km'
    ]
    
    # Check title first
    first_few_lines = text.split('\n')[:10]
    title = ' '.join(first_few_lines)
    
    for pattern in pursuit_patterns:
        # Try title first
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            try:
                distance = float(match.group(1)) + float(match.group(2))
                return sex, distance, 'Skiathlon', False, False, None
            except (ValueError, IndexError):
                continue
        
        # Then try full text
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                distance = float(match.group(1)) + float(match.group(2))
                return sex, distance, 'Skiathlon', False, False, None
            except (ValueError, IndexError):
                continue
    
    # Additional check for skiathlon indicators with course info
    if 'SKIATHLON' in text.upper():
        # Look for distance in course info
        lap_match = re.search(r'Length of Lap:\s*(\d+(?:\.\d+)?)', text)
        laps_match = re.search(r'Number of Laps:\s*(\d+)', text)
        if lap_match and laps_match:
            try:
                lap_length = float(lap_match.group(1))
                num_laps = float(laps_match.group(1))
                total_distance = (lap_length * num_laps) / 1000  # Convert meters to km
                return sex, total_distance, 'Skiathlon', False, False, None
            except (ValueError, IndexError):
                pass
    
    return None

def _check_mass_start(text, race_title, sex):
    """Check if race is a mass start"""
    patterns = [
        r'(\d+(?:\.?\d)?)\s*km\s+Mass\s+Start',          # 15 km Mass Start
        r'(\d+(?:\.?\d)?)\s*km.*?Mass\s+Start',          # More flexible
        r'Mass\s+Start.*?(\d+(?:\.?\d)?)\s*km',          # Reverse order
        r'(\d+(?:\.?\d)?)\s*km.*?[CF]\b'                 # Distance with technique
    ]
    
    for search_text in [race_title, text]:
        for pattern in patterns:
            match = re.search(pattern, search_text, re.IGNORECASE)
            if match:
                distance = float(match.group(1))
                technique = extract_technique(search_text)
                return sex, distance, technique, False, True, 'MS'
                
    # If we found Mass Start but no distance pattern matched
    if 'MASS START' in text.upper():
        # Look for any distance in the text
        distance_match = re.search(r'(\d+(?:\.?\d)?)\s*km', text)
        if distance_match:
            distance = float(distance_match.group(1))
            technique = extract_technique(text)
            return sex, distance, technique, False, True, 'MS'
            
    return None

def _check_general_race(text, race_title, sex):
    """Check for standard distance race"""
    pattern = r'(\d+(?:\.?\d)?)\s*km\s+([F|C])'
    match = re.search(pattern, race_title or text)
    
    if match:
        distance = float(match.group(1))
        technique = match.group(2)
        # If no mass start/pursuit indicators, it's an individual start
        if not re.search(r'Mass Start|Pursuit|Sprint|Relay', text):
            return sex, distance, technique, False, False, 'Ind'
        else:
            return sex, distance, technique, False, False, None
    return None

def _extract_basic_components(text, sex):
    """Extract basic components as last resort"""
    # Look for pattern with technique
    pattern = r'(\d+(?:\.?\d)?)\s*km\s+(?:Classical|Classic|Free(?:style)?|[CF])'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return sex, float(match.group(1)), extract_technique(text), False, False, None
        
    # Try to find any distance mention
    distance_match = re.search(r'(\d+(?:\.?\d)?)\s*km', text)
    distance = float(distance_match.group(1)) if distance_match else None
    
    technique = extract_technique(text)
    
    return sex, distance, technique, False, False, None

def _extract_relay_distance(text):
    """Extract the total distance for relay events"""
    # Look for relay distance patterns
    patterns = [
        r'(\d+)\s*x\s*(\d+(?:\.\d)?)\s*km',  # e.g., "4 x 5 km" or "3 x 3.3 km"
        r'(\d+)(?:\s*\+\s*\d+)*\s*x\s*(\d+(?:\.\d)?)\s*km',  # e.g., "2+2 x 5 km"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                legs = float(match.group(1))
                leg_distance = float(match.group(2))
                return legs * leg_distance
            except (ValueError, TypeError):
                continue
    
    # If we can't find explicit distances, try course information
    lap_match = re.search(r'Length of Lap:\s*(\d+(?:\.\d+)?)', text)
    laps_match = re.search(r'Number of Laps:\s*(\d+)', text)
    if lap_match and laps_match:
        try:
            lap_length = float(lap_match.group(1))
            num_laps = float(laps_match.group(1))
            return (lap_length * num_laps) / 1000  # Convert meters to km
        except (ValueError, TypeError):
            pass
            
    return None


def process_pdf_content(text, filename, sex):
    """Process the PDF content and extract all relevant information"""
    if text is None:
        logging.warning(f"No text content for {filename}")
        return {
            'filename': filename,
            'date': filename[:8],  # Extract YYYYMMDD from filename
            'city': None,
            'country': None,
            'sex': sex,
            'distance': None,
            'technique': None,
            'height_difference': None,
            'maximum_climb': None,
            'total_climb': None,
            'lap_length': None,
            'number_of_laps': None,
            'total_race_climb': None,
            'is_sprint': None,
            'mass_start': None,
            'race_type': None
        }
    
    # Extract basic information with race type, but use provided sex and filename date
    _, distance, technique, is_sprint, is_mass_start, race_type = extract_race_info(text)
    city, country = extract_location(text)
    date = filename[:8]  # Extract YYYYMMDD from filename
    course_info = extract_course_info(text)
    
    # Calculate total race climb if possible
    total_race_climb = None
    if course_info['total_climb'] is not None and course_info['number_of_laps'] is not None:
        total_race_climb = course_info['total_climb'] * course_info['number_of_laps']
    
    # Special handling for relay and team sprint distances
    if race_type in ['Rel', 'TS']:
        relay_distance = _extract_relay_distance(text)
        distance = relay_distance if relay_distance is not None else -1
    
    # Create race data dictionary with provided sex and filename date
    race_data = {
        'filename': filename,
        'date': date,
        'city': city,
        'country': country,
        'sex': sex,
        'distance': float(distance) if isinstance(distance, (int, float)) else None,
        'technique': technique,
        'height_difference': course_info['height_difference'],
        'maximum_climb': course_info['maximum_climb'],
        'total_climb': course_info['total_climb'],
        'lap_length': course_info['lap_length'],
        'number_of_laps': course_info['number_of_laps'],
        'total_race_climb': total_race_climb,
        'is_sprint': is_sprint,
        'mass_start': int(is_mass_start) if is_mass_start is not None else None,
        'race_type': race_type
    }
    
    return race_data

def process_single_pdf(pdf_path, sex):
    """Process a single PDF file"""
    logging.info(f"Processing {pdf_path}")
    text = extract_text_from_pdf(pdf_path)
    return process_pdf_content(text, os.path.basename(pdf_path), sex)


def process_pdf_content(text, filename, sex):
    """Process the PDF content and extract all relevant information"""
    # Extract date and city from filename (format: yyyymmddCity.pdf)
    date = filename[:8]  # First 8 characters for date
    city = filename[8:-4]  # Remove date and .pdf extension
    
    if text is None:
        logging.warning(f"No text content for {filename}")
        return {
            'filename': filename,
            'date': date,
            'city': city,
            'country': None,
            'sex': sex,
            'distance': None,
            'technique': None,
            'height_difference': None,
            'maximum_climb': None,
            'total_climb': None,
            'lap_length': None,
            'number_of_laps': None,
            'total_race_climb': None,
            'is_sprint': None,
            'mass_start': None,
            'race_type': None
        }
    
    # Extract basic information with race type, but use provided sex and filename date/city
    _, distance, technique, is_sprint, is_mass_start, race_type = extract_race_info(text)
    _, country = extract_location(text)  # Only use country from PDF text
    course_info = extract_course_info(text)
    
    # Calculate total race climb if possible
    total_race_climb = None
    if course_info['total_climb'] is not None and course_info['number_of_laps'] is not None:
        total_race_climb = course_info['total_climb'] * course_info['number_of_laps']
    
    # Special handling for relay and team sprint distances
    if race_type in ['Rel', 'TS']:
        relay_distance = _extract_relay_distance(text)
        distance = relay_distance if relay_distance is not None else -1
    
    # Create race data dictionary with provided sex and filename date/city
    race_data = {
        'filename': filename,
        'date': date,
        'city': city,
        'country': country,
        'sex': sex,
        'distance': float(distance) if isinstance(distance, (int, float)) else None,
        'technique': technique,
        'height_difference': course_info['height_difference'],
        'maximum_climb': course_info['maximum_climb'],
        'total_climb': course_info['total_climb'],
        'lap_length': course_info['lap_length'],
        'number_of_laps': course_info['number_of_laps'],
        'total_race_climb': total_race_climb,
        'is_sprint': is_sprint,
        'mass_start': int(is_mass_start) if is_mass_start is not None else None,
        'race_type': race_type
    }
    
    return race_data


def process_multiple_years(start_year=2002, end_year=2024):
    """Process PDFs for multiple years with separate files for each gender"""
    base_pdf_dir = os.path.expanduser('~/ski/elo/python/elo/polars/excel365/fis_pdfs')
    output_dir = os.path.expanduser('~/ski/elo/python/elo/polars/excel365')
    
    # Rest of the function remains the same
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create separate results lists for each gender
    results_by_gender = {
        'M': [],
        'W': [],
        'Unknown': []
    }
    all_skipped_files = {}
    
    # Process each gender directory
    gender_dirs = {
        'M': 'Men',
        'W': 'Women'
    }
    
    for gender_code, gender_name in gender_dirs.items():
        gender_base_dir = os.path.join(base_pdf_dir, gender_code)
        
        for year in range(start_year, end_year + 1):
            pdf_dir = os.path.join(gender_base_dir, str(year))
            
            if not os.path.exists(pdf_dir):
                logging.warning(f"Directory for year {year} and gender {gender_name} not found: {pdf_dir}")
                continue
                
            logging.info(f"\nProcessing {gender_name}'s races for year {year}...")
            
            # Get list of PDF files
            pdf_files = list(Path(pdf_dir).glob('*.pdf'))
            
            # Process PDFs in parallel
            with ThreadPoolExecutor(max_workers=None) as executor:
                futures = [executor.submit(process_single_pdf, pdf_file, gender_name) 
                          for pdf_file in pdf_files]
                
                for future, pdf_file in zip(futures, pdf_files):
                    try:
                        result = future.result()
                        if result is not None:
                            # Add result to appropriate gender list
                            if result['sex'] == 'Men':
                                results_by_gender['M'].append(result)
                            elif result['sex'] == 'Women':
                                results_by_gender['W'].append(result)
                            else:
                                results_by_gender['Unknown'].append(result)
                        else:
                            filename = os.path.basename(pdf_file)
                            all_skipped_files[filename] = "Failed to extract data"
                    except Exception as e:
                        filename = os.path.basename(pdf_file)
                        all_skipped_files[filename] = f"Error: {str(e)}"
    
    # Process and save results for each gender
    dataframes = {}
    for gender_code, results in results_by_gender.items():
        if results:  # Only process if we have results for this gender
            df = pd.DataFrame(results)
            
            # Sort by date
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            df = df.sort_values('date')
            df['date'] = df['date'].dt.strftime('%Y%m%d')
            
            # Define type conversions with nullable types
            type_conversions = {
                'filename': 'string',
                'date': 'string',
                'city': 'string',
                'country': 'string',
                'sex': 'string',
                'distance': 'float64',
                'technique': 'string',
                'height_difference': 'float64',
                'maximum_climb': 'float64',
                'total_climb': 'float64',
                'lap_length': 'float64',
                'number_of_laps': 'float64',
                'total_race_climb': 'float64',
                'is_sprint': 'boolean',
                'mass_start': 'Int64',
                'race_type': 'string'
            }
            
            # Convert each column carefully
            for col, dtype in type_conversions.items():
                if col in df.columns:
                    try:
                        if dtype == 'boolean':
                            df[col] = df[col].fillna(False)
                        elif dtype == 'Int64':
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        elif dtype.startswith('float'):
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        else:
                            df[col] = df[col].astype(dtype)
                    except Exception as e:
                        logging.warning(f"Error converting column {col} to {dtype}: {str(e)}")
            
            # Replace NaN values with None for string columns
            string_columns = ['filename', 'date', 'city', 'country', 'sex', 'technique', 'race_type']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].where(df[col].notna(), None)
            
            # Save gender-specific results
            gender_name = gender_code
            feather_file = os.path.join(output_dir, f'race_data_{gender_name}.feather')
            csv_file = os.path.join(output_dir, f'race_data_{gender_name}.csv')
            
            df.to_feather(feather_file)
            df.to_csv(csv_file, index=False)
            
            dataframes[gender_code] = df
            
            # Print summary statistics for this gender
            print(f"\nProcessing Summary for {gender_name}:")
            print(f"Successfully processed: {len(df)} races")
            
            print(f"\nRaces by year ({gender_name}):")
            df['year'] = pd.to_numeric(df['date'].str[:4])
            print(df['year'].value_counts().sort_index())
            
            print(f"\nRaces by technique ({gender_name}):")
            print(df['technique'].value_counts())
    
    if all_skipped_files:
        print("\nSkipped files summary:")
        for filename, reason in sorted(all_skipped_files.items()):
            print(f"{filename}: {reason}")
    
    return dataframes

def main():
    """Main function to run the PDF processing"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    dataframes = process_multiple_years(start_year=2002, end_year=2025)
    
    if dataframes:
        logging.info("Processing complete!")
        for gender, df in dataframes.items():
            logging.info(f"Processed {len(df)} races for {gender}")
    else:
        logging.error("No data was processed successfully")

if __name__ == "__main__":
    main()