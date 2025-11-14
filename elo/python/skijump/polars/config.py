# Nation quotas based on FIS World Cup rules
NATION_QUOTAS = {
    'Andorra': {'men': 3, 'ladies': 3},
    'Armenia': {'men': 2, 'ladies': 3},
    'Australia': {'men': 3, 'ladies': 3},
    'Austria': {'men': 5, 'ladies': 5},
    'Bulgaria': {'men': 3, 'ladies': 2},
    'Canada': {'men': 5, 'ladies': 5},
    'China': {'men': 3, 'ladies': 2},
    'Czechia': {'men': 5, 'ladies': 5},
    'Estonia': {'men': 4, 'ladies': 4},
    'Spain': {'men': 3, 'ladies': 2},
    'Finland': {'men': 6, 'ladies': 6},
    'France': {'men': 6, 'ladies': 6},
    'Great Britain': {'men': 5, 'ladies': 2},
    'Germany': {'men': 6, 'ladies': 6},
    'Italy': {'men': 6, 'ladies': 6},
    'Ireland': {'men': 4, 'ladies': 2},
    'Japan': {'men': 4, 'ladies': 3},
    'Kazakhstan': {'men': 3, 'ladies': 4},
    'Latvia': {'men': 3, 'ladies': 5},
    'Norway': {'men': 6, 'ladies': 6},
    'Poland': {'men': 3, 'ladies': 4},
    'Slovenia': {'men': 4, 'ladies': 4},
    'Switzerland': {'men': 6, 'ladies': 6},
    'Sweden': {'men': 6, 'ladies': 6},
    'Ukraine': {'men': 2, 'ladies': 3},
    'USA': {'men': 6, 'ladies': 6}
}

# Default quota for unlisted nations
DEFAULT_QUOTA = {'men': 2, 'ladies': 2}

# World Cup leaders (get extra start)
WC_LEADERS = {
    'men': ['Norway'],
    'ladies': ['USA']
}

def get_nation_quota(nation: str, gender: str, is_host: bool = False) -> int:
    """
    Get the quota for a nation considering:
    1. Base quota from NATION_QUOTAS or default
    2. Host country bonus (+5 if applicable)
    3. World Cup leader bonus (+1 if applicable)
    
    Example:
    >>> get_nation_quota('Norway', 'men', is_host=True)
    12  # Base(6) + Leader(1) + Host(5)
    
    Args:
        nation: The nation code
        gender: 'men' or 'ladies'
        is_host: Whether this nation is hosting
    
    Returns:
        int: The total quota for the nation
    """
    # Get base quota
    base_quota = NATION_QUOTAS.get(nation, DEFAULT_QUOTA).get(gender, DEFAULT_QUOTA[gender])
    
    # Add host country bonus
    if is_host:
        base_quota += 5
        
    # Add World Cup leader bonus
    if nation in WC_LEADERS.get(gender, []):
        base_quota += 1
        
    return base_quota

# Add additional skiers organized by country
ADDITIONAL_SKIERS_MEN = {

}

ADDITIONAL_SKIERS_LADIES = {

}

def get_additional_skiers(gender: str) -> dict:
    """Get additional skiers for specified gender"""
    if gender == 'men':
        return ADDITIONAL_SKIERS_MEN
    elif gender == 'ladies':
        return ADDITIONAL_SKIERS_LADIES
    else:
        raise ValueError(f"Unknown gender: {gender}")

# For backward compatibility
ADDITIONAL_SKIERS = ADDITIONAL_SKIERS_MEN

# Championships-specific quotas (for Olympics, World Championships, etc.)
# Base quota is 4 per nation, but actual quota is limited by number of available athletes
CHAMPS_BASE_QUOTA = {'men': 4, 'ladies': 4}

# Default Championships quota for unlisted nations
DEFAULT_CHAMPS_QUOTA = {'men': 4, 'ladies': 4}

# Championships athlete lists (populate with actual athlete names as needed)
CHAMPS_ATHLETES_MEN = {
    # Major ski jumping nations (4 athletes - full quota)
    'Austria': ['Jan Hörl', 'Stefan Kraft', 'Daniel Tschofenig', 'Manuel Fettner'],
    'Germany': ['Andreas Wellinger', 'Karl Geiger', 'Pius Paschke', 'Philipp Raimund'],
    'Norway': ['Johann Forfang', 'Kristoffer Eriksen Sundal', 'Marius Lindvik', 'Halvor Egner Granerud'],
    'Poland': ['Pawel Wasek', 'Aleksander Zniszczol', 'Kamil Stoch', 'Dawid Kubacki'],
    'Slovenia': ['Anze Lanisek', 'Domen Prevc', 'Timi Zajc', 'Lovro Kos'],
    'Switzerland': ['Gregor Deschwanden', 'Killian Peier', 'Sandro Hauswirth', 'Yannick Wasser'],
    'Japan': ['Ryoyu Kobayashi', 'Ren Nikaido', 'Naoki Nakamura', 'Yukiya Sato'],
    
    # Medium nations (2-3 athletes - partial quota)
    'Finland': ['Antti Aalto', 'Tomas Kuisma', 'Paavo Romppainen', 'Vilho Palosaari'],
    'Italy': ['Alex Insam', 'Francesco Cecon'],
    'France': ['Valentin Foubert', 'Enzo Milesi'],
    'Czechia': ['Roman Koudelka'],
    'USA': ['Tate Frantz', 'Kevin Bickner', 'Jason Colby', 'Erik Belshaw'],
    'Canada': [],
    'Sweden': [],
    
    # Small nations (1-2 athletes)
    'Estonia': ['Artti Aigro', 'Kaimar Vagul'],
    'Latvia': [],
    'Lithuania': [],
    'Russia': [],
    'Ukraine': ['Yevhen Marusiak', 'Vitaliy Kalinichenko'],
    'South Korea': [],
    'China': ['Weijie Zhen', 'Qiwu Song'],
    'Kazakhstan': ['Ilya Mizernykh', 'Danil Vassilyev'],
    'Romania': [],
    'Slovakia': ['Hektor Kapustik'],
    'Bulgaria': ['Vladimir Zografski'],
    'Great Britain': [],
    'Netherlands': [],
    'Belgium': [],
    'Denmark': [],
    'Iceland': [],
    'Belarus': [],
    'Spain': [],
    'Andorra': [],
    'Turkey': ['Muhammed Ali Bedir', 'Fatih Arda Ipcioglu'],
    'Greece': [],
    'Serbia': [],
    'Croatia': [],
    'Montenegro': [],
    'Bosnia and Herzegovina': [],
    'North Macedonia': [],
    'Moldova': [],
    'Georgia': [],
    'Armenia': [],
    'Azerbaijan': []
}

CHAMPS_ATHLETES_LADIES = {
    # Major ski jumping nations (4 athletes - full quota)
    'Austria': ['Eva Pinkelnig', 'Jacqueline Seifriedsberger', 'Lisa Eder', 'Marita Kramer'],
    'Germany': ['Selina Freitag', 'Katharina Schmid', 'Agnes Reisch', 'Juliane Seyfarth'],
    'Norway': ['Eirin Maria Kvandal', 'Anna Odine Strøm', 'Thea Minyan Bjørseth', 'Silje Opseth'],
    'Slovenia': ['Nika Prevc', 'Ema Klinec', 'Nika Kriznar', 'Jerica Jesenko'],
    'Switzerland': ['Rea Kindlimann', 'Sina Arnet'],
    'Japan': ['Sara Takanashi', 'Yuki Ito', 'Nozomi Maruyama', 'Yuka Seto'],
    
    # Medium nations (2-3 athletes - partial quota)
    'Poland': ['Natalia Slowik', 'Pola Beltowska', 'Anna Twardosz', 'Nicole Konderla'],
    'Finland': ['Jenny Rautionaho', 'Heta Hirvonen', 'Oosa Thure', 'Julia Kykkänen'],
    'Italy': ['Lara Malsiner', 'Annika Sieff', 'Martina Zanitzer', 'Jessica Malsiner'],
    'France': ['Josephine Pagnier', 'Emma Chervet'],
    'Czechia': ['Veronika Jencova', 'Karolina Indrackova', 'Anezka Indrackova', 'Klara Ulrichova'],
    'USA': ['Sandra Sproch', 'Annika Belshaw', 'Josie Johnson', 'Samantha Macuga'],
    'Canada': ['Alexandria Loutitt', 'Abigail Strate', 'Nicole Maurer'],
    'Sweden': ['Frida Westman'],
    
    # Small nations (1-2 athletes)
    'Estonia': [],
    'Latvia': [],
    'Lithuania': [],
    'Russia': [],
    'Ukraine': ['Zhanna Hlukhova'],
    'South Korea': [],
    'China': ['Qi Liu', 'Yutong Pan', 'Shiyu Zhou', 'Yangning Weng'],
    'Kazakhstan': [],
    'Romania': ['Daniela Haralambie'],
    'Slovakia': ['Kira Maria Kapustikova', 'Tamara Mesikova'],
    'Bulgaria': [],
    'Great Britain': [],
    'Netherlands': [],
    'Belgium': [],
    'Denmark': [],
    'Iceland': [],
    'Belarus': [],
    'Spain': [],
    'Turkey': [],
    'Serbia': [],
    'Croatia': [],
    'Montenegro': [],
    'Bosnia and Herzegovina': [],
    'North Macedonia': [],
    'Moldova': [],
    'Georgia': [],
    'Armenia': [],
    'Azerbaijan': []
}

def get_champs_quota(nation: str, gender: str) -> int:
    """
    Get the Championships quota for a nation
    Base quota is 4, but limited by the number of athletes available for that nation
    
    Args:
        nation: The nation code
        gender: 'men' or 'ladies'
    
    Returns:
        int: The actual Championships quota (min of 4 or number of athletes available)
    """
    # Get the number of athletes available for this nation/gender
    available_athletes = len(get_champs_athletes(nation, gender))
    
    # Base quota is 4 for all nations
    base_quota = CHAMPS_BASE_QUOTA.get(gender, 4)
    
    # Actual quota is the minimum of base quota and available athletes
    # If no athletes are configured, return base quota (allows for future population)
    if available_athletes == 0:
        return base_quota
    else:
        return min(base_quota, available_athletes)

def get_champs_athletes(nation: str, gender: str) -> list:
    """
    Get Championships athlete list for a nation
    
    Args:
        nation: The nation code
        gender: 'men' or 'ladies'
    
    Returns:
        list: List of athlete names for the nation and gender
    """
    if gender == 'men':
        return CHAMPS_ATHLETES_MEN.get(nation, [])
    elif gender == 'ladies':
        return CHAMPS_ATHLETES_LADIES.get(nation, [])
    else:
        raise ValueError(f"Unknown gender: {gender}")

# For backward compatibility with Championships functions
def get_championships_quota(nation: str, gender: str) -> int:
    """Alias for get_champs_quota for backward compatibility"""
    return get_champs_quota(nation, gender)

def get_championships_athletes(nation: str, gender: str) -> list:
    """Alias for get_champs_athletes for backward compatibility"""
    return get_champs_athletes(nation, gender)