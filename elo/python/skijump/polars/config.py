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
    # Major ski jumping nations (6 or more athletes total)
    'Austria': ['Stephan Embacher', 'Jan Hörl', 'Stefan Kraft', 'Daniel Tschofenig'],
    'Finland': ['Antti Aalto', 'Niko Kytösaho' ,'Vilho Palosaari'],
    'Germany': ['Felix Hoffmann', 'Pius Paschke',  'Philipp Raimund', 'Andreas Wellinger'],
    'Italy': ['Giovanni Bresadola', 'Francesco Cecon', 'Alex Insam'],
    'Japan': ['Ryoyu Kobayashi', 'Naoki Nakamura', 'Ren Nikaido'],
    'Norway': ['Johann Forfang', 'Marius Lindvik', 'Kristoffer Eriksen Sundal'],
    'Slovenia': ['Anze Lanisek', 'Domen Prevc', 'Timi Zajc'],
    'USA': ['Kevin Bickner','Jason Colby', 'Tate Frantz'],

    # Medium nations (4-5 athletes total)
    'Canada': ['Mackenzie Boyd-Clowes'],
    'China': ['Qiwu Song'],
    'Czechia': ['Roman Koudelka'],
    'France': ['Jules Chervet', 'Valentin Foubert', 'Enzo Milesi'],
    'Poland': ['Kamil Stoch', 'Kacper Tomasiak', 'Pawel Wasek'],
    'Romania': ['Daniel Andrei Cacina', 'Mihnea Alexandru Spulber'],
    'Switzerland': ['Gregor Deschwanden', 'Sandro Hauswirth', 'Felix Trunz'],
    
    # Small nations (3 or fewer athletes)
    'Bulgaria': ['Vladimir Zografski'],
    'Estonia': ['Artti Aigro', 'Kaimar Vagul'],
    'Kazakhstan': ['Ilya Mizernykh', 'Danil Vassilyev'],
    'Slovakia': ['Hektor Kapustik'],
    'Turkey': ['Muhammed Ali Bedir', 'Fatih Arda Ipcioglu'],
    'Ukraine': ['Vitaliy Kalinichenko', 'Yevhen Marusiak']
}

CHAMPS_ATHLETES_LADIES = {
    # Major ski jumping nations (6 or more athletes total)
    'Austria': ['Lisa Eder', 'Lisa Hirner', 'Julia Mühlbacher', 'Meghann Wadsak'],
    'Finland': ['Heta Hirvonen', 'Minja Korhonen', 'Sofia Mattila', 'Jenny Rautionaho'],
    'Germany': ['Selina Freitag', 'Agnes Reisch', 'Katharina Schmid',  'Juliane Seyfarth'],
    'Italy': ['Martina Ambrosi', 'Jessica Malsiner', 'Annika Sieff', 'Martina Zanitzer'],
    'Japan': ['Yuki Ito', 'Nozomi Maruyama', 'Yuka Seto', 'Sara Takanashi'],
    'Norway': ['Eirin Maria Kvandal', 'Silje Opseth', 'Anna Odine Strøm', 'Hyde Dyhre Tråserud'],
    'Slovenia': ['Katra Komar', 'Maja Kovacic', 'Nika Prevc', 'Nika Vodan'],
    'USA': ['Annika Belshaw', 'Josie Johnson', 'Paige Jones'],

    # Medium nations (4-5 athletes total)
    'Canada': ['Natalie Eilers', 'Nicole Maurer', 'Abigail Strate', ],
    'China': ['Bing Dong', 'Qi Liu', 'Yangning Weng', 'Ping Zeng'],
    'Czechia': ['Anezka Indrackova', 'Veronika Jencova', 'Klara Ulrichova'],
    'France': ['Emma Chervet', 'Josephine Pagnier'],
    'Poland': ['Pola Beltowska', 'Anna Twardosz'],
    'Romania': ['Delia Anamaria Folea', 'Daniela Haralambie'],
    'Switzerland': ['Sina Arnet'],

    # Small nations (3 or fewer athletes)
    'Slovakia': ['Kira Maria Kapustikova'],
    'Sweden': ['Frida Westman']
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