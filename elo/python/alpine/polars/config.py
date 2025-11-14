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
    # Major alpine nations (4 athletes - full quota)
    'Austria': ['Vincent Kriechmayr', 'Stefan Brennsteiner', 'Manuel Feller', 'Marco Schwarz', 'Fabio Gstrein', 'Raphael Haaser', 'Daniel Hemetsberger', 'Patrick Feurstein', 'Stefan Babinsky', 'Johannes Strolz', 'Stefan Eichberger'],
    'Switzerland': ['Marco Odermatt', 'Loic Meillard', 'Franjo Von Allmen', 'Stefan Rogentin', 'Thomas Tumler', 'Alexis Monney', 'Tanguy Nef', 'Justin Murisier', 'Gino Caviezel', 'Daniel Yule', 'Marc Rochat'],
    'Norway': ['Henrik Kristoffersen', 'Timon Haugan', 'Atle Lie McGrath', 'Adrian Smiseth Sejersted', 'Alexander Steen Olsen', 'Fredrik Møller', 'Rasmus Windingstad', 'Rasmus Bakkevig', 'Oscar Andreas Sandvik', 'Jesper Wahlqvist'],
    'Italy': ['Dominik Paris', 'Luca De Aliprandini', 'Mattia Casse', 'Alex Vinatzer', 'Florian Schieder', 'Filippo Della Vite', 'Tommaso Sala', 'Christof Innerhofer', 'Giovanni Borsotti', 'Stefano Gross', 'Giovanni Franzoni'],
    
    # Medium nations (2-3 athletes - partial quota)
    'France': ['Nils Allegre', 'Cyprien Sarrazin', 'Thibaut Favrot', 'Clement Noel', 'Alexis Pinturault', 'Steven Amiez', 'Victor Muffat-Jeandet', 'Blaise Giezendanner', 'Paco Rassat', 'Leo Anguenot'],
    'Slovenia': ['Zan Kranjec', 'Miha Hrobat', 'Anze Gartner', 'Nejc Naralocnik'],
    'Germany': ['Linus Strasser', 'Alexander Schmid', 'Simon Jocher', 'Romed Baumann', 'Fabian Gratz'],
    
    # Small nations (1 athlete)
    'Croatia': ['Filip Zubcic', 'Samuel Kolega', 'Istok Rodes'],
    'Canada': ['Cameron Alexander', 'James Crawford', 'Jeffrey Read', 'Brodie Seger', 'Jesse Kertesz-Knight', 'Erik Read'],
    'USA': ['Ryan Cochran-Siegle', 'River Radamus', 'Tommy Ford', 'Benjamin Ritchie', 'Bryce Bennett', 'Jared Goldberg'],
    'Sweden': ['Kristoffer Jakobsen', 'Felix Monsen', 'Adam Hofstedt', 'Emil Pettersson'],
    'Czechia': ['Jan Zabystran', 'Patryk Forejtek'],
    'Finland': ['Elian Lehto', 'Eduard Hallberg', 'Jaakko Tapanainen'],
    'Albania': ['Denni Xhepa'],
    'Andorra': ['Joan Verdu Sanchez', 'Alex Rius Gimenez'],
    'Belgium': ['Sam Maes', 'Armand Marchant'],
    'Brazil': ['Lucas Pinheiro Braathen', 'Giovanni Ongaro'],
    'New Zealand': ['Sam Hadley'],
    'Slovakia': ['Adam Novacek'],
    'Argentina': ['Nicolas Quintero'],
    'Australia': ['Hugh McAdam'],
    'Bosnia and Herzegovina': ['Dino Terzic'],
    'Bulgaria': ['Albert Popov', 'Konstantin Stoilov'],
    'Estonia': ['Tormis Laine', 'Juhan Luik'],
    'Great Britain': ['Dave Ryding', 'Billy Major'],
    'Poland': ['Piotr Habdas']
}

CHAMPS_ATHLETES_LADIES = {
    # Major alpine nations (4 athletes - full quota)
    'Switzerland': ['Lara Gut-Behrami', 'Wendy Holdener', 'Michelle Gisin', 'Camille Rast'],
    'Austria': ['Cornelia Hütter', 'Katharina Liensberger', 'Stephanie Venier', 'Mirjam Puchner', 'Ariane Raedler', 'Julia Scheib', 'Ricarda Haaser', 'Katharina Gallhuber', 'Stephanie Brunner', 'Nina Ortlieb', 'Katharina Truppe'],
    'Italy': ['Federica Brignone', 'Sofia Goggia', 'Elena Curtoni', 'Marta Bassino', 'Laura Pirovano', 'Martina Peterlini', 'Lara Della Mea', 'Marta Rossetti', 'Nicol Delago', 'Asja Zenere', 'Roberta Melesi'],
    'USA': ['Mikaela Shiffrin', 'Paula Moltzan', 'Lindsey Vonn', 'Lauren Macuga', 'AJ Hurt', 'Breezy Johnson', 'Nina O\'Brien', 'Katie Hensien', 'Jacqueline Wiles', 'Isabella Wright', 'Mia Hunt'],
    
    # Medium nations (2-3 athletes - partial quota)
    'Slovenia': ['Neja Dvornik', 'Ilka Stuhec', 'Ana Bucik Jogan', 'Andreja Slokar', 'Anja Oplotnik', 'Nika Tomsic'],
    'France': ['Romane Miradoli', 'Laura Gauche', 'Marie Lamure', 'Clara Direz', 'Chiara Pogneaux', 'Marion Chevrier', 'Clarisse Breche', 'June Brand'],
    'Germany': ['Lena Dürr', 'Kira Weidle-Winkelmann', 'Emma Aicher', 'Jessica Hilzinger', 'Jana Fritz', 'Anna Schillinger', 'Fabiana Dorigo'],
    
    # Small nations (1 athlete)
    'Slovakia': ['Petra Vlhova'],
    'Sweden': ['Sara Hector', 'Anna Swenn-Larsson', 'Cornelia Öhlund', 'Estelle Alphand', 'Hanna Aronsson Elfman', 'Elsa Fermbäck', 'Lisa Nyberg'],
    'Norway': ['Kajsa Vickhoff Lie', 'Mina Fürst Holtmann', 'Thea Louise Stjernesund', 'Bianca Bakke Westhoff', 'Madeline Sylvester-Davik', 'Marte Monsen', 'Kristin Lysdahl'],
    'Canada': ['Valerie Grenier', 'Brett Richardson', 'Ali Nullmeyer', 'Laurence St-Germain', 'Amelia Smart', 'Cassidy Gray'],
    'Croatia': ['Zrinka Ljutic', 'Leona Popovic'],
    'Czechia': ['Ester Ledecka', 'Martina Dubovska', 'Barbora Novakova'],
    'Finland': ['Aada Kanto', 'Silja Koskinen'],
    'Albania': ['Lara Colturi'],
    'Andorra': ['Cande Moreno Becerra'],
    'New Zealand': ['Alice Robinson'],
    'Slovakia': ['Rebeka Jancova'],
    'Argentina': ['Francesca Baruzzi Farriol'],
    'Bosnia and Herzegovina': ['Esma Alic', 'Nikolina Dragljevic'],
    'Great Britain': ['Victoria Palla'],
    'Poland': ['Maryna Gasienica Daniel', 'Magdalena Luczak']
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