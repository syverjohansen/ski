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

# ============================================================================
# CHAMPIONSHIPS CONFIGURATION
# ============================================================================

# Championships quota settings - 6 athletes per nation for biathlon championships
CHAMPS_BASE_QUOTA = {'men': 4, 'ladies': 4}

# Championships athlete rosters
CHAMPS_ATHLETES_MEN = {
    #Major nations
    'Czechia': ['Petr Hak', 'Vitezslav Hornig',  'Mikulas Karlik', 'Michal Krcmar', 'Tomas Mikyska'],
    'Finland': ['Tuomas Harjula', 'Olli Hiidensalo', 'Otto Invenius', 'Jimi Klemettinen', 'Tero Seppälä'],
    'France': ['Emilien Claude', 'Fabien Claude', 'Quentin Fillon Maillet', 'Emilien Jacquelin', 'Oscar Lombardot', 'Eric Perrot'],
    'Germany': ['Lucas Fratzscher', 'Philipp Horn', 'Philipp Nawrath', 'Justus Strelow', 'David Zobel'],
    'Italy': ['Patrick Braunhofer', 'Tommaso Giacomel', 'Lukas Hofer',  'Nicola Romanin', 'Elia Zeni'],
    'Norway': ['Johan-Olav Botn', 'Vetle Sjåstad Christiansen', 'Johannes Dale-Skjevdal', 'Isak Leknes Frey', 'Sturla Holm Lægreid', 'Martin Uldal'],
    'Sweden': ['Victor Brandt','Jesper Nelin', 'Martin Ponsiluoma', 'Sebastian Samuelsson', 'Henning Sjökvist', 'Malte Stefansson'],
    'Switzerland': ['Joscha Burkhalter', 'Jeremy Finello', 'Niklas Hartweg',   'James Pacal', 'Sebastian Stalder'],
    'Ukraine': ['Bohdan Borkovskyi', 'Anton Dudchenko', 'Taras Lesiuk', 'Vitalii Mandzyn','Dmytro Pidruchnyi'],

    #Medium nations
    'Austria': ['Simon Eder', 'Patrick Jacob', 'Fabian Müllauer', 'Dominic Unterweger'],
    'Belgium': ['Florent Claude', 'Thierry Langer', 'Marek Mackels', 'Sam Parmantier'],
    'Bulgaria': ['Vladimir Iliev', 'Anton Sinapov', 'Blagoy Todev', 'Konstantin Vasilev'],
    'Canada': ['Zachary Connelly', 'Jasper Fleming', 'Logan Pletz', 'Adam Runnalls' ],
    'Estonia': ['Mark-Markos Kehva', 'Jakob Kulbin',  'Kristo Siimer',  'Rene Zahkna'],
    'Kazakhstan': ['Asset Dyussenov', 'Vladislav Kireyev'],
    'Latvia': ['Renars Birkentals', 'Rihards Lozbers', 'Edgars Mise', 'Andrejs Rastorgujevs'],
    'Lithuania': ['Nikita Cigak', 'Karol Dombrovski', 'Maksim Fomin', 'Vytautas Strolia'],
    'Poland': ['Konrad Badacz', 'Grzegorz Galica', 'Jan Gunka', 'Marcin Zawol'],
    'Romania': ['George Buta', 'George Coltea', 'Raul Flore', 'Dmitrii Shamaev'],
    'Slovakia': ['Simon Adamov', 'Jakub Borgula'],
    'Slovenia': ['Miha Dovzan', 'Jakov Fak',  'Lovro Planko', 'Matic Repnik', 'Anton Vidmar'],
    'USA': ['Sean Doherty', 'Maxime Germain', 'Paul Schommer', 'Campbell Wright'],

    #Fewer than 4 total athletes Nations
    'China': ['Xingyuan Yan'],
    'Croatia': ['Kresimir Crnkovic', 'Matija Legovic'],
    'Denmark': ['Sondre Slettemark'],
    'Great Britain': ['Jacques Jefferies'],
    'Moldova': ['Pavel Magazeev', 'Maksim Makarov'],
    'South Korea': ['Dujin Choi']

}

CHAMPS_ATHLETES_LADIES = {
    #Major Nations (10 or more)
    'Czechia': ['Lucie Charvatova', 'Marketa Davidova', 'Jessica Jislova', 'Tereza Vinklarkova', 'Tereza Vobornikova'],
    'Finland': ['Inka Hämäläinen', 'Noora Kaisa Keränen', 'Venla Lehtonen', 'Sonja Leinamo', 'Suvi Minkkinen'],
    'France': ['Camille Bened', 'Justine Braisaz-Bouchet', 'Lou Jeanmonnot', 'Oceane Michelon', 'Jeanne Richard', 'Julia Simon'],
    'Germany': ['Selina Grotian', 'Janina Hettich-Walz', 'Franziska Preuss',  'Julia Tannheimer', 'Vanessa Voigt',  'Anna Weidel'],
    'Italy': ['Hannah Auchentaller','Michela Carrara', 'Lisa Vittozzi', 'Dorothea Wierer'],
    'Norway': ['Juni Arnekleiv', 'Marthe Kråkstad Johansen', 'Maren Kirkeeide', 'Karoline Offigstad Knotten', 'Ingrid Landmark Tandrevold'],
    'Sweden': ['Linn Gestblom', 'Ella Halvarsson', 'Anna-Karin Heijdenberg', 'Anna Magnusson', 'Elvira Öberg', 'Hanna Öberg'],
    'Switzerland': ['Amy Baserga', 'Aita Gasparin',  'Lena Häcki-Gross', 'Lea Meier', 'Lydia Mettler'],
    'Ukraine': ['Darnya Chalyk', 'Khrystyna Dmytrenko','Yuliia Dzhima',  'Olena Horodna', 'Anastasiya Merkushyna'],

    #Medium Nations
    'Austria': ['Anna Andexer', 'Anna Gandler', 'Lisa Theresa Hauser', 'Anna Juppe', 'Tamara Steiner'],
    'Belgium': [ 'Eve Bouvard', 'Maya Cloetens', 'Marisa Emonts', 'Lotte Lie'],
    'Bulgaria': ['Valentina Dimitrova','Lora Hristova', 'Milena Todorova',   'Maria Zdravkova'],
    'Canada': ['Nadia Moser', 'Pascale Paradis', 'Benita Peiffer', 'Shilo Rousseau'],
    'Estonia': ['Regina Ermits','Susan Külm', 'Johanna Talihaerm', 'Tuuli Tomingas'],
    'Kazakhstan': ['Milana Geneva', 'Aisha Rakisheva'],
    'Latvia': ['Baiba Bendika', 'Sanita Bulina', 'Annija Sabule', 'Estere Volfa'],
    'Lithuania': ['Natalja Kocergina', 'Judita Traubaite', 'Sara Urumova', 'Lidiia Zhurauskaite'],
    'Poland': ['Joanna Jakiela', 'Anna Maka', 'Natalia Sidorowicz', 'Kamila Zuk'],
    'Romania': ['Anastasia Tolmacheva'],
    'Slovakia': ['Paulina Batovska Fialkova', 'Ema Kapustova', 'Anastasiya Kuzmina',  'Maria Remenova'],
    'Slovenia': ['Manca Caserman', 'Polona Klemencic', 'Anamarija Lampic',  'Lena Repinc'], 
    'USA': ['Lucinda Anderson', 'Margie Freed', 'Deedra Irwin', 'Joanne Reid'],

    #Minor nations (fewer than 4)
    'Australia': ['Darcie Morton'],
    'China': ['Yuanmeng Chu', 'Fanqi Meng'],
    'Croatia': ['Anika Kozica'],
    'Denmark': ['Anne de Besche', 'Ukaleq Astri Slettemark'],
    'Great Britain': ['Shawna Pendry'],
    'Moldova': ['Alina Stremous'],
    'South Korea': ['Ekaterina Avvakumova']
    
    
}

def get_champs_athletes(nation, gender):
    """
    Get the list of championships athletes for a given nation and gender.
    
    Args:
        nation (str): Nation name
        gender (str): 'men' or 'ladies'
    
    Returns:
        list: List of athlete names, empty list if nation not found
    """
    if gender == 'men':
        return CHAMPS_ATHLETES_MEN.get(nation, [])
    elif gender in ['ladies', 'women']:
        return CHAMPS_ATHLETES_LADIES.get(nation, [])
    else:
        return []

def get_champs_quota(gender):
    """
    Get the championships quota for a given gender.
    
    Args:
        gender (str): 'men' or 'ladies'
        
    Returns:
        int: Number of athletes allowed per nation
    """
    return CHAMPS_BASE_QUOTA.get(gender, 6)  # Default to 6 if not found

def get_all_champs_nations():
    """
    Get all nations that have championships athletes configured.
    
    Returns:
        set: Set of all nation names
    """
    men_nations = set(CHAMPS_ATHLETES_MEN.keys())
    ladies_nations = set(CHAMPS_ATHLETES_LADIES.keys())
    return men_nations.union(ladies_nations)

# Relay team configuration
def get_relay_team_size(race_type):
    """
    Get the team size for different relay race types.
    
    Args:
        race_type (str): Type of relay race
        
    Returns:
        int: Number of athletes per team
    """
    if 'Single Mixed' in race_type:
        return 2  # 1 man + 1 woman
    elif 'Mixed' in race_type:
        return 4  # 2 men + 2 women
    elif 'Relay' in race_type:
        return 4  # 4 athletes of same gender
    else:
        return 4  # Default

def get_mixed_team_composition(race_type):
    """
    Get the gender composition for mixed relay races.
    
    Args:
        race_type (str): Type of relay race
        
    Returns:
        dict: Number of men and ladies required
    """
    if 'Single Mixed' in race_type:
        return {'men': 1, 'ladies': 1}
    elif 'Mixed' in race_type:
        return {'men': 2, 'ladies': 2}
    else:
        return {'men': 0, 'ladies': 0}