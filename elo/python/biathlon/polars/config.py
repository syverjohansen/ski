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
    'Austria': ['Simon Eder', 'Patrick Jacob' 'Fabian Müllauer', 'Dominic Unterweger'],
    'Belgium': ['Florent Claude', 'Thierry Langer', 'Marek Mackels', 'Cesar Beauvais'],
    'Bulgaria': ['Blagoy Todev', 'Vladimir Iliev', 'Konstantin Vasilev', 'Vasil Zashev'],
    'Canada': ['Adam Runnalls', 'Gavin Johnston', 'Daniel Gilfillan', 'Jasper Fleming'],
    'China': ['Cang Gu'],
    'Croatia': ['Matija Legovic', 'Kresimir Crnkovic'],
    'Czechia': ['Vitezslav Hornig', 'Michal Krcmar', 'Tomas Mikyska', 'Jonas Marecek', 'Adam Vaclavik'],
    'Denmark': ['Rasmus Schiellerup'],
    'Estonia': ['Rene Zahkna', 'Kristo Siimer', 'Jakob Kulbin', 'Karl Rasmus Tiislar'],
    'Finland': ['Tero Seppälä', 'Olli Hiidensalo', 'Otto Invenius', 'Arttu Heikkinen', 'Jimi Klemettinen'],
    'France': ['Eric Perrot', 'Quentin Fillon Maillet', 'Emilien Jacquelin', 'Fabien Claude', 'Emilien Claude', 'Antonin Guigonnat'],
    'Germany': ['Philipp Nawrath', 'Justus Strelow', 'Philipp Horn', 'Roman Rees', 'Johannes Kühn'],
    'Italy': ['Tommaso Giacomel', 'Lukas Hofer', 'Didier Bionaz', 'Patrick Braunhofer', 'Daniele Cappellari'],
    'Japan': ['Mikito Tachizaki'],
    'Kazakhstan': ['Nikita Akimov', 'Vladislav Kireyev'],
    'Latvia': ['Andrejs Rastorgujevs', 'Rihards Lozbers', 'Renars Birkentals', 'Matiss Meirans'],
    'Lithuania': ['Vytautas Strolia', 'Maksim Fomin', 'Nikita Cigak', 'Tomas Kaukenas'],
    'Moldova': ['Pavel Magazeev', 'Maksim Makarov'],
    'Norway': ['Sturla Holm Lægreid', 'Vetle Sjåstad Christiansen', 'Endre Strømsheim', 'Johannes Dale-Skjevdal', 'Vebjørn Sørum', 'Martin Uldal'],
    'Poland': ['Jan Gunka', 'Konrad Badacz', 'Fabian Suchodolski', 'Andrzej Nedza-Kubiniec'],
    'Romania': ['Dmitrii Shamaev', 'George Buta', 'George Coltea', 'Cornel Puchianu'],
    'Slovakia': ['Simon Adamov', 'Jakub Borgula'],
    'Slovenia': ['Jakov Fak', 'Miha Dovzan', 'Anton Vidmar', 'Lovro Planko', 'Matic Repnik'],
    'South Korea': ['Timofei Lapshin'],
    'Sweden': ['Sebastian Samuelsson', 'Martin Ponsiluoma', 'Jesper Nelin', 'Victor Brandt', 'Emil Nykvist', 'Malte Stefansson'],
    'Switzerland': ['Niklas Hartweg', 'Joscha Burkhalter', 'Sebastian Stalder', 'Jeremy Finello', 'James Pacal'],
    'Ukraine': ['Dmytro Pidruchnyi', 'Vitalii Mandzyn', 'Anton Dudchenko', 'Artem Tyshchenko', 'Denys Nasyko'],
    'USA': ['Sean Doherty', 'Maxime Germain', 'Paul Schommer', 'Campbell Wright'],
}

CHAMPS_ATHLETES_LADIES = {
    'Australia': ['Darcie Morton'],
    'Austria': ['Anna Andexer', 'Anna Gandler', 'Lisa Theresa Hauser', 'Anna Juppe', 'Tamara Steiner'],
    'Belgium': ['Lotte Lie', 'Maya Cloetens', 'Marisa Emonts', 'Rieke de Maeyer'],
    'Brazil': ['Gaia Brunello'],
    'Bulgaria': ['Milena Todorova', 'Lora Hristova', 'Valentina Dimitrova', 'Stefani Yolova'],
    'Canada': ['Emma Lunder', 'Nadia Moser', 'Pascale Paradis', 'Benita Peiffer'],
    'China': ['Fanqi Meng', 'Jialin Tang'],
    'Croatia': ['Anika Kozica'],
    'Czechia': ['Marketa Davidova', 'Tereza Vobornikova', 'Jessica Jislova', 'Katerina Pavlu', 'Lucie Charvatova'],
    'Estonia': ['Tuuli Tomingas', 'Susan Külm', 'Regina Ermits', 'Violetta Konopljova'],
    'Finland': ['Suvi Minkkinen', 'Venla Lehtonen', 'Sonja Leinamo', 'Inka Hämäläinen', 'Noora Kaisa Keränen'],
    'France': ['Lou Jeanmonnot', 'Julia Simon', 'Oceane Michelon', 'Justine Braisaz-Bouchet', 'Jeanne Richard', 'Gilonne Guigonnat'],
    'Germany': ['Franziska Preuss', 'Selina Grotian', 'Vanessa Voigt', 'Julia Tannheimer', 'Sophia Schneider', 'Julia Kink'],
    'Italy': ['Dorothea Wierer', 'Michela Carrara', 'Samuela Comola', 'Hannah Auchentaller', 'Beatrice Trabucchi'],
    'Kazakhstan': ['Milana Geneva'],
    'Latvia': ['Baiba Bendika', 'Estere Volfa', 'Elza Bleidele', 'Sanita Bulina'],
    'Lithuania': ['Sara Urumova', 'Lidiia Zhurauskaite', 'Judita Traubaite', 'Natalija Kocergina'],
    'Moldova': ['Alina Stremous', 'Aliona Makarova'],
    'Norway': ['Maren Kirkeeide', 'Ingrid Landmark Tandrevold', 'Ida Lien', 'Karoline Offigstad Knotten', 'Marthe Kråkstad Johansen'],
    'Poland': ['Natalia Sidorowicz', 'Anna Nedza-Kubiniec', 'Joanna Jakiela', 'Anna Maka'],
    'Romania': ['Adelina Rimbeu', 'Anastasia Tolmacheva', 'Elena Chirkova', 'Andreea Mezdrea'],
    'Slovakia': ['Paulina Batovska Fialkova', 'Anastasiya Kuzmina', 'Ema Kapustova', 'Maria Remenova'],
    'Slovenia': ['Anamarija Lampic', 'Polona Klemencic', 'Klara Vindisar', 'Lena Repinc'],
    'South Korea': ['Ekaterina Avvakumova'],
    'Sweden': ['Elvira Öberg', 'Hanna Öberg', 'Anna Magnusson', 'Ella Halvarsson', 'Sara Andersson', 'Anna-Karin Heijdenberg'],
    'Switzerland': ['Aita Gasparin', 'Amy Baserga', 'Lena Häcki-Gross', 'Elisa Gasparin', 'Lea Meier'],
    'Ukraine': ['Yuliia Dzhima', 'Khrystyna Dmytrenko', 'Olena Horodna', 'Liliia Steblyna', 'Anastasiya Merkushyna'],
    'USA': ['Lucinda Anderson', 'Margie Freed', 'Deedra Irwin', 'Joanne Reid'],
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