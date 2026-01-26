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
    'Russia': {'men': 1, 'ladies': 1},
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
# Format:
#   - Just a name string: all races calculated from historical probability
#   - Dict with 'yes' list: confirmed racing those races, others calculated
#   - Dict with 'no' list: confirmed NOT racing those races, others calculated
#   - Dict with both: 'yes' = 1.0, 'no' = 0.0, unlisted = calculated
# Examples:
#   'Skier Name'                           # All races calculated from history
#   {'name': 'Skier', 'yes': [2]}          # Race 2 confirmed, race 1 calculated
#   {'name': 'Skier', 'no': [1]}           # Race 1 not racing, race 2 calculated
#   {'name': 'Skier', 'yes': [2], 'no': [1]}  # Race 2 confirmed, race 1 not racing
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

# Cross-country Championships Athletes (4 per nation maximum)
CHAMPS_ATHLETES_MEN_XC = {
    'Andorra': ['Irineu Esteve Altimiras'],
    'Argentina': ['Mateo Lorenzo Sauma', 'Franco Dal Farra'],
    'Armenia': ['Mikayel Mikayelyan'],
    'Australia': ['Lars Young Vik', 'Hugo Hinckfuss', 'Seve De Campo', 'Phil Bellingham'],
    'Austria': ['Mika Vermeulen', 'Benjamin Moser', 'Michael Föttinger'],
    'Belgium': ['Samuel Maes'],
    'Bolivia': ['Timo Juhani Gronlund'],
    'Bosnia&Herzegovina': ['Srdjan Lalovic'],
    'Brazil': ['Guilherme Pereira Santos'],
    'Bulgaria': ['Mario Matikanov', 'Daniel Peshkov'],
    'Canada': ['Antoine Cyr', 'Xavier McKeever', 'Remi Drolet', 'Max Hollmann', 'Tom Stephen'],
    'Chile': ['Sigurd Herrera'],
    'China': ['Qiang Wang', 'Minglin Li'],
    'Colombia': ['Samuel Jaramillo'],
    'Croatia': ['Marko Skender'],
    'Czechia': ['Michal Novak', 'Ondrej Cerny', 'Matyas Bauer', 'Jiri Tuz', 'Mike Ophoff'],
    'Denmark': ['Magnus Tobiassen'],
    'Estonia': ['Alvar Johannes Alev', 'Karl Sebastian Dremljuga', 'Hendrik Peterson'],
    'Finland': ['Iivo Niskanen', 'Lauri Vuorinen', 'Ristomatti Hakola', 'Joni Mäki', 'Arsi Ruuskanen', 'Emil Liekari', 'Nilo Moilanen', 'Niko Anttola'],
    'France': ['Hugo Lapalus', 'Richard Jouve', 'Jules Chappaz', 'Lucas Chanavat', 'Mathis Desloges', 'Remi Bourdin', 'Jules Lapierre'],
    'Germany': ['Friedrich Moch', 'Jan Stölben', 'Florian Notz', 'Elias Keck', 'Janosch Brugger'],
    'Great Britain': ['Andrew Musgrave', 'Joe Davies', 'James Clugnet'],
    'Greece': ['Panagiotis Papasis'],
    'Haiti': ['Theo Mallett'],
    'Hungary': ['Adam Büki', 'Adam Konya'],
    'Iceland': ['Einar Arni Gislason'],
    'India': ['Shubam Parihar'],
    'Iran': ['Seyed Ahmad Reza Seyd'],
    'Ireland': ['Thomas Hjalmar Westgård', 'Dylan Longridge'],
    'Italy': ['Federico Pellegrino', 'Davide Graz', 'Elia Barp', 'Michael Hellweger', 'Giovanni Ticco'],
    'Japan': ['Naoto Baba', 'Ryo Hirose', 'Haruki Yamashita'],
    'Kazakhstan': ['Svyatoslav Matassov', 'Vitaliy Pukhkalo', 'Sultan Bazarbekov'],
    'Kyrgyzstan': ['Artur Saparbekov'],
    'Latvia': ['Raimo Vigants', 'Jekabs Skolnieks', 'Sandijs Suhanovs'],
    'Liechtenstein': ['Micha Büchel'],
    'Lithuania': ['Modestas Vaiciulis', 'Matas Grazys'],
    'Mexico': ['Allan Corona'],
    'Mongolia': ['Khuslen Ariunjargal'],
    'Montenegro': ['Aleksandar Grbovic'],
    'Nigeria': ['Samuel Uduigowme Ikpefan'],
    'North Macedonia': ['Darko Damjanovski'],
    'Norway': ['Johannes Høsflot Klæbo', 'Harald Østberg Amundsen', 'Martin Løwstrøm Nyenget', 'Erik Valnes', 'Einar Hedegart', 'Mattis Stenshagen', 'Emil Iversen', 'Oscar Opstad Vike'],
    'Poland': ['Dominik Bury', 'Maciej Starega', 'Piotr Jarecki'],
    'Portugal': ['Jose Cabeca'],
    'Romania': ['Gabriel Cojocaru', 'Ionut Alexandru Costea'],
    'Saudi Arabia': ['Rakan Alireza'],
    'Serbia': ['Rejhan Smrkovic'],
    'Slovakia': ['Michal Adamov', 'Denis Tilesch'],
    'Slovenia': ['Miha Simenc', 'Nejc Stern', 'Valeriy Gontar'],
    'South Africa': ['Matthew Smith'],
    'South Korea': ['Joon-Seo Lee'],
    'Spain': ['Jaume Pueyo', 'Marc Colell Pantebre', 'Bernat Selles Gasch'],
    'Sweden': ['Edvin Anger', 'William Poromaa', 'Calle Halfvarsson', 'Anton Grahn', 'Alvar Myhlback', 'Gustaf Berglund', 'Truls Gisselman', 'Johan Häggström'],
    'Switzerland': ['Valerio Grond', 'Janik Riebli', 'Jonas Baumann', 'Jason Rüesch', 'Beda Klee'],
    'Taiwan': ['Chieh-Han Lee'],
    'Thailand': ['Tanathip Bunrit'],
    'Turkey': ['Abdullah Yilmaz'],
    'Ukraine': ['Ruslan Denysenko', 'Andriy Dotsenko'],
    'USA': ['Gus Schumacher', 'Ben Ogden', 'James Clinton Schoonmaker', 'Zak Ketterson', 'Jack Young', 'Zanden McMullen', 'Hunter Wonders', 'Johnny Hagenbuch']
}

CHAMPS_ATHLETES_LADIES_XC = {
    'Andorra': ['Gina del Rio'],
    'Argentina': ['Agustina Groetzner', 'Nahiara Diaz Gonzalez'],
    'Armenia': ['Katya Galstyan'],
    'Australia': ['Rosie Fordham', 'Phoebe Cridland', 'Ellen Søhol Lie', 'Maddie Hooker'],
    'Austria': ['Teresa Stadlober', 'Magdalena Scherz', 'Lisa Achleitner', 'Katharina Brudermann'],
    'Brazil': ['Eduarda Ribera', 'Bruna Moura'],
    'Bulgaria': ['Kalina Nedyalkova'],
    'Canada': ['Liliane Gagnon', 'Kathrine Stewart-Jones', 'Sonjaa Schmidt', 'Alison Mackie', 'Jasmine Drolet', 'Amelia Wells'],
    'China': ['Bayani Jialin', 'Lingshuang Chen', 'Dinigeer Yilamujiang'],
    'Croatia': ['Ema Sobol', 'Leona Garac'],
    'Czechia': ['Katerina Janatova', 'Tereza Beranova', 'Barbora Antosova', 'Anna Marie Jaklova', 'Anna Milerska', 'Barbora Havlickova', 'Sandra Schutzova'],
    'Estonia': ['Mariel Merlii Pulles', 'Kaidy Kaasiku', 'Keidy Kaasiku', 'Teiloora Ojaste'],
    'Finland': ['Jasmi Joensuu', 'Kerttu Niskanen', 'Krista Pärmäkoski', 'Johanna Matintalo', 'Jasmin Kähärä', 'Amanda Saari', 'Vilma Nissinen', 'Vilma Ryytty'],
    'France': ['Flora Dolci', 'Delphine Claudel', 'Lena Quintin', 'Melissa Gal', 'France Pignot', 'Juliette Ducordeau'],
    'Germany': ['Katharina Hennig', 'Coletta Rydzek', 'Laura Gimmler', 'Pia Fink', 'Sofie Krehl', 'Katherine Sauerbrey', 'Helen Hoffmann', 'Anna-Maria Dietze'],
    'Great Britain': ['Anna Pryce'],
    'Greece': ['Konstantina Charalampidou', 'Maria Dimitra Tsiarka'],
    'Hungary': ['Sara Ponya', 'Larissza Vanda Bere'],
    'Iceland': ['Kristrun Gudnadottir'],
    'Iran': ['Atefah Salehi'],
    'Italy': ['Caterina Ganz', 'Federica Cassol', 'Nicole Monsorno', 'Anna Comarella', 'Maria Gismondi', 'Cristina Pittin'],
    'Japan': ['Masae Tsuchiya', 'Chika Honda', 'Chika Kobayashi'],
    'Kazakhstan': ['Yelizaveta Tolmachyova', 'Laura Kinybaeyeva', 'Darya Ryazhko', 'Angelina Shuryga'],
    'Latvia': ['Patricijia Eiduka', 'Adriana Suminska', 'Linda Kaparkaleja'],
    'Liechtenstein': ['Nina Riedener'],
    'Lithuania': ['Egle Savickaite', 'Ieva Dainyte'],
    'Mexico': ['Karla Schleske'],
    'Mongolia': ['Ariunbold Tumur'],
    'Norway': ['Heidi Weng', 'Astrid Øyre Slind', 'Kristine Stavås Skistad', 'Milla Grosberghaugen Andreassen', 'Kristin Austgulen Fosnæs', 'Karoline Simpson-Larsen', 'Julie Bjervig Drivenes', 'Ingrid Bergene Aabrekk'],
    'Poland': ['Izabela Marcisz', 'Monika Skinder', 'Aleksandra Kolodziej', 'Andzelika Szyszka'],
    'Romania': ['Delia Ioana Reit'],
    'Serbia': ['Anja Ilic'],
    'Slovakia': ['Maria Danielova'],
    'Slovenia': ['Anja Mandeljc', 'Eva Urevc'],
    'South Korea': ['Eui Jin Lee', 'Da-Som Han'],
    'Sweden': ['Jonna Sundling', 'Frida Karlsson', 'Ebba Andersson', 'Linn Svahn', 'Maja Dahlqvist', 'Emma Ribom', 'Johanna Hagström', 'Moa Ilar'],
    'Switzerland': ['Nadine Fähndrich', 'Anja Weber', 'Alina Meier', 'Nadja Kälin', 'Marina Kälin', 'Lea Fischer'],
    'Taiwan': ['Sophia Tsu Velicer'],
    'Turkey': ['Rabia Akyol'],
    'Ukraine': ['Yelizaveta Nopriienko', 'Sofiia Shkatula', 'Anastasiia Nikon'],
    'USA': ['Jessie Diggins', 'Julia Kern', 'Rosie Brennan', 'Kendall Kramer', 'Sammy Smith', 'Lauren Jortberg', 'Novie McCabe', 'Hailey Swirbul']
}

def get_champs_athletes_xc(nation: str, gender: str) -> list:
    """Get cross-country championships athletes for specified nation and gender"""
    if gender == 'men':
        return CHAMPS_ATHLETES_MEN_XC.get(nation, [])
    elif gender == 'ladies':
        return CHAMPS_ATHLETES_LADIES_XC.get(nation, [])
    else:
        raise ValueError(f"Unknown gender: {gender}")

def validate_champs_quota_xc(nation: str, gender: str) -> bool:
    """Validate that nation has exactly 4 or fewer athletes for cross-country championships"""
    athletes = get_champs_athletes_xc(nation, gender)
    return len(athletes) <= 4