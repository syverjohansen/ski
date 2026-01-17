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
    'Czechia': [
        'Matyáš Bauer',
        'Ondřej Černý',
        'Tomas Dufek',
        'Michal Novák',
        'Mike Ophoff',
        'Jiri Tuž'
    ],
    'Austria': [
        'Mika Vermeulen',
        'Christian Steiner',
        'Lukas Mrkonjic'
    ],
    'Switzerland': [
        'Roman Alder',
        'Cyril Fähndrich',
        'Valerio Grond',
        'Beda Klee',
        'Isai Näff',
        'Noe Näff',
        'Candide Pralong',
        'Janik Riebli',
        'Jason Rüesch',
        'Antonin Savary',
        'Nicola Wigger'
    ],
    'Norway': [
        {'name': 'Martin Løwstrøm Nyenget', 'yes': [2]},
        {'name': 'Edward Sandvik', 'yes': [2]},
        {'name': 'Lars Heggen', 'yes': [2]},
        {'name': 'Philip Skari', 'yes': [2]},
        {'name': 'Didrik Tønseth', 'yes': [2]},
        {'name': 'Erik Valnes', 'yes': [2]},
        {'name': 'Håvard Moseby', 'yes': [2]}
    ],
    'Germany': [
        'Thomas Bing',
        'Lucas Bögl',
        'Janosch Brugger',
        'Korbinian Fagerer',
        'Jannis Grimmecke',
        'Korbinian Heiland',
        'Simon Jung',
        'Elias Keck',
        'Niklas Schmid',
        'Anian Sossau',
        'Jakob Walther',
        'Tom Emilio Wagner',
        'Janik Weidlich'
    ],
    'Italy': [
        'Federico Pellegrino',
        'Martino Carollo',
        'Elia Barp',
        'Davide Graz',
        'Simone Daprà',
        'Simone Mocellini',
        'Michael Hellweger',
        'Giandomenico Salvadori'
    ],
    'Finland': [
        'Ristomatti Hakola',
        'Perttu Hyvärinen',
        'Emil Liekari',
        'Ike Melnits',
        'Iivo Niskanen',
        'Eero Rantala',
        'Arsi Ruuskanen',
        'Markus Vuorela',
        'Lauri Vuorinen'
    ],
    'Sweden': [
        {'name': 'Oskar Algotsson', 'yes': [1], 'no': [2]},
        {'name': 'Emil Danielsson', 'yes': [1, 2]},
        {'name': 'George Ersson', 'yes': [1], 'no': [2]},
        {'name': 'Johan Häggström', 'yes': [1, 2]},
        {'name': 'Erik Johansson', 'yes': [1], 'no': [2]},
        {'name': 'Jesper Persson', 'yes': [1], 'no': [2]},
        {'name': 'Johan Ekberg', 'yes': [2], 'no': [1]},
        {'name': 'Calle Halfvarsson', 'yes': [2], 'no': [1]},
        {'name': 'Leo Johansson', 'yes': [2], 'no': [1]}
    ]
}

ADDITIONAL_SKIERS_LADIES = {
    'Czechia': [
        'Barbora Havlíčková',
        'Anna Marie Jaklová',
        'Kateřina Janatová',
        'Anna Milerská'
    ],
    'Austria': [
        'Teresa Stadlober'
    ],
    'Switzerland': [
        'Fabienne Alder',
        'Nadine Fähndrich',
        'Lea Fischer',
        'Marina Kälin',
        'Nadja Kälin'
    ],
    'Norway': [
        {'name': 'Eva Ingebrigtsen', 'yes': [2]},
        {'name': 'Lotta Udnes Weng', 'yes': [2]},
        {'name': 'Tiril Udnes Weng', 'yes': [2]},
        {'name': 'Milla Andreassen', 'yes': [2]},
        {'name': 'Maren Wangensteen', 'yes': [2]},
        {'name': 'Mali Eidnes Bakken', 'yes': [2]},
        {'name': 'Caroline Grötting', 'yes': [2]}
    ],
    'Germany': [
        'Charlotte Böhme',
        'Anna-Maria Dietze',
        'Pia Fink',
        'Laura Gimmler',
        'Katharina Hennig',
        'Helen Hoffmann',
        'Lena Keck',
        'Sofie Krehl',
        'Lisa Lohmann',
        'Saskia Nürnberger',
        'Katherine Sauerbrey',
        'Germania Thannheimer',
        'Coletta Rydzek',
        'Katja Veit',
        'Verena Veit'
    ],
    'Italy': [
        'Caterina Ganz',
        'Nicole Monsorno',
        'Federica Cassol',
        'Nadine Laurent',
        'Iris De Martin Pinter'
    ],
    'Finland': [
        'Jasmin Kähärä',
        'Hilla Niemelä',
        'Tiia Olkkonen',
        'Krista Pärmäkoski',
        'Amanda Saari'
    ],
    'Sweden': [
        {'name': 'Maja Dahlqvist', 'yes': [1, 2]},
        {'name': 'Evelina Crüsell', 'yes': [1, 2]},
        {'name': 'Moa Ilar', 'yes': [1, 2]},
        {'name': 'Moa Lundgren', 'yes': [1, 2]},
        {'name': 'Jonna Sundling', 'yes': [1, 2]}
    ]
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
    'Andorra': ['Irineu Esteve Alti'],
    'Argentina': ['Mateo Lorenzo Sauma', 'Franco Dal Farra'],
    'Armenia': ['Mikayel Mikayelyan'],
    'Australia': ['Lars Young Vik', 'Hugo Hinckfuss', 'Seve De Campo'],
    'Austria': ['Mika Vermeulen', 'Benjamin Moser', 'Michael Föttinger'],
    'Belgium': ['Samuel Maes'],
    'Bolivia': ['Timo Juhani Gronlund'],
    'Bosnia&Herzegovina': ['Srdjan Lalovic'],
    'Brazil': ['Guilherme Pereira Santos'],
    'Bulgaria': ['Mario Matikanov', 'Daniel Peshkov'],
    'Canada': ['Antoine Cyr', 'Olivier Leveille', 'Xavier McKeever', 'Graham Ritchie'],
    'Chile': ['Sigurd Herrera'],
    'China': ['Qiang Wang', 'Minglin Li'],
    'Colombia': ['Samuel Jaramillo'],
    'Croatia': ['Marko Skender'],
    'Czechia': ['Michal Novak', 'Ondrej Cerny', 'Adam Fellner', 'Jiri Tuz', 'Ludek Seller'],
    'Denmark': ['Magnus Tobiassen'],
    'Estonia': ['Alvar Johannes Alev', 'Karl Sebastian Dremljuga', 'Hendrik Peterson'],
    'Finland': ['Iivo Niskanen', 'Lauri Vuorinen', 'Ristomatti Hakola', 'Joni Mäki', 'Perttu Hyvärinen', 'Arsi Ruuskanen', 'Ville Ahonen'],
    'France': ['Hugo Lapalus', 'Richard Jouve', 'Jules Chappaz', 'Lucas Chanavat', 'Mathis Desloges', 'Remi Bourdin', 'Jules Lapierre'],
    'Germany': ['Friedrich Moch', 'Jan Stölben', 'Florian Notz', 'Elias Keck', 'Janosch Brugger'],
    'Great Britain': ['Andrew Musgrave', 'Joe Davies', 'Andrew Young'],
    'Greece': ['Panagiotis Papasis'],
    'Haiti': ['Theo Mallett'],
    'Hungary': ['Daniel Szollos', 'Adam Konya'],
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
    'Norway': ['Johannes Høsflot Klæbo', 'Harald Østberg Amundsen', 'Martin Løwstrøm Nyenget', 'Erik Valnes', 'Simen Hegstad Krüger', 'Andreas Fjorden Ree', 'Even Northug'],
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
    'Sweden': ['Edvin Anger', 'William Poromaa', 'Calle Halfvarsson', 'Jens Burman', 'Emil Danielsson', 'Gustaf Berglund', 'Oskar Svensson'],
    'Switzerland': ['Valerio Grond', 'Janik Riebli', 'Jonas Baumann', 'Jason Rüesch', 'Beda Klee'],
    'Taiwan': ['Chieh-Han Lee'],
    'Thailand': ['Tanathip Bunrit'],
    'Turkey': ['Abdullah Yilmaz'],
    'Ukraine': ['Ruslan Denysenko', 'Andriy Dotsenko'],
    'USA': ['Gus Schumacher', 'Ben Ogden', 'James Clinton Schoonmaker', 'Kevin Bolger', 'Zak Ketterson', 'Jack Young', 'Zanden McMullen']
}

CHAMPS_ATHLETES_LADIES_XC = {
    'Andorra': ['Gina del Rio'],
    'Argentina': ['Agustina Groetzner', 'Nahiara Diaz Gonzalez'],
    'Armenia': ['Katya Galstyan'],
    'Australia': ['Rosie Fordham', 'Phoebe Cridland', 'Ellen Søhol Lie', 'Tuva Bygrave'],
    'Austria': ['Teresa Stadlober', 'Magdalena Scherz', 'Lisa Achleitner', 'Katharina Brudermann'],
    'Brazil': ['Eduarda Ribera', 'Bruna Moura'],
    'Bulgaria': ['Kalina Nedyalkova'],
    'Canada': ['Liliane Gagnon', 'Kathrine Stewart-Jones', 'Sonjaa Schmidt', 'Katherine Weaver', 'Alison Mackie', 'Olivia Bouffard-Nesbitt'],
    'China': ['Bayani Jialin', 'Lingshuang Chen', 'Dinigeer Yilamujiang'],
    'Croatia': ['Ema Sobol', 'Leona Garac'],
    'Czechia': ['Katerina Janatova', 'Tereza Beranova', 'Barbora Antosova', 'Anna Marie Jaklova', 'Anna Milerska', 'Barbora Havlickova'],
    'Estonia': ['Mariel Merlii Pulles', 'Kaidy Kaasiku', 'Keidy Kaasiku', 'Teiloora Ojaste'],
    'Finland': ['Jasmi Joensuu', 'Kerttu Niskanen', 'Krista Pärmäkoski', 'Johanna Matintalo', 'Jasmin Kähärä', 'Katri Lylynperä', 'Anne Kyllönen', 'Amanda Saari'],
    'France': ['Flora Dolci', 'Delphine Claudel', 'Lena Quintin', 'Melissa Gal', 'France Pignot', 'Juliette Ducordeau'],
    'Germany': ['Katharina Hennig', 'Coletta Rydzek', 'Laura Gimmler', 'Pia Fink', 'Sofie Krehl', 'Katherine Sauerbrey', 'Helen Hoffmann', 'Anna-Maria Dietze'],
    'Greece': ['Konstantina Charalampidou', 'Maria Dimitra Tsiarka'],
    'Hungary': ['Evelin Vivien Laczko', 'Larissza Vanda Bere'],
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
    'Norway': ['Heidi Weng', 'Astrid Øyre Slind', 'Kristine Stavås Skistad', 'Lotta Udnes Weng', 'Kristin Austgulen Fosnæs', 'Julie Myhre', 'Mathilde Myhrvold', 'Nora Sanness'],
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
    'USA': ['Jessie Diggins', 'Julia Kern', 'Rosie Brennan', 'Sophia Laukli', 'Kate Oldham', 'Kendall Kramer', 'Luci Anderson', 'Sammy Smith']
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