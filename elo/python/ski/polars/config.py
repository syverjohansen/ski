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
    'Argentina': ['Franco Dal Farra', 'Mateo Lorenzo Sauma'],
    'Armenia': ['Mikayel Mikayelyan'],
    'Australia': ['Seve De Campo', 'Hugo Hinckfuss', 'Lars Young Vik' ], 
    'Austria': ['Michael Föttinger', 'Benjamin Moser','Mika Vermeulen'],
    'Bolivia': ['Timo Juhani Gronlund'],
    'Bosnia&Herzegovina': ['Srdjan Lalovic'],
    'Brazil': ['Manex Silva'],
    'Bulgaria': ['Mario Matikanov', 'Daniel Peshkov'],
    'Canada': ['Antoine Cyr', 'Remi Drolet', 'Max Hollmann', 'Xavier McKeever', 'Tom Stephen'],
    'Chile': ['Sigurd Herrera'],
    'China': ['Minglin Li', 'Qiang Wang'],
    'Colombia': ['Samuel Jaramillo'],
    'Croatia': ['Marko Skender'],
    'Czechia': ['Matyas Bauer', 'Ondrej Cerny', 'Michal Novak', 'Mike Ophoff', 'Jiri Tuz'],
    'Denmark': ['Magnus Tobiassen'],
    'Estonia': ['Alvar Johannes Alev', 'Karl Sebastian Dremljuga', 'Martin Himma'],
    'Finland': ['Niko Anttola', 'Ristomatti Hakola', 'Emil Liekari', 'Joni Mäki', 'Nilo Moilanen', 'Iivo Niskanen', 'Arsi Ruuskanen', 'Lauri Vuorinen'],
    'France': ['Lucas Chanavat', 'Jules Chappaz', 'Mathis Desloges', 'Richard Jouve', 'Hugo Lapalus', 'Jules Lapierre', 'Victor Lovera', 'Theo Schely'],
    'Germany': ['Janosch Brugger', 'Friedrich Moch', 'Jakob Elias Moch', 'Florian Notz', 'Jan Stölben'],
    'Great Britain': ['James Clugnet', 'Joe Davies', 'Andrew Musgrave'],
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
    'Norway': ['Harald Østberg Amundsen', 'Einar Hedegart', 'Emil Iversen', 'Johannes Høsflot Klæbo', 'Martin Løwstrøm Nyenget', 'Mattis Stenshagen', 'Erik Valnes', 'Oskar Opstad Vike'],
    'Poland': ['Sebastian Bryja', 'Dominik Bury', 'Maciej Starega'],
    'Portugal': ['Jose Cabeca'],
    'Romania': ['Gabriel Cojocaru', 'Ionut Alexandru Costea'],
    'Russia': ['Saveliy Korostelev'],
    'Saudi Arabia': ['Rakan Alireza'],
    'Serbia': ['Rejhan Smrkovic'],
    'Slovakia': ['Michal Adamov', 'Denis Tilesch'],
    'Slovenia': ['Vili Crv', 'Miha Simenc', 'Nejc Stern'],
    'South Africa': ['Matthew Smith'],
    'South Korea': ['Joon-Seo Lee'],
    'Spain': ['Jaume Pueyo', 'Marc Colell Pantebre', 'Bernat Selles Gasch'],
    'Sweden': ['Edvin Anger',  'Gustaf Berglund', 'Truls Gisselman', 'Anton Grahn','Johan Häggström', 'Calle Halfvarsson', 'Alvar Myhlback','William Poromaa'],
    'Switzerland': ['Valerio Grond', 'Beda Klee', 'Noe Näff', 'Janik Riebli', 'Nicola Wigger'],
    'Taiwan': ['Chieh-Han Lee'],
    'Thailand': ['Tanathip Bunrit'],
    'Turkey': ['Abdullah Yilmaz'],
    'Ukraine': ['Ruslan Denysenko', 'Andriy Dotsenko'],
    'USA': ['Johnny Hagenbuch', 'Zak Ketterson', 'Zanden McMullen', 'Ben Ogden', 'Gus Schumacher', 'James Clinton Schoonmaker', 'Hunter Wonders', 'Jack Young']
}

CHAMPS_ATHLETES_LADIES_XC = {
    'Andorra': ['Gina del Rio'],
    'Argentina': ['Agustina Groetzner', 'Nahiara Diaz Gonzalez'],
    'Armenia': ['Katya Galstyan'],
    'Australia': ['Phoebe Cridland', 'Rosie Fordham',  'Maddie Hooker', 'Ellen Søhol Lie', ],
    'Austria': ['Lisa Achleitner', 'Katharina Brudermann', 'Heidi Bucher', 'Magdalena Scherz','Teresa Stadlober'],
    'Brazil': ['Bruna Moura', 'Eduarda Ribera'],
    'Bulgaria': ['Kalina Nedyalkova'],
    'Canada': ['Jasmine Drolet', 'Liliane Gagnon', 'Alison Mackie', 'Sonjaa Schmidt', 'Kathrine Stewart-Jones', 'Amelia Wells'],
    'China': ['Bayani Jialin', 'Lingshuang Chen', 'Dinigeer Yilamujiang'],
    'Croatia': ['Ema Sobol', 'Leona Garac'],
    'Czechia': ['Barbora Antosova', 'Tereza Beranova','Barbora Havlickova','Anna Marie Jaklova', 'Katerina Janatova', 'Anna Milerska', 'Sandra Schutzova'],
    'Estonia': ['Kaidy Kaasiku', 'Keidy Kaasiku', 'Teiloora Ojaste', 'Mariel Merlii Pulles', 'Teesi Tuul'],
    'Finland': ['Jasmin Kähärä', 'Jasmi Joensuu', 'Johanna Matintalo', 'Kerttu Niskanen', 'Vilma Nissinen', 'Krista Pärmäkoski', 'Vilma Ryytty', 'Amanda Saari'],
    'France': ['Delphine Claudel', 'Clemence Didierlaurent', 'Justine Gaillard', 'Melissa Gal', 'Cloe Pagnier', 'Leonie Perry', 'Julie Pierrel'],
    'Germany': ['Pia Fink', 'Laura Gimmler', 'Katharina Hennig Dotzler', 'Helen Hoffmann', 'Sofie Krehl', 'Coletta Rydzek',   'Katherine Sauerbrey'],
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
    'Norway': ['Ingrid Bergene Aabrekk', 'Milla Grosberghaugen Andreassen', 'Julie Bjervig Drivenes', 'Kristin Austgulen Fosnæs', 'Karoline Simpson-Larsen', 'Kristine Stavås Skistad', 'Astrid Øyre Slind', 'Heidi Weng'],
    'Poland': ['Aleksandra Kolodziej','Izabela Marcisz', 'Eliza Rucka-Michalek', 'Monika Skinder'],
    'Romania': ['Delia Ioana Reit'],
    'Russia': ['Dariya Nepryaeva'],
    'Serbia': ['Anja Ilic'],
    'Slovakia': ['Maria Danielova'],
    'Slovenia': ['Tia Janezic', 'Anja Mandeljc', 'Lucija Medja', 'Neza Zerjav'],
    'South Korea': ['Eui Jin Lee', 'Da-Som Han'],
    'Sweden': ['Ebba Andersson', 'Maja Dahlqvist', 'Johanna Hagström', 'Moa Ilar', 'Frida Karlsson', 'Emma Ribom', 'Jonna Sundling', 'Linn Svahn'],
    'Switzerland': ['Fabienne Alder', 'Nadine Fähndrich', 'Lea Fischer', 'Marina Kälin', 'Nadja Kälin', 'Alina Meier', 'Anja Weber'],
    'Turkey': ['Rabia Akyol'],
    'Ukraine': ['Yelizaveta Nopriienko', 'Sofiia Shkatula', 'Anastasiia Nikon'],
    'USA': ['Rosie Brennan', 'Jessie Diggins', 'Lauren Jortberg', 'Julia Kern',  'Kendall Kramer', 'Novie McCabe', 'Sammy Smith', 'Hailey Swirbul']
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