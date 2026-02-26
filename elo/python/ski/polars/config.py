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
    'Finland': [
        'Niko Anttola', 'Juuso Haarala', 'Perttu Hyvärinen', 'Petteri Koivisto',
        'Emil Liekari', 'Ike Melnits', 'Niilo Moilanen', 'Joni Mäki',
        'Arsi Ruuskanen', 'Alexander Ståhlberg', 'Markus Vuorela', 'Lauri Vuorinen'
    ],
    'France': [
        'Rémi Bourdin', 'Lucas Chanavat', 'Jules Chappaz', 'Sabin Coupat',
        'Matteo Correia', 'Victor Cullet', 'Hugo Lapalus', 'Jules Lapierre',
        'Victor Lovera', 'Théo Schely'
    ],
    'Italy': [
        {'name': 'Federico Pellegrino', 'yes': [1], 'no': [2]},
        'Elia Barp', 'Martino Carollo', 'Davide Graz', 'Simone Daprà',
        'Simone Mocellini', 'Michael Hellweger'
    ],
    'Norway': [
        {'name': 'Even Northug', 'yes': [1], 'no': [2]},
        {'name': 'Ansgar Evensen', 'yes': [1], 'no': [2]},
        {'name': 'Oskar Opstad Vike', 'yes': [1], 'no': [2]},
        {'name': 'Harald Østberg Amundsen', 'yes': [1, 2]},
        {'name': 'Mathias Holbæk', 'yes': [1], 'no': [2]},
        {'name': 'Lars Heggen', 'yes': [1, 2]},
        {'name': 'Johannes Høsflot Klæbo', 'yes': [1, 2]},
        {'name': 'Even Solem Michelsen', 'yes': [1, 2]},
        {'name': 'Martin Løwstrøm Nyenget', 'no': [1], 'yes': [2]},
        {'name': 'Emil Iversen', 'no': [1], 'yes': [2]},
        {'name': 'Martin Kirkeberg Mørk', 'no': [1], 'yes': [2]},
        {'name': 'Andreas Fjorden Ree', 'no': [1], 'yes': [2]}
    ],
    'Sweden': [
        {'name': 'Emil Danielsson', 'yes': [1], 'no': [2]},
        {'name': 'George Ersson', 'yes': [1], 'no': [2]},
        {'name': 'Anton Grahn', 'yes': [1], 'no': [2]},
        {'name': 'Marcus Grate', 'yes': [1], 'no': [2]},
        {'name': 'Johan Häggström', 'yes': [1, 2]},
        {'name': 'Erik Johansson', 'yes': [1], 'no': [2]},
        {'name': 'Olof Jonsson', 'yes': [1], 'no': [2]},
        {'name': 'Jesper Persson', 'yes': [1], 'no': [2]},
        {'name': 'Wilhelm Sterner', 'yes': [1], 'no': [2]},
        {'name': 'Gustaf Berglund', 'no': [1], 'yes': [2]},
        {'name': 'Johan Ekberg', 'no': [1], 'yes': [2]},
        {'name': 'Jonas Eriksson', 'no': [1], 'yes': [2]},
        {'name': 'Truls Gisselman', 'no': [1], 'yes': [2]},
        {'name': 'Oskar Algotsson', 'yes': [1, 2]},
        {'name': 'Edvin Anger', 'yes': [1, 2]},
        {'name': 'Erik Bergström', 'yes': [1], 'no': [2]},
        {'name': 'Calle Halfvarsson', 'no': [1], 'yes': [2]},
        {'name': 'Leo Johansson', 'no': [1], 'yes': [2]},
        {'name': 'Jonatan Lindberg', 'no': [1], 'yes': [2]},
        {'name': 'Theo Mattei', 'no': [1], 'yes': [2]},
        {'name': 'Eric Rosjö', 'no': [1], 'yes': [2]}
    ]
}

ADDITIONAL_SKIERS_LADIES = {
    'Finland': [
        'Jasmi Joensuu', 'Jasmin Kähärä', 'Johanna Matintalo', 'Hilla Niemelä',
        'Kerttu Niskanen', 'Tiia Olkkonen', 'Vilma Ryytty'
    ],
    'France': [
        'Delphine Claudel', 'Juliette Ducordeau', 'Melissa Gal', 'Léna Quintin'
    ],
    'Italy': [
        'Caterina Ganz', 'Nicole Monsorno', 'Iris De Martin Pinter'
    ],
    'Norway': [
        {'name': 'Kristine Stavås Skistad', 'yes': [1], 'no': [2]},
        {'name': 'Mathilde Myhrvold', 'yes': [1], 'no': [2]},
        {'name': 'Julie Myhre', 'yes': [1], 'no': [2]},
        {'name': 'Julie Bjervig Drivenes', 'yes': [1, 2]},
        {'name': 'Milla Grosberghaugen Andreassen', 'yes': [1], 'no': [2]},
        {'name': 'Lotta Udnes Weng', 'yes': [1, 2]},
        {'name': 'Tiril Udnes Weng', 'yes': [1, 2]},
        {'name': 'Karoline Simpson-Larsen', 'no': [1], 'yes': [2]},
        {'name': 'Heidi Weng', 'no': [1], 'yes': [2]},
        {'name': 'Kristin Austgulen Fosnæs', 'no': [1], 'yes': [2]},
        {'name': 'Nora Sanness', 'no': [1], 'yes': [2]}
    ],
    'Sweden': [
        {'name': 'Maja Dahlqvist', 'yes': [1, 2]},
        {'name': 'Johanna Hagström', 'yes': [1], 'no': [2]},
        {'name': 'Moa Hansson', 'yes': [1], 'no': [2]},
        {'name': 'Elin Henriksson', 'yes': [1], 'no': [2]},
        {'name': 'Moa Ilar', 'yes': [1, 2]},
        {'name': 'Lisa Ingesson', 'yes': [1, 2]},
        {'name': 'Frida Karlsson', 'yes': [1, 2]},
        {'name': 'Moa Lundgren', 'yes': [1, 2]},
        {'name': 'Elin Näslund', 'yes': [1], 'no': [2]},
        {'name': 'Emma Ribom', 'yes': [1, 2]},
        {'name': 'Märta Rosenberg', 'yes': [1, 2]},
        {'name': 'Linn Svahn', 'yes': [1, 2]},
        {'name': 'Ebba Andersson', 'no': [1], 'yes': [2]},
        {'name': 'Emilia Eriksson', 'no': [1], 'yes': [2]},
        {'name': 'Tove Ericsson', 'no': [1], 'yes': [2]},
        {'name': 'Elina Rönnlund', 'no': [1], 'yes': [2]}
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
    #Major Nations (More than 10)
    'Canada': ['Antoine Cyr', 'Remi Drolet', 'Max Hollmann', 'Xavier McKeever', 'Tom Stephen'],
    'Czechia': ['Matyas Bauer', 'Ondrej Cerny', 'Michal Novak', 'Mike Ophoff', 'Jiri Tuz'],
    'Finland': ['Niko Anttola', 'Ristomatti Hakola', 'Emil Liekari', 'Joni Mäki', 'Nilo Moilanen', 'Iivo Niskanen', 'Arsi Ruuskanen', 'Lauri Vuorinen'],
    'France': ['Lucas Chanavat', 'Jules Chappaz', 'Mathis Desloges', 'Richard Jouve', 'Hugo Lapalus', 'Jules Lapierre', 'Victor Lovera', 'Theo Schely'],
    'Germany': ['Janosch Brugger', 'Friedrich Moch', 'Jakob Elias Moch', 'Florian Notz', 'Jan Stölben'],
    'Italy': ['Elia Barp', 'Martino Carollo', 'Simone Dapra', 'Davide Graz', 'Simone Mocellini', 'Federico Pellegrino'],
    'Norway': ['Harald Østberg Amundsen', 'Einar Hedegart', 'Emil Iversen', 'Johannes Høsflot Klæbo', 'Martin Løwstrøm Nyenget', 'Mattis Stenshagen', 'Erik Valnes', 'Oskar Opstad Vike'],
    'Sweden': ['Edvin Anger',  'Gustaf Berglund', 'Truls Gisselman', 'Anton Grahn','Johan Häggström', 'Calle Halfvarsson', 'Alvar Myhlback','William Poromaa'],
    'Switzerland': ['Valerio Grond', 'Beda Klee', 'Noe Näff', 'Janik Riebli', 'Nicola Wigger'],
    'USA': ['Johnny Hagenbuch', 'Zak Ketterson', 'Zanden McMullen', 'Ben Ogden', 'James Clinton Schoonmaker', 'Gus Schumacher',  'Hunter Wonders', 'Jack Young'],

    #Medium Nations (4 or more)
    'Argentina': ['Franco Dal Farra', 'Mateo Lorenzo Sauma'],
    'Australia': ['Seve De Campo', 'Hugo Hinckfuss', 'Lars Young Vik' ],
    'Austria': ['Michael Föttinger', 'Benjamin Moser','Mika Vermeulen'],
    'China': ['Minglin Li', 'Qiang Wang'],
    'Estonia': ['Alvar Johannes Alev', 'Karl Sebastian Dremljuga', 'Martin Himma'],
    'Great Britain': ['James Clugnet', 'Joe Davies', 'Andrew Musgrave'],
    'Hungary': ['Adam Büki', 'Adam Konya'],
    'Japan': ['Naoto Baba', 'Ryo Hirose', 'Haruki Yamashita'],
    'Kazakhstan': ['Nail Bashmakov', 'Amirgali Muratbekov', 'Vitaliy Pukhkalo'],
    'Latvia': ['Lauris Kaparkalejs', 'Nika Saulitis', 'Raimo Vigants'],
    'Lithuania': ['Tautvydas Strolia', 'Modestas Vaiciulis'],
    'Poland': ['Sebastian Bryja', 'Dominik Bury', 'Maciej Starega'],
    'Slovenia': ['Vili Crv', 'Miha Simenc', 'Nejc Stern'],
    'Ukraine': ['Dmytro Dragun', 'Oleksandr Lisohor'],

    #Minor Nations (3 or more)
    
    'Brazil': ['Manex Silva'],
    'Bulgaria': ['Mario Matikanov', 'Daniel Peshkov'],
    'Croatia': ['Marko Skender'],
    'Greece': ['Apostolos Angelis'],
    'Romania': ['Gabriel Cojocaru', 'Paul Constantin Pepene'],
    'Slovakia': ['Tomas Cenek', 'Peter Hinds'],
    'South Korea': ['Joon-Seo Lee'],
    'Spain': ['Marc Colell Pantebre', 'Jaume Pueyo',  'Bernat Selles Gasch'],

    #Charity Cases (1-2)
    'Andorra': ['Irineu Esteve Altimiras'],
    'Armenia': ['Mikayel Mikayelyan'],
    'Bolivia': ['Timo Juhani Gronlund'],
    'Bosnia&Herzegovina': ['Strahinja Eric'],
    'Chile': ['Sebastian Kristoffer Endrestad'],
    'Colombia': ['Fredrik Gerardo Fodstad'],
    'Ecuador': ['Klaus Jungbluth Rodriguez'],
    'Haiti': ['Stevenson Savart'],
    'Iceland': ['Dagur Benediktsson'],
    'India': ['Stanzin Lundup'],    
    'Iran': ['Danyal Saveh Shemshaki'],
    'Ireland': ['Thomas Hjalmar Westgård'],
    'Israel':['Attila Mihaly Kertesz'],
    'Kyrgyzstan': ['Artur Saparbekov'],
    'Lebanon': ['Samer Tawk'],
    'Liechtenstein': ['Robin Frommelt'],
    'Mexico': ['Allan Corona'],
    'Moldova': ['Iulian Luchin'],
    'Mongolia': ['Achbadrakh Batmunkh'],
    'Montenegro': ['Aleksandar Grbovic'],
    'Morocco': ['Abderrahim Kemmissa'],
    'Nigeria': ['Samuel Uduigowme Ikpefan'],
    'North Macedonia': ['Stavre Jada'],
    'Portugal': ['Jose Cabeca'],
    'Russia': ['Saveliy Korostelev'],
    'Saudi Arabia': ['Rakan Alireza'],
    'Serbia': ['Milos Milosavljevic'],
    'South Africa': ['Matthew Smith'],
    'Taiwan': ['Chieh-Han Lee'],
    'Thailand': ['Mark Chanloung'],
    'Turkey': ['Abdullah Yilmaz'],
    'Venezuela': ['Nicolas Claveau-Laviolette']
}

CHAMPS_ATHLETES_LADIES_XC = {
    #Major Nations (More than 10)
    'Canada': ['Olivia Bouffard-Nesbitt', 'Jasmine Drolet', 'Liliane Gagnon', 'Alison Mackie', 'Sonjaa Schmidt', 'Kathrine Stewart-Jones', 'Amelia Wells'],
    'Czechia': ['Barbora Antosova', 'Tereza Beranova','Barbora Havlickova','Anna Marie Jaklova', 'Katerina Janatova', 'Anna Milerska', 'Sandra Schutzova'],
    'Finland': ['Jasmi Joensuu', 'Jasmin Kähärä',  'Johanna Matintalo', 'Kerttu Niskanen', 'Vilma Nissinen', 'Krista Pärmäkoski', 'Vilma Ryytty', 'Amanda Saari'],
    'France': ['Delphine Claudel', 'Clemence Didierlaurent', 'Justine Gaillard', 'Melissa Gal', 'Cloe Pagnier', 'Leonie Perry', 'Julie Pierrel'],
    'Germany': ['Pia Fink', 'Theresa Fürstenberg', 'Laura Gimmler', 'Katharina Hennig Dotzler', 'Helen Hoffmann', 'Sofie Krehl', 'Coletta Rydzek',   'Katherine Sauerbrey'],
    'Italy': ['Federica Cassol', 'Anna Comarella', 'Iris De Martin Pinter', 'Martina Di Centa', 'Caterina Ganz',  'Maria Gismondi', 'Nicole Monsorno'],
    'Norway': ['Ingrid Bergene Aabrekk', 'Milla Grosberghaugen Andreassen', 'Julie Bjervig Drivenes', 'Kristin Austgulen Fosnæs', 'Karoline Simpson-Larsen', 'Kristine Stavås Skistad', 'Astrid Øyre Slind', 'Heidi Weng'],
    'Sweden': ['Ebba Andersson', 'Maja Dahlqvist', 'Johanna Hagström', 'Moa Ilar', 'Frida Karlsson', 'Emma Ribom', 'Jonna Sundling', 'Linn Svahn'],
    'Switzerland': ['Fabienne Alder', 'Nadine Fähndrich', 'Lea Fischer', 'Marina Kälin', 'Nadja Kälin', 'Alina Meier', 'Anja Weber'],
    'USA': ['Rosie Brennan', 'Jessie Diggins', 'Lauren Jortberg', 'Julia Kern',  'Kendall Kramer', 'Novie McCabe', 'Sammy Smith', 'Hailey Swirbul'],

    #Medium Nations (4 or more)
    'Argentina': ['Agustina Groetzner', 'Nahiara Diaz Gonzalez'],
    'Australia': ['Phoebe Cridland', 'Rosie Fordham',  'Maddie Hooker', 'Ellen Søhol Lie', ],
    'Austria': ['Lisa Achleitner', 'Katharina Brudermann', 'Heidi Bucher', 'Magdalena Scherz','Teresa Stadlober'],
    'China': ['Chunxue Chi', 'Kaile He', 'Yundi Wang', 'Dinigeer Yilamujiang'],
    'Estonia': ['Kaidy Kaasiku', 'Keidy Kaasiku', 'Teiloora Ojaste', 'Mariel Merlii Pulles', 'Teesi Tuul'],
    'Great Britain': ['Anna Pryce'],
    'Hungary': ['Larissza Vanda Bere', 'Sara Ponya'],
    'Japan': ['Masae Tsuchiya'],
    'Kazakhstan': ['Anna Melnik', 'Darya Ryazhko', 'Kseniya Shalygina', 'Nadezhda Stepashkina'],
    'Latvia': ['Kitija Auzina', 'Patricijia Eiduka', 'Linda Kaparkaleja', 'Samanta Krampe'],
    'Lithuania': ['Ieva Dainyte', 'Egle Savickaite'],
    'Poland': ['Aleksandra Kolodziej','Izabela Marcisz', 'Eliza Rucka-Michalek', 'Monika Skinder'],
    'Slovenia': ['Tia Janezic', 'Anja Mandeljc', 'Lucija Medja', 'Neza Zerjav'],
    'Ukraine': ['Daryna Mihal', 'Anastasiia Nikon', 'Yelizaveta Nopriienko', 'Sofiia Shkatula'],

    #Minor Nations (3 or more)
    'Brazil': ['Bruna Moura', 'Eduarda Ribera'],
    'Bulgaria': ['Kalina Nedyalkova'],
    'Croatia': ['Tena Hadzic', 'Ema Sobol'],
    'Greece': ['Konstantina Charalampidou', 'Nefeli Tita'],
    'Romania': ['Delia Ioana Reit'],
    'Slovakia': ['Maria Danielova'],
    'South Korea': ['Da-Som Han', 'Eui Jin Lee'],

    #Charity Cases (1-2)
    'Andorra': ['Gina del Rio'],
    'Armenia': ['Katya Galstyan'],
    'Belarus': ['Hanna Karaliova'],
    'Bosnia&Herzegovina': ['Teodora Delipara'],
    'Iceland': ['Kristrun Gudnadottir'],
    'Iran': ['Samaneh Beyrami Baher'],
    'Malta': ['Jenny Axisa Eriksen'],
    'Mexico': ['Regina Martinez Lorenzo'],
    'Moldova': ['Elizaveta Hlusovici'],
    'Mongolia': ['Ariuntungalag Enkhbayar'],
    'North Macedonia': ['Ana Cvetanovska'],
    'Russia': ['Dariya Nepryaeva'],
    'Serbia': ['Anja Ilic'],
    'Taiwan': ['Sophia Tsu Velicer'],
    'Thailand': ['Karen Chanloung'],
    'Turkey': ['Irem Dursun']
    
    
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