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
    # Major alpine nations (More than 10)
    'Austria': ['Stefan Babinsky', 'Stefan Brennsteiner', 'Manuel Feller', 'Lukas Feurstein', 'Patrick Feurstein', 'Fabio Gstrein','Raphael Haaser', 'Daniel Hemetsberger', 'Vincent Kriechmayr',  'Michael Matt', 'Marco Schwarz'],
    'Canada': ['Cameron Alexander', 'James Crawford', 'Jeffrey Read', 'Brodie Seger', 'Riley Seger'],
    'France': ['Nils Allegre', 'Nils Alphand', 'Steven Amiez', 'Leo Anguenot', 'Alban Elezi Cannaferina', 'Maxence Muzaton', 'Clement Noel', 'Paco Rassat'],
    'Italy': ['Mattia Casse', 'Luca De Aliprandini', 'Giovanni Franzoni', 'Christof Innerhofer', 'Tobias Kastlunger', 'Dominik Paris',  'Tommasso Saccardi', 'Tomasso Sala', 'Florian Schieder', 'Alex Vinatzer'],
    'Norway': ['Hans Grahl-Madsen', 'Timon Haugan', 'Henrik Kristoffersen',  'Atle Lie McGrath', 'Fredrik Møller', 'Oscar Andreas Sandvik', 'Adrian Smiseth Sejersted', 'Simen Sællæg', 'Eirik Hystad Solberg'],
    'Switzerland': ['Luca Aerni', 'Niels Hintermann', 'Matthias Iten', 'Loic Meillard', 'Alexis Monney', 'Tanguy Nef', 'Marco Odermatt', 'Stefan Rogentin', 'Thomas Tumler', 'Franjo Von Allmen', 'Daniel Yule'],
    'USA': ['Bryce Bennett', 'Ryan Cochran-Siegle', 'Sam Morse', 'Kyle Negomir', 'River Radamus', 'Ryder Sarchett'],
    
    # Medium nations (More than 4)
    'Andorra': ['Xavier Cornella Guitart', 'Joan Verdu Sanchez'],
    'Croatia': ['Samuel Kolega', 'Istok Rodes', 'Filip Zubcic'],
    'Czechia': ['Marek Müller', 'Jan Zabystran'],
    'Finland': ['Eduard Hallberg', 'Elian Lehto',  'Jesper Pohjolainen'],
    'Germany': ['Anton Grammel', 'Fabian Gratz', 'Simon Jocher', 'Alexander Schmid', 'Linus Strasser'],
    'Slovenia': ['Martin Cater', 'Miha Hrobat', 'Zan Kranjec'],
    'Sweden': ['Fabian Ax Swartz', 'Kristoffer Jakobsen'],
 
    # Small nations (more than 2 men/women)
    'Albania': ['Denni Xhepa'],
    'Argentina': ['Tiziano Gravier'],
    'Australia': ['Harry Laidlaw'],
    'Belgium': ['Sam Maes', 'Armand Marchant'],
    'Bosnia&Herzegovina': ['Marko Sljivic'],
    'Brazil': ['Giovanni Ongaro', 'Lucas Pinheiro Braathen', 'Christian Oliveira Søvik'],
    'Bulgaria': ['Albert Popov', 'Kalin Zlatkov'],
    'Great Britain': ['Billy Major', 'Dave Ryding', 'Laurie Taylor'],
    'Latvia': ['Elvis Opmanis'],
    'New Zealand': ['Sam Hadley'],
    'Poland': ['Michal Jasiczek'],
    'Russia': ['Simon Efimov'],
    'Slovakia': ['Andreas Zampa'],
    'South Korea': ['Dong-Hyun Jung'],

    # Charity Cases (1-2 athletes)
    'Armenia': ['Harutyun Harutyunyan'],
    'Benin': ['Nathan Tchibozo'],
    'Chile': ['Henrik Von Appen'],
    'China': ['Xiaochen Liu'],
    'Cyprus': ['Yianno Kouyoumdjian'],
    'Denmark': ['Christian Borgnaes'],
    'Eritrea': ['Shannon Abeda'],
    'Estonia': ['Tormis Laine'],
    'Georgia': ['Luka Buchukuri'],
    'Greece': ['AJ Ginnis'],
    'Guinea-Bissau': ['Winston Tang'],
    'Haiti': ['Richardson Viano'],
    'Hong Kong': ['Hau Tsuen Adrian Yung'],
    'Hungary':['Balint Ury'],
    'India': ['Arif Mohd Khan'],
    'Iceland': ['Jon Erik Sigurdsson'],
    'Iran': ['Mohammad Kiyadarbandsari'],
    'Ireland': ['Cormac Comerford'],
    'Israel': ['Barnabas Szollos'],
    'Jamaica': ['Henri Rivers IV'],
    'Japan': ['Shiro Aikhara'],
    'Kazakhstan': ['Rotislav Khokhlov'],
    'Kenya': ['Issa Gachingiri Laborde Dit Pere'],
    'Kosovo': ['Drin Kokaj'],
    'Kyrgyzstan': ['Timur Shakirov'],
    'Lebanon': ['Andrea Elie Antoine El Hayek'],
    'Liechtenstein': ['Marco Pfiffner'],
    'Lithuania': ['Andrej Drukarov'],
    'Luxembourg': ['Matthieu Osch'],
    'Madagascar': ['Mathieu Gravier'],
    'Mexico': ['Lasse Gaxiola'],
    'Monaco': ['Arnaud Alessandria'],
    'Mongolia': ['Ariunbat Altanzul'],
    'Montenegro': ['Branislav Pekovic'],
    'Morocco': ['Pietro Tranchina'],
    'North Macedonia': ['Mirko Lazareski'],
    'Pakistan': ['Muhammad Karim'],
    'Philippines': ['Francis Ceccarelli'],
    'Portugal': ['Emeric Guerillot'],
    'Romania': ['Alexandru Stefan Stefanescu'],
    'Russia': [],
    'San Marino': ['Rafael Mini'],
    'Saudi Arabia': ['Fayik Abdi'],
    'Serbia': ['Aleksa Tomovic'],
    'Singapore': ['Faiz Basha'],
    'South Africa': ['Thomas Weir'],
    'Spain': ['Joaquim Salarich'],
    'Taiwan': ['Troy Samuel Chang'],
    'Thailand': ['Fabian Wiest'],
    'Trinidad & Tobago': ['Nikhil Alleyne'],
    'Turkey': ['Thomas Kaan Onol Lang'],
    'UAE': ['Alexander Astridge'],
    'Ukraine': ['Dmytro Shepiuk'],
    'Uruguay': ['Nicolas Pirozzi Mayer'],
    'Uzbekistan': ['Medet Nazarov']
}

CHAMPS_ATHLETES_LADIES = {
    # Major alpine nations
    'Austria': ['Nina Astner', 'Stephanie Brunner', 'Katharina Gallhuber', 'Lisa Hörhager', 'Katharina Huber', 'Cornelia Hütter', 'Nina Ortlieb', 'Mirjam Puchner', 'Ariane Raedler', 'Julia Scheib', 'Katharina Truppe'],
    'Canada': ['Kiara Alexander', 'Cassidy Gray', 'Valerie Grenier', 'Justine Lamontagne', 'Ali Nullmeyer', 'Britt Richardson',  'Amelia Smart', 'Laurence St-Germain'],
    'France': ['Camille Cerutti', 'Marion Chevrier', 'Clara Direz','Doriane Escane', 'Laura Gauche', 'Marie Lamure', 'Caitlin McFarlane', 'Romane Miradoli'],
    'Italy': ['Federica Brignone', 'Elena Curtoni', 'Giada D\'Antonio', 'Nadia Delago', 'Nicol Delago', 'Lara Della Mea', 'Sofia Goggia',  'Martina Peterlini', 'Laura Pirovano', 'Anna Trocker', 'Asja Zenere'],
    'Norway': ['Mina Fürst Holtmann', 'Kajsa Vickhoff Lie', 'Marte Monsen', 'Thea Louise Stjernesund', 'Madeline Sylvester-Davik', 'Bianca Bakke Westhoff'],
    'Switzerland': ['Malorie Blanc', 'Eliane Christen', 'Delia Durrer', 'Jasmine Flury',  'Wendy Holdener',  'Vanessa Kasper', 'Melanie Meillard', 'Sue Piller', 'Camille Rast', 'Janine Schmitt', 'Corinne Suter'],    
    'USA': ['Mary Bocock', 'Keely Cashman', 'Katie Hensien','AJ Hurt', 'Breezy Johnson', 'Paula Moltzan', 'Nina O\'Brien', 'Mikaela Shiffrin', 'Lindsey Vonn',  'Jacqueline Wiles', 'Isabella Wright'],
    
    # Medium nations
    'Andorra': ['Jordina Caminal Santure', 'Carla Mijares Ruf', 'Cande Moreno Becerra'],
    'Croatia': ['Zrinka Ljutic', 'Leona Popovic', 'Pia Vucinic'],
    'Czechia': ['Martina Dubovska',  'Alena Labastova', 'Ester Ledecka',  'Elisa Maria Negri', 'Barbora Novakova'],
    'Finland': ['Silja Koskinen', 'Rosa Pohjolainen', 'Erika Pykäläinen'],
    'Germany': ['Emma Aicher', 'Lena Dürr', 'Jessica Hilzinger', 'Kira Weidle-Winkelmann'],
    'Slovenia': ['Ana Bucik Jogan', 'Lila Lapanja', 'Caterina Sinigoi', 'Ilka Stuhec', 'Nika Tomsic'],
    'Sweden': ['Estelle Alphand', 'Hanna Aronsson Elfman', 'Sara Hector', 'Cornelia Öhlund', 'Anna Swenn-Larsson'],
    

    # Small nations (More than 2 athletes)
    'Albania': ['Lisa Brunga', 'Lara Colturi', 'Semire Dauti'],
    'Argentina': ['Francesca Baruzzi Farriol', 'Nicole Begue'],
    'Australia': ['Phoebe Heaydon', 'Madison Hoffman'],
    'Belgium': ['Kim Vanreusel'],
    'Bosnia&Herzegovina': ['Esma Alic', 'Elevdina Muzaferija'],
    'Brazil': ['Alice Padilha'],
    'Bulgaria': ['Anina Zurbriggen'],
    'Great Britain': ['Victoria Palla'],
    'Latvia': ['Liene Bondare', 'Dzenifera Germane'],
    'Poland': ['Maryna Gasienica Daniel', 'Aniela Sawicka'],
    'Slovakia': ['Rebeka Jancova', 'Katarina Srobova', 'Petra Vlhova'],
    'South Korea': ['So-hui Gim', 'Seoyun Park'],

    
    #Charity Cases
    'Azerbaijan': ['AA Papathoma Paraskevaidou'],
    'Belarus': ['Maria Shkanova'],
    'Chile': ['Matilde Schwencke'],
    'China': ['Yuying Zhang'],
    'Cyprus': ['Andrea Loizidou'],
    'Denmark': ['Clara-Marie Holmer Vorre'],
    'Estonia': ['Hanna Gret Teder'],
    'Georgia': ['Nino Tsiklauri'],
    'Greece': ['Maria Eleni Tsiovolou'],
    'Hong Kong': ['Eloise Yung Shih King'],
    'Hungary':['Zita Toth'],
    'Iceland': ['Elin Van Pelt'],
    'Iran': ['Sadaf Savehshemshaki'],
    'Ireland': ['Anabelle Zurbay'],
    'Israel': ['Noa Szollos'],
    'Japan': ['Asa Ando'],
    'Kazakhstan': ['Alexandra Skorokhodova'],
    'Kenya': ['Sabrina Simader'],
    'Kosovo': ['Kiana Kryeziu'],
    'Liechtenstein': ['Madeleine Beck'],
    'Lithuania': ['Neringa Stapanauskaite'],
    'Luxembourg': ['Gwyneth Ten Raa'],
    'Madagascar': ['Mialitiana Clerc'],
    'Malaysia': ['Aruwin Salehhuddin'],
    'Mexico': ['Sarah Schleper'],
    'New Zealand': ['Alice Robinson'],
    'North Macedonia': ['Jana Atanasovska'],
    'Philippines': ['Tallulah Proulx'],
    'Portugal': ['Vanina Guerillot'],
    'Romania': ['Sofia Maria Moldovan'],
    'Russia': ['Iulija Pleshkova'],
    'South Africa': ['Lara Markthaler'],
    'Spain': ['Arrieta Rodriguez Elosegui'],
    'Taiwan': ['Wen-Yi Lee'],
    'Trinidad & Tobago': ['Emma Gatcliffe'],
    'Turkey': ['Ada Hasirci'],
    'UAE': ['Piera Hudson'],
    'Ukraine': ['Anastasiya Shepilenko']
    
    
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