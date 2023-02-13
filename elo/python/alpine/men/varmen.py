#rework elo so each id has an elo that is updated for each race.  
#at the end of the season the whole vector is multiplied by .85 + 1300*.15

import pandas as pd
import numpy as np
import time
start_time = time.time()


mendf = pd.read_pickle("~/ski/elo/python/alpine/excel365/mendf.pkl")

update_mendf = pd.read_pickle("~/ski/elo/python/alpine/excel365/menupdate_setup.pkl")
mendf = mendf.append(update_mendf, ignore_index=True)
#print(mendf)
pd.options.mode.chained_assignment = None



def dates(mendf, date1, date2):
    mendf = mendf.loc[mendf['date']>=date1]
    mendf = mendf.loc[mendf['date']<=date2]
    #print(mendf)
    return mendf

def city(mendf, cities):
    mendf = mendf.loc[mendf['city'].isin(cities)]
    return mendf

def country(mendf, countries):
    mendf = mendf.loc[mendf['country'].isin(countries)]
    return mendf

def distance(mendf, distances):
   # print(pd.unique(mendf['distance']))
    if(distances=="Tech"):
        mendf = mendf.loc[mendf['distance']!="Downhill"]
        mendf = mendf.loc[mendf['distance']!="Super G"]
        mendf = mendf.loc[mendf['distance']!="Combined"]

    elif(distances=="Speed"):
        mendf = mendf.loc[mendf['distance']!="Giant Slalom"]
        mendf = mendf.loc[mendf['distance']!="Slalom"]
        mendf = mendf.loc[mendf['distance']!="Combined"]
    elif(distances in pd.unique(mendf['distance'])):
        print("true")
        mendf = mendf.loc[mendf['distance']==distances]
    else:
        mendf = mendf.loc[mendf['distance']!="Sprint"]
    return mendf


def place(mendf, place1, place2):
    mendf = mendf.loc[mendf['place'] >= place1]
    mendf = mendf.loc[mendf['place'] <= place2]
    return mendf

def names(mendf, names):
    mendf = mendf.loc[mendf['name'].isin(names)]
    return mendf

def season(mendf, season1, season2):
    mendf = mendf.loc[mendf['season']>=season1]
    mendf = mendf.loc[mendf['season']<=season2]
    return mendf

def nation (mendf, nations):
    mendf = mendf.loc[mendf['nation'].isin(nations)]
    return mendf

def race (mendf, pct1, pct2):
    seasons = (pd.unique(mendf['season']))

    pcts = []
    for season in seasons:
        seasondf = mendf.loc[seasondf['season']==season]
        seasondf['pct'] = seasondf['race']
        num_races = max(seasondf['race'])
        races = pd.unique(mendf['race'])
        for race in races:
            pct = float(race/num_races)
            seasondf['pct'].mask(seasondf['pct']==race, 'pct', inplace=True)
        pcts.append(list(seasondf['pct']))
    mendf['pct'] = pcts
    mendf = mendf.loc[mendf['pct']>=pct1]
    mendf = mendf.loc[mendf['pct']>=pct2]
    return mendf



def calc_Evec(R_vector, basis = 10, difference = 400):
    '''
        compute the expected value of each athlete winning against all the other (n-1) athletes.
        Input:
            R_vector: the ELO rating of each athlete, an array of float values, [R1, R2, ... Rn]
        Output:
            E_vector: the expected value of each athlete winning against all the (n-1) athletes, an array of float values, [E1, E2, ... En]
    '''
    R_vector = np.array(R_vector)
    n = R_vector.size
    R_mat_col = np.tile(R_vector,(n,1))
    R_mat_row = np.transpose(R_mat_col)

    E_matrix = 1 / (1 + basis**((R_mat_row - R_mat_col) / difference)) 
    np.fill_diagonal(E_matrix, 0)
    E_vector = np.sum(E_matrix , axis=0)
    return E_vector

def calc_Svec(Place_vector):
    '''
        convert the race results (place vector) into actual value for each athlete.
        Input:
            Place_vector: the place of each athlete, an array of sorted integer values, [P1, P2, ... Pn] so that P1 <= P2 ... <= Pn
        Output:
            S_vector: the actual score of each athlete (winning, drawing, or losing) against the (n-1) athletes, an array of float values, [S1, S2, ... Sn]
                win = 1
                draw = 1 / 2
                loss = 0 
    '''
    n, n_unique = len(Place_vector), len(set(Place_vector))
    # If no draws: ex. [1, 2, ... Pn] -> [n-1, n-2, ... 0]
    if n == n_unique:
        S_vector = np.arange(n)[::-1]
        return S_vector
    # Else draws: ex. [1, 1, ... Pn]
    else:
        Place_vector, S_vector = np.array(Place_vector), list()
        for p in Place_vector:
            draws = np.count_nonzero(Place_vector == p) - 1
            wins = (Place_vector > p).sum()
            score = wins*1 + draws*0.5
            S_vector.append(score)
        return np.array(S_vector)

    

def EA (pelos, index):
    players = len(pelos)
    ra = pelos[index]
    QA = 10**(ra/400) 
    QA = np.repeat(QA, players - 1)
    rb = np.delete(np.array(pelos), index)
    QB = 10**(rb/400)
    EA = QA / (QA + QB)
    return EA

def SA(place_vector, place):
    place_vector = np.array(place_vector)
    losses = (place_vector < place).sum()
    draws = np.count_nonzero(place_vector == place) - 1
    wins = (place_vector > place).sum()
    return losses*[0] + draws*[0.5] + wins*[1]

def k_finder(men, varmendf, season, max_var_length):
    max_races = max(mendf['race'])
    seasondf = mendf.loc[mendf['season']==season]
    max_season_races = max(seasondf['race'])
    #print(max_season_races)
    varseasondf = varmendf.loc[varmendf['season']==season]
    varraces = len(pd.unique(varseasondf['race']))

    #k = max(1,min(float(max_season_races/2), float(max_season_races/varraces)))
    #k = float(max_races/varraces)
    k = max(1,min(5, float(max_var_length/2), float(max_var_length/varraces)))
#    k = max(1,min(float(max_var_length/3), float(max_var_length/varraces)))
    return k



def male_elo(varmendf, base_elo=1300, K=1, discount=.85):
    #Step 1: Figure out the person's previous elo.  
    #If they aren't in the new dataframe, make them a new score (1300)
    menelodf = pd.DataFrame()
    #menelodf.columns = ['date', 'city', 'country', 'level', 'sex', 'distance', 
    #'discipline', 'place', 'name', 'nation','season','race', 'pelo', 'elo']
    id_dict_list = list(pd.unique(varmendf['id']))
    v = 1300
    id_dict = {k:1300 for k in id_dict_list}
   # print(id_dict)

    id_pool = []
    max_races = max(varmendf['race'])
    #Print the unique seasons
    seasons = (pd.unique(varmendf['season']))
    max_var_length =1
    for season in range(len(seasons)):
        seasondf = varmendf.loc[varmendf['season']==seasons[season]]
        max_var_length = max(max_var_length, len(pd.unique(seasondf['race'])))
    #print(seasons)
    for season in range(len(seasons)):
        K = k_finder(mendf, varmendf, seasons[season], max_var_length)
        print(K)

    
        print(seasons[season])

        seasondf = varmendf.loc[varmendf['season']==seasons[season]]
        races = pd.unique(seasondf['race'])
        
        

        for race in range(len(races)):
            racedf = seasondf.loc[seasondf['race']==races[race]]
            ski_ids_r = list(racedf['id'])
            pelo_list = [id_dict[idd] for idd in ski_ids_r]
            places_list = racedf['place']
           # print(places_list)
            racedf['pelo'] = pelo_list
            E = calc_Evec(pelo_list)
            S = calc_Svec(places_list)
           # print(S)

            elo_list = np.array(pelo_list) + K * (S-E)
            
            racedf['elo'] = elo_list
            menelodf = menelodf.append(racedf)

            for i, idd in enumerate(ski_ids_r):
                id_dict[idd] = elo_list[i]
        ski_ids_s = list(pd.unique(seasondf["id"]))
        endseasondate = int(str(seasons[season])+'0500')
        for idd in ski_ids_s:
            endskier = seasondf.loc[seasondf['id']==idd]
            endname = endskier['name'].iloc[-1]
            endnation = endskier['nation'].iloc[-1]
            endpelo = id_dict[idd]
            endelo = endpelo*discount+base_elo*(1-discount)
            endf = pd.DataFrame([[endseasondate, "Summer", "Break", "end", "M", None, 0,
                 endname, endnation, idd ,seasons[season], 0, endpelo, endelo]], columns = menelodf.columns)
            menelodf = menelodf.append(endf)
            id_dict[idd] = endelo

           
            #print(menelodf)

        

    return menelodf 

varmendf = mendf

#varmendf = dates(varmendf, 0, 20210128)
#varmendf = distance(varmendf, "Giant Slalom")
#varmendf = discipline(varmendf, "F")
#varmendf = ms(varmendf, "1")
#varmendf = season(varmendf, 0, 9999)
varmenelo = male_elo(varmendf)
varmenelo.to_pickle("~/ski/elo/python/alpine/excel365/varmen_all_k.pkl")
varmenelo.to_excel("~/ski/elo/python/alpine/excel365/varmen_all_k.xlsx")
print(time.time() - start_time)

