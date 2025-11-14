import pandas as pd
import numpy as np
import time
start_time = time.time()

ladiesdf = pd.read_pickle("~/ski/elo/python/alpine/excel365/ladiesdf.pkl")

#mendf = pd.read_pickle("~/ski/elo/python/ski/mendf.pkl")
pd.options.mode.chained_assignment = None
import numpy as np
K=1

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

def lady_elo(ladiesdf, base_elo=1300, K=1, discount=.85):
    #Step 1: Figure out the person's previous elo.  
    #If they aren't in the new dataframe, make them a new score (1300)
    ladieselodf = pd.DataFrame()
    #ladieselodf.columns = ['date', 'city', 'country', 'level', 'sex', 'distance', 
    #'discipline', 'place', 'name', 'nation','season','race', 'pelo', 'elo']
    id_dict_list = list(pd.unique(ladiesdf['id']))
    v = 1300
    id_dict = {k:1300 for k in id_dict_list}
   # print(id_dict)

    id_pool = []
    max_races = max(ladiesdf['race'])
    #Print the unique seasons
    seasons = (pd.unique(ladiesdf['season']))
    #print(seasons)
    for season in range(len(seasons)):
    #for season in range(10):
        print(seasons[season])

        seasondf = ladiesdf.loc[ladiesdf['season']==seasons[season]]
        races = pd.unique(seasondf['race'])
        #K = float(38/len(races))
        K=1

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
            ladieselodf = ladieselodf.append(racedf)

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
            endf = pd.DataFrame([[endseasondate, "Summer", "Break", "end", "L", None, None
                , endname, endnation, idd ,seasons[season], 0, endpelo, endelo]], columns = ladieselodf.columns)
            ladieselodf = ladieselodf.append(endf)
            id_dict[idd] = endelo

           
            #print(ladieselodf)

        

    return ladieselodf 


ladyelo2 = (lady_elo(ladiesdf))
ladyelo2.to_pickle("~/ski/elo/python/alpine/excel365/ladyelo2.pkl")
ladyelo2.to_excel("~/ski/elo/python/alpine/excel365/ladyelo2.xlsx")

print(time.time() - start_time)



