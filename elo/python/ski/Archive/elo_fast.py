import pandas as pd
import numpy as np


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

def elo(pickle_df, base_elo = 1300, K = 1, discount = 0.85):
    # Initialize, all athlete's ELO
    ski_ids = list(pd.unique(pickle_df["id"]))
    elo_dict = dict(zip(ski_ids,[base_elo]*len(ski_ids)))
    elo_df = pd.DataFrame()

    # Seasons, for each season
    seasons = pd.unique(pickle_df['season'])
    for season in seasons:
        print(season)
        season_df = pickle_df.loc[pickle_df['season']==season]
        # Races, update athlete ELO each race
        races = pd.unique(season_df['race'])
        K = max(pickle_df['race'])/len(races)
      #  print(K)
        for race in races:
            race_df = season_df.loc[season_df['race']==race]
            ski_ids_r = list(race_df['id'])
            pelo_list = [elo_dict[idd] for idd in ski_ids_r]
            places_list = race_df['place']
            E = calc_Evec(pelo_list)
            S = calc_Svec(places_list)
            
            # ELO calculation,
            elo_list = np.array(pelo_list) + K * (S - E) #https://en.wikipedia.org/wiki/Elo_rating_system#mathematicaldetails
            for i, idd in enumerate(ski_ids_r):
                elo_dict[idd] = elo_list[i]
        
        # Seasons, record all athlete's ELO for season
        ski_ids_s = list(pd.unique(season_df["id"]))
        for idd in ski_ids_s:
            #print(idd)
            end_skier = season_df.loc[season_df['id']==idd]
            end_name = end_skier['name'].iloc[-1]
            end_nation = end_skier['nation'].iloc[-1]
            end_pelo = elo_dict[idd]
            end_elo = end_pelo * discount + base_elo * (1-discount)
            end_df = pd.DataFrame([[season, idd, end_name, end_nation, end_pelo, end_elo]], columns = ['season', 'id', 'name', 'nation', 'pelo', 'elo'])
            elo_df = elo_df.append(end_df)
            # Discount ELO next Season
            elo_dict[idd] = end_elo
            
    return elo_df


if __name__ == "__main__":
    ## Execute only if run as a script

    # Ladies:
    ladies_df = pd.read_pickle("~/ski/elo/python/ski/ladiesdf.pkl")
    df = elo(ladies_df)
    df.to_csv('ladies_test.csv',index=False)
    
    # Men: 
    men_df = pd.read_pickle("~/ski/elo/python/ski/mendf.pkl")
    df = elo(men_df)
    df.to_csv('men_test.csv',index=False)








