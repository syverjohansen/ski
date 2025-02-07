import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from urllib.request import urlopen
from datetime import datetime
import re
import time
import logging
import numpy as np
import sys
import json
import warnings
import concurrent.futures
warnings.filterwarnings('ignore')
start_time = time.time()

def sex(df, sex):
    if(sex=="M"):
        df = pd.read_excel("~/ski/elo/python/ski/excel365/setup_demo.xlsx", sheet_name="Men")
    else:
        df = pd.read_excel("~/ski/elo/python/ski/excel365/setup_demo.xlsx", sheet_name="Ladies")
    return df

def relay(df, relay):
    if(relay==1):
        return df
    else:
        df = df.loc[df['Distance']!="Rel"]
        df = df.loc[df['Distance']!="Ts"]
        return df


def date1(df, date1):
    df = df.loc[df['Date']>=date1]
    return df

def date2(df, date2):
    df = df.loc[df['Date']<=date2]
    return df

def city(df, cities):
    df = df.loc[df['City'].isin(cities)]
    return df

def country(df, countries):
    df = df.loc[df['Country'].isin(countries)]
    return df

def event(df, events):
    df = df.loc[df['Event'].isin(countries)]
    return df

def distance(df, distances):
   # print(pd.unique(df['Distance']))
    if(distances=="Sprint"):
        #df = df.loc[(df['Distance']=="Sprint") | (df['City']=="Tour de Ski")]
        df = df.loc[(df['Distance']=="Sprint")|(df['Distance']=="Ts")]
        
    elif(distances in pd.unique(df['Distance'])):
        print("true")
        df = df.loc[df['Distance']==distances]
    else:
        df = df.loc[(df['Distance']!="Sprint")&(df['Distance']!="Ts")]
    return df

def technique(df, technique):
    if(technique == "F"):
        #df = df.loc[(df['Technique']=="F") | (df['City']=="Tour de Ski")]
        df = df.loc[(df['Technique']=="F") ]
    elif(technique =="P"):
        df = df.loc[df['Technique']=="P"]
    else:
        df = df.loc[df['Technique']!="P"]
        df = df.loc[df['Technique']!="F"]
        df = df.loc[(df['Distance']!="Stage") & (df['Distance']!="Etappeløp")]
    return df

def ms(df, ms):
    
    if(ms=="1"):
        print("yo")
        df = df.loc[df['MS']==1]
       # print(df)
    else:
        df = df.loc[df['MS']==0]
    return df

def place1(df, place1):
    df = df.loc[df['Place'] >= place1]
    return df

def place(df,  place2):
    df = df.loc[df['Place'] <= place2]
    return df

def names(df, names):
    df = df.loc[df['Name'].isin(names)]
    return df

def season1(df, season1):
    df = df.loc[df['Season']>=season1]
    return df

def season(df,season2):
    df = df.loc[df['Season']<=season2]
    return df

def nation (df, nations):
    df = df.loc[df['Nation'].isin(nations)]
    return df

def race1 (df, pct1):
    seasons = (pd.unique(df['Season']))

    pcts = []
    for season in seasons:
        seasondf = df.loc[seasondf['Season']==season]
        seasondf['pct'] = seasondf['Race']
        num_races = max(seasondf['Race'])
        races = pd.unique(df['Race'])
        for race in races:
            pct = float(race/num_races)
            seasondf['pct'].mask(seasondf['pct']==race, 'pct', inplace=True)
        pcts.append(list(seasondf['pct']))
    df['pct'] = pcts
    df = df.loc[df['pct']>=pct1]
    return df

def race2 (df, pct2):
    seasons = (pd.unique(df['Season']))

    pcts = []
    for season in seasons:
        seasondf = df.loc[seasondf['Season']==season]
        seasondf['pct'] = seasondf['Race']
        num_races = max(seasondf['Race'])
        races = pd.unique(df['Race'])
        for race in races:
            pct = float(race/num_races)
            seasondf['pct'].mask(seasondf['pct']==race, 'pct', inplace=True)
        pcts.append(list(seasondf['pct']))
    df['pct'] = pcts
    df = df.loc[df['pct']<=pct2]
    return df

def handle_value(key, value, df):
    if key == "sex":
        df = sex(df, value)
    elif key == "date1":
        df = date1(df, value)
    elif key == "date2":
        df = date2(df, value)
    elif key == "city":
        df = city(df, value)
    elif key == "country":
        df = country(df, value)
    elif key == "distance":
        df = distance(df, value)
    elif key == "ms":
        df = ms(df, value)
    elif key == "technique":
        df = technique(df, value)
    elif key == "place1":
        df = place1(df, value)
    elif key == "place2":
        df = place2(df, value)
    elif key == "name":
        df = name(df, value)
    elif key == "season1":
        df = season1(df, value)
    elif key == "season2":
        df = season2(df, value)
    elif key == "nation":
        df = nation(df, value)
    elif key == "race1":
        df = race1(df, value)
    elif key == "race2":
        df = race2(df, value)
    return df





def calc_Svec(Place_vector):
    '''
            convert the race results (place vector) into actual value for each athlete.
            Input:
                Place_vector: the place of each athlete, an array of sorted integer values, [P1, P2, ... Pn] so that P1 <= P2 ... <= Pn
            Output:
                S_vector: the actual score of each athlete (winning, drawing, or losing) against the (n-1) athletes, an array of float values, [S1, S2, ... Sn]
                    win = 1
                    draw = 1 / 2
    '''
    #Getting the length of the place vector and the set of the place_vector
    n, n_unique = len(Place_vector), len(set(Place_vector))
    
    #If no draws:
    if n==n_unique:
        #Reverses the order of the places to show how many wins a person got
        S_vector = np.arange(n)[::-1]
        return S_vector
    #If there are draws
    else:
        Place_vector, S_vector = np.array(Place_vector), list()
        for p in Place_vector:
            draws = np.count_nonzero(Place_vector == p)-1
            wins = (Place_vector>p).sum()
            score = wins*1 + draws*.5
            S_vector.append(score)
        return np.array(S_vector)

def calc_Evec(R_vector, basis=10, difference=400):
    '''
        compute the expected value of each athlete winning against all the other (n-1) athletes.
        Input:
            R_vector: the ELO rating of each athlete, an array of float values, [R1, R2, ... Rn]
        Output:
            E_vector: the expected value of each athlete winning against all the (n-1) athletes, an array of float values, [E1, E2, ... En]
    '''
    
    #R_vector is the list of previous elo scores for each skier.  Going to convert to an array and get its size
    R_vector = np.array(R_vector)
    n = R_vector.size
    
    #R_mat_col creates a matrix of the elo scores
    #Then we transpose the matrix.  Now each row is each skiers pelo, and each column represents all the pelos
    R_mat_col = np.tile(R_vector, (n,1))
    R_mat_row = np.transpose(R_mat_col)
    
    #Now this computes the expected score for the Elo matchup
    E_matrix = 1/(1+basis**((R_mat_row-R_mat_col)/difference))
    
    #We are not interested in self-matchups
    np.fill_diagonal(E_matrix, 0)
    
    #Sum of expected scores for each player against all other players
    E_vector = np.sum(E_matrix, axis=0)
    
    return E_vector

#Getting the K value for a season
def k_finder(season_df, max_var_length):
    #Figuring out how many races there are for a given season
    races = len(pd.unique(season_df['Race']))
    
    #Calculating the k-value for that season
    #The lowest k-value is 1.  This is for the season with the most races
    #The highest is 5
    #What ever is smallest between 5, most races in a season/2, and most races in a season/races in that season
    k = max(1, min(5, float(max_var_length/2), float(max_var_length/races)))
    print(k)
    return k

#Creating the elo function
#The initial score we are setting is 1300, arbitrary number that is subject to change from testing
#K score is 1 by default, we will change this, and I want to do testing to determine the best overall K eventually
#Discount is .85.  This is how much we will reduce an athletes elo by at the end of a season.  Again to be tested


def process_skier(idd, season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df_columns):
    endseasondate = datetime(seasons[season], 5, 1)
    endskier = season_df.loc[season_df['ID'] == idd]
    endname = endskier['Name'].iloc[-1]
    endnation = endskier['Nation'].iloc[-1]
    endpelo = id_dict[idd]
    endelo = endpelo * discount + base_elo * (1 - discount)
    endbirthday = endskier['Birthday'].iloc[-1]
    endage = endskier['Age'].iloc[-1]
    endexp = endskier['Exp'].iloc[-1]
    endf = pd.DataFrame([[endseasondate, "Summer", "Break", "End", sex, 0, 0, None, 0, endname, endnation, idd,
                         seasons[season], 0, endbirthday, endage, endexp, endpelo, endelo]], columns=elo_df_columns)
    id_dict[idd] = endelo
    return idd, endelo, endf

def parallel_process_skiers(season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df):
    ski_ids_s = list(pd.unique(season_df["ID"]))

    
    results = []
    new_id_dict = id_dict.copy()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_skier, idd, season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df.columns): idd for idd in ski_ids_s}
        for future in concurrent.futures.as_completed(futures):
            idd, endelo, endf = future.result()
            new_id_dict[idd] = endelo
            results.append(endf)
    
    # Concatenate all the DataFrames returned by the parallel processing
    new_rows = pd.concat(results, ignore_index=True)
    elo_df = pd.concat([elo_df, new_rows], ignore_index=True)
    
    return elo_df, new_id_dict


def elo(df, base_elo=1300, K=1, discount=.85):
    #Get the sex
    sex = df['Sex'].iloc[1]
    
    
    #Create an empty DataFrame
    elo_df = pd.DataFrame()
    
    #Create a list of the IDs in the df
    id_dict_list = list(pd.unique(df['ID']))
    
    #Assign everyone a value of 1300 to start out with
    id_dict = {k:1300 for k in id_dict_list}
    
    #Getting the maximum number of races in the df
    max_races = max(df['Race'])
    
    #Getting a list of all the seasons in the df.  Can't assume 1924 to present 
    seasons = pd.unique(df['Season'])
    
    #This is for figuring out dynamic K values based on the max races in a season for a certain type of race
    max_var_length = 0
    for season in range(len(seasons)):
        season_df = df.loc[df['Season']==seasons[season]]
        max_var_length = max(max_var_length, len(pd.unique(season_df['Race'])))

    k_total = 0
    evec_total = 0
    svec_total = 0
    summer_total = 0
    update_total = 0
        
    for season in range(len(seasons)):
        #Creating a season df
        season_df = df.loc[df['Season']==seasons[season]]
        
        
        #Get the K value. K-value will be it's own write up and will do testing to find the optimal solution
        #Doesn't seem like too much open source research has been done on the topic
        k_time = time.time()
        K = k_finder(season_df, max_var_length)
        k_total = k_total + time.time()-k_time

        print(seasons[season])
        
        races = pd.unique(season_df['Race'])
        #Now we get to the calculating elo part for each race
        for race in range(len(races)):
            #Isolate the race into a df
            race_df = season_df.loc[season_df['Race']==races[race]]
            
            #Create a list of all the IDs in that race
            ski_ids_r = list(race_df['ID'])
            
            #Get the most recent elo score for each skier in the race
            pelo_list = [id_dict[idd] for idd in ski_ids_r]
            
            #Get a list of all the places in the race
            places_list = race_df['Place']
            
            #Create a column called Pelo that has all the previous elo values
            race_df['Pelo'] = pelo_list
            
            #Get the expected elos based on everyone's previous elo
            evec_time = time.time()
            E = calc_Evec(pelo_list)
            evec_total = evec_total + time.time()-evec_time

            svec_time = time.time()
            S = calc_Svec(places_list)
            svec_total = svec_total+time.time()-svec_time

            #The new elo scores
            update_time = time.time()
            elo_list = np.array(pelo_list) + K*(S-E)
  
            #Putting the elo_list into the race_df and adding race_df to the elo_df
            race_df['Elo'] = elo_list
            elo_df = pd.concat([elo_df, race_df], ignore_index=True)
            
            #Updating the most recent elos for each id
            id_dict.update(dict(zip(ski_ids_r, elo_list)))
            update_total = update_total+time.time()-update_time

        summer_time = time.time()
        #Making the end season date May 1st    
        elo_df, id_dict = parallel_process_skiers(season_df, id_dict, discount, base_elo, seasons, season, sex, elo_df)
        summer_total = summer_total + time.time()-summer_time
    print("K-score calculation: "+str(k_total))
    print("evec calculation: "+str(evec_total))
    print("svec calculation: "+str(svec_total))
    print("Update calculation: "+str(update_total))
    print("summer calculation: "+str(summer_total))
    return elo_df

df = pd.DataFrame()
load_time = time.time()
json_str = sys.argv[1]
data = json.loads(json_str)
print(data)
print("Done loading data "+str(time.time()-load_time))

file_filter_time = time.time()
file_string = ""
for key, value in data.items():
    if key=="relay":
        if value==1:
            file_string = file_string+"rel_"
        else:
            df = handle_value(key, 0, df)
    elif value is not None:
        file_string = file_string+value+"_"
        df = handle_value(key, value, df)
file_string = file_string[:-1]
print("Done loading file and filtering data "+str(time.time()-file_filter_time))

print(file_string)
elo_df = elo(df)
print(elo_df)


elo_df.to_pickle("~/ski/elo/python/ski/excel365/"+file_string+".pkl")
elo_df.to_excel("~/ski/elo/python/ski/excel365/"+file_string+".xlsx")
print(time.time() - start_time)


