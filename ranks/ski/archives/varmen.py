#rework elo so each id has an elo that is updated for each race.  
#at the end of the season the whole vector is multiplied by .85 + 1300*.15

import pandas as pd
import numpy as np
import time
start_time = time.time()


mendf = pd.read_pickle("~/ski/ranks/ski/excel365/mendf.pkl")


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
    if(distances=="Sprint"):
        mendf = mendf.loc[mendf['distance']=="Sprint"]
    elif(distances in pd.unique(mendf['distance'])):
        print("true")
        mendf = mendf.loc[mendf['distance']==distances]
    else:
        mendf = mendf.loc[mendf['distance']!="Sprint"]
    return mendf

def discipline(mendf, discipline):
    if(discipline == "F"):
        mendf = mendf.loc[mendf['discipline']=="F"]
    elif(discipline =="P"):
        mendf = mendf.loc[mendf['discipline']=="P"]
    else:
        mendf = mendf.loc[mendf['discipline']!="P"]
        mendf = mendf.loc[mendf['discipline']!="F"]
        mendf = mendf.loc[mendf['distance']!="Stage"]
    return mendf

def ms(mendf, ms):
    
    if(ms=="1"):
        print("yo")
        mendf = mendf.loc[mendf['ms']==1]
       # print(mendf)
    else:
        mendf = mendf.loc[mendf['ms']==0]
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





    



def k_finder(men, varmendf, season):
    max_races = max(mendf['race'])
    seasondf = mendf.loc[mendf['season']==season]
    max_season_races = max(seasondf['race'])
    #print(max_season_races)
    varseasondf = varmendf.loc[varmendf['season']==season]
    varraces = len(pd.unique(varseasondf['race']))

    k = max(1,min(float(max_season_races/2), float(max_season_races/varraces)))
    #k = float(max_season_races/varraces)
    return k



def male_points(varmendf, K=1):
    #print(varmendf)
    #Step 1: Figure out the person's previous elo.  
    #If they aren't in the new dataframe, make them a new score (1300)
    pointsdf = pd.DataFrame()
   # pointsdf.columns = ["name", "nation", "id", "Olympics", "WSC","Tour", "WC", "Table", "Total"]
    id_dict_list = list(pd.unique(varmendf['id']))
    
   # print(id_dict)

    id_pool = []
    #max_races = max(varmendf['race'])
    #Print the unique seasons
    seasons = (pd.unique(varmendf['season']))
    #print(seasons)
    for season in range(len(seasons)):
        K = k_finder(mendf, varmendf, seasons[season])
        #print(K)

    
        print(seasons[season])

        seasondf = varmendf.loc[varmendf['season']==seasons[season]]
        

    for iden in id_dict_list:
        olympic_points = 0
        wsc_points = 0
        tour_points = 0
        wc_points = 0
        table_points = 0
        total_points = 0
        tempdf = varmendf.loc[varmendf['id']==iden]
        tempdf = tempdf.reset_index(drop=True)
        for a in range(len(tempdf['place'])):
            if(tempdf['category'][a]=="Olympics"):
                olympic_points+=tempdf['points'][a]
            elif(tempdf['category'][a]=="WSC"):
                wsc_points+=tempdf['points'][a]
            elif(tempdf['category'][a]=="Tour"):
               # continue
                tour_points+=tempdf['points'][a]
            elif((tempdf['category'][a]=="WC")):
                wc_points+=tempdf['points'][a]
            elif(tempdf['category'][a]=="table"):
               # continue
                table_points+=tempdf['points'][a]
            
            total_points+=tempdf['points'][a]
        temp_pointsdf = pd.DataFrame()
        #temp_pointsdf.columns = ["name", "nation", "id", "Olympics", "WSC","Tour", "WC", "Table", "Total"]
        
        temp_pointsdf['name'] = [(tempdf['name'][0])]
       
        temp_pointsdf['nation'] = [tempdf['nation'][0]]
        temp_pointsdf['id'] = [tempdf['id'][0]]
        temp_pointsdf["Olympics"] = [olympic_points]
        temp_pointsdf["WSC"] = [wsc_points]
        temp_pointsdf["Tour"] = [tour_points]
        temp_pointsdf["WC"] = [wc_points]
        temp_pointsdf["Table"] = [table_points]
        #print(temp_pointsdf)
        temp_pointsdf["Total"] = [total_points]
        
        pointsdf = pointsdf.append(temp_pointsdf, ignore_index=True)
        #print(pointsdf)
    pointsdf = pointsdf.sort_values(by='Total', ascending=False)
    pointsdf = pointsdf.reset_index(drop=True)
    return pointsdf










           
            #print(menelodf)

        

    #return menelodf 

varmendf = mendf

#varmendf = dates(varmendf, 20180501, 20190500)
#varmendf = distance(varmendf, "Distance")
#varmendf = discipline(varmendf, "F")
#varmendf = ms(varmendf, "1")
#varmendf = season(varmendf, 0, 9999)
varmenelo = male_points(varmendf)
varmenelo.to_pickle("~/ski/ranks/ski/excel365/varmen_points.pkl")
varmenelo.to_excel("~/ski/ranks/ski/excel365/varmen_points.xlsx")
print(time.time() - start_time)

