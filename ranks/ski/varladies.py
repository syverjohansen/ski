#rework elo so each id has an elo that is updated for each race.  
#at the end of the season the whole vector is multiplied by .85 + 1300*.15

import pandas as pd
import numpy as np
import time
start_time = time.time()


ladiesdf = pd.read_pickle("~/ski/ranks/ski/excel365/ladiesdf.pkl")


#print(ladiesdf)
#pd.options.mode.chained_assignladiest = None



def dates(ladiesdf, date1, date2):
    ladiesdf = ladiesdf.loc[ladiesdf['date']>=date1]
    ladiesdf = ladiesdf.loc[ladiesdf['date']<=date2]
    #print(ladiesdf)
    return ladiesdf

def city(ladiesdf, cities):
    ladiesdf = ladiesdf.loc[ladiesdf['city'].isin(cities)]
    return ladiesdf

def country(ladiesdf, countries):
    ladiesdf = ladiesdf.loc[ladiesdf['country'].isin(countries)]
    return ladiesdf

def distance(ladiesdf, distances):
   # print(pd.unique(ladiesdf['distance']))
    if(distances=="Sprint"):
        ladiesdf = ladiesdf.loc[ladiesdf['distance']=="Sprint"]
    elif(distances in pd.unique(ladiesdf['distance'])):
        print("true")
        ladiesdf = ladiesdf.loc[ladiesdf['distance']==distances]
    else:
        ladiesdf = ladiesdf.loc[ladiesdf['distance']!="Sprint"]
    return ladiesdf

def discipline(ladiesdf, discipline):
    if(discipline == "F"):
        ladiesdf = ladiesdf.loc[ladiesdf['discipline']=="F"]
    elif(discipline =="P"):
        ladiesdf = ladiesdf.loc[ladiesdf['discipline']=="P"]
    else:
        ladiesdf = ladiesdf.loc[ladiesdf['discipline']!="P"]
        ladiesdf = ladiesdf.loc[ladiesdf['discipline']!="F"]
        ladiesdf = ladiesdf.loc[ladiesdf['distance']!="Stage"]
    return ladiesdf

def ms(ladiesdf, ms):
    
    if(ms=="1"):
        print("yo")
        ladiesdf = ladiesdf.loc[ladiesdf['ms']==1]
       # print(ladiesdf)
    else:
        ladiesdf = ladiesdf.loc[ladiesdf['ms']==0]
    return ladiesdf

def place(ladiesdf, place1, place2):
    ladiesdf = ladiesdf.loc[ladiesdf['place'] >= place1]
    ladiesdf = ladiesdf.loc[ladiesdf['place'] <= place2]
    return ladiesdf

def names(ladiesdf, names):
    ladiesdf = ladiesdf.loc[ladiesdf['name'].isin(names)]
    return ladiesdf

def season(ladiesdf, season1, season2):
    ladiesdf = ladiesdf.loc[ladiesdf['season']>=season1]
    ladiesdf = ladiesdf.loc[ladiesdf['season']<=season2]
    return ladiesdf

def nation (ladiesdf, nations):
    ladiesdf = ladiesdf.loc[ladiesdf['nation'].isin(nations)]
    return ladiesdf

def race (ladiesdf, pct1, pct2):
    seasons = (pd.unique(ladiesdf['season']))

    pcts = []
    for season in seasons:
        seasondf = ladiesdf.loc[seasondf['season']==season]
        seasondf['pct'] = seasondf['race']
        num_races = max(seasondf['race'])
        races = pd.unique(ladiesdf['race'])
        for race in races:
            pct = float(race/num_races)
            seasondf['pct'].mask(seasondf['pct']==race, 'pct', inplace=True)
        pcts.append(list(seasondf['pct']))
    ladiesdf['pct'] = pcts
    ladiesdf = ladiesdf.loc[ladiesdf['pct']>=pct1]
    ladiesdf = ladiesdf.loc[ladiesdf['pct']>=pct2]
    return ladiesdf





    



def k_finder(ladies, varladiesdf, season):
    max_races = max(ladiesdf['race'])
    seasondf = ladiesdf.loc[ladiesdf['season']==season]
    max_season_races = max(seasondf['race'])
    #print(max_season_races)
    varseasondf = varladiesdf.loc[varladiesdf['season']==season]
    varraces = len(pd.unique(varseasondf['race']))

    k = max(1,min(float(max_season_races/2), float(max_season_races/varraces)))
    #k = float(max_season_races/varraces)
    return k



def lady_points(varladiesdf, K=1):
    #print(varladiesdf)
    #Step 1: Figure out the person's previous elo.  
    #If they aren't in the new dataframe, make them a new score (1300)
    pointsdf = pd.DataFrame()
   # pointsdf.columns = ["name", "nation", "id", "Olympics", "WSC","Tour", "WC", "Table", "Total"]
    id_dict_list = list(pd.unique(varladiesdf['id']))
    
   # print(id_dict)

    id_pool = []
    #max_races = max(varladiesdf['race'])
    #Print the unique seasons
    seasons = (pd.unique(varladiesdf['season']))
    #print(seasons)
    for season in range(len(seasons)):
        K = k_finder(ladiesdf, varladiesdf, seasons[season])
        #print(K)

    
        print(seasons[season])

        seasondf = varladiesdf.loc[varladiesdf['season']==seasons[season]]
        

    for iden in id_dict_list:
        olympic_points = 0
        wsc_points = 0
        tour_points = 0
        wc_points = 0
        table_points = 0
        total_points = 0
        tempdf = varladiesdf.loc[varladiesdf['id']==iden]
        
        tempdf = tempdf.reset_index(drop=True)
        for a in range(len(tempdf['place'])):
            
            if(tempdf['category'][a]=="Olympics"):
                olympic_points+=tempdf['points'][a]
            elif(tempdf['category'][a]=="WSC"):
                wsc_points+=tempdf['points'][a]
            elif(tempdf['category'][a]=="Tour"):
                #continue
                tour_points+=tempdf['points'][a]
            elif((tempdf['category'][a]=="WC")):
                wc_points+=tempdf['points'][a]
            elif(tempdf['category'][a]=="table"):
                #continue
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










           
            #print(ladieselodf)

        

    #return ladieselodf 

varladiesdf = ladiesdf

#varladiesdf = dates(varladiesdf, 0, 20210500)
#varladiesdf = distance(varladiesdf, "Sprint")
#varladiesdf = discipline(varladiesdf, "F")
#varladiesdf = ms(varladiesdf, "1")
#varladiesdf = season(varladiesdf, 0, 9999)
varladieselo = lady_points(varladiesdf)
varladieselo.to_pickle("~/ski/ranks/ski/excel365/varladies_points.pkl")
varladieselo.to_excel("~/ski/ranks/ski/excel365/varladies_points.xlsx")
print(time.time() - start_time)

