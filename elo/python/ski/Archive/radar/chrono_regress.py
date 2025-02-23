
import pandas as pd
import time
pd.options.mode.chained_assignment = None
start_time = time.time()


def dates(ladiesdf, date1, date2):
    ladiesdf = ladiesdf.loc[ladiesdf['date']>=date1]
    ladiesdf = ladiesdf.loc[ladiesdf['date']<=date2]
    return ladiesdf

def city(ladiesdf, cities):
    ladiesdf = ladiesdf.loc[ladiesdf['city'].isin(cities)]
    return ladiesdf

def country(ladiesdf, countries):
    ladiesdf = ladiesdf.loc[ladiesdf['country'].isin(countries)]
    return ladiesdf

def distance(ladiesdf, distances):
    print(pd.unique(ladiesdf['distance']))
    if(distances=="Sprint"):
        #ladiesdf = ladiesdf.loc[(ladiesdf['distance']=="Sprint") | (ladiesdf['city']=="Tour de Ski")]
        ladiesdf = ladiesdf.loc[(ladiesdf['distance']=="Sprint")]
    elif(distances in pd.unique(ladiesdf['distance'])):
        print("true")
        ladiesdf = ladiesdf.loc[ladiesdf['distance']==distances]
    else:
        ladiesdf = ladiesdf.loc[ladiesdf['distance']!="Sprint"]
    return ladiesdf

def discipline(ladiesdf, discipline):
    if(discipline == "F"):
       # ladiesdf = ladiesdf.loc[(ladiesdf['discipline']=="F") | (ladiesdf['city']=="Tour de Ski")]
        ladiesdf = ladiesdf.loc[(ladiesdf['discipline']=="F")]
    elif(discipline =="P"):
        ladiesdf = ladiesdf.loc[ladiesdf['discipline']=="P"]
    else:
        ladiesdf = ladiesdf.loc[ladiesdf['discipline']!="P"]
        ladiesdf = ladiesdf.loc[ladiesdf['discipline']!="F"]
        ladiesdf = ladiesdf.loc[(ladiesdf['distance']!="Stage") & (ladiesdf['distance']!="Etappeløp")]
    return ladiesdf

def ms(ladies, ms):
    if(ms==1):
        ladiesdf = ladiesdf.loc[ladiesdf['ms']==1]
    else:
        ladiesdf = ladiesdf.loc[ladiesdf['ms']==0]

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


def pct(df):
	points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	df = df.loc[df['level']=="all"]
	df2 = pd.DataFrame()
	seasons = pd.unique(df['season'])

	

	for season in range(len(seasons)):
		seasondf = df.loc[df['season']==seasons[season]]
		#print(seasondf)
		#print(seasondf)
		races = pd.unique(seasondf['race'])
		#print(races)
		for race in range(len(races)):
			points_list = points
			
			if(season==0 and race==0):
				continue
			else:
				racedf = seasondf.loc[seasondf['race']==races[race]]
				if(len(racedf['place'])>30):
					#print(len(racedf['place']))
					#print([0]*(len(racedf['place'])-len(points)))
					points_list = points_list + ([0]*(len(racedf['place'])-len(points)))
				else:
					points_list = points[0:len(racedf['place'])]
				max_pelo = max(racedf['pelo'])

				#print(max(racedf['place']))
				max_place = max(racedf['place'])
				racedf['pelo'] = racedf['pelo'].apply(lambda x: 100*(x/max_pelo))
				racedf['distance_pelo'] = racedf['distance_pelo'].apply(lambda x: 100*(x/max(racedf['distance_pelo'])))
				racedf['distance_classic_pelo'] = racedf['distance_classic_pelo'].apply(lambda x: 100*(x/max(racedf['distance_classic_pelo'])))
				racedf['distance_freestyle_pelo'] = racedf['distance_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['distance_freestyle_pelo'])))
				racedf['sprint_pelo'] = racedf['sprint_pelo'].apply(lambda x: 100*(x/max(racedf['sprint_pelo'])))
				racedf['sprint_classic_pelo'] = racedf['sprint_classic_pelo'].apply(lambda x: 100*(x/max(racedf['sprint_classic_pelo'])))
				racedf['sprint_freestyle_pelo'] = racedf['sprint_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['sprint_freestyle_pelo'])))

				racedf['classic_pelo'] = racedf['classic_pelo'].apply(lambda x: 100*(x/max(racedf['classic_pelo'])))
				racedf['freestyle_pelo'] = racedf['freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['freestyle_pelo'])))
				racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
				racedf['points'] = points_list
				


				df2 = df2.append(racedf)
	df2['home'] = (df2['nation']==df2['country'])
	return df2

 



df = pd.read_pickle('~/ski/elo/python/ski/radar/men_chrono.pkl')

df = distance(df, "Distance")
df = discipline(df, "C")

df = pct(df)
df.to_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/men_chrono_regress.pkl")
df.to_excel("/Users/syverjohansen/ski/elo/python/ski/radar/men_chrono_regress.xlsx")
print(time.time() - start_time)

