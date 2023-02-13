
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
	points = [100,95,90,85,80,75,72,69,66,63,60,58,56,54,52,50,48,46,44,42,40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	#points = [50, 47, 44, 41, 38, 35, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	#points = [300, 285, 270, 255, 240, 216, 207, 198, 189, 180, 174, 168, 162, 156, 150, 144, 138, 132, 126, 120, 114, 108, 102, 96, 90, 84, 78, 72, 66, 60, 57, 54, 51, 48, 45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3]
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
				if(len(racedf['place'])>50):
					#print(len(racedf['place']))
					#print([0]*(len(racedf['place'])-len(points)))
					points_list = points_list + ([0]*(len(racedf['place'])-len(points)))
				else:
					points_list = points[0:len(racedf['place'])]
				max_pelo = max(racedf['pelo'])
				max_elo = max(racedf['elo'])

				#print(max(racedf['place']))
				max_place = max(racedf['place'])
				racedf['total_pelo'] = racedf['pelo']
				racedf['total_elo'] = racedf['elo']
				racedf['pelo'] = racedf['pelo'].apply(lambda x: 100*(x/max_pelo))
				racedf['distance_pelo'] = racedf['distance_pelo'].apply(lambda x: 100*(x/max(racedf['distance_pelo'])))
				racedf['distance_classic_pelo'] = racedf['distance_classic_pelo'].apply(lambda x: 100*(x/max(racedf['distance_classic_pelo'])))
				racedf['distance_freestyle_pelo'] = racedf['distance_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['distance_freestyle_pelo'])))
				racedf['sprint_pelo'] = racedf['sprint_pelo'].apply(lambda x: 100*(x/max(racedf['sprint_pelo'])))
				racedf['sprint_classic_pelo'] = racedf['sprint_classic_pelo'].apply(lambda x: 100*(x/max(racedf['sprint_classic_pelo'])))
				racedf['sprint_freestyle_pelo'] = racedf['sprint_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['sprint_freestyle_pelo'])))

				racedf['classic_pelo'] = racedf['classic_pelo'].apply(lambda x: 100*(x/max(racedf['classic_pelo'])))
				racedf['freestyle_pelo'] = racedf['freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['freestyle_pelo'])))
				
				racedf['elo'] = racedf['elo'].apply(lambda x: 100*(x/max_elo))
				racedf['distance_elo'] = racedf['distance_elo'].apply(lambda x: 100*(x/max(racedf['distance_elo'])))
				racedf['distance_classic_elo'] = racedf['distance_classic_elo'].apply(lambda x: 100*(x/max(racedf['distance_classic_elo'])))
				racedf['distance_freestyle_elo'] = racedf['distance_freestyle_elo'].apply(lambda x: 100*(x/max(racedf['distance_freestyle_elo'])))
				racedf['sprint_elo'] = racedf['sprint_elo'].apply(lambda x: 100*(x/max(racedf['sprint_elo'])))
				racedf['sprint_classic_elo'] = racedf['sprint_classic_elo'].apply(lambda x: 100*(x/max(racedf['sprint_classic_elo'])))
				racedf['sprint_freestyle_elo'] = racedf['sprint_freestyle_elo'].apply(lambda x: 100*(x/max(racedf['sprint_freestyle_elo'])))

				racedf['classic_elo'] = racedf['classic_elo'].apply(lambda x: 100*(x/max(racedf['classic_elo'])))
				racedf['freestyle_elo'] = racedf['freestyle_elo'].apply(lambda x: 100*(x/max(racedf['freestyle_elo'])))


				racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
				racedf['points'] = points_list
				


				df2 = df2.append(racedf)
	df2['home'] = (df2['nation']==df2['country'])
	return df2

 
def pts_avg(df):
	df2 = pd.DataFrame()
	ids = list(df.id.unique())
	
	df['pavg_points'] = 0
	df['avg_points'] = 0

	for a in range(len(ids)):
		dfid = df.loc[df['id']==ids[a]]
		dfid['race_num'] = list(range(1,len(dfid['id'])+1))
		#dfid['race_num'] = pd.to_numeric(dfid['race_num'])
		dfid['total_points'] = dfid['points'].cumsum()
		#dfid['total_points'] = pd.to_numeric(dfid['total_points'])
		
		dfid['avg_points'] = dfid['total_points']/dfid['race_num']
		
		dfid['pavg_points'] = 0
		dfid['pavg_points'][1:len(dfid['pavg_points'])] = dfid['avg_points'][:len(dfid['avg_points'])-1]


		#dfid['avg_points'] = dfid.apply(lambda x: x['total_points']/x['race_num'])
		df2 = df2.append(dfid)
	return df2		
	'''for a in range(len(ids)):
		df.loc[df.id==ids[a], "race_num"] = range(1,1+len(df.loc[df.id==ids[a], 'id']))
		df.loc[df.id==ids[a], "total_points"] = df.loc[df.id==ids[a], "points"].cumsum()
		df.loc[df.id==ids[a], "avg_points"] = df.loc[df.id==ids[a], "total_points"]/df.loc[df.id==ids[a], "race_num"]
		#print(df.loc[df.id==ids[a], "avg_points"][:len(df.loc[df.id==ids[a], "avg_points"])-1])
		df.loc[df.id==ids[a], "pavg_points"][1:len(df.loc[df.id==ids[a], 'avg_points'])] = df.loc[df.id==ids[a], "avg_points"][:len(df.loc[df.id==ids[a], "avg_points"])-1]
		if(len(df.loc[df.id==ids[a], "pavg_points"][1:len(df.loc[df.id==ids[a], 'avg_points'])])>0):
			#print((df.loc[df.id==ids[a], "pavg_points"][1:len(df.loc[df.id==ids[a], 'pavg_points'])]))
			df.loc[df.id==ids[a], "pavg_points"][1:len(df.loc[df.id==ids[a], 'avg_points'])] = df.loc[df.id==ids[a], "avg_points"][:len(df.loc[df.id==ids[a], "avg_points"])-1]
		

		#print(df.loc[df.id==ids[a], "pavg_points"])

	return df'''
		



df = pd.read_pickle('~/ski/elo/python/ski/age/excel365/ladies_chrono.pkl')

#df = city(df, ["Tour de Ski"])

df = distance(df, "Distance")
df = discipline(df, "F")

df = pct(df)

df = pts_avg(df)

df.to_pickle("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress_distance_freestyle.pkl")
df.to_excel("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress_distance_freestyle.xlsx")
print(time.time() - start_time)

