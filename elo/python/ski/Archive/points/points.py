#Looks at points per skier per race

import pandas as pd
import time
pd.options.mode.chained_assignment = None
start_time = time.time()

def get_points(df):#, pkl):
	

	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]
	#points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	
	#Set what type of race it is
	points = wc
	df = df.loc[df['level']=="all"]
	#df = df.loc[df['season']>=2018]
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
			
			#if(season==0 and race==0):
			#	continue
		#else:
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
			racedf['pelopct'] = racedf['pelo'].apply(lambda x: 100*(x/max_pelo))
			#racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
			racedf['points'] = points_list
				


			df2 = df2.append(racedf)
	
	
	return df2


#Points gives the total and average points for each skier
def points(df):
	df2 = pd.DataFrame()
	ids = list(df.id.unique())
	

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



menpkls = ["~/ski/elo/python/ski/excel365/varmen_sprint_freestyle_k.pkl",
	"~/ski/elo/python/ski/excel365/varmen_distance_freestyle_k.pkl"]
	#"~/ski/elo/python/ski/men/varmen_sprint_classic.pkl"]

ladiespkls = ["~/ski/elo/python/ski/excel365/varladies_sprint_freestyle_k.pkl",
	"~/ski/elo/python/ski/excel365/varladies_distance_freestyle_k.pkl"]

filename = "varmen_sprint_freestyle_k"
df = pd.read_pickle('~/ski/elo/python/ski/excel365/'+str(filename)+'.pkl')
df = get_points(df)
df = points(df)
df.to_pickle("/Users/syverjohansen/ski/elo/python/ski/points/"+str(filename)+'.pkl')
df.to_excel("/Users/syverjohansen/ski/elo/python/ski/points/"+str(filename)+'.xlsx')
print(time.time() - start_time)
		




