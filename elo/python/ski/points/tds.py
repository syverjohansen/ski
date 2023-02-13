#Looks at points per skier per race

import pandas as pd
import time
pd.options.mode.chained_assignment = None
start_time = time.time()

def get_points(df):#, pkl):
	

	#stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	#wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
	#points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	
	#Set what type of race it is
	points = tour
	df = df.loc[df['level']=="all"]
	df = df.loc[df['city']=="Tour de Ski"]
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
			if(len(racedf['place'])>40):
				#print(len(racedf['place']))
				#print([0]*(len(racedf['place'])-len(points)))
				points_list = points_list + ([5]*(len(racedf['place'])-len(points)))
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



menpkls = ["varmen_all_k",
	"varmen_sprint_k",
	"varmen_distance_k",
	"varmen_sprint_freestyle_k",
	"varmen_sprint_classic_k",
	"varmen_distance_freestyle_k",
	"varmen_distance_classic_k"]
	#"~/ski/elo/python/ski/men/varmen_sprint_classic.pkl"]

ladiespkls = ["varladies_all_k",
	"varladies_sprint_k",
	"varladies_distance_k",
	"varladies_sprint_freestyle_k",
	"varladies_sprint_classic_k",
	"varladies_distance_freestyle_k",
	"varladies_distance_classic_k"]

for a in range(len(menpkls)):
	df = pd.read_pickle('~/ski/elo/python/ski/excel365/'+str(menpkls[a])+'_tds.pkl')
	df = get_points(df)
	df = points(df)
	df.to_pickle("/Users/syverjohansen/ski/elo/python/ski/points/"+str(menpkls[a])+'_tds.pkl')
	df.to_excel("/Users/syverjohansen/ski/elo/python/ski/points/"+str(menpkls[a])+'_tds.xlsx')

for a in range(len(ladiespkls)):
	df = pd.read_pickle('~/ski/elo/python/ski/excel365/'+str(ladiespkls[a])+'_tds.pkl')
	df = get_points(df)
	df = points(df)
	df.to_pickle("/Users/syverjohansen/ski/elo/python/ski/points/"+str(ladiespkls[a])+'_tds.pkl')
	df.to_excel("/Users/syverjohansen/ski/elo/python/ski/points/"+str(ladiespkls[a])+'_tds.xlsx')


men_all = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varmen_all_k_tds.pkl")
men_distance = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varmen_distance_k_tds.pkl")
men_distance_freestyle = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varmen_distance_freestyle_k_tds.pkl")
men_distance_classic = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varmen_distance_classic_k_tds.pkl")
men_sprint = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varmen_sprint_k_tds.pkl")
men_sprint_freestyle = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varmen_sprint_freestyle_k_tds.pkl")
men_sprint_classic = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varmen_sprint_classic_k_tds.pkl")

men = pd.DataFrame(men_all, columns = ["name", "season", "points", "pelopct"])
men = men.rename({"pelopct":"pelopct_all"}, axis='columns')
men['pelopct_distance'] = men_distance['pelopct']
men['pelopct_distance_freestyle'] = men_distance_freestyle['pelopct']
men['pelopct_distance_classic'] = men_distance_classic['pelopct']
men['pelopct_sprint'] = men_sprint['pelopct']
men['pelopct_sprint_freestyle'] = men_sprint_freestyle['pelopct']
men['pelopct_sprint_classic'] = men_sprint_classic['pelopct']

men.to_pickle("/Users/syverjohansen/ski/elo/python/ski/points/men_tds.pkl")
men.to_excel("/Users/syverjohansen/ski/elo/python/ski/points/men_tds.xlsx")

ladies_all = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varladies_all_k_tds.pkl")
ladies_distance = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varladies_distance_k_tds.pkl")
ladies_distance_freestyle = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varladies_distance_freestyle_k_tds.pkl")
ladies_distance_classic = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varladies_distance_classic_k_tds.pkl")
ladies_sprint = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varladies_sprint_k_tds.pkl")
ladies_sprint_freestyle = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varladies_sprint_freestyle_k_tds.pkl")
ladies_sprint_classic = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/points/varladies_sprint_classic_k_tds.pkl")

ladies = pd.DataFrame(ladies_all, columns = ["name", "season", "points", "pelopct"])
ladies = ladies.rename({"pelopct":"pelopct_all"}, axis='columns')
ladies['pelopct_distance'] = ladies_distance['pelopct']
ladies['pelopct_distance_freestyle'] = ladies_distance_freestyle['pelopct']
ladies['pelopct_distance_classic'] = ladies_distance_classic['pelopct']
ladies['pelopct_sprint'] = ladies_sprint['pelopct']
ladies['pelopct_sprint_freestyle'] = ladies_sprint_freestyle['pelopct']
ladies['pelopct_sprint_classic'] = ladies_sprint_classic['pelopct']

ladies.to_pickle("/Users/syverjohansen/ski/elo/python/ski/points/ladies_tds.pkl")
ladies.to_excel("/Users/syverjohansen/ski/elo/python/ski/points/ladies_tds.xlsx")


print(time.time() - start_time)
		




