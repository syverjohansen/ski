
import pandas as pd
import time
pd.options.mode.chained_assignment = None
start_time = time.time()



def pct(df):
	points = [60, 54, 48, 43, 40, 38, 36, 34, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 
	13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
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
				if(len(racedf['place'])>40):
					#print(len(racedf['place']))
					#print([0]*(len(racedf['place'])-len(points)))
					points_list = points_list + ([0]*(len(racedf['place'])-len(points)))
				else:
					points_list = points[0:len(racedf['place'])]
				max_pelo = max(racedf['pelo'])
				#print(max(racedf['place']))
				max_place = max(racedf['place'])
				racedf['pelopct'] = racedf['pelo'].apply(lambda x: 100*(x/max_pelo))
				racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
				racedf['points'] = points_list
				


				df2 = df2.append(racedf)
	return df2

 


filename = "varmen_all"
ladiesdf = pd.read_pickle('~/ski/elo/python/biathlon/excel365/'+str(filename)+'.pkl')
ladiesdf = pct(ladiesdf)
ladiesdf.to_pickle("/Users/syverjohansen/ski/elo/python/biathlon/radar/"+str(filename)+'.pkl')
ladiesdf.to_excel("/Users/syverjohansen/ski/elo/python/biathlon/radar/"+str(filename)+'.xlsx')
print(time.time() - start_time)

