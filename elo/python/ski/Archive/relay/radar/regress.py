
import pandas as pd
import time
pd.options.mode.chained_assignment = None
start_time = time.time()



def pct(df):
	points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	relay_points = [x * 2 for x in points]
	#print(relay_points)
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
			points_list = relay_points
			
			if(season==0 and race==0):
				continue
			else:
				racedf = seasondf.loc[seasondf['race']==races[race]]
				if(len(racedf['place'])>30):
					#print(len(racedf['place']))
					#print([0]*(len(racedf['place'])-len(points)))
					points_list = points_list + ([0]*(len(racedf['place'])-len(relay_points)))
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

def ts(df):
	points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	ts_points = [x * 2 for x in points]

	#print(len(ts_points))
	df = df.loc[df['level']=="all"]
	df = df.loc[df['distance']=="Ts"]
	team_pelo = list(df['pelo'])
	team_elo = list(df['elo'])
	for a in range(0,len(df['pelo']),2):
		for b in range(2):
			team_pelo[a+b] = df['pelo'].iloc[a]+df['pelo'].iloc[a+1]
	for a in range(0,len(df['elo']),2):
		for b in range(2):
			team_elo[a+b] = df['elo'].iloc[a]+df['elo'].iloc[a+1]
	df['team_pelo'] = team_pelo
	df['team_elo'] = team_elo
	df = df.iloc[::2]
	#print(df)
	df2 = pd.DataFrame()
	seasons = pd.unique(df['season'])
	for season in range(len(seasons)):
		seasondf = df.loc[df['season']==seasons[season]]
		#print(seasondf)
		#print(seasondf)
		races = pd.unique(seasondf['race'])
		#print(races)
		for race in range(len(races)):
			points_list = ts_points
			#print(points_list)
			
			if(season==0 and race==0):
				continue
			else:
				racedf = seasondf.loc[seasondf['race']==races[race]]
				if(len(racedf['place'])>30):
					#print(len(racedf['place']))
					#print([0]*(len(racedf['place'])-len(points)))
					points_list = points_list + ([0]*(len(racedf['place'])-len(ts_points)))
					#print(points_list)
				else:
					points_list = ts_points[0:len(racedf['place'])]
					#print(points_list)
				max_pelo = max(racedf['team_pelo'])
				#print(max(racedf['place']))
				max_place = max(racedf['place'])
				racedf['pelopct'] = racedf['team_pelo'].apply(lambda x: 100*(x/max_pelo))
				racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
				racedf['points'] = points_list
				


				df2 = df2.append(racedf)
	
	return df2

def rel(df):
	points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	rel_points = [x * 2 for x in points]

	#print(len(ts_points))
	df = df.loc[df['level']=="all"]
	df = df.loc[df['distance']=="Rel"]

	#print(df)
	df2 = pd.DataFrame()
	seasons = pd.unique(df['season'])
	for season in range(len(seasons)):
		seasondf = df.loc[df['season']==seasons[season]]
		#print(seasondf)
		#print(seasondf)
		races = pd.unique(seasondf['race'])
		#print(races)
		for race in range(len(races)):
			points_list = rel_points
			#print(points_list)
			
			if(season==0 and race==0):
				continue
			else:
				racedf = seasondf.loc[seasondf['race']==races[race]]

				team_pelo = list(racedf['pelo'])
				team_elo = list(racedf['elo'])
				#print(len(team_elo))
				#print(len(racedf['pelo']))
				try:
					for a in range(0,len(racedf['pelo']),4):
						for b in range(4):
							team_pelo[a+b] = racedf['pelo'].iloc[a]+racedf['pelo'].iloc[a+1]+racedf['pelo'].iloc[a+2]+racedf['pelo'].iloc[a+3]
					for a in range(0,len(racedf['elo']),4):
						for b in range(4):
							team_elo[a+b] = racedf['elo'].iloc[a]+racedf['elo'].iloc[a+1]+racedf['elo'].iloc[a+2]+racedf['elo'].iloc[a+3]
				except:
					for a in range(0,len(racedf['pelo']),3):
						for b in range(3):
							team_pelo[a+b] = racedf['pelo'].iloc[a]+racedf['pelo'].iloc[a+1]+racedf['pelo'].iloc[a+2]
					for a in range(0,len(racedf['elo']),3):
						for b in range(3):
							team_elo[a+b] = racedf['elo'].iloc[a]+racedf['elo'].iloc[a+1]+racedf['elo'].iloc[a+2]
				racedf['team_pelo'] = team_pelo
				racedf['team_elo'] = team_elo
				racedf = racedf.iloc[::4]
				if(len(racedf['place'])>30):
					#print(len(racedf['place']))
					#print([0]*(len(racedf['place'])-len(points)))
					points_list = points_list + ([0]*(len(racedf['place'])-len(rel_points)))
					#print(points_list)
				else:
					points_list = rel_points[0:len(racedf['place'])]
					#print(points_list)
				max_pelo = max(racedf['team_pelo'])
				#print(max(racedf['place']))
				max_place = max(racedf['place'])
				racedf['pelopct'] = racedf['team_pelo'].apply(lambda x: 100*(x/max_pelo))
				racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
				racedf['points'] = points_list
				


				df2 = df2.append(racedf)
	
	return df2


filename = "varladies_sprint_k"
ladiesdf = pd.read_pickle('~/ski/elo/python/ski/relay/excel365/'+str(filename)+'.pkl')
ladiesdf = ts(ladiesdf)
#ladiesdf = rel(ladiesdf)
ladiesdf.to_pickle("/Users/syverjohansen/ski/elo/python/ski/relay/radar/"+str(filename)+'.pkl')
ladiesdf.to_excel("/Users/syverjohansen/ski/elo/python/ski/relay/radar/"+str(filename)+'.xlsx')
print(time.time() - start_time)

