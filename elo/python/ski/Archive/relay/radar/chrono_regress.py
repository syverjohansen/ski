
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
	
	return df2

 
def ts(df):
	points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	ts_points = [x * 2 for x in points]

	#print(len(ts_points))
	df = df.loc[df['level']=="all"]
	df = df.loc[df['distance']=="Ts"]

	team_pelo = list(df['pelo'])
	team_distance_pelo = list(df['distance_pelo'])
	team_distance_classic_pelo = list(df['distance_classic_pelo'])
	team_distance_freestyle_pelo = list(df['distance_freestyle_pelo'])
	team_sprint_pelo = list(df['sprint_pelo'])
	team_sprint_classic_pelo = list(df['sprint_classic_pelo'])
	team_sprint_freestyle_pelo = list(df['sprint_freestyle_pelo'])
	team_classic_pelo = list(df['classic_pelo'])
	team_freestyle_pelo = list(df['freestyle_pelo'])



	team_elo = list(df['elo'])
	team_distance_elo = list(df['distance_elo'])
	team_distance_classic_elo = list(df['distance_classic_elo'])
	team_distance_freestyle_elo = list(df['distance_freestyle_elo'])
	team_sprint_elo = list(df['sprint_elo'])
	team_sprint_classic_elo = list(df['sprint_classic_elo'])
	team_sprint_freestyle_elo = list(df['sprint_freestyle_elo'])
	team_classic_elo = list(df['classic_elo'])
	team_freestyle_elo = list(df['freestyle_elo'])

	for a in range(0,len(df['pelo']),2):
		for b in range(2):
			team_pelo[a+b] = df['pelo'].iloc[a]+df['pelo'].iloc[a+1]
			team_distance_pelo[a+b] = df['distance_pelo'].iloc[a]+df['distance_pelo'].iloc[a+1]
			team_distance_classic_pelo[a+b] = df['distance_classic_pelo'].iloc[a]+df['distance_classic_pelo'].iloc[a+1]
			team_distance_freestyle_pelo[a+b] = df['distance_freestyle_pelo'].iloc[a]+df['distance_freestyle_pelo'].iloc[a+1]
			team_sprint_pelo[a+b] = df['sprint_pelo'].iloc[a]+df['sprint_pelo'].iloc[a+1]
			team_sprint_classic_pelo[a+b] = df['sprint_classic_pelo'].iloc[a]+df['sprint_classic_pelo'].iloc[a+1]
			team_sprint_freestyle_pelo[a+b] = df['sprint_freestyle_pelo'].iloc[a]+df['sprint_freestyle_pelo'].iloc[a+1]
			team_classic_pelo[a+b] = df['classic_pelo'].iloc[a]+df['classic_pelo'].iloc[a+1]
			team_freestyle_pelo[a+b] = df['freestyle_pelo'].iloc[a]+df['freestyle_pelo'].iloc[a+1]
	for a in range(0,len(df['elo']),2):
		for b in range(2):
			team_elo[a+b] = df['elo'].iloc[a]+df['elo'].iloc[a+1]
			team_distance_elo[a+b] = df['distance_elo'].iloc[a]+df['distance_elo'].iloc[a+1]
			team_distance_classic_elo[a+b] = df['distance_classic_elo'].iloc[a]+df['distance_classic_elo'].iloc[a+1]
			team_distance_freestyle_elo[a+b] = df['distance_freestyle_elo'].iloc[a]+df['distance_freestyle_elo'].iloc[a+1]
			team_sprint_elo[a+b] = df['sprint_elo'].iloc[a]+df['sprint_elo'].iloc[a+1]
			team_sprint_classic_elo[a+b] = df['sprint_classic_elo'].iloc[a]+df['sprint_classic_elo'].iloc[a+1]
			team_sprint_freestyle_elo[a+b] = df['sprint_freestyle_elo'].iloc[a]+df['sprint_freestyle_elo'].iloc[a+1]
			team_classic_elo[a+b] = df['classic_elo'].iloc[a]+df['classic_elo'].iloc[a+1]
			team_freestyle_elo[a+b] = df['freestyle_elo'].iloc[a]+df['freestyle_elo'].iloc[a+1]
	df['team_pelo'] = team_pelo
	df['team_distance_pelo'] = team_distance_pelo
	df['team_distance_classic_pelo'] = team_distance_classic_pelo
	df['team_distance_freestyle_pelo'] = team_distance_freestyle_pelo
	df['team_sprint_pelo'] = team_sprint_pelo
	df['team_sprint_classic_pelo'] = team_sprint_classic_pelo
	df['team_sprint_freestyle_pelo'] = team_sprint_freestyle_pelo
	df['team_classic_pelo'] = team_classic_pelo
	df['team_freestyle_pelo'] = team_freestyle_pelo


	df['team_elo'] = team_elo
	df['team_distance_elo'] = team_distance_elo
	df['team_distance_classic_elo'] = team_distance_classic_elo
	df['team_distance_freestyle_elo'] = team_distance_freestyle_elo
	df['team_sprint_elo'] = team_sprint_elo
	df['team_sprint_classic_elo'] = team_sprint_classic_elo
	df['team_sprint_freestyle_elo'] = team_sprint_freestyle_elo
	df['team_classic_elo'] = team_classic_elo
	df['team_freestyle_elo'] = team_freestyle_elo



	
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
				racedf['pelo'] = racedf['team_pelo'].apply(lambda x: 100*(x/max_pelo))
				racedf['distance_pelo'] = racedf['team_distance_pelo'].apply(lambda x: 100*(x/max(racedf['team_distance_pelo'])))
				racedf['distance_classic_pelo'] = racedf['team_distance_classic_pelo'].apply(lambda x: 100*(x/max(racedf['team_distance_classic_pelo'])))
				racedf['distance_freestyle_pelo'] = racedf['team_distance_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['team_distance_freestyle_pelo'])))
				racedf['sprint_pelo'] = racedf['team_sprint_pelo'].apply(lambda x: 100*(x/max(racedf['team_sprint_pelo'])))
				racedf['sprint_classic_pelo'] = racedf['team_sprint_classic_pelo'].apply(lambda x: 100*(x/max(racedf['team_sprint_classic_pelo'])))
				racedf['sprint_freestyle_pelo'] = racedf['team_sprint_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['team_sprint_freestyle_pelo'])))

				racedf['classic_pelo'] = racedf['team_classic_pelo'].apply(lambda x: 100*(x/max(racedf['team_classic_pelo'])))
				racedf['freestyle_pelo'] = racedf['team_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['team_freestyle_pelo'])))
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
			
			if(season==9999999 and race==999999999):
				continue
			else:
				racedf = seasondf.loc[seasondf['race']==races[race]]

				team_pelo = list(racedf['pelo'])
				team_distance_pelo = list(racedf['distance_pelo'])
				team_distance_classic_pelo = list(racedf['distance_classic_pelo'])
				team_distance_freestyle_pelo = list(racedf['distance_freestyle_pelo'])
				team_sprint_pelo = list(racedf['sprint_pelo'])
				team_sprint_classic_pelo = list(racedf['sprint_classic_pelo'])
				team_sprint_freestyle_pelo = list(racedf['sprint_freestyle_pelo'])
				team_classic_pelo = list(racedf['classic_pelo'])
				team_freestyle_pelo = list(racedf['freestyle_pelo'])



				team_elo = list(racedf['elo'])
				team_distance_elo = list(racedf['distance_elo'])
				team_distance_classic_elo = list(racedf['distance_classic_elo'])
				team_distance_freestyle_elo = list(racedf['distance_freestyle_elo'])
				team_sprint_elo = list(racedf['sprint_elo'])
				team_sprint_classic_elo = list(racedf['sprint_classic_elo'])
				team_sprint_freestyle_elo = list(racedf['sprint_freestyle_elo'])
				team_classic_elo = list(racedf['classic_elo'])
				team_freestyle_elo = list(racedf['freestyle_elo'])
				team_elo = list(racedf['elo'])
				#print(len(team_elo))
				#print(len(racedf['pelo']))
				try:
					for a in range(0,len(racedf['pelo']),4):
						for b in range(4):
							team_pelo[a+b] = racedf['pelo'].iloc[a]+racedf['pelo'].iloc[a+1]+racedf['pelo'].iloc[a+2]+racedf['pelo'].iloc[a+3]

							team_distance_pelo[a+b] = racedf['distance_pelo'].iloc[a]+racedf['distance_pelo'].iloc[a+1]+racedf['distance_pelo'].iloc[a+2]+racedf['distance_pelo'].iloc[a+3]
							team_distance_classic_pelo[a+b] = racedf['distance_classic_pelo'].iloc[a]+racedf['distance_classic_pelo'].iloc[a+1]+racedf['distance_classic_pelo'].iloc[a+2]+racedf['distance_classic_pelo'].iloc[a+3]
							team_distance_freestyle_pelo[a+b] = racedf['distance_freestyle_pelo'].iloc[a]+racedf['distance_freestyle_pelo'].iloc[a+1]+racedf['distance_freestyle_pelo'].iloc[a+2]+racedf['distance_freestyle_pelo'].iloc[a+3]
							team_sprint_pelo[a+b] = racedf['sprint_pelo'].iloc[a]+racedf['sprint_pelo'].iloc[a+1]+racedf['sprint_pelo'].iloc[a+2]+racedf['sprint_pelo'].iloc[a+3]
							team_sprint_classic_pelo[a+b] = racedf['sprint_classic_pelo'].iloc[a]+racedf['sprint_classic_pelo'].iloc[a+1]+racedf['sprint_classic_pelo'].iloc[a+2]+racedf['sprint_classic_pelo'].iloc[a+3]
							team_sprint_freestyle_pelo[a+b] = racedf['sprint_freestyle_pelo'].iloc[a]+racedf['sprint_freestyle_pelo'].iloc[a+1]+racedf['sprint_freestyle_pelo'].iloc[a+2]+racedf['sprint_freestyle_pelo'].iloc[a+3]
							team_classic_pelo[a+b] = racedf['classic_pelo'].iloc[a]+racedf['classic_pelo'].iloc[a+1]+racedf['classic_pelo'].iloc[a+2]+racedf['classic_pelo'].iloc[a+3]
							team_freestyle_pelo[a+b] = racedf['freestyle_pelo'].iloc[a]+racedf['freestyle_pelo'].iloc[a+1]+racedf['freestyle_pelo'].iloc[a+2]+racedf['freestyle_pelo'].iloc[a+3]
					for a in range(0,len(racedf['elo']),4):
						for b in range(4):
							team_elo[a+b] = racedf['elo'].iloc[a]+racedf['elo'].iloc[a+1]+racedf['elo'].iloc[a+2]+racedf['elo'].iloc[a+3]
							team_distance_elo[a+b] = racedf['distance_elo'].iloc[a]+racedf['distance_elo'].iloc[a+1]+racedf['distance_elo'].iloc[a+2]+racedf['distance_elo'].iloc[a+3]
							team_distance_classic_elo[a+b] = racedf['distance_classic_elo'].iloc[a]+racedf['distance_classic_elo'].iloc[a+1]+racedf['distance_classic_elo'].iloc[a+2]+racedf['distance_classic_elo'].iloc[a+3]
							team_distance_freestyle_elo[a+b] = racedf['distance_freestyle_elo'].iloc[a]+racedf['distance_freestyle_elo'].iloc[a+1]+racedf['distance_freestyle_elo'].iloc[a+2]+racedf['distance_freestyle_elo'].iloc[a+3]
							team_sprint_elo[a+b] = racedf['sprint_elo'].iloc[a]+racedf['sprint_elo'].iloc[a+1]+racedf['sprint_elo'].iloc[a+2]+racedf['sprint_elo'].iloc[a+3]
							team_sprint_classic_elo[a+b] = racedf['sprint_classic_elo'].iloc[a]+racedf['sprint_classic_elo'].iloc[a+1]+racedf['sprint_classic_elo'].iloc[a+2]+racedf['sprint_classic_elo'].iloc[a+3]
							team_sprint_freestyle_elo[a+b] = racedf['sprint_freestyle_elo'].iloc[a]+racedf['sprint_freestyle_elo'].iloc[a+1]+racedf['sprint_freestyle_elo'].iloc[a+2]+racedf['sprint_freestyle_elo'].iloc[a+3]
							team_classic_elo[a+b] = racedf['classic_elo'].iloc[a]+racedf['classic_elo'].iloc[a+1]+racedf['classic_elo'].iloc[a+2]+racedf['classic_elo'].iloc[a+3]
							team_freestyle_elo[a+b] = racedf['freestyle_elo'].iloc[a]+racedf['freestyle_elo'].iloc[a+1]+racedf['freestyle_elo'].iloc[a+2]+racedf['freestyle_elo'].iloc[a+3]
				except:
					for a in range(0,len(racedf['pelo']),3):
						for b in range(3):
							team_pelo[a+b] = racedf['pelo'].iloc[a]+racedf['pelo'].iloc[a+1]+racedf['pelo'].iloc[a+2]
							team_distance_pelo[a+b] = racedf['distance_pelo'].iloc[a]+racedf['distance_pelo'].iloc[a+1]+racedf['distance_pelo'].iloc[a+2]
							team_distance_classic_pelo[a+b] = racedf['distance_classic_pelo'].iloc[a]+racedf['distance_classic_pelo'].iloc[a+1]+racedf['distance_classic_pelo'].iloc[a+2]
							team_distance_freestyle_pelo[a+b] = racedf['distance_freestyle_pelo'].iloc[a]+racedf['distance_freestyle_pelo'].iloc[a+1]+racedf['distance_freestyle_pelo'].iloc[a+2]
							team_sprint_pelo[a+b] = racedf['sprint_pelo'].iloc[a]+racedf['sprint_pelo'].iloc[a+1]+racedf['sprint_pelo'].iloc[a+2]
							team_sprint_classic_pelo[a+b] = racedf['sprint_classic_pelo'].iloc[a]+racedf['sprint_classic_pelo'].iloc[a+1]+racedf['sprint_classic_pelo'].iloc[a+2]
							team_sprint_freestyle_pelo[a+b] = racedf['sprint_freestyle_pelo'].iloc[a]+racedf['sprint_freestyle_pelo'].iloc[a+1]+racedf['sprint_freestyle_pelo'].iloc[a+2]
							team_classic_pelo[a+b] = racedf['classic_pelo'].iloc[a]+racedf['classic_pelo'].iloc[a+1]+racedf['classic_pelo'].iloc[a+2]
							team_freestyle_pelo[a+b] = racedf['freestyle_pelo'].iloc[a]+racedf['freestyle_pelo'].iloc[a+1]+racedf['freestyle_pelo'].iloc[a+2]
					for a in range(0,len(racedf['elo']),3):
						for b in range(3):
							team_elo[a+b] = racedf['elo'].iloc[a]+racedf['elo'].iloc[a+1]+racedf['elo'].iloc[a+2]
							team_distance_elo[a+b] = racedf['distance_elo'].iloc[a]+racedf['distance_elo'].iloc[a+1]+racedf['distance_elo'].iloc[a+2]
							team_distance_classic_elo[a+b] = racedf['distance_classic_elo'].iloc[a]+racedf['distance_classic_elo'].iloc[a+1]+racedf['distance_classic_elo'].iloc[a+2]
							team_distance_freestyle_elo[a+b] = racedf['distance_freestyle_elo'].iloc[a]+racedf['distance_freestyle_elo'].iloc[a+1]+racedf['distance_freestyle_elo'].iloc[a+2]
							team_sprint_elo[a+b] = racedf['sprint_elo'].iloc[a]+racedf['sprint_elo'].iloc[a+1]+racedf['sprint_elo'].iloc[a+2]
							team_sprint_classic_elo[a+b] = racedf['sprint_classic_elo'].iloc[a]+racedf['sprint_classic_elo'].iloc[a+1]+racedf['sprint_classic_elo'].iloc[a+2]
							team_sprint_freestyle_elo[a+b] = racedf['sprint_freestyle_elo'].iloc[a]+racedf['sprint_freestyle_elo'].iloc[a+1]+racedf['sprint_freestyle_elo'].iloc[a+2]
							team_classic_elo[a+b] = racedf['classic_elo'].iloc[a]+racedf['classic_elo'].iloc[a+1]+racedf['classic_elo'].iloc[a+2]
							team_freestyle_elo[a+b] = racedf['freestyle_elo'].iloc[a]+racedf['freestyle_elo'].iloc[a+1]+racedf['freestyle_elo'].iloc[a+2]
				#print(racedf)
				racedf['team_pelo'] = team_pelo
				racedf['team_distance_pelo'] = team_distance_pelo
				racedf['team_distance_classic_pelo'] = team_distance_classic_pelo
				racedf['team_distance_freestyle_pelo'] = team_distance_freestyle_pelo

				racedf['team_sprint_pelo'] = team_sprint_pelo
				racedf['team_sprint_classic_pelo'] = team_sprint_classic_pelo
				racedf['team_sprint_freestyle_pelo'] = team_sprint_freestyle_pelo
				racedf['team_classic_pelo'] = team_classic_pelo
				racedf['team_freestyle_pelo'] = team_freestyle_pelo


				racedf['team_elo'] = team_elo
				racedf['team_distance_elo'] = team_distance_elo
				racedf['team_distance_classic_elo'] = team_distance_classic_elo
				racedf['team_distance_freestyle_elo'] = team_distance_freestyle_elo
				racedf['team_sprint_elo'] = team_sprint_elo
				racedf['team_sprint_classic_elo'] = team_sprint_classic_elo
				racedf['team_sprint_freestyle_elo'] = team_sprint_freestyle_elo
				racedf['team_classic_elo'] = team_classic_elo
				racedf['team_freestyle_elo'] = team_freestyle_elo
				racedf = racedf.iloc[::4]

				if(len(racedf['place'])>30):
					#print(len(racedf['place']))
					#print([0]*(len(racedf['place'])-len(points)))
					points_list = points_list + ([0]*(len(racedf['place'])-len(rel_points)))
					#print(points_list)
				else:
					points_list = rel_points[0:len(racedf['place'])]
					#print(points_list)
				#max_pelo = max(racedf['team_pelo'])
				#print(max(racedf['place']))
				max_place = max(racedf['place'])
				racedf['pelo'] = racedf['team_pelo'].apply(lambda x: 100*(x/max(racedf['team_pelo'])))
				racedf['distance_pelo'] = racedf['team_distance_pelo'].apply(lambda x: 100*(x/max(racedf['team_distance_pelo'])))
				racedf['distance_classic_pelo'] = racedf['team_distance_classic_pelo'].apply(lambda x: 100*(x/max(racedf['team_distance_classic_pelo'])))
				racedf['distance_freestyle_pelo'] = racedf['team_distance_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['team_distance_freestyle_pelo'])))
				racedf['sprint_pelo'] = racedf['team_sprint_pelo'].apply(lambda x: 100*(x/max(racedf['team_sprint_pelo'])))
				racedf['sprint_classic_pelo'] = racedf['team_sprint_classic_pelo'].apply(lambda x: 100*(x/max(racedf['team_sprint_classic_pelo'])))
				racedf['sprint_freestyle_pelo'] = racedf['team_sprint_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['team_sprint_freestyle_pelo'])))

				racedf['classic_pelo'] = racedf['team_classic_pelo'].apply(lambda x: 100*(x/max(racedf['team_classic_pelo'])))
				racedf['freestyle_pelo'] = racedf['team_freestyle_pelo'].apply(lambda x: 100*(x/max(racedf['team_freestyle_pelo'])))
				racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
				racedf['points'] = points_list
				


				df2 = df2.append(racedf)
	
	return df2


df = pd.read_pickle('~/ski/elo/python/ski/relay/radar/men_chrono.pkl')

df = distance(df, "Ts")
df = discipline(df, "C")

#df = pct(df)
df = ts(df)
#df = rel(df)

df.to_pickle("/Users/syverjohansen/ski/elo/python/ski/relay/radar/men_chrono_regress.pkl")
df.to_excel("/Users/syverjohansen/ski/elo/python/ski/relay/radar/men_chrono_regress.xlsx")
print(time.time() - start_time)

