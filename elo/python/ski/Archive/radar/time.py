import pandas as pd
import time
import numpy as np
from functools import reduce
pd.options.mode.chained_assignment = None
start_time = time.time()

def ladies():
	#new df will have all dates from season with all people from that season on those dates
	#Will need unique names from seasondf and unique dates from season df.
	#Make list of unique dates that is len(unique_dates)*len(unique_names)
	#Dates sequence is date1name1, date1name2...date1 namen

	chrono_df =pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/ladies_chrono.pkl")
	timedf = pd.DataFrame()
	seasons = pd.unique(chrono_df['season'])
	#for season in range(10):
	for season in range(len(seasons)):
		print(seasons[season])
		seasondf = chrono_df.loc[chrono_df['season']==seasons[season]]

		seasondf = seasondf.reset_index()
		yeardf = pd.DataFrame()
		races = pd.unique(seasondf['race'])
		racers = pd.unique(seasondf['id'])
		
		yeardf['season'] = [seasons[season]]*len(racers)*len(races)
		yeardf['race'] = np.repeat(races, len(racers))
		yeardf['id'] = list(racers)*len(races)
		yeardf['date'] = [None]*len(yeardf['id'])
		dates = []
		year_elos = []
		for race in range(len(races)):
			date = chrono_df.loc[(chrono_df['race']==races[race]) & (chrono_df['season']==seasons[season])]['date'].iloc[0]
			date_ind = yeardf.index[(yeardf['race']==races[race]) & (yeardf['season']==seasons[season])].tolist()
			'''racedf = seasondf.loc[seasondf['race']==races[race]]
			racedf = racedf.reset_index()
			
			year_racedf = yeardf.loc[(yeardf['race']==races[race]) & (yeardf['season']==seasons[season])]
			year_racedf['elo'] = [None]*len(year_racedf['id']) 
			race_ids = list(yeardf.loc[(yeardf['race']==races[race]) & (yeardf['season']==seasons[season])]['id'])
			
			#print(season, race, race_ids)
			race_elos = []
			for a in range(len(race_ids)):
				
				racer_ind = year_racedf.index[year_racedf['id']==race_ids[a]].to_list()
				try:
					racer_elo = racedf.loc[racedf['id']==race_ids[a]]['elo'].iloc[0]
				except:
					racer_elo = None
				
				race_elos.append(racer_elo)

			year_elos = year_elos + race_elos'''

			yeardf.loc[date_ind, 'date'] = date
		#yeardf['elo'] = year_elos

		timedf = timedf.append(yeardf)

		
	racers = pd.unique(timedf['id'])

	timedf['name'] = [None]*len(timedf['id'])
	timedf['nation'] = [None]*len(timedf['id'])
	timedf = timedf.reset_index()

	
	for a in range(len(racers)):
		name_ind = timedf.index[timedf['id'] == racers[a]].tolist()
		nation_ind = timedf.index[timedf['id'] == racers[a]].tolist()

		name = chrono_df.loc[chrono_df['id']==racers[a]]['name'].iloc[0]
		nation = chrono_df.loc[chrono_df['id']==racers[a]]['nation'].iloc[0]

		timedf.loc[name_ind, 'name'] = name
		timedf.loc[name_ind, 'nation'] = nation

	timedf = timedf.merge(chrono_df, on=['season', 'race', 'date', 'id',  'name', 'nation'], how='left')
	print("Done merging ladies")
	unique_ids = pd.unique(timedf['id'])
	newdf = pd.DataFrame()

	for a in range(len(unique_ids)):
		if((len(unique_ids)-a)%100==0):
			print(len(unique_ids)-a)
		skierdf = timedf.loc[timedf['id']==unique_ids[a]]
		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['distance_elo'] = skierdf['distance_elo'].ffill()
		skierdf['distance_classic_elo'] = skierdf['distance_classic_elo'].ffill()
		skierdf['distance_freestyle_elo'] = skierdf['distance_freestyle_elo'].ffill()
		skierdf['sprint_elo'] = skierdf['sprint_elo'].ffill()
		skierdf['sprint_classic_elo'] = skierdf['sprint_classic_elo'].ffill()
		skierdf['sprint_freestyle_elo'] = skierdf['sprint_freestyle_elo'].ffill()
		skierdf['classic_elo'] = skierdf['classic_elo'].ffill()
		skierdf['freestyle_elo'] = skierdf['freestyle_elo'].ffill()
		
		

		newdf = newdf.append(skierdf)
	
	newdf = newdf.sort_index()

	####Maybe try a merge for the elos.  Everything else is ok.
	print(newdf)
	#return timedf
	return newdf


def men():
	#new df will have all dates from season with all people from that season on those dates
	#Will need unique names from seasondf and unique dates from season df.
	#Make list of unique dates that is len(unique_dates)*len(unique_names)
	#Dates sequence is date1name1, date1name2...date1 namen

	chrono_df =pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/men_chrono.pkl")
	timedf = pd.DataFrame()
	seasons = pd.unique(chrono_df['season'])
	#for season in range(10):
	for season in range(len(seasons)):
		print(seasons[season])
		seasondf = chrono_df.loc[chrono_df['season']==seasons[season]]

		seasondf = seasondf.reset_index()
		yeardf = pd.DataFrame()
		races = pd.unique(seasondf['race'])
		racers = pd.unique(seasondf['id'])
		
		yeardf['season'] = [seasons[season]]*len(racers)*len(races)
		yeardf['race'] = np.repeat(races, len(racers))
		yeardf['id'] = list(racers)*len(races)
		yeardf['date'] = [None]*len(yeardf['id'])
		dates = []
		year_elos = []
		for race in range(len(races)):
			date = chrono_df.loc[(chrono_df['race']==races[race]) & (chrono_df['season']==seasons[season])]['date'].iloc[0]
			date_ind = yeardf.index[(yeardf['race']==races[race]) & (yeardf['season']==seasons[season])].tolist()
			'''racedf = seasondf.loc[seasondf['race']==races[race]]
			racedf = racedf.reset_index()
			
			year_racedf = yeardf.loc[(yeardf['race']==races[race]) & (yeardf['season']==seasons[season])]
			year_racedf['elo'] = [None]*len(year_racedf['id']) 
			race_ids = list(yeardf.loc[(yeardf['race']==races[race]) & (yeardf['season']==seasons[season])]['id'])
			
			#print(season, race, race_ids)
			race_elos = []
			for a in range(len(race_ids)):
				
				racer_ind = year_racedf.index[year_racedf['id']==race_ids[a]].to_list()
				try:
					racer_elo = racedf.loc[racedf['id']==race_ids[a]]['elo'].iloc[0]
				except:
					racer_elo = None
				
				race_elos.append(racer_elo)

			year_elos = year_elos + race_elos'''

			yeardf.loc[date_ind, 'date'] = date
		#yeardf['elo'] = year_elos

		timedf = timedf.append(yeardf)

		
	racers = pd.unique(timedf['id'])

	timedf['name'] = [None]*len(timedf['id'])
	timedf['nation'] = [None]*len(timedf['id'])
	timedf = timedf.reset_index()

	
	for a in range(len(racers)):
		name_ind = timedf.index[timedf['id'] == racers[a]].tolist()
		nation_ind = timedf.index[timedf['id'] == racers[a]].tolist()

		name = chrono_df.loc[chrono_df['id']==racers[a]]['name'].iloc[0]
		nation = chrono_df.loc[chrono_df['id']==racers[a]]['nation'].iloc[0]

		timedf.loc[name_ind, 'name'] = name
		timedf.loc[name_ind, 'nation'] = nation

	timedf = timedf.merge(chrono_df, on=['season', 'race', 'date', 'id', 'name', 'nation'], how='left')
	print("Done merging men")
	unique_ids = pd.unique(timedf['id'])
	newdf = pd.DataFrame()

	for a in range(len(unique_ids)):
		if((len(unique_ids)-a)%100==0):
			print(len(unique_ids)-a)
		skierdf = timedf.loc[timedf['id']==unique_ids[a]]
		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['distance_elo'] = skierdf['distance_elo'].ffill()
		skierdf['distance_classic_elo'] = skierdf['distance_classic_elo'].ffill()
		skierdf['distance_freestyle_elo'] = skierdf['distance_freestyle_elo'].ffill()
		skierdf['sprint_elo'] = skierdf['sprint_elo'].ffill()
		skierdf['sprint_classic_elo'] = skierdf['sprint_classic_elo'].ffill()
		skierdf['sprint_freestyle_elo'] = skierdf['sprint_freestyle_elo'].ffill()
		skierdf['classic_elo'] = skierdf['classic_elo'].ffill()
		skierdf['freestyle_elo'] = skierdf['freestyle_elo'].ffill()
		
		

		newdf = newdf.append(skierdf)
	
	newdf = newdf.sort_index()

	####Maybe try a merge for the elos.  Everything else is ok.
	print(newdf)
	#return timedf
	return newdf
#ladiesdf = ladies()
#ladiesdf.to_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/ladies_time.pkl")
#ladiesdf.to_excel("/Users/syverjohansen/ski/elo/python/ski/radar/ladies_time.xlsx")



mendf = men()
mendf.to_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/men_time.pkl")
mendf.to_excel("/Users/syverjohansen/ski/elo/python/ski/radar/men_time.xlsx")

print(time.time() - start_time)

