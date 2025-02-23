import pandas as pd
import numpy as np


ladiesdf = pd.read_pickle("~/ski/elo/python/ski/ladiesdf.pkl")

def EA (pelos, index):
    players = len(pelos)
    ra = pelos[index]
    QA = 10**(ra/400) 
    QA = np.repeat(QA, players - 1)
    rb = np.delete(np.array(pelos), index)
    QB = 10**(rb/400)
    EA = QA / (QA + QB)
    return EA

def SA(place_vector, place):
    place_vector = np.array(place_vector)
    losses = (place_vector < place).sum()
    draws = np.count_nonzero(place_vector == place) - 1
    wins = (place_vector > place).sum()
    return losses*[0] + draws*[0.5] + wins*[1]



def lady_elo():
	#Step 1: Figure out the person's previous elo.  
	#If they aren't in the new dataframe, make them a new score (1300)
	ladieselodf = pd.DataFrame()
	#ladieselodf.columns = ['date', 'city', 'country', 'level', 'sex', 'distance', 
	#'discipline', 'place', 'name', 'nation','season','race', 'pelo', 'elo']
	
	id_pool = []
	max_races = max(ladiesdf['race'])
	#print(max_races)
	#Print the unique seasons
	seasons = (pd.unique(ladiesdf['season']))
	for season in range(len(seasons)):
	#for season in range(10):
		print(seasons[season])

		seasondf = ladiesdf.loc[ladiesdf['season']==seasons[season]]
		races = pd.unique(seasondf['race'])
		K = float(max_races/len(races))
		for race in range(len(races)):
			pelo_list = []
			elo_list = []
			racedf = seasondf.loc[seasondf['race']==races[race]]
			racedf.reset_index(inplace=True, drop=True)
			for a in range(len(racedf['id'])):
				if (racedf['id'][a] not in id_pool):
					id_pool.append(racedf['id'][a])
					pelo_list.append(1300)
				else:
					#print("yo")
					#Get the vet skiers line
					vetskier = ladieselodf.loc[ladieselodf['id']==racedf['id'][a]]
					pelo_list.append(vetskier['elo'].iloc[-1])

				#else we have to find them and see if they are the same person
				#else we have to find their last elo in ladieselodf

			racedf['pelo'] = pelo_list
			PLACES = list(racedf['place'])
			for i,p in enumerate(PLACES):
				elo_list.append(pelo_list[i] + K*(sum(SA(PLACES, PLACES[i])) - sum(EA(pelo_list,i))))

			racedf['elo'] = elo_list
			ladieselodf = ladieselodf.append(racedf)
			#print(ladieselodf)

		endseasondate = int(str(seasons[season])+'0500')
		#print(endseasondate)
		for n in range(len(id_pool)):
			endskier = ladieselodf.loc[ladieselodf['id']==id_pool[n]]
			endname = endskier['name'].iloc[-1]
			endpelo = endskier['elo'].iloc[-1]
			endelo = endpelo*.85+1300*.15
			endnation = endskier['nation'].iloc[-1]
			endf = pd.DataFrame([[endseasondate, "Summer", "Break", "end", "M", 0, None, 0
				, endname, endnation, id_pool[n],seasons[season], 0, endpelo, endelo]], columns = ladieselodf.columns)
			ladieselodf = ladieselodf.append(endf)

	return ladieselodf	

ladieselodf = lady_elo()
ladieselodf.to_pickle("~/ski/elo/python/ski/simlady.pkl") #changed
ladieselodf.to_excel("~/ski/elo/python/ski/simlady.xlsx") #changed


#def lady_elo():