import pandas as pd
import numpy as np
import time
start_time = time.time()
K=1

#1) Import update
#2) Convert it so you can see 
refdf = pd.read_pickle("~/ski/elo/python/nc/excel365/menelodf2.pkl")
	#print(refdf)

refdf = refdf.loc[refdf['date']==20200500]
pd.options.mode.chained_assignment = None


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

def setup():

	
	#print(refdf)
	xlsx = pd.ExcelFile("~/ski/elo/python/nc/excel365/update_scrape.xlsx")
	df = pd.read_excel(xlsx, sheet_name = "Men", header=None)
	df.columns = ['date', 'city', 'country', 'level', 'sex','hill', 'distance',  'place', 'name', 'nation', 'id']
	df['nation'] = df['nation'].str.lstrip()
	


	seasons = []
	for a in range(len(df['date'])):
		date = str(df['date'][a])
		year = date[0:4]
		day = date[4:8]
		if(day>'0800'):
			season = int(year)+1
		else:
			season = int(year)
		seasons.append(season)

	df['season'] = seasons
	races = []
	race = 1

	for a in range(len(df['place'])):
		if (a==0):
			races.append(race)
			continue
		if (seasons[a]!=seasons[a-1]):
			race=1
			races.append(race)
		elif(df['date'][a]!=df['date'][a-1]):
			race+=1
			races.append(race)
		elif(str(df['place'][a])=='1'):
			if(str(df['place'][a-1])>'1'):
				race+=1
				races.append(race)
			else:
				races.append(race)
		else:
			races.append(race)
	df['race'] = races
	return df

def elo(refdf, df):
	elodf = pd.DataFrame()
	refid_pool = list(refdf['id'])
	id_pool = []
	seasons = pd.unique(df['season'])
	for season in range(len(seasons)):
		print(seasons[season])
		seasondf = df.loc[df['season']==seasons[season]]
		races = pd.unique(seasondf['race'])
		for race in range(len(races)):
			pelo_list = []
			elo_list = []
			racedf = seasondf.loc[seasondf['race']==races[race]]
			racedf.reset_index(inplace=True, drop=True)
			for a in range(len(racedf['id'])):
				if(racedf['id'][a] not in id_pool):
					id_pool.append(racedf['id'][a])
					if(racedf['id'][a] not in refid_pool):
						pelo_list.append(1300)
					else:
						vetskier = refdf.loc[refdf['id']==racedf['id'][a]]
						pelo_list.append(vetskier['elo'].iloc[-1])
				else:
					vetskier = elodf.loc[elodf['id']==racedf['id'][a]]
					pelo_list.append(vetskier['elo'].iloc[-1])
			racedf['pelo'] = pelo_list
			PLACES = list(racedf['place'])
			for i,p in enumerate(PLACES):
				elo_list.append(pelo_list[i] + K*(sum(SA(PLACES, PLACES[i])) - sum(EA(pelo_list,i))))

			racedf['elo'] = elo_list
			elodf = elodf.append(racedf)

	return elodf



	#print(id_pool)



df = setup()
df.to_pickle("~/ski/elo/python/nc/excel365/menupdate_setup.pkl")
df = elo(refdf, df)
df.to_pickle("~/ski/elo/python/nc/excel365/menupdate.pkl")
df.to_excel("~/ski/elo/python/nc/excel365/menupdate.xlsx")

print(time.time() - start_time)


