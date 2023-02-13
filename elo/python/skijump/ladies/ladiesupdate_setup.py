import pandas as pd
import numpy as np
import time
start_time = time.time()
K=1

#1) Import update
#2) Convert it so you can see 

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
	xlsx = pd.ExcelFile("~/ski/elo/python/skijump/excel365/update_scrape.xlsx")
	df = pd.read_excel(xlsx, sheet_name = "Ladies", header=None)
	df.columns = ['date', 'city', 'country', 'level', 'sex','hill',  'place', 'name', 'nation', 'id']
	df['nation'] = df['nation'].str.lstrip()
	df['id'] = df['id'].str.split("&")
	df['id'] = df['id'].str[0]
	df['id'] = df['id'].astype(int)
	

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


df = setup()
print(df)
df.to_pickle("~/ski/elo/python/skijump/excel365/ladiesupdate_setup.pkl")
df.to_pickle("~/ski/elo/python/skijump/excel365/ladiesupdate_setup.xlsx")

print(time.time() - start_time)
