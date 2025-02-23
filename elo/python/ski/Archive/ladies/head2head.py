import pandas as pd
import numpy as np
import time
start_time = time.time()


ladiesdf = pd.read_pickle("~/ski/elo/python/ski/excel365/ladiesdf.pkl")

update_ladiesdf = pd.read_pickle("~/ski/elo/python/ski/excel365/ladiesupdate_setup.pkl")
ladiesdf = ladiesdf.append(update_ladiesdf, ignore_index=True)
#print(ladiesdf)
#pd.options.mode.chained_assignladiest = None


def distance(ladiesdf, distances):
   # print(pd.unique(ladiesdf['distance']))
    if(distances=="Sprint"):
        #ladiesdf = ladiesdf.loc[(ladiesdf['distance']=="Sprint") | (ladiesdf['city']=="Tour de Ski")]
        ladiesdf = ladiesdf.loc[(ladiesdf['distance']=="Sprint")]
        
    elif(distances in pd.unique(ladiesdf['distance'])):
        print("true")
        ladiesdf = ladiesdf.loc[ladiesdf['distance']==distances]
    else:
        ladiesdf = ladiesdf.loc[ladiesdf['distance']!="Sprint"]
    return ladiesdf

def winpct(ladiesdf, skier1):
	skierdf = pd.DataFrame()
	seasons = (pd.unique(ladiesdf['season']))
	wins = 0
	losses = 0
	ties = 0


	for season in range(len(seasons)):
		seasondf = ladiesdf.loc[ladiesdf['season']==seasons[season]]
		races = pd.unique(seasondf['race'])
		for race in range(len(races)):
			#print(race)
			racedf = seasondf.loc[seasondf['race']==races[race]]
			#print(racedf)
			#print(racedf)
			there = racedf.loc[racedf['name']==skier1]
			if(len(there)>0):
				#print(racedf)
				place = racedf.loc[racedf['name']==skier1, 'place'].iloc[0]
				wins+=len(racedf.loc[racedf['place']>place])
				#print(wins)
				losses+=len(racedf.loc[racedf['place']<place])
				ties +=len(racedf.loc[racedf['place']==place])-1
	return [wins, losses, ties, wins/(wins+losses+ties)]

def whosbetter(ladiesdf, skier1):
	
	skierdf = pd.DataFrame()
	seasons = (pd.unique(ladiesdf['season']))

	for season in range(len(seasons)):
		seasondf = ladiesdf.loc[ladiesdf['season']==seasons[season]]
		races = pd.unique(seasondf['race'])
		for race in range(len(races)):
			#print(race)
			racedf = seasondf.loc[seasondf['race']==races[race]]
			#print(racedf)
			#print(racedf)
			there = racedf.loc[racedf['name']==skier1]
			if(len(there)>0):
				#print(racedf)
				skierdf = skierdf.append(racedf)
	
	#print(skierdf)
	otherskiers = pd.unique(skierdf['id'])
	for a in range(len(otherskiers)):
		wins = 0
		losses = 0
		ties = 0
		tempdf = skierdf.loc[(skierdf['name']==skier1) | (skierdf['id']==otherskiers[a])]

		seasons = (pd.unique(skierdf['season']))

		races = pd.unique(seasondf['race'])
		for season in range(len(seasons)):
			seasondf = tempdf.loc[tempdf['season']==seasons[season]]

			races = pd.unique(seasondf['race'])

			for race in range(len(races)):
				racedf = seasondf.loc[seasondf['race']==races[race]]

				
				if(len(racedf['name'])<2):
					continue
				else:
					
					place1 = racedf.loc[racedf['name']==skier1, 'place'].iloc[0]
					
					place2 = racedf.loc[racedf['id']==otherskiers[a], 'place'].iloc[0]
					#print(place1, place2)
					#print(place1)
					#print(place2)
					if(place1>place2):
						losses+=1
					elif(place1<place2):
						wins+=1
					else:
						ties+=1

		#print(tempdf['id'])
		#print(tempdf.loc[tempdf['id']==otherskiers[a], 'name'].iloc[0])
		if(wins<losses):
			print(tempdf.loc[tempdf['id']==otherskiers[a], 'name'].iloc[0], wins, losses)
	#return -1

def head2head(ladiesdf, skier1, skier2):
	
	ladiesdf = ladiesdf.loc[(ladiesdf['name']==skier1) | (ladiesdf['name']==skier2)]
	
	seasons = (pd.unique(ladiesdf['season']))
	wins = 0
	losses = 0
	ties = 0

	for season in range(len(seasons)):
		seasondf = ladiesdf.loc[ladiesdf['season']==seasons[season]]
		races = pd.unique(seasondf['race'])
		for race in range(len(races)):
			racedf = seasondf.loc[seasondf['race']==races[race]]
			
			if(len(racedf['name'])<2):
				continue
			else:
				
				place1 = racedf.loc[racedf['name']==skier1, 'place'].iloc[0]
				
				place2 = racedf.loc[racedf['name']==skier2, 'place'].iloc[0]
				#print(place1, place2)
				#print(place1)
				#print(place2)
				if(place1>place2):
					losses+=1
				elif(place1<place2):
					wins+=1
				else:
					ties+=1
	return [wins, losses, ties]


ladiesdf['name'] = ladiesdf['name'].str.replace('Ã¸', 'ø')
ladiesdf['name'] = ladiesdf['name'].str.replace('Ã¦', 'æ')
ladiesdf['name'] = ladiesdf['name'].str.replace('Ã¼', 'ü')
ladiesdf['name'] = ladiesdf['name'].str.replace('Ã', 'Ø')

ladiesdf = distance(ladiesdf, "Distance")



#record = head2head(ladiesdf, "Stina Nilsson", "Maja Dahlqvist")
#whosbetter = whosbetter(ladiesdf, "Therese Johaug")
pct = winpct(ladiesdf, "Therese Johaug")
print(pct)


