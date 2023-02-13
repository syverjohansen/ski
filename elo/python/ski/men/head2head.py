import pandas as pd
import numpy as np
import time
start_time = time.time()


mendf = pd.read_pickle("~/ski/elo/python/ski/age/excel365/mendf.pkl")

update_mendf = pd.read_pickle("~/ski/elo/python/ski/age/excel365/menupdate_setup.pkl")
mendf = mendf.append(update_mendf, ignore_index=True)
#print(mendf)
pd.options.mode.chained_assignment = None


def season(mendf, season1, season2):
    mendf = mendf.loc[mendf['season']>=season1]
    mendf = mendf.loc[mendf['season']<=season2]
    return mendf

def distance(mendf, distances):
   # print(pd.unique(mendf['distance']))
    if(distances=="Sprint"):
        #mendf = mendf.loc[(mendf['distance']=="Sprint") | (mendf['city']=="Tour de Ski")]
        mendf = mendf.loc[(mendf['distance']=="Sprint")]
        
    elif(distances in pd.unique(mendf['distance'])):
        print("true")
        mendf = mendf.loc[mendf['distance']==distances]
    else:
        mendf = mendf.loc[mendf['distance']!="Sprint"]
    return mendf

def discipline(mendf, discipline):
    if(discipline == "F"):
        #mendf = mendf.loc[(mendf['discipline']=="F") | (mendf['city']=="Tour de Ski")]
        mendf = mendf.loc[(mendf['discipline']=="F") ]
    elif(discipline =="P"):
        mendf = mendf.loc[mendf['discipline']=="P"]
    else:
        mendf = mendf.loc[mendf['discipline']!="P"]
        mendf = mendf.loc[mendf['discipline']!="F"]
        mendf = mendf.loc[(mendf['distance']!="Stage") & (mendf['distance']!="Etappeløp")]
    return mendf


def winpct(mendf, skier1):
	skierdf = pd.DataFrame()
	seasons = (pd.unique(mendf['season']))
	wins = 0
	losses = 0
	ties = 0


	for season in range(len(seasons)):
		seasondf = mendf.loc[mendf['season']==seasons[season]]
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


def whosbetter(mendf, skier1):
	
	skierdf = pd.DataFrame()
	seasons = (pd.unique(mendf['season']))

	for season in range(len(seasons)):
		seasondf = mendf.loc[mendf['season']==seasons[season]]
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
	#return otherskiers
	#print(otherskiers)


def head2head(mendf, skier1, skier2):
	
	mendf = mendf.loc[(mendf['name']==skier1) | (mendf['name']==skier2)]
	
	seasons = (pd.unique(mendf['season']))
	wins = 0
	losses = 0
	ties = 0

	for season in range(len(seasons)):
		seasondf = mendf.loc[mendf['season']==seasons[season]]
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



mendf['name'] = mendf['name'].str.replace('Ã¸', 'ø')
mendf['name'] = mendf['name'].str.replace('Ã¦', 'æ')
mendf['name'] = mendf['name'].str.replace('Ã¼', 'ü')
mendf['name'] = mendf['name'].str.replace('Ã', 'Ø')
mendf['name'] = mendf['name'].str.replace('Ã¶', 'ö')
mendf['name'] = mendf['name'].str.replace('Ã¤', 'ä')
mendf['name'] = mendf['name'].str.replace('Ã¥', 'å')

#mendf = distance(mendf, "Sprint")
#mendf = discipline(mendf, "F")
#print(mendf)

mendf = season(mendf, 2023, 2023)

record = head2head(mendf, "Johannes Høsflot Klæbo", "Alexander Bolshunov")
whosbetter = whosbetter(mendf, "Johannes Høsflot Klæbo")
skier = "Johannes Høsflot Klæbo"
print(skier)
pct = winpct(mendf, skier)
print(pct)

print(whosbetter)
#print(record)

