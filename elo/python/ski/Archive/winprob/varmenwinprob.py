import pandas as pd
import numpy as np


#refdf2 = pd.read_pickle("~/ski/elo/python/ski/men/varmen.pkl")
#refdf2 = pd.read_pickle("~/ski/elo/python/ski/men/varmen_distance.pkl")
refdf2 = pd.read_pickle("~/ski/elo/python/ski/men/varmen_all.pkl")
#refdf2 = pd.read_pickle("~/ski/elo/python/ski/men/varmen_sprint.pkl")
#refdf2 = refdf2.loc[refdf2['season']==2020]
refdf2 = refdf2.loc[refdf2['city']=="Tour de Ski"]
print(refdf2)
#race = refdf2.loc[refdf2['race']==max(refdf2['race'])]



#racer = pelo_list[0]


win_probs = []
win = []
seasons = pd.unique(refdf2['season'])


for season in seasons:
	seasondf = refdf2.loc[refdf2['season']==season]
	print(season)
	races = pd.unique(seasondf['race'])

	


	#races=[1]

	for race in races:
		#print(race)
		racedf = seasondf.loc[seasondf['race']==race]
		#print(racedf)
		ski_ids = list(racedf['id'])
		pelo_list = list(racedf['pelo'])
		#print(pelo_list)
		ranked_pelo = sorted(pelo_list, reverse=True)
		
		try:
			top30 = ranked_pelo[14]
		except:
			top30  = ranked_pelo[int(len(ranked_pelo)/2-1)]
		#print(top30)
		place = list(racedf["place"])
		for a in range(len(pelo_list)):
			if(pelo_list[a]<top30):
				continue

			for b in range(len(pelo_list)):
				if(pelo_list[b]<top30):
					
					continue
				if(a==b):
					pass
				else:
					win_prob = 1/(1+10**((pelo_list[b]-pelo_list[a])/400))
					win_probs.append(win_prob)
					if(place[a]<place[b]):
						win.append(1)
					elif(place[a]==place[b]):
						win.append(.5)
					else:
						win.append(0)

#pelo_list = np.array(pelo_list)
#print(pelo_list[:0]+pelo_list[1:])
#print(pelo_list[np.arange(len(pelo_list))!=0])

d = {'win_prob':win_probs, 'win':win}
df = pd.DataFrame(d)
df.to_excel("~/ski/elo/python/ski/winprob/varmenwinprob.xlsx")

#look at specific types of races
#look at for only top-30
#look at age