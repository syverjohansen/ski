import pandas as pd

ladiesdf = pd.read_pickle("~/ski/elo/python/ski/ladiesdf.pkl")

mendf = pd.read_pickle("~/ski/elo/python/ski/mendf.pkl")
K=1

def lady_elo():
	#Step 1: Figure out the person's previous elo.  
	#If they aren't in the new dataframe, make them a new score (1300)
	ladieselodf = pd.DataFrame()
	#ladieselodf.columns = ['date', 'city', 'country', 'level', 'sex', 'distance', 
	#'discipline', 'place', 'name', 'nation','season','race', 'pelo', 'elo']

	name_pool = []
	#Print the unique seasons
	seasons = (pd.unique(ladiesdf['season']))

	
	for season in range(len(seasons)):
	
	#for season in range(10):
		print(seasons[season])

		seasondf = ladiesdf.loc[ladiesdf['season']==seasons[season]]
		races = pd.unique(seasondf['race'])
		for race in range(len(races)):
			pelo_list = []
			elo_list = []
			racedf = seasondf.loc[seasondf['race']==races[race]]
			racedf.reset_index(inplace=True, drop=True)
			for a in range(len(racedf['name'])):
				if (racedf['name'][a] not in name_pool):
					name_pool.append(racedf['name'][a])
					pelo_list.append(1300)
				else:
					#print("yo")
					#Get the vet skiers line
					vetskier = ladieselodf.loc[ladieselodf['name']==racedf['name'][a]]
					pelo_list.append(vetskier['elo'].iloc[-1])


				#else we have to find them and see if they are the same person
				#else we have to find their last elo in ladieselodf
			racedf['pelo'] = pelo_list
			for b in range(len(pelo_list)):
			
			#for b in range(1):
				#winning places are the ones greater than the current index
				#losing places are the ones less than the current index
				#draw places are the ones equal with the current index
				
				wplaces = racedf.loc[racedf['place']>racedf['place'][b]]
				lplaces = racedf.loc[racedf['place']<racedf['place'][b]]
				dplaces = racedf.loc[racedf['place']==racedf['place'][b]]
				if(len(dplaces)>1):
					dskier = (racedf.iloc[b]['name'])
					dplaces = (dplaces.loc[dplaces['name']!=dskier])
					#print(dplaces.loc[dplaces['name'].loc[[0]]])
					#dplaces = racedf.loc[racedf['name'][b]!=dplaces['name']]
					print(dplaces)
				else:
					dplaces = []
				r1 = pelo_list[b]
				R1 = 10**(r1/400)
				#print(len(wplaces))
				if(len(wplaces)>0):
					wR2 = sum(10**(wplaces['pelo']/400))/len(wplaces)
					wE1 = R1/(R1+wR2)
					wS1 = len(wplaces)
				else:
					wS1 = 0
					wE1 = 0
				if len(dplaces)>0:
					dR2 = sum(10**(dplaces['pelo']/400))/len(dplaces)
					dE1 = R1/(R1+dR2)
					dS1 = len(dplaces)
				else:
					dE1 = 0
					dS1 =0
				if len(lplaces)>0:
					lR2 = sum(10**(lplaces['pelo']/400))/len(lplaces)
					lE1 = R1/(R1+lR2)
					lS1 = len(lplaces)
				else:
					lE1 = 0
					lS1 =0
				r11 = r1+wS1*K*(1-wE1)+dS1*K*(.5-dE1)+lS1*K*(0-lE1)
				elo_list.append(r11)
			racedf['elo'] = elo_list


			ladieselodf = ladieselodf.append(racedf)
			#print(ladieselodf)
		endseasondate = int(str(seasons[season])+'0500')
		#print(endseasondate)
		for n in range(len(name_pool)):
			endskier = ladieselodf.loc[ladieselodf['name']==name_pool[n]]
			endpelo = endskier['elo'].iloc[-1]
			endelo = endpelo*.85+1300*.15
			endnation = endskier['nation'].iloc[-1]
			endf = pd.DataFrame([[endseasondate, "Summer", "Break", "end", "L", 0, None, 0
				, name_pool[n], endnation, seasons[season], 0, endpelo, endelo]], columns = ladieselodf.columns)
			ladieselodf = ladieselodf.append(endf)


	return ladieselodf	


def male_elo():
	#Step 1: Figure out the person's previous elo.  
	#If they aren't in the new dataframe, make them a new score (1300)
	menelodf = pd.DataFrame()
	#menelodf.columns = ['date', 'city', 'country', 'level', 'sex', 'distance', 
	#'discipline', 'place', 'name', 'nation','season','race', 'pelo', 'elo']

	name_pool = []
	#Print the unique seasons
	seasons = (pd.unique(mendf['season']))

	
	for season in range(len(seasons)):
	
	#for season in range(10):
		print(seasons[season])

		seasondf = mendf.loc[mendf['season']==seasons[season]]
		races = pd.unique(seasondf['race'])
		for race in range(len(races)):
			pelo_list = []
			elo_list = []
			racedf = seasondf.loc[seasondf['race']==races[race]]
			racedf.reset_index(inplace=True, drop=True)
			for a in range(len(racedf['name'])):
				if (racedf['name'][a] not in name_pool):
					name_pool.append(racedf['name'][a])
					pelo_list.append(1300)
				else:
					#print("yo")
					#Get the vet skiers line
					vetskier = menelodf.loc[menelodf['name']==racedf['name'][a]]
					pelo_list.append(vetskier['elo'].iloc[-1])


				#else we have to find them and see if they are the same person
				#else we have to find their last elo in menelodf
			racedf['pelo'] = pelo_list
			for b in range(len(pelo_list)):
			
			#for b in range(1):
				#winning places are the ones greater than the current index
				#losing places are the ones less than the current index
				#draw places are the ones equal with the current index
				
				wplaces = racedf.loc[racedf['place']>racedf['place'][b]]
				lplaces = racedf.loc[racedf['place']<racedf['place'][b]]
				dplaces = racedf.loc[racedf['place']==racedf['place'][b]]
				if(len(dplaces)>1):
					dskier = (racedf.iloc[b]['name'])
					dplaces = (dplaces.loc[dplaces['name']!=dskier])
					#print(dplaces.loc[dplaces['name'].loc[[0]]])
					#dplaces = racedf.loc[racedf['name'][b]!=dplaces['name']]
					print(dplaces)
				else:
					dplaces = []
				r1 = pelo_list[b]
				R1 = 10**(r1/400)
				#print(len(wplaces))
				if(len(wplaces)>0):
					wR2 = sum(10**(wplaces['pelo']/400))/len(wplaces)
					wE1 = R1/(R1+wR2)
					wS1 = len(wplaces)
				else:
					wS1 = 0
					wE1 = 0
				if len(dplaces)>0:
					dR2 = sum(10**(dplaces['pelo']/400))/len(dplaces)
					dE1 = R1/(R1+dR2)
					dS1 = len(dplaces)
				else:
					dE1 = 0
					dS1 =0
				if len(lplaces)>0:
					lR2 = sum(10**(lplaces['pelo']/400))/len(lplaces)
					lE1 = R1/(R1+lR2)
					lS1 = len(lplaces)
				else:
					lE1 = 0
					lS1 =0
				r11 = r1+wS1*K*(1-wE1)+dS1*K*(.5-dE1)+lS1*K*(0-lE1)
				elo_list.append(r11)
			racedf['elo'] = elo_list


			menelodf = menelodf.append(racedf)
			#print(menelodf)
		endseasondate = int(str(seasons[season])+'0500')
		#print(endseasondate)
		for n in range(len(name_pool)):
			endskier = menelodf.loc[menelodf['name']==name_pool[n]]
			endpelo = endskier['elo'].iloc[-1]
			endelo = endpelo*.85+1300*.15
			endnation = endskier['nation'].iloc[-1]
			endf = pd.DataFrame([[endseasondate, "Summer", "Break", "end", "M", 0, None, 0
				, name_pool[n], endnation, seasons[season], 0, endpelo, endelo]], columns = menelodf.columns)
			menelodf = menelodf.append(endf)


	return menelodf	




ladieselodf = lady_elo()
ladieselodf.to_pickle("~/ski/elo/python/ski/ladieselodf.pkl")
ladieselodf.to_excel("~/ski/elo/python/ski/ladieselodf.xlsx")

menelodf = male_elo()
menelodf.to_pickle("~/ski/elo/python/ski/menelodf.pkl")
menelodf.to_excel("~/ski/elo/python/ski/menelodf.xlsx")

#def male_elo():