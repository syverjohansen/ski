from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime
from sklearn import linear_model
from sklearn import preprocessing
from sklearn.model_selection import KFold
from sklearn.linear_model import LinearRegression
#import statsmodels.api as sm
#import matplotlib.pyplot as plt

import time
import json
from pandas.io.json import json_normalize
pd.options.mode.chained_assignment = None


import time
start_time = time.time()



'''
ids
Austria
Men


Women
#Stadlober -- 3055067

---------------------

Czechia
Men
#Fellner -- 3150519
#Knop -- 3150509

Women

-----------------------
Finland
Men
#Hyyvarinen -- 3180557
Lepistoe -- 3180984
#Lindholm, Remi -- 3181180
#Niskanen -- 3180535
Suhonen -- 3181098


Ladies
Matinalo -- 3185579
#Niskanen -- 3185168
Parmakoski - 3185256
Piippo -- 3185702
#Roponen -- 1255374
-------

France
Men
#Backscheider -- 3190268
Gaillard -- 1345875
#Lapalus -- 3190529
#Lapierre -- 3190398
#Manificat -- 3190111
#Parisse -- 3190302

Ladies
#Bentz -- 3195205
#Bulle - 3195224
#Claudel -- 3195219
#Dolci -- 3195289
#Ducordeau -- 3195253

------
Germany
Men
#Boegl -- 3200205
#Dobler -- 3200356
#Katz -- 3200072
#Moch -- 3200802
#Notz -- 3200376

Ladies
Carl -- 3205403
#Fink-- 3205407
#Frabel -- 3205491
#Hennig -- 3205460
#Krehl -- 3205434
Rydzek -- 3505003
-------
Italy
Men
Gardener -- 3290321
Ventura -- 3290504

Ladies
Di Centa -- 3295439
-------
Norway
Men
#Amundsen -- 3423264
#Holund -- 3420586
#Krueger -- 3421779
Moseby -- 3423782
Mørk -- 3424611
#Nyenget -- 3421154
#Roethe -- 3420605
Turtveit -- 3422186
Toenseth -- 3420994


Ladies
#Fossessholm -- 3427109
#Haga -- 3425421
#Johaug -- 3425301
#Kalva -- 3425669
#Oestberg -- 3425410
Slind, Silje -- 3425355
Theodorsen -- 3425896
#Weng -- 3425499

Russia
Men
#Chervotkin -- 3482119
Maltsev -- 3481432
#Melnichenko -- 3481803
Semikov -- 3481988
#Spitsov -- 3482280
Yakimushkin -- 3482105

Ladies
Istomina -- 3486548
Kirpichenko -- 3486314
#Kupritskaya -- 3487123
#Rygalina -- 3486176

------

Sweden
Men
Andersson -- 3501577
#Burman -- 3501223
#Eriksson -- 3501462
#Halfvarsson -- 3500664
Herbert -- 3501639
#Johansson -- 3501662
#Poromaa -- 3501741
Rosjö -- 3501555

Ladies
#Andersson-- 3505990
Kalla -- 3505217
#Karlsson -- 3506154
#Olsson -- 3505998
-----

Switzerland
Men
#Baumann -- 3510342
Cologna -- 3510023
Furger -- 3510351
#Klee -- 3510534
#Pralong -- 3510361
#Rueesch -- 3510479

Women


'''








#This is for team sprint events
def fis_team_sprint():
	ids = []
	teams = []
	sex = []
	count = 0

	#start with the men
	#There will be 4 because of the semifinals
	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=36550',
	'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=38608',
	'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=36549',
	'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=38607']
	for a in range(len(startlist_list)):
		startlist = BeautifulSoup(urlopen(startlist_list[a]), 'html.parser')
	#print(startlist)
		names = startlist.find_all('div', {'g-lg-14 g-md-14 g-sm-11 g-xs-10 justify-left bold'})
		body = startlist.find_all('div', {'g-lg-2 g-md-2 g-sm-3 hidden-xs justify-right gray pr-1'})
		print(startlist_list[a])

		for b in range(len(body)):
			#print(body[a].text.strip())
			if(b%3!=0):
				ids.append(int(body[b].text.strip()))
			else:
				team = names[b].text.strip()
				if(a<2):
					team = "m"+team
				else:
					team = "f"+team
				ids.append(team)
			#if(count==0):
			#	sex.append('M')
			#else:
			#	sex.append('L')
		
		

		#print(team)
		count+=1

	print(ids)

	

	#now for the ladies
	
	return ids

def fantasy_team_sprint(startlist):
	name = []
	team_name = []
	team_id = []
	team_price = []
	team_sex = []
	ski_id = []
	price =[]
	sex = []
	#sex = startlist['sex']
	#startlist = startlist['id']
	#print(sex)
	#print(startlist)
	fantasy = 'https://www.fantasyxc.se/api/athletes'
	#soup = BeautifulSoup(urlopen(fantasy), 'html5lib')
	#print(soup)
	with requests.Session() as s:
		r=s.get(fantasy)
		soup = BeautifulSoup(r.content, 'html5lib')
	API_json = json.loads(soup.get_text())
	API_df = pd.DataFrame.from_dict(pd.json_normalize(API_json), orient='columns')

	##Change to locate for increased speed
	for a in range(len(startlist)):
		if(a%3==0):
			
			if(startlist[a].startswith("m")):
				#print(startlist[a])
				country_name = startlist[a]
				country_name = country_name.split("m")
				country_name = country_name[1]
				if(country_name.endswith(" I") or country_name.endswith(" II")):
					pass
				else:
					country_name = country_name + " I"

				nation= API_df.loc[API_df['name']==country_name]
				nation = nation.loc[nation['gender']=='m']
				sex.append('m')
				name.append("Male"+country_name)
				#print(nation)
			else:
				country_name = startlist[a]
				country_name = country_name.split("f")
				country_name = country_name[1]
				if(country_name.endswith(" I") or country_name.endswith(" II")):
					pass
				else:
					country_name = country_name + " I"
				nation= API_df.loc[API_df['name']==country_name]
				nation = nation.loc[nation['gender']=='f']
				sex.append('f')
				name.append("Female" + country_name)
			try:
				ski_id.append(nation['athlete_id'].iloc[0])
				price.append(nation['price'].iloc[0])
				#sex.append(nation['gender'].iloc[0])
			except:
				print(country_name)
				ski_id.append(999999)
				price.append(23096)





		else:

			athlete = API_df.loc[API_df['athlete_id']==startlist[a]]
			
			first_name = []
			last_name = []
			try:
				test_name = (athlete['name'].iloc[0])
			except:
				test_name = "NAME Generic" 
			test_name = test_name.split(" ")
			for word in test_name:
				if word.isupper():
					last_name.append(word)
				else:
					first_name.append(word)
			first_name = ' '.join(first_name)
			last_name = ' '.join(last_name)
			test_name = first_name + " " + last_name


			name.append(test_name)
			try:
				ski_id.append(athlete['athlete_id'].iloc[0])
				price.append(athlete['price'].iloc[0])
				sex.append(athlete['gender'].iloc[0])
			except:
				ski_id.append(999999)
				price.append(999999)
				sex.append('mf')
				#pass
				print(test_name)
		
	d = {'name':name, 'id':ski_id, 'price':price, 'sex':sex}
	fantasy_df = pd.DataFrame(data=d)
	return fantasy_df

def elo_team_sprint(fantasydf):
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [i*2 for i in wc]
	skier_elo = []
	team_elos = []
	
	df = pd.read_pickle("~/ski/elo/python/ski/excel365/varmen_sprint_freestyle.pkl")
	ladiesdf = pd.read_pickle("~/ski/elo/python/ski/excel365/varladies_sprint_freestyle.pkl")
	df = df.append(ladiesdf, ignore_index = True)
	df['name'] = df['name'].str.replace('ø', 'oe')
	df['name'] = df['name'].str.replace('ä', 'ae')
	df['name'] = df['name'].str.replace('æ', 'ae')
	df['name']= df['name'].str.replace('ö', 'oe')
	df['name']= df['name'].str.replace('ü', 'ue')
	df['name']= df['name'].str.replace('å', 'aa')
	df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	df['name'] = df['name'].str.replace('Irineu Esteve Altimiras', 'ireneu esteve altimiras')
	df['name'] = df['name'].str.replace('Thomas Hjalmar Westgaard', 'thomas maloney westgaard')
	df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	df['name'] = df['name'].str.replace('Lauri Lepistoe', 'lauri lepisto')
	df['name'] = df['name'].str.replace('Philip Bellingham', 'phillip bellingham')
	df['name'] = df['name'].str.replace('Snorri Einarsson', 'snorri eythor einarsson')
	df['name'] = df['name'].str.replace('Krista Paermaekoski', 'krista parmakoski')
	df['name'] = df['name'].str.replace('Jessica Diggins', 'jessie diggins')
	df['name'] = df['name'].str.replace('Patricijia Eiduka', 'patricija eiduka')
	df['name'] = df['name'].str.replace('Katri Lylynperae', 'katri lylynpera')
	df['name'] = df['name'].str.replace('Julia Belger', 'julia preussger')
	df['name'] = df['name'].str.replace('Perttu Hyvaerinen', 'perttu hyvarinen')
	df['name'] = df['name'].str.replace('Kathrine Stewart-Jones', 'katherine stewart-jones')
	df['name'] = df['name'].str.replace('Ailja Iksanova', 'alija iksanova')
	df['name'] = df['name'].str.replace('Joni Maeki', 'joni maki')

	print(fantasydf)
	teamsdf = fantasydf.iloc[::3, :]
	fantasydf = fantasydf[fantasydf.index % 3 !=0]

	#print(fantasydf)

	fantasy_names = fantasydf['name']
	fantasy_names = fantasy_names.str.lower()
	fantasy_names  = fantasy_names.tolist()
	count = 0
	team_elo = 0
	for a in range(len(fantasy_names)):

		skier = df.loc[df['name'].str.lower() == fantasy_names[a]]
		if(len(skier['name'])==0):
			print(fantasy_names[a])
		#print(skier)
		try:
			elo = skier['elo'].iloc[-1]
			team_elo+=elo
			#skier_elo.append(elo)
		#elo = (skier.loc[skier['date']==20210500]['elo'])
		except:
			print(fantasy_names[a])
			team_elo+=1300

		if(a%2==1):
			team_elos.append(team_elo)
			team_elo = 0
			#skier_elo.append(1300)
	print(team_elos)
	teamsdf['elo'] = team_elos
	fantasydf = teamsdf
	mendf = fantasydf.loc[fantasydf['sex']=='m']
	#Edit out these next three and the ladies three for pursuit.  One for actual
	
	mendf = mendf.sort_values(by='elo', ascending=False)
	print(len(mendf))
	mendf = mendf[:30]

	mendf['points'] = wc[:len(mendf)]
	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	
	ladiesdf = ladiesdf[:30]
	ladiesdf['points'] = wc[:len(ladiesdf)]
	mendf['place'] = np.arange(1, len(mendf['name'])+1, 1)
	ladiesdf['place'] = np.arange(1,len(ladiesdf['name'])+1,1)
	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)

	return fantasydf


def fis_relay():
	ids = []
	teams = []
	sex = []
	count = 0
	#start with the men
	startlist_list = ["https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=39196",
'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=39195']
	for a in range(len(startlist_list)):
		startlist = BeautifulSoup(urlopen(startlist_list[a]), 'html.parser')
	#print(startlist)
		names = startlist.find_all('div', {'g-lg-14 g-md-14 g-sm-11 g-xs-10 justify-left bold'})
		body = startlist.find_all('div', {'g-lg-2 g-md-2 g-sm-3 hidden-xs justify-right gray pr-1'})
		print(startlist_list[a])

		for b in range(len(body)):
			#print(body[a].text.strip())
			if(b%5!=0):
				ids.append(int(body[b].text.strip()))
			else:
				team = names[b].text.strip()
				if(a==0):
					team = "m"+team
				else:
					team = "f"+team
				ids.append(team)
			#if(count==0):
			#	sex.append('M')
			#else:
			#	sex.append('L')
		
		

		#print(team)
		count+=1

	print(ids)

	

	#now for the ladies
	
	return ids

def fantasy_relay(startlist):
	name = []
	team_name = []
	team_id = []
	team_price = []
	team_sex = []
	ski_id = []
	price =[]
	sex = []
	#sex = startlist['sex']
	#startlist = startlist['id']
	#print(sex)
	#print(startlist)
	fantasy = 'https://www.fantasyxc.se/api/athletes'
	#soup = BeautifulSoup(urlopen(fantasy), 'html5lib')
	#print(soup)
	with requests.Session() as s:
		r=s.get(fantasy)
		soup = BeautifulSoup(r.content, 'html5lib')
	API_json = json.loads(soup.get_text())
	API_df = pd.DataFrame.from_dict(pd.json_normalize(API_json), orient='columns')

	##Change to locate for increased speed
	for a in range(len(startlist)):
		if(a%5==0):
			
			if(startlist[a].startswith("m")):
				#print(startlist[a])
				country_name = startlist[a]
				country_name = country_name.split("m")
				country_name = country_name[1]
				if(country_name.endswith(" I") or country_name.endswith(" II")):
					pass
				else:
					country_name = country_name + " I"

				nation= API_df.loc[API_df['name']==country_name]
				nation = nation.loc[nation['gender']=='m']
				sex.append('m')
				name.append("Male"+country_name)
				#print(nation)
			else:
				country_name = startlist[a]
				country_name = country_name.split("f")
				country_name = country_name[1]
				if(country_name.endswith(" I") or country_name.endswith(" II")):
					pass
				else:
					country_name = country_name + " I"
				nation= API_df.loc[API_df['name']==country_name]
				nation = nation.loc[nation['gender']=='f']
				sex.append('f')
				name.append("Female" + country_name)
			try:
				ski_id.append(nation['athlete_id'].iloc[0])
				price.append(nation['price'].iloc[0])
				#sex.append(nation['gender'].iloc[0])
			except:
				print(country_name)
				ski_id.append(999999)
				price.append(23096)





		else:

			athlete = API_df.loc[API_df['athlete_id']==startlist[a]]
			
			first_name = []
			last_name = []
			try:
				test_name = (athlete['name'].iloc[0])
			except:
				test_name = "NAME Generic" 
			test_name = test_name.split(" ")
			for word in test_name:
				if word.isupper():
					last_name.append(word)
				else:
					first_name.append(word)
			first_name = ' '.join(first_name)
			last_name = ' '.join(last_name)
			test_name = first_name + " " + last_name


			name.append(test_name)
			try:
				ski_id.append(athlete['athlete_id'].iloc[0])
				price.append(athlete['price'].iloc[0])
				sex.append(athlete['gender'].iloc[0])
			except:
				ski_id.append(999999)
				price.append(999999)
				sex.append('mf')
				#pass
				print(test_name)
		
	d = {'name':name, 'id':ski_id, 'price':price, 'sex':sex}
	fantasy_df = pd.DataFrame(data=d)
	return fantasy_df






def elo_relay(fantasydf):
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [i*2 for i in wc]
	skier_elo = []
	team_elos = []
	
	df = pd.read_pickle("~/ski/elo/python/ski/excel365/varmen_distance_k.pkl")
	ladiesdf = pd.read_pickle("~/ski/elo/python/ski/excel365/varladies_distance_k.pkl")
	df = df.append(ladiesdf, ignore_index = True)
	df['name'] = df['name'].str.replace('ø', 'oe')
	df['name'] = df['name'].str.replace('ä', 'ae')
	df['name'] = df['name'].str.replace('æ', 'ae')
	df['name']= df['name'].str.replace('ö', 'oe')
	df['name']= df['name'].str.replace('ü', 'ue')
	df['name']= df['name'].str.replace('å', 'aa')
	df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	df['name'] = df['name'].str.replace('Irineu Esteve Altimiras', 'ireneu esteve altimiras')
	df['name'] = df['name'].str.replace('Thomas Hjalmar Westgaard', 'thomas maloney westgaard')
	df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	df['name'] = df['name'].str.replace('Lauri Lepistoe', 'lauri lepisto')
	df['name'] = df['name'].str.replace('Philip Bellingham', 'phillip bellingham')
	df['name'] = df['name'].str.replace('Snorri Einarsson', 'snorri eythor einarsson')
	df['name'] = df['name'].str.replace('Krista Paermaekoski', 'krista parmakoski')
	df['name'] = df['name'].str.replace('Jessica Diggins', 'jessie diggins')
	df['name'] = df['name'].str.replace('Patricijia Eiduka', 'patricija eiduka')
	df['name'] = df['name'].str.replace('Katri Lylynperae', 'katri lylynpera')
	df['name'] = df['name'].str.replace('Julia Belger', 'julia preussger')
	df['name'] = df['name'].str.replace('Perttu Hyvaerinen', 'perttu hyvarinen')
	df['name'] = df['name'].str.replace('Kathrine Stewart-Jones', 'katherine stewart-jones')
	df['name'] = df['name'].str.replace('Ailja Iksanova', 'alija iksanova')
	df['name'] = df['name'].str.replace('Joni Maeki', 'joni maki')

	teamsdf = fantasydf.iloc[::5, :]
	fantasydf = fantasydf[fantasydf.index % 5 !=0]

	#print(fantasydf)

	fantasy_names = fantasydf['name']
	fantasy_names = fantasy_names.str.lower()
	fantasy_names  = fantasy_names.tolist()
	count = 0
	team_elo = 0
	for a in range(len(fantasy_names)):

		skier = df.loc[df['name'].str.lower() == fantasy_names[a]]
		if(len(skier['name'])==0):
			print(fantasy_names[a])
		#print(skier)
		try:
			elo = skier['elo'].iloc[-1]
			team_elo+=elo
			#skier_elo.append(elo)
		#elo = (skier.loc[skier['date']==20210500]['elo'])
		except:
			print(fantasy_names[a])
			team_elo+=1300

		if(a%4==3):
			team_elos.append(team_elo)
			team_elo = 0
			#skier_elo.append(1300)
	print(team_elos)
	teamsdf['elo'] = team_elos
	fantasydf = teamsdf
	mendf = fantasydf.loc[fantasydf['sex']=='m']
	mendf = mendf.sort_values(by='elo', ascending = False)
	mendf = mendf.reset_index(drop=True)
	#Edit out these next three and the ladies three for pursuit.  One for actual
	
	#mendf = mendf.sort_values(by='elo', ascending=False)
	print(len(mendf))
	#mendf = mendf[:len(mendf)]
	mendf['points'] = wc[:len(mendf)]
	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	ladiesdf = ladiesdf.reset_index(drop=True)


	#ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	#ladiesdf = ladiesdf[:len(team_elos)]
	ladiesdf['points'] = wc[:len(ladiesdf)]
	mendf['place'] = np.arange(1, len(mendf['name'])+1, 1)
	ladiesdf['place'] = np.arange(1,len(ladiesdf['name'])+1,1)
	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)

	return fantasydf

	


def fis():
	ids = []
	sex = []
	'''ids_men = [3422819, 3420586, 3420605, 3420909, 3422619, 3421320, 3482277, 3482280, 3482105, 3482119,
	3481988, 3050342, 3020003, 3100406, 3100409, 3150570, 3150519, 3180535, 3180508, 3180557,
	3181180, 3190529, 3190111, 3190302, 3190268, 3190345, 3200802, 3200205, 3200356, 3200676,
	3220002, 3220016, 3270010, 3290379, 3290407, 3290326, 3290383, 3300494, 3300373, 3670115,
	3430249, 3490145, 3500664, 3501223, 3501741, 3501010, 3501255, 3510479, 3510342, 3510351,
	3510361, 3510023, 3530882, 3530532]'''
	fuzz = pd.read_excel("~/ski/elo/knapsack/excel365/fuzzy_match.xlsx")
	men = fuzz.loc[fuzz['gender']=='m']
	ids_men = list((men['athlete_id']))
	ladies = fuzz.loc[fuzz['gender']=='f']
	ids_ladies = list(ladies['athlete_id'])

	count = 0
	#start with the men
	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=39236',
'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=39235']
	for a in startlist_list:
		startlist = BeautifulSoup(urlopen(a), 'html.parser')
	#print(startlist)
		body = startlist.find_all('div', {'class':'pr-1 g-lg-2 g-md-2 g-sm-2 hidden-xs justify-right gray'})
		print(a)
		if(count==0):
			for a in range(len(ids_men)):
				ids.append(int(ids_men[a]))
		else:
			for a in range(len(ids_ladies)):
				ids.append(int(ids_ladies[a]))
		for b in range(len(body)):
			#print(body[a].text.strip())
			id_check = int(body[b].text.strip())
			if(id_check in ids):
				continue
			else:
				ids.append(int(body[b].text.strip()))
			#if(count==0):
			#	sex.append('M')
			#else:
			#	sex.append('L')
		ids.append
		count+=1

	#now for the ladies
	
	return ids

def fantasy(startlist):
	name = []
	ski_id = []
	price =[]
	sex = []
	#sex = startlist['sex']
	#startlist = startlist['id']
	#print(sex)
	#print(startlist)
	fantasy = 'https://www.fantasyxc.se/api/athletes'
	#soup = BeautifulSoup(urlopen(fantasy), 'html5lib')
	#print(soup)
	with requests.Session() as s:
		r=s.get(fantasy)
		soup = BeautifulSoup(r.content, 'html5lib')
	API_json = json.loads(soup.get_text())
	API_df = pd.DataFrame.from_dict(pd.json_normalize(API_json), orient='columns')

	##Change to locate for increased speed
	for a in range(len(startlist)):
		athlete = API_df.loc[API_df['athlete_id']==startlist[a]]
		
		first_name = []
		last_name = []
		try:
			test_name = (athlete['name'].iloc[0])
		except:
			print(startlist[a])
			continue
		test_name = test_name.split(" ")
		for word in test_name:
			if word.isupper():
				last_name.append(word)
			else:
				first_name.append(word)
		first_name = ' '.join(first_name)
		last_name = ' '.join(last_name)
		test_name = first_name + " " + last_name
		#print(test_name)


		name.append(test_name)
		try:
			ski_id.append(athlete['athlete_id'].iloc[0])
			price.append(athlete['price'].iloc[0])
			sex.append(athlete['gender'].iloc[0])
		except:
			print(test_name)
		
	d = {'name':name, 'id':ski_id, 'price':price, 'sex':sex}
	fantasy_df = pd.DataFrame(data=d)
	return fantasy_df

def pursuit(fantasydf):
	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]

	mendf = fantasydf.loc[fantasydf['sex']=='m']
	mendf = mendf.sort_values(by='elo', ascending=False)
	mendf['pursuit'] = np.arange(1, len(mendf['name'])+1, 1)
	mendf['pursuit'] = .3*mendf['pursuit'] + .7*mendf['place']
	mendf = mendf.sort_values(by='pursuit', ascending=True)
	mendf['pursuit'] = np.arange(1, len(mendf['name'])+1, 1)
	mendf = mendf[:30]
	mendf['points'] = tour

	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	ladiesdf['pursuit'] = np.arange(1,len(ladiesdf['name'])+1,1)
	ladiesdf['pursuit'] = .3*ladiesdf['pursuit'] + .7*ladiesdf['place']
	ladiesdf = ladiesdf.sort_values(by='pursuit', ascending=True)
	ladiesdf['pursuit'] = np.arange(1,len(ladiesdf['name'])+1,1)
	ladiesdf = ladiesdf[:30]
	ladiesdf['points'] = tour

	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)
	return fantasydf


def elo(fantasydf, menpkls, ladiespkls):
	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]
	mendfs = []
	ladiesdfs = []
	fantasydf.loc[:,'points'] =0
	
	#"~/ski/elo/python/ski/ladies/varladies_10_classic.pkl",
	#"~/ski/elo/python/ski/ladies/varladies_sprint_classic.pkl"]

	for a in range(len(menpkls)):
		skier_elo = []
		avg_points= []
		df = pd.read_pickle(menpkls[a])

		#It's all unless it's the first race of the years
		df = df.loc[df['level']=="all"]
		df = get_points(df)
		df = points(df)
		

		menintslope = regress(df)
		
		ladiespkl = pd.read_pickle(ladiespkls[a])
		ladiespkl = ladiespkl.loc[ladiespkl['level']=="all"]
		ladiespkl = get_points(ladiespkl)
		ladiespkl = points(ladiespkl)
		ladiesintslope = regress(ladiespkl)
		df = df.append(ladiespkl, ignore_index = True)
		df['name'] = df['name'].str.replace('ø', 'oe')
		df['name'] = df['name'].str.replace('Ø', 'oe')
		df['name'] = df['name'].str.replace('ä', 'ae')
		df['name'] = df['name'].str.replace('æ', 'ae')
		df['name']= df['name'].str.replace('ö', 'oe')
		df['name']= df['name'].str.replace('ü', 'ue')
		df['name']= df['name'].str.replace('å', 'aa')
		df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
		df['name'] = df['name'].str.replace('Irineu Esteve Altimiras', 'ireneu esteve altimiras')
		df['name'] = df['name'].str.replace('Thomas Hjalmar Westgaard', 'thomas maloney westgaard')
		df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
		df['name'] = df['name'].str.replace('Lauri Lepistoe', 'lauri lepisto')
		df['name'] = df['name'].str.replace('Philip Bellingham', 'phillip bellingham')
		df['name'] = df['name'].str.replace('Snorri Einarsson', 'snorri eythor einarsson')
		df['name'] = df['name'].str.replace('Krista Paermaekoski', 'krista parmakoski')
		df['name'] = df['name'].str.replace('Jessica Diggins', 'jessie diggins')
		df['name'] = df['name'].str.replace('Patricijia Eiduka', 'patricija eiduka')
		df['name'] = df['name'].str.replace('Katri Lylynperae', 'katri lylynpera')
		df['name'] = df['name'].str.replace('Julia Belger', 'julia preussger')
		df['name'] = df['name'].str.replace('Perttu Hyvaerinen', 'perttu hyvarinen')
		df['name'] = df['name'].str.replace('Kathrine Stewart-Jones', 'katherine stewart-jones')
		df['name'] = df['name'].str.replace('Ailja Iksanova', 'alija iksanova')
		df['name'] = df['name'].str.replace('Eric Silfver', 'erik silfver')
		df['name'] = df['name'].str.replace('Joni Maeki', 'joni maki')
		#df['name'] = df['name'].str.replace('H', 'ailja iksanova')



		fantasy_names = fantasydf['name']
		fantasy_names = fantasy_names.str.lower()
		fantasy_names  = fantasy_names.tolist()
		#print(fantasy_names)
		for a in range(len(fantasy_names)):
			skier = df.loc[df['name'].str.lower() == fantasy_names[a]]
			if(len(skier['name'])==0):
				print(fantasy_names[a])
			#print(skier)
			try:
				elo = skier['elo'].iloc[-1]
				avg = skier['avg_points'].iloc[-1]

				skier_elo.append(elo)
				avg_points.append(avg)
			#elo = (skier.loc[skier['date']==20210500]['elo'])
			except:
				print(fantasy_names[a])
				skier_elo.append(1300)
				avg_points.append(0)

			
		fantasydf['elo'] = skier_elo
		fantasydf['avg_points'] = avg_points
		mendf = fantasydf.loc[fantasydf['sex']=='m']
		max_elo = max(mendf['elo'])
		mendf['elopct'] = mendf['elo'].apply(lambda x: 100*(x/max_elo))
		#Edit out these next three and the ladies three for pursuit.  One for actual
		
		mendf = mendf.sort_values(by='elo', ascending=False)
		mendf = mendf.reset_index(drop=True)

		#comment out next block for regression		
		'''
		mendf = mendf[:30]
		men_wc_scores = mendf['points'].tolist()
		men_wc_scores = men_wc_scores[:30]		
		men_wc_scores = np.add(men_wc_scores, wc)		
		men_wc_scores = pd.Series(men_wc_scores)
		mendf.loc[:30, 'points'] = men_wc_scores
		'''


		#Comment out next block for non-regression
	
		mendf['points'] = (menintslope[0]+menintslope[1]*(mendf['avg_points']**(1/np.exp(1)))+ menintslope[2]*mendf['elopct'])**np.exp(1)
		mendf['points'] = mendf['points'].fillna(0)
		
		#print(mendf['points'])
		

		
		
		
		mendfs.append(mendf)
		
		ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
		max_elo = max(ladiesdf['elo'])
		ladiesdf['elopct'] = ladiesdf['elo'].apply(lambda x: 100*(x/max_elo))
		ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
		ladiesdf = ladiesdf.reset_index(drop=True)

		#Next block is for regression
		
		ladiesdf['points'] = ladiesdf.apply(lambda x: (ladiesintslope[0]+ladiesintslope[1]*(ladiesdf['avg_points']**(1/np.exp(1)))+ ladiesintslope[2]*ladiesdf['elopct'])**np.exp(1))
		ladiesdf['points'] = ladiesdf['points'].fillna(0)
		


		#Next block is ladies non-regression
		'''
		ladies_wc_scores = ladiesdf['points'].tolist()
		ladies_wc_scores = ladies_wc_scores[:30]
		ladies_wc_scores = np.add(ladies_wc_scores, wc)
		ladies_wc_scores = pd.Series(ladies_wc_scores)
		ladiesdf.loc[:30, 'points'] = ladies_wc_scores
		'''
		ladiesdfs.append(ladiesdf)

	mendf = fantasydf.loc[fantasydf['sex']=='m']
	mendf = mendf.sort_values(by='id')
	mendf = mendf.reset_index(drop=True)
	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	ladiesdf = ladiesdf.sort_values(by='id')
	ladiesdf = ladiesdf.reset_index(drop=True)
	

	for a in range(len(mendfs)):
		mendfs[a] = mendfs[a].sort_values(by='id')
		mendfs[a] = mendfs[a].reset_index(drop=True)
		elo_name = "elo"+str(a+1)
		race_name = "race"+str(a+1)
		mendf[elo_name] = mendfs[a]['elo']
		mendf[race_name] = mendfs[a]['points']
		mendf['points'] = mendf['points'] + mendf[race_name]
		ladiesdfs[a] = ladiesdfs[a].sort_values(by='id')
		ladiesdfs[a] = ladiesdfs[a].reset_index(drop=True)
		ladiesdf[elo_name] = ladiesdfs[a]['elo']
		ladiesdf[race_name] = ladiesdfs[a]['points']
		ladiesdf['points'] = ladiesdf['points'] + ladiesdf[race_name]







	
	#ladiesdf = ladiesdf[:30]
	#ladiesdf[:30, 'points'] += wc
	mendf=mendf.sort_values(by='points', ascending=False)
	ladiesdf =ladiesdf.sort_values(by='points', ascending=False)
	mendf = mendf[mendf['points']>0]
	ladiesdf = ladiesdf[ladiesdf['points']>0]
	mendf['place'] = np.arange(1, len(mendf['name'])+1, 1)
	ladiesdf['place'] = np.arange(1,len(ladiesdf['name'])+1,1)
	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)

	return fantasydf
			

#The point of pkl_setup is to add points and pct to the pkl
def pkl_setup(pkl):
	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]
	df = pd.read_pickle(pkl)





#The point of regress is to take the pkl from pkl_setup, add a regression to it to get expected points for the current race
#So it should return an intercept and a coefficient
def regress(df):#, pkl):
	

	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]
	#points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	
	#Set what type of race it is
	#points = wc
	#df = df.loc[df['level']=="all"]
	df = df.loc[df['season']>=2018]
	df2 = df

	
	
	#print(df2['pelopct'])
	#print(df2['points'])
	#print("total points", sum(df2['points']))
	df2['points'] = df2['points'].apply(lambda x: x**(1/np.exp(1)))
	df2['pavg_points'] = df2['pavg_points'].apply(lambda x: x**(1/np.exp(1)))
	#print("points", sum(df2['points']))
	
	#x = df2['pelopct'].values.reshape((-1,1))
	x = df2[['pavg_points', 'pelopct']]
	#print(x)
	y = df2['points']
	lm = LinearRegression()
	#lm.fit(x,y)
	lm = LinearRegression().fit(x,y)
	print(lm.intercept_, lm.coef_[0], lm.coef_[1])
	
	return [lm.intercept_, lm.coef_[0], lm.coef_[1]]


	
	#lm = 

def get_points(df):#, pkl):
	

	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]
	#points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	
	#Set what type of race it is
	points = wc
	df = df.loc[df['level']=="all"]
	max_elo = max(df['elo'])
	df = df.loc[df['season']>=2018]
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
			
			#if(season==0 and race==0):
			#	continue
		#else:
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
			racedf['pelopct'] = racedf['pelo'].apply(lambda x: 100*(x/max_pelo))
			#racedf['placepct'] = racedf['place'].apply(lambda x: 1-(x/max_place))
			racedf['points'] = points_list
				


			df2 = df2.append(racedf)

	#The next  line negates the block before.  It uses all elos ever as the pelopct
	#df2['pelopct'] = (df2['pelo']/max_elo)*100
	
	
	return df2

def points(df):
	df2 = pd.DataFrame()
	ids = list(df.id.unique())
	

	for a in range(len(ids)):
		dfid = df.loc[df['id']==ids[a]]
		dfid['race_num'] = list(range(1,len(dfid['id'])+1))
		#dfid['race_num'] = pd.to_numeric(dfid['race_num'])
		dfid['total_points'] = dfid['points'].cumsum()
		#dfid['total_points'] = pd.to_numeric(dfid['total_points'])
		
		dfid['avg_points'] = dfid['total_points']/dfid['race_num']
		
		dfid['pavg_points'] = 0
		dfid['pavg_points'][1:len(dfid['pavg_points'])] = dfid['avg_points'][:len(dfid['avg_points'])-1]


		#dfid['avg_points'] = dfid.apply(lambda x: x['total_points']/x['race_num'])
		df2 = df2.append(dfid)
	return df2		



	#WebDriverWait(driver, 30).until(EC.invisibility_of_element_located((By.XPATH,
	#	"//div[@class='js-off-canvas-overlay is-overlay-fixed']")))


#menpkls = ["~/ski/elo/python/ski/excel365/varmen_sprint_classic_k.pkl",
#"~/ski/elo/python/ski/excel365/varmen_distance_freestyle_k.pkl"]
#"~/ski/elo/python/ski/excel365/varmen_distance_classic_k.pkl"]
menpkls = ["~/ski/elo/python/ski/excel365/varmen_sprint_classic_k.pkl"]#,
	#"~/ski/elo/python/ski/excel365/varmen_distance_freestyle_k.pkl"]
	#"~/ski/elo/python/ski/men/varmen_sprint_classic.pkl"]

#ladiespkls = ["~/ski/elo/python/ski/excel365/varladies_sprint_classic_k.pkl",
#"~/ski/elo/python/ski/excel365/varladies_distance_freestyle_k.pkl"]
#"~/ski/elo/python/ski/excel365/varladies_distance_classic_k.pkl"]
ladiespkls = ["~/ski/elo/python/ski/excel365/varladies_sprint_classic_k.pkl"]#,
	#"~/ski/elo/python/ski/excel365/varladies_distance_freestyle_k.pkl"]

#df = pd.read_pickle(ladiespkls[0])
#regress(df)

startlist = fis()
#startlist = fis_relay()
#startlist = fis_team_sprint()

fantasydf = (fantasy(startlist))
#fantasydf = fantasy_relay(startlist)
#fantasydf = fantasy_team_sprint(startlist)
#with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
 #   print(fantasydf)
#print(fantasydf)
#print(fantasydf)

fantasydf = elo(fantasydf, menpkls, ladiespkls)
#fantasydf = pursuit(fantasydf)
#fantasydf = elo_relay(fantasydf)
print(fantasydf)
#fantasydf = elo_team_sprint(fantasydf)
#print(fantasydf)


fantasydf.to_pickle("~/ski/elo/knapsack/points/excel365/fantasydf_falun.pkl")
fantasydf.to_excel("~/ski/elo/knapsack/points/excel365/fantasydf_falun.xlsx")

print(time.time() - start_time)

