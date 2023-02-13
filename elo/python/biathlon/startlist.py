from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import pandas as pd
import time
import json
from pandas.io.json import json_normalize
#We will have to use selenium

def fis():
	ids = []
	sex = []
	count = 0
	#start with the men
	startlist_list = ['https://www.biathlonworld.com/competitions/world-cup/events/bmw-ibu-world-cup-biathlon-oberhof-2021/bt2021swrlcp05/race/women-75km-sprint/bt2021swrlcp05swsp/',
'https://www.biathlonworld.com/competitions/world-cup/events/bmw-ibu-world-cup-biathlon-oberhof-2021/bt2021swrlcp05/race/men-10km-sprint/bt2021swrlcp05smsp/']
	for a in startlist_list:
		startlist = BeautifulSoup(urlopen(a), 'html.parser')
	#print(startlist)
		body = startlist.find_all('div', {'class':'pr-1 g-lg-2 g-md-2 g-sm-2 hidden-xs justify-right gray'})
		print(a)

		for b in range(len(body)):
			#print(body[a].text.strip())
			ids.append(int(body[b].text.strip()))
			#if(count==0):
			#	sex.append('M')
			#else:
			#	sex.append('L')
		count+=1

	#now for the ladies
	
	return ids
	#return {'id':ids, 'sex':sex}

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
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 2, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	mendf = fantasydf.loc[fantasydf['sex']=='m']
	mendf = mendf.sort_values(by='elo', ascending=False)
	mendf['pursuit'] = np.arange(1, len(mendf['name'])+1, 1)
	mendf['pursuit'] = .3*mendf['pursuit'] + .7*mendf['place']
	mendf = mendf.sort_values(by='pursuit', ascending=True)
	mendf['pursuit'] = np.arange(1, len(mendf['name'])+1, 1)
	mendf = mendf[:30]
	mendf['points'] = stage

	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	ladiesdf['pursuit'] = np.arange(1,len(ladiesdf['name'])+1,1)
	ladiesdf['pursuit'] = .3*ladiesdf['pursuit'] + .7*ladiesdf['place']
	ladiesdf = ladiesdf.sort_values(by='pursuit', ascending=True)
	ladiesdf['pursuit'] = np.arange(1,len(ladiesdf['name'])+1,1)
	ladiesdf = ladiesdf[:30]
	ladiesdf['points'] = stage

	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)
	return fantasydf

def elo(fantasydf):
	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 2, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	skier_elo = []
	df = pd.read_pickle("~/ski/elo/python/ski/men/varmen.pkl")
	ladiesdf = pd.read_pickle("~/ski/elo/python/ski/ladies/varladies.pkl")
	df = df.append(ladiesdf, ignore_index = True)
	df['name'] = df['name'].str.replace('ø', 'oe')

	df['name'] = df['name'].str.replace('ä', 'ae')
	df['name'] = df['name'].str.replace('æ', 'ae')
	df['name']= df['name'].str.replace('ö', 'oe')
	df['name']= df['name'].str.replace('ü', 'ue')
	df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	df['name'] = df['name'].str.replace('Irineu Esteve Altimiras', 'ireneu esteve altimiras')
	df['name'] = df['name'].str.replace('Thomas Hjalmar Westgård', 'thomas maloney westgaard')
	df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	df['name'] = df['name'].str.replace('Lauri Lepistoe', 'lauri lepisto')
	df['name'] = df['name'].str.replace('Philip Bellingham', 'phillip bellingham')
	df['name'] = df['name'].str.replace('Snorri Einarsson', 'snorri eythor einarsson')
	df['name'] = df['name'].str.replace('Krista Paermaekoski', 'krista parmakoski')
	df['name'] = df['name'].str.replace('Jessica Diggins', 'jessie diggins')
	df['name'] = df['name'].str.replace('Patricijia Eiduka', 'patricija eiduka')
	df['name'] = df['name'].str.replace('Katri Lylynperae', 'katri lylynpera')
	df['name'] = df['name'].str.replace('Julia Belger', 'julia preussger')


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
			skier_elo.append(elo)
		#elo = (skier.loc[skier['date']==20210500]['elo'])
		except:
			print(fantasy_names[a])
			skier_elo.append(1300)
		
	fantasydf['elo'] = skier_elo
	mendf = fantasydf.loc[fantasydf['sex']=='m']
	#Edit out these next three and the ladies three for pursuit.  One for actual
	
	mendf = mendf.sort_values(by='elo', ascending=False)
	mendf = mendf[:30]
	mendf['points'] = stage
	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	ladiesdf = ladiesdf[:30]
	ladiesdf['points'] = stage
	mendf['place'] = np.arange(1, len(mendf['name'])+1, 1)
	ladiesdf['place'] = np.arange(1,len(ladiesdf['name'])+1,1)
	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)

	return fantasydf
			




	#WebDriverWait(driver, 30).until(EC.invisibility_of_element_located((By.XPATH,
	#	"//div[@class='js-off-canvas-overlay is-overlay-fixed']")))

startlist = fis()
#print(startlist)
#print(startlist)
fantasydf = (fantasy(startlist))
fantasydf = elo(fantasydf)
#fantasydf = pursuit(fantasydf)

fantasydf.to_pickle("~/ski/elo/knapsack/fantasydf.pkl")
fantasydf.to_excel("~/ski/elo/knapsack/fantasydf.xlsx")

