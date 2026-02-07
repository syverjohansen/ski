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
from sklearn.linear_model import LogisticRegression
#import statsmodels.api as sm
#import matplotlib.pyplot as plt
import math
import time
import json
from pandas.io.json import json_normalize
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 

import time
import traceback
start_time = time.time()


#For individuals
#1. Read Chrono Regress
#2. Scrape ids from Fis
#3. Convert startlist names to Elo names
#4. Take Elo Ids and filter out the Chrono Regress to those Ids
#5. Save sheet with Name, Id, and select columns from chrono regress so that the R file can do the actual regress.




#1. Read chrono_regress.
men_chrono = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/age/excel365/men_chrono_regress.pkl")
ladies_chrono = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress.pkl")
namelist = pd.read_pickle("/Users/syverjohansen/ski/elo/knapsack/excel365/namelist.pkl")

men_pkl= pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/age/excel365/men_chrono_regress_distance.pkl")
ladies_pkl = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress_distance.pkl")


#2. Scrape ids from Fis.
def ids_scrape():
	everyone = pd.read_excel("~/ski/elo/knapsack/fantasy_api.xlsx")
	print(everyone)
	everyone = everyone.loc[everyone['active']==True]
	everyone = everyone.loc[everyone['is_team']==False]
	everyone = everyone.loc[everyone['country']!='RUS']
	all_ladies = everyone.loc[everyone['gender']=='f']
	all_ladies_ids = all_ladies['athlete_id'].tolist()
	all_men = everyone.loc[everyone['gender']=='m']
	all_men_ids = all_men['athlete_id'].tolist()



	fuzz = pd.read_excel("~/ski/elo/knapsack/excel365/fuzzy_match.xlsx")
	men = fuzz.loc[fuzz['gender']=='m']
	ids_men = list((men['athlete_id']))
	ladies = fuzz.loc[fuzz['gender']=='f']
	ids_ladies = list(ladies['athlete_id'])
	ids_men =all_men_ids
	ids_ladies=all_ladies_ids
	#ids_men = []
	#ids_ladies = []



	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=41613',
	'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=41612']

	ids = []
	count = 0
	for a in startlist_list:
		startlist = BeautifulSoup(urlopen(a), 'html.parser')
	#print(startlist)
		body = startlist.find_all('div', {'class':'pr-1 g-lg-2 g-md-2 g-sm-2 hidden-xs justify-right gray'})
		#print(a)
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


#4
def get_startlist(namelist, ids, men_chrono, ladies_chrono, men_pkl, ladies_pkl):
	startlist = namelist[namelist['fantasy_id'].isin(ids)]
	startlist_men = startlist.loc[startlist['sex']=="M"]
	startlist_men = men_chrono[men_chrono['id'].isin(startlist_men['chrono_id'])]
	
	startlist_ladies = startlist.loc[startlist['sex']=="L"]
	startlist_ladies = ladies_chrono[ladies_chrono['id'].isin(startlist_ladies['chrono_id'])]

	startlist_pkl = namelist[namelist['fantasy_id'].isin(ids)]
	startlist_men_pkl = startlist_pkl.loc[startlist_pkl['sex']=="M"]
	startlist_men_pkl = men_pkl[men_chrono['id'].isin(startlist_men_pkl['chrono_id'])]
	
	startlist_ladies_pkl = startlist_pkl.loc[startlist_pkl['sex']=="L"]
	startlist_ladies_pkl = ladies_pkl[ladies_pkl['id'].isin(startlist_ladies_pkl['chrono_id'])]


	startlist_men = startlist_men.groupby(by=['id'], as_index=False).last()
	startlist_ladies = startlist_ladies.groupby(by=['id'], as_index=False).last()

	startlist_men_pkl = startlist_men_pkl.groupby(by=['id'], as_index=False).last()
	startlist_ladies_pkl = startlist_ladies_pkl.groupby(by=['id'], as_index=False).last()

	id_list = startlist_men['id'].tolist()
	for a in range(len(id_list)):
		athlete = startlist_men_pkl.loc[startlist_men_pkl['id']==id_list[a]]
		try:
			startlist_men['pavg_points'][a] = athlete['pavg_points'].iloc[0]
		except:
			startlist_men['pavg_points'][a] = 0
	id_list = startlist_ladies['id'].tolist()
	for a in range(len(id_list)):
		athlete = startlist_ladies_pkl.loc[startlist_ladies_pkl['id']==id_list[a]]
		try:
			startlist_ladies['pavg_points'][a] = athlete['pavg_points'].iloc[0]
		except:
			startlist_ladies['pavg_points'][a] = 0

	#startlist_men = startlist_men.groupby('id')['date'].transform('max')
	#startlist_ladies = startlist_ladies.groupby('id')['date'].transform('max')
	return [startlist_men, startlist_ladies]


	



#3. Name convertor
#4. Filter out namelist to startlist



ids = ids_scrape()
startlists = get_startlist(namelist, ids, men_chrono, ladies_chrono, men_pkl, ladies_pkl)
startlist_men = startlists[0]
startlist_ladies = startlists[1]

startlist_men.to_pickle("~/ski/elo/knapsack/excel365/startlist_men.pkl")
startlist_men.to_excel("~/ski/elo/knapsack/excel365/startlist_men.xlsx")

startlist_ladies.to_pickle("~/ski/elo/knapsack/excel365/startlist_ladies.pkl")
startlist_ladies.to_excel("~/ski/elo/knapsack/excel365/startlist_ladies.xlsx")


print(time.time() - start_time)


