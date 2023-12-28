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
import math
import time
import json
from pandas.io.json import json_normalize
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 

import time
import traceback
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import Levenshtein as lev





#1. Read fantasy_api.xlsx and chrono_regress
#2. Create new dataframe which is fantasy_api.xlsx that has fantasy_name, fantasy_athlete_id
#3. Match fantasy_name to chrono_regress name
#4. Save a datafame with the two names and the two ids



def name_match(men_chrono, ladies_chrono, fantasy_men, fantasy_ladies):

	fantasy_men_name = []
	
	for a in range(len(fantasy_men['id'])):
		athlete = fantasy_men.loc[fantasy_men['id']==fantasy_men['id'].iloc[a]]
		first_name = []
		last_name = []
		try:
			test_name = (athlete['name'].iloc[0])
		except:
			#print(startlist[a])
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
		fantasy_men_name.append(test_name)
	fantasy_men['name'] = fantasy_men_name

	fantasy_ladies_name = []
	print(fantasy_ladies['id'])
	for a in range(len(fantasy_ladies['id'])):
		athlete = fantasy_ladies.loc[fantasy_ladies['id']==fantasy_ladies['id'].iloc[a]]
		first_name = []
		last_name = []
		try:
			test_name = (athlete['name'].iloc[0])
		except:
			#print(startlist[a])
			continue
		try:
			test_name = test_name.split(" ")
		except:
			test_name = ["a", "b"]
		for word in test_name:
			if word.isupper():
				last_name.append(word)
			else:
				first_name.append(word)
		first_name = ' '.join(first_name)
		last_name = ' '.join(last_name)
		test_name = first_name + " " + last_name
		fantasy_ladies_name.append(test_name)
	fantasy_ladies['name'] = fantasy_ladies_name
	fantasy_men['name'] = fantasy_men['name'].str.lower()
	fantasy_ladies['name'] = fantasy_ladies['name'].str.lower()
	men_chrono['name'] = men_chrono['name'].str.lower()
	men_chrono['name'] = men_chrono['name'].str.replace('ø', 'oe')
	men_chrono['name'] = men_chrono['name'].str.replace('ä', 'ae')
	men_chrono['name'] = men_chrono['name'].str.replace('æ', 'ae')
	men_chrono['name']= men_chrono['name'].str.replace('ö', 'oe')
	men_chrono['name']= men_chrono['name'].str.replace('ü', 'ue')
	men_chrono['name']= men_chrono['name'].str.replace('å', 'aa')
	

	ladies_chrono['name'] = ladies_chrono['name'].str.lower()
	ladies_chrono['name'] = ladies_chrono['name'].str.lower()
	ladies_chrono['name'] = ladies_chrono['name'].str.replace('ø', 'oe')
	ladies_chrono['name'] = ladies_chrono['name'].str.replace('ä', 'ae')
	ladies_chrono['name'] = ladies_chrono['name'].str.replace('æ', 'ae')
	ladies_chrono['name']= ladies_chrono['name'].str.replace('ö', 'oe')
	ladies_chrono['name']= ladies_chrono['name'].str.replace('ü', 'ue')
	ladies_chrono['name']= ladies_chrono['name'].str.replace('å', 'aa')
	

	#make name list for fantasy and chrono.  Go through fantasy_men and fuzzy match the highest level from chrono.  If not 100, print
	fantasy_men_name = fantasy_men['name'].tolist()
	fantasy_men_id = fantasy_men['id'].tolist()
	fantasy_men_sex = ['M']
	fantasy_men_sex = fantasy_men_sex*len(fantasy_men_id)
	men_chrono_name = men_chrono['name'].tolist()
	fuzzy_men_chrono_name = []
	fuzzy_men_chrono_id = []
	for a in range(len(fantasy_men_name)):
		fuzzz = (process.extractOne(fantasy_men_name[a], men_chrono_name, scorer=fuzz.token_sort_ratio))
		if(fuzzz[1]==100):
			skier = fuzzz[0]
			athlete = men_chrono.loc[men_chrono['name']==skier]['name'].values[-1]
			fuzzy_men_chrono_name.append(athlete)
			athlete_id = men_chrono.loc[men_chrono['name']==skier]['id'].values[-1]
			fuzzy_men_chrono_id.append(athlete_id)
		else:
			print(fantasy_men_name[a], fuzzz[1])
			skier = fuzzz[0]
			athlete = men_chrono.loc[men_chrono['name']==skier]['name'].values[-1]
			fuzzy_men_chrono_name.append(athlete)
			athlete_id = men_chrono.loc[men_chrono['name']==skier]['id'].values[-1]
			fuzzy_men_chrono_id.append(athlete_id)
	fantasy_ladies_name = fantasy_ladies['name'].tolist()
	fantasy_ladies_id = fantasy_ladies['id'].tolist()
	fantasy_ladies_sex = ['L']
	fantasy_ladies_sex = fantasy_ladies_sex*len(fantasy_ladies_id)
	ladies_chrono_name = ladies_chrono['name'].tolist()
	fuzzy_ladies_chrono_name = []
	fuzzy_ladies_chrono_id = []
	for a in range(len(fantasy_ladies_name)):
		fuzzz = (process.extractOne(fantasy_ladies_name[a], ladies_chrono_name, scorer=fuzz.token_sort_ratio))
		if(fuzzz[1]==100):
			skier = fuzzz[0]
			athlete = ladies_chrono.loc[ladies_chrono['name']==skier]['name'].values[0]
			fuzzy_ladies_chrono_name.append(athlete)
			athlete_id = ladies_chrono.loc[ladies_chrono['name']==skier]['id'].values[0]
			fuzzy_ladies_chrono_id.append(athlete_id)
		else:
			print(fantasy_ladies_name[a], fuzzz[1])
			skier = fuzzz[0]
			athlete = ladies_chrono.loc[ladies_chrono['name']==skier]['name'].values[-1]
			fuzzy_ladies_chrono_name.append(athlete)
			athlete_id = ladies_chrono.loc[ladies_chrono['name']==skier]['id'].values[-1]
			fuzzy_ladies_chrono_id.append(athlete_id)
	
	retdf = pd.DataFrame()
	fantasy_names = fantasy_men_name+fantasy_ladies_name
	chrono_names = fuzzy_men_chrono_name+fuzzy_ladies_chrono_name
	fantasy_id = fantasy_men_id+fantasy_ladies_id
	chrono_id = fuzzy_men_chrono_id+fuzzy_ladies_chrono_id
	sex = fantasy_men_sex+fantasy_ladies_sex
	retdf['fantasy_names'] = fantasy_names
	retdf['fantasy_id'] = fantasy_id
	retdf['chrono_names'] = chrono_names
	retdf['chrono_id'] = chrono_id
	retdf['sex'] = sex
	return retdf








#1. Ready fantasy_api.xlsx and chrono_regress
#2. Create new data frame which is fantasy_api.xlsx that has fantasy_name, fantasy_athlete_id
men_chrono = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/age/excel365/men_chrono_regress.pkl")
ladies_chrono = pd.read_pickle("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress.pkl")
men_chrono = men_chrono.loc[men_chrono['season']>=2010]
ladies_chrono = ladies_chrono.loc[ladies_chrono['season']>=2010]
print(ladies_chrono)

fantasy = pd.read_excel("~/ski/elo/knapsack/fantasy_api.xlsx")
fantasy = fantasy.loc[fantasy['is_team']==False]
fantasy_men = fantasy.loc[fantasy['gender']=='m']
fantasy_ladies = fantasy.loc[fantasy['gender']=='f']
fantasy_mendf = pd.DataFrame()
fantasy_mendf['name'] = fantasy_men['name']
fantasy_mendf['id'] = fantasy_men['athlete_id']
fantasy_ladiesdf = pd.DataFrame()
fantasy_ladiesdf['name'] = fantasy_ladies['name']
fantasy_ladiesdf['id'] = fantasy_ladies['athlete_id']
fantasy_men = fantasy_mendf
fantasy_ladies = fantasy_ladiesdf
men_chronodf = pd.DataFrame()
chrono_id_list = pd.unique(men_chrono['id']).tolist()
chrono_name_list = []
for a in range(len(chrono_id_list)):
	name = men_chrono.loc[men_chrono['id']==chrono_id_list[a]]
	name = name['name'].iloc[-1]
	chrono_name_list.append(name)
men_chronodf['name'] = chrono_name_list
men_chronodf['id'] = chrono_id_list
ladies_chronodf = pd.DataFrame()
chrono_id_list = pd.unique(ladies_chrono['id']).tolist()
chrono_name_list = []
for a in range(len(chrono_id_list)):
	name = ladies_chrono.loc[ladies_chrono['id']==chrono_id_list[a]]
	name = name['name'].iloc[-1]
	chrono_name_list.append(name)
ladies_chronodf['name'] = chrono_name_list
ladies_chronodf['id'] = chrono_id_list
men_chrono = men_chronodf
ladies_chrono = ladies_chronodf
print(ladies_chrono)



#3. Match fantasy_name to chrono_regress name
names_df = name_match(men_chrono, ladies_chrono, fantasy_men, fantasy_ladies)
names_df.to_pickle("~/ski/elo/knapsack/excel365/namelist.pkl")
names_df.to_excel("~/ski/elo/knapsack/excel365/namelist.xlsx")












