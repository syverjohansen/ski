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
pd.options.mode.chained_assignment = None

import time
start_time = time.time()


racedf = pd.read_pickle("~/ski/elo/python/ski/excel365/varmen_distance.pkl")


def mock_relay(racedf):
	team = dict()
	team_size = dict()
	for a in range(len(racedf['nation'])):
		
		if(racedf['nation'][a] not in team):
			
			team[racedf['nation'][a]] = racedf['elo'][a]
			
			team_size[racedf['nation'][a]]=1
		elif(team_size[racedf['nation'][a]]<4):
			team[racedf['nation'][a]] += racedf['elo'][a]
			team_size[racedf['nation'][a]]+=1
		else:
			pass
	return team


racedf = racedf.loc[racedf['date']==20210500]
racedf = racedf.sort_values(by="elo", ascending=False)
racedf = racedf.reset_index(drop = True)
racedf = mock_relay(racedf)

racedf = pd.DataFrame(list(racedf.items()), columns = ["nation", 'elo'])
racedf = racedf.sort_values(by="elo", ascending=False)
racedf = racedf.reset_index(drop = True)
racedf.to_pickle("~/ski/elo/knapsack/excel365/mock_relay_distance.pkl")
racedf.to_excel("~/ski/elo/knapsack/excel365/mock_relay_distance.xlsx")
