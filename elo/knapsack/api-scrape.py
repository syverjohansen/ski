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

fantasy = 'https://www.fantasyxc.se/api/athletes'

with requests.Session() as s:
	r=s.get(fantasy)
	soup = BeautifulSoup(r.content, 'html5lib')
	API_json = json.loads(soup.get_text())
	API_df = pd.DataFrame.from_dict(pd.json_normalize(API_json), orient='columns')

print(API_df)

#API_df.to_pickle("~/ski/elo/knapsack/excel365/fantasydf_lahti.pkl")
API_df.to_excel("~/ski/elo/knapsack/fantasy_api.xlsx")