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





	#return {'id':ids, 'sex':sex}





	


def fis():
	ids = []
	sex = []
	count = 0
	#start with the men
	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=104435']
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



def elo(startlist):
	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]
	dfs = []
	ladiesdfs = []
	#fantasydf.loc[:,'points'] =0
	
	#"~/ski/elo/python/ski/men/varmen_15_classic.pkl",
	#"~/ski/elo/python/ski/men/varmen_sprint_classic.pkl"]

	pkls = ["~/ski/elo/python/ski/ladies/varladies_all.pkl"]
	#"~/ski/elo/python/ski/ladies/varladies_10_classic.pkl",
	#"~/ski/elo/python/ski/ladies/varladies_sprint_classic.pkl"]

	for a in range(len(pkls)):
		skier_elo = []
		df = pd.read_pickle(pkls[a])
		
		
		df['name'] = df['name'].str.replace('ø', 'oe')
		df['name'] = df['name'].str.replace('Ø', 'oe')
		df['name'] = df['name'].str.replace('ä', 'ae')
		df['name'] = df['name'].str.replace('æ', 'ae')
		df['name']= df['name'].str.replace('ö', 'oe')
		df['name']= df['name'].str.replace('ü', 'ue')
		df['name']= df['name'].str.replace('å', 'aa')
		df['name'] = df['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
		
		#df['name'] = df['name'].str.replace('H', 'ailja iksanova')



		fantasy_names = startlist['name']
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
			
		startlist['elo'] = skier_elo
		
		#Edit out these next three and the ladies three for pursuit.  One for actual
		
		df = df.sort_values(by='elo', ascending=False)
		df = df.reset_index(drop=True)
		
		#mendf = mendf[:30]
		wc_scores = df['points'].tolist()
		wc_scores = df[:30]
		
		wc_scores = np.add(wc_scores, wc)
		
		wc_scores = pd.Series(wc_scores)
		
		df.loc[:30, 'points'] = df_scores
		dfs.append(df)
		
		
		

	







	
	#ladiesdf = ladiesdf[:30]
	#ladiesdf[:30, 'points'] += wc
	

	return dfs
			




	#WebDriverWait(driver, 30).until(EC.invisibility_of_element_located((By.XPATH,
	#	"//div[@class='js-off-canvas-overlay is-overlay-fixed']")))

startlist = fis()





startlist = elo(startlist)

#print(fantasydf)

fantasydf.to_pickle("~/ski/elo/python/alpine/startlist.pkl")
fantasydf.to_excel("~/ski/elo/python/alpine/startlist.xlsx")
print(time.time() - start_time)

