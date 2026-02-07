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
	ids_men = []
	ids_ladies =[]

	#start with the men
	#There will be 4 because of the semifinals
	startlist_list = ["https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=44216",
	"https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=44215"]
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
				if(a<1):
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
		

	#print(ids)

	'''for a in range(len(startlist_list)):
		startlist = BeautifulSoup(urlopen(startlist_list[a]), 'html.parser')
	#print(startlist)
		body = startlist.find_all('div', {'class':'pr-1 g-lg-2 g-md-2 g-sm-2 hidden-xs justify-right gray'})
		#print(body)
		for b in range(len(body)):
			#print(b)
			if(a==0):
				#print(int(body[b].text.strip()))
				ids_men.append(int(body[b].text.strip()))
			else:
				ids_ladies.append(int(body[b].text.strip()))

	print(len(ids_men))
	

	male_team_ids = ["mNORWAY I", "mITALY I", "mSWEDEN I", "mFINLAND I", "mGERMANY I", "mFRANCE I", "mCZECH REPUBLIC I", "mCANADA I", "mUNITED STATES OF AMERICA I", "mGREAT BRITAIN I", "mJAPAN I", "mSWITZERLAND I", "mROMANIA I", "mPOLAND I", "mESTONIA I", "mAUSTRIA I", "mKAZAKHSTAN I", "mBULGARIA I",  "mSLOVENIA I", "mLATVIA I", "mICELAND I", "mAUSTRALIA I", "mUKRAINE I", 'mMACEDONIA I', "mBOSNIA AND HERZEGOVINA I", "mHUNGARY I", "mCROATIA I", "mARGENTINA I", "mLITHUANIA I", "mTURKEY I", "mGREECE I", "mHAITI I", "mCHILE I", "mPORTUGAL I"]
	print(len(male_team_ids))
	ladies_team_ids = ["fSWEDEN I", "fNORWAY I", "fUNITED STATES OF AMERICA I", "fFINLAND I", "fGERMANY I", "fSWITZERLAND I", "fSLOVENIA I", "fCZECH REPUBLIC I", "fESTONIA I", "fFRANCE I", "fCANADA I", "fPOLAND I", "fITALY I", "fKAZAKHSTAN I", "fLATVIA I", "fARGENTINA I", "fUKRAINE I", "fCROATIA I", "fAUSTRALIA I", "fICELAND I", "fSLOVAKIA I", "fGREECE I", "fMONGOLIA I", "fLITHUANIA I", "fMACEDONIA I", "fBRAZIL I"]
	print(len(ladies_team_ids))
	count = 0
	male_team_ids2 = male_team_ids+ids_men
	print(len(male_team_ids2))
	for a in range(len(male_team_ids)):
		ids.append(male_team_ids2[a])
		ids.append(male_team_ids2[a+34])
		ids.append(male_team_ids2[a+68])

	ladies_team_ids2 = ladies_team_ids+ids_ladies
	for a in range(len(ladies_team_ids)):
		ids.append(ladies_team_ids2[a])
		ids.append(ladies_team_ids2[a+26])
		ids.append(ladies_team_ids2[a+52])

	#now for the ladies
	print(ids)
	return ids'''
	

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
				if(country_name == "ROC I"):
					country_name = "RUSSIA I"
				elif(country_name == "P.R. CHINA I"):
					country_name = "PEOPLES REPUBLIC OF CHINA I"
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
				if(country_name == "ROC I"):
					country_name = "RUSSIA I"
				elif(country_name == "P.R. CHINA I"):
					country_name = "PEOPLES REPUBLIC OF CHINA I"
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

def elo_team_sprint(fantasydf, menpkls, ladiespkls):
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [i*2 for i in wc]
	skier_elo = []
	team_elos = []
	fantasydf.loc[:, 'points'] = 0
	menrelaypkls = ["~/ski/elo/python/ski/age/relay/excel365/varmen_sprint_freestyle_k.pkl"]
	ladiesrelaypkls = ["~/ski/elo/python/ski/age/relay/excel365/varladies_sprint_freestyle_k.pkl"]


	for a in range(len(menpkls)):
		skier_elo = []
		df = pd.read_pickle(menpkls[a])
		df = df.loc[df['level']=="all"]
		
		
		ladiespkl = pd.read_pickle(ladiespkls[a])
		ladiespkl = ladiespkl.loc[ladiespkl['level']=="all"]
		df = df.append(ladiespkl, ignore_index = True)
		

		team_elos = []
	
		
		df['name'] = df['name'].str.replace('ø', 'oe')
		df['name'] = df['name'].str.replace('ä', 'ae')
		df['name'] = df['name'].str.replace('æ', 'ae')
		df['name']= df['name'].str.replace('ö', 'oe')
		df['name']= df['name'].str.replace('ü', 'ue')
		df['name']= df['name'].str.replace('å', 'aa')
		df['name']= df['name'].str.replace('Ã¸', 'oe')
		df['name']= df['name'].str.replace('Ã¤', 'ae')
		df['name']= df['name'].str.replace('Ã¼', 'ue')
		df['name'] = df['name'].str.replace('Ã¶', 'oe')
		df['name'] = df['name'].str.replace('Ã', 'oe')
		df['name'] = df['name'].str.replace('Ã¦', 'ae')
		df['name']= df['name'].str.replace('Ã¥', 'aa')
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

	teamsdf = fantasydf.iloc[::3, :]
	print(teamsdf)
	fantasydf = fantasydf[fantasydf.index % 3 !=0]

	#print(fantasydf)

	fantasy_names = fantasydf['name']
	fantasy_names = fantasy_names.str.lower()
	fantasy_names  = fantasy_names.tolist()
	count = 0
	team_elo = 0
	for b in range(len(fantasy_names)):

		skier = df.loc[df['name'].str.lower() == fantasy_names[b]]
		if(len(skier['name'])==0):
			print("Name not registered", fantasy_names[b])
		#print(skier)
		try:

			elo = skier['elo'].iloc[-1]
			print(fantasy_names[b], elo)
			team_elo+=elo
			#skier_elo.append(elo)
		#elo = (skier.loc[skier['date']==20210500]['elo'])
		except:
			print(fantasy_names[b])
			team_elo+=1300

		if(b%2==1):
			team_elos.append(team_elo)
			team_elo = 0
			#skier_elo.append(1300)
	
	teamsdf['elo'] = team_elos
	fantasydf = teamsdf
	
	menrelaypkl = pd.read_pickle(menrelaypkls[a])
	menrelaypkl = menrelaypkl.loc[menrelaypkl['level']=="all"]
	menintslope = regress_relay(menrelaypkl)
	mendf = fantasydf.loc[fantasydf['sex']=='m']
	max_elo = max(mendf['elo'])
	mendf['elopct'] = mendf['elo'].apply(lambda x: 100*(x/max_elo))
	mendf = mendf.sort_values(by='elo', ascending = False)
	mendf = mendf.reset_index(drop=True)
	#Edit out these next three and the ladies three for pursuit.  One for actual
	
	#mendf = mendf.sort_values(by='elo', ascending=False)
	
	#mendf = mendf[:len(mendf)]
	mendf['points'] = mendf['elopct'].apply(lambda x: (menintslope[0]+menintslope[1]*x)**np.exp(1))
	mendf['points'] = mendf['points'].fillna(0)
	mendf['points'] = mendf['points']/max(mendf['points'])
	mendf['place'] = np.arange(1, len(mendf['name'])+1, 1)
	ladiesrelaypkl = pd.read_pickle(ladiesrelaypkls[a])
	ladiesrelaypkl = ladiesrelaypkl.loc[ladiesrelaypkl['level']=="all"]
	ladiesintslope = regress_relay(ladiesrelaypkl)
	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	max_elo = max(ladiesdf['elo'])
	ladiesdf['elopct'] = ladiesdf['elo'].apply(lambda x: 100*(x/max_elo))
	ladiesdf = ladiesdf.sort_values(by='elo', ascending = False)
	ladiesdf = ladiesdf.reset_index(drop=True)
	#Edit out these next three and the ladies three for pursuit.  One for actual

	#ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	print(len(ladiesdf))
	#ladiesdf = ladiesdf[:len(ladiesdf)]
	ladiesdf['points'] = ladiesdf['elopct'].apply(lambda x: (ladiesintslope[0]+ladiesintslope[1]*x)**np.exp(1))
	ladiesdf['points'] = ladiesdf['points'].fillna(0)
	ladiesdf['points'] = ladiesdf['points']/max(ladiesdf['points'])
	ladiesdf['place'] = np.arange(1, len(ladiesdf['name'])+1, 1)
	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)

	return fantasydf


def fis_mixed_ts():
	'''men_ids = [3420909, 3420605, 3420586, 3422819, 34819883, 3482119, 3482280, 3482277, 3180535, 3180861,
	3180557, 3180508, 3530882, 3530532, 3530902, 3530911, 3200802, 3200205, 3200356, 3200676,
	3501741, 3501223, 3500664, 3501010, 3510342, 3510023, 3510361, 3510351, 3150570, 3150519, 
	3150664, 3150637, 3100406, 3100409, 3100412, 1111111, 3190529, 3190111, 3190302, 3190345,
	3290379, 3290326, 3290407, 3290504, 1111111, 1111111, 3300494, 3300373, 3430249, 3430233,
	3430276, 3430103, 3390167, 3390240, 3390169, 3390207, 1111111, 1111111, 1111111, 1111111,
	3040101, 3040125, 1111111, 1111111, 1111111, 3560145, 3560101, 3560121]'''
	

	men_ids = [3500664, 3421154, 3501255, 3510588, 3190323, 3426456, 3200676, 3180865, 3200356, 3180861,
	3050267, 9999999999999, 3560145, 3290326, 3550147, 3100412, 3100409, 99999999999]
	ladies_ids = [3505809, 3426201, 3506105, 3510619, 3195263, 3426200, 3205305, 3185579, 3205407, 3185551, 
	3050155, 3195205, 3560121, 999999999, 3555052, 3105146, 3105179, 999999999]

	ids = []
	'''men_teams = ["mNORWAY I", "mRUSSIA I", "mFINLAND I", "mUNITED STATES OF AMERICA I", "mGERMANY I", "mSWEDEN I", "mSWITZERLAND I", "mCZECH REPUBLIC I", "mCANADA I", "mFRANCE I",
	"mITALY I", "mJAPAN I", "mPOLAND I", "mESTONIA I", "mPEOPLES REPUBLIC OF CHINA I", "mAUSTRALIA I", "mSLOVENIA I"]'''
	#ladies_teams = []
	teams = []
	sex = []
	count = 0
	#start with the men
	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=40882']
	for a in range(len(startlist_list)):

		startlist = BeautifulSoup(urlopen(startlist_list[a]), 'html.parser')
		

	#print(startlist)
		names = startlist.find_all('div', {'g-lg-18 g-md-18 g-sm-16 g-xs-16 justify-left bold'})
		body = startlist.find_all('div', {'pr-1 g-lg-2 g-md-2 g-sm-2 hidden-xs justify-right gray'})
		
		count = 0
	
		for b in range(len(body)):
			#print(body[a].text.strip())
			#if(b%3!=0):
				#ids.append(int(body[b].text.strip()))
			team = names[b].text.strip()
				
			ids.append(team)
			ids.append(ladies_ids[b])
			ids.append(men_ids[b])
				

			#else:
			
			#if(count==0):
			#	sex.append('M')
			#else:
			#	sex.append('L')
		
		

		#print(team)
		#count+=1

	print(ids)

	

	#now for the ladies
	
	return ids

def fantasy_mixed_ts(startlist):
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
			
			
				#print(startlist[a])
			country_name = startlist[a]
			
			
			if(country_name.endswith(" I") or country_name.endswith(" II")):
				pass
			else:
				country_name = country_name + " I"

			nation= API_df.loc[API_df['name']==country_name]
			nation = nation.loc[nation['gender']=='mixed']
			sex.append('mixed')
			name.append(country_name)
				#print(nation)
			
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

def elo_mixed_ts(fantasydf, menpkls, ladiespkls, relaypkls):
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [i*2 for i in wc]
	
	team_elos = []
	fantasydf.loc[:,'points'] =0

	for a in range(len(menpkls)):
		
		df = pd.read_pickle(menpkls[a])
		df = df.loc[df['level']=="all"]
		ladiespkl = pd.read_pickle(ladiespkls[a])
		ladiespkl = ladiespkl.loc[ladiespkl['level']=="all"]
		df = df.append(ladiespkl, ignore_index = True)


		df['name'] = df['name'].str.replace('ø', 'oe')
		df['name'] = df['name'].str.replace('ä', 'ae')
		df['name'] = df['name'].str.replace('æ', 'ae')
		df['name']= df['name'].str.replace('ö', 'oe')
		df['name']= df['name'].str.replace('ü', 'ue')
		df['name']= df['name'].str.replace('å', 'aa')
		df['name']= df['name'].str.replace('Ã¸', 'oe')
		df['name']= df['name'].str.replace('Ã¤', 'ae')
		df['name']= df['name'].str.replace('Ã¼', 'ue')
		df['name'] = df['name'].str.replace('Ã¶', 'oe')
		df['name'] = df['name'].str.replace('Ã', 'oe')
		df['name'] = df['name'].str.replace('Ã¦', 'ae')
		df['name']= df['name'].str.replace('Ã¥', 'aa')
		
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
		df['name'] = df['name'].str.replace('Paal Golberg', 'pal golberg')

		teamsdf = fantasydf.iloc[::3, :]

		fantasydf = fantasydf[fantasydf.index % 3 !=0]

		#print(fantasydf)
		

		fantasy_names = fantasydf['name']
		fantasy_names = fantasy_names.str.lower()
		fantasy_names  = fantasy_names.tolist()
		count = 0
		team_elo = 0
		for b in range(len(fantasy_names)):

			skier = df.loc[df['name'].str.lower() == fantasy_names[b]]
			if(len(skier['name'])==0):
				print(fantasy_names[b])
			#print(skier)
			try:
				elo = skier['elo'].iloc[-1]
				team_elo+=elo
				#print(team_elo)
				#skier_elo.append(elo)
			#elo = (skier.loc[skier['date']==20210500]['elo'])
			except:
				print("Name not registered", fantasy_names[b])
				team_elo+=1300

			if(b%2==1):
				team_elos.append(team_elo)
				team_elo = 0
				#skier_elo.append(1300)
		
		teamsdf['team_elo'] = team_elos
		fantasydf = teamsdf
		
		relaypkl = pd.read_pickle(relaypkls[a])
		
		relaypkl = relaypkl.loc[relaypkl['level']=="all"]
		
		intslope = regress_relay(relaypkl)

		mixeddf = fantasydf.loc[fantasydf['sex']=='mixed']
		max_elo = max(mixeddf['team_elo'])
		mixeddf['elopct'] = mixeddf['team_elo'].apply(lambda x: 100*(x/max_elo))
		mixeddf = mixeddf.sort_values(by='team_elo', ascending = False)
		mixeddf = mixeddf.reset_index(drop=True)
		#Edit out these next three and the ladies three for pursuit.  One for actual
		
		#mendf = mendf.sort_values(by='elo', ascending=False)
		
		#mendf = mendf[:len(mendf)]
		mixeddf['points'] = mixeddf['elopct'].apply(lambda x: (intslope[0]+intslope[1]*x)**np.exp(1))
		mixeddf['points'] = mixeddf['points'].fillna(0)


		#ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
		#ladiesdf = ladiesdf[:len(team_elos)]
		
		mixeddf['place'] = np.arange(1, len(mixeddf['name'])+1, 1)
		
		fantasydf = mixeddf
	

	return fantasydf


def country_code_to_country(country_codes):
	cc = country_codes
	rl = []
	for a in range(len(country_codes)):
		if(cc[a]=="FRA"):
			rl.append("France")
		elif(cc[a]=="AUS"):
			rl.append("Australia")
		elif(cc[a]=="AUT"):
			rl.append("Austria")
		elif(cc[a]=="BGR"):
			rl.append("Bulgaria")
		elif(cc[a]=="BIH"):
			rl.append("Bosnia and Herzegovina")
		elif(cc[a]=="BRA"):
			rl.append("Brazil")
		elif(cc[a]=="CAN"):
			rl.append("Canada")
		elif(cc[a]=="CHE"):
			rl.append("Switzerland")
		elif(cc[a]=="CZE"):
			rl.append("Czech Republic")
		elif(cc[a]=="DEU"):
			rl.append("Germany")
		elif(cc[a]=="DNK"):
			rl.append("Denmark")
		elif(cc[a]=="ESP"):
			rl.append("Spain")
		elif(cc[a]=="EST"):
			rl.append("Estonia")
		elif(cc[a]=="FIN"):
			rl.append("Finland")
		elif(cc[a]=="GBR"):
			rl.append("Great Britain")
		elif(cc[a]=="GRC"):
			rl.append("Greece")
		elif(cc[a]=="HUN"):
			rl.append("Hungary")
		elif(cc[a]=="ISL"):
			rl.append("Iceland")
		elif(cc[a]=="ITA"):
			rl.append("Italy")
		elif(cc[a]=="KAZ"):
			rl.append("Kazakhstan")
		elif(cc[a]=="KOR"):
			rl.append("South Korea")
		elif(cc[a]=="LTU"):
			rl.append("Lithuania")
		elif(cc[a]=="LVA"):
			rl.append("Latvia")
		elif(cc[a]=="MNG"):
			rl.append("Mongolia")
		elif(cc[a]=="NOR"):
			rl.append("Norway")
		elif(cc[a]=="POL"):
			rl.append("Poland")
		elif(cc[a]=="ROU"):
			rl.append("Romania")
		elif(cc[a]=="SVK"):
			rl.append("Slovakia")
		elif(cc[a]=="SVN"):
			rl.append("Slovenia")
		elif(cc[a]=="SWE"):
			rl.append("Sweden")
		elif(cc[a]=="USA"):
			rl.append("USA")
		elif(cc[a]=="JPN"):
			rl.append("Japan")
		elif(cc[a]=="UKR"):
			rl.append("Ukraine")
		elif(cc[a]=="CHN"):
			rl.append("China")
		elif(cc[a]=="TUR"):
			rl.append("Turkey")
		elif(cc[a]=="HRV"):
			rl.append("Croatia")
		elif(cc[a]=="BEL"):
			rl.append("Belgium")
	return rl

def country_to_team(team_list):
	cc = team_list
	rl = []
	for a in range(len(team_list)):
		if(cc[a]=="France"):
			rl.append("FRANCE I")
		elif(cc[a]=="Australia"):
			rl.append("AUSTRALIA I")
		elif(cc[a]=="Austria"):
			rl.append("AUSTRIA I")
		elif(cc[a]=="Bulgaria"):
			rl.append("BULGARIA I")
		elif(cc[a]=="Bostnia and Herzegovina"):
			rl.append("BOSNIA AND HERZEGOVINA I")
		elif(cc[a]=="Brazil"):
			rl.append("BRAZIL I")
		elif(cc[a]=="Canada"):
			rl.append("CANADA I")
		elif(cc[a]=="Switzerland"):
			rl.append("SWITZERLAND I")
		elif(cc[a]=="Czech Republic"):
			rl.append("CZECHIA I")
		elif(cc[a]=="Germany"):
			rl.append("GERMANY I")
		elif(cc[a]=="Denmark"):
			rl.append("DENMARK I")
		elif(cc[a]=="Spain"):
			rl.append("SPAIN I")
		elif(cc[a]=="Estonia"):
			rl.append("ESTONIA I")
		elif(cc[a]=="Finland"):
			rl.append("FINLAND I")
		elif(cc[a]=="Great Britain"):
			rl.append("GREAT BRITAIN I")
		elif(cc[a]=="Greece"):
			rl.append("GREECE I")
		elif(cc[a]=="Hungary"):
			rl.append("HUNGARY I")
		elif(cc[a]=="Iceland"):
			rl.append("ICELAND I")
		elif(cc[a]=="Italy"):
			rl.append("ITALY I")
		elif(cc[a]=="Kazakhstan"):
			rl.append("KAZAKHSTAN I")
		elif(cc[a]=="South Korea"):
			rl.append("KOREA I")
		elif(cc[a]=="Lithuania"):
			rl.append("LITHUANIA I")
		elif(cc[a]=="Latvia"):
			rl.append("LATVIA I")
		elif(cc[a]=="Mongolia"):
			rl.append("MONGOLIA I")
		elif(cc[a]=="Norway"):
			rl.append("NORWAY I")
		elif(cc[a]=="Poland"):
			rl.append("POLAND I")
		elif(cc[a]=="Romania"):
			rl.append("ROMANIA I")
		elif(cc[a]=="Slovakia"):
			rl.append("SLOVAKIA I")
		elif(cc[a]=="Slovenia"):
			rl.append("SLOVENIA I")
		elif(cc[a]=="Sweden"):
			rl.append("SWEDEN I")
		elif(cc[a]=="USA"):
			rl.append("UNITED STATES OF AMERICA I")
		elif(cc[a]=="Japan"):
			rl.append("JAPAN I")
		elif(cc[a]=="Ukraine"):
			rl.append("UKRAINE I")
		elif(cc[a]=="China"):
			rl.append("PEOPLES REPUBLIC OF CHINA I")
		elif(cc[a]=="Turkey"):
			rl.append("TURKEY I")
		elif(cc[a]=="Croatia"):
			rl.append("CROATIA I")
		elif(cc[a]=="Belgium"):
			rl.append("BELGIUM I")
		
	return rl

def df_to_name(name_list):
	print(name_list)
	name_list['name'] = name_list['name'].str.replace('ø', 'oe')
	name_list['name'] = name_list['name'].str.replace('ä', 'ae')
	name_list['name'] = name_list['name'].str.replace('æ', 'ae')
	name_list['name']= name_list['name'].str.replace('ö', 'oe')
	name_list['name']= name_list['name'].str.replace('ü', 'ue')
	name_list['name']= name_list['name'].str.replace('å', 'aa')
	name_list['name']= name_list['name'].str.replace('Ã¸', 'oe')
	name_list['name']= name_list['name'].str.replace('Ã¤', 'ae')
	name_list['name']= name_list['name'].str.replace('Ã¼', 'ue')
	name_list['name']= name_list['name'].str.replace('Ã¥', 'aa')
	name_list['name']= name_list['name'].str.replace('Ã¦', 'ae')
	name_list['name']= name_list['name'].str.replace('Ø', 'Oe')
	name_list['name'] = name_list['name'].str.replace('Ã¶', 'oe')
	name_list['name'] = name_list['name'].str.replace('Ã', 'oe')
	
	
	

	name_list['name'] = name_list['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	name_list['name'] = name_list['name'].str.replace('Irineu Esteve Altimiras', 'ireneu esteve altimiras')
	name_list['name'] = name_list['name'].str.replace('Thomas Hjalmar Westgaard', 'thomas maloney westgaard')
	name_list['name'] = name_list['name'].str.replace('Aleksandr Terentev', 'alexander terentev')
	name_list['name'] = name_list['name'].str.replace('Lauri Lepistoe', 'lauri lepisto')
	name_list['name'] = name_list['name'].str.replace('Philip Bellingham', 'phillip bellingham')
	name_list['name'] = name_list['name'].str.replace('Snorri Einarsson', 'snorri eythor einarsson')
	name_list['name'] = name_list['name'].str.replace('Krista Paermaekoski', 'krista parmakoski')
	name_list['name'] = name_list['name'].str.replace('Jessica Diggins', 'jessie diggins')
	name_list['name'] = name_list['name'].str.replace('Patricijia Eiduka', 'patricija eiduka')
	name_list['name'] = name_list['name'].str.replace('Katri Lylynperae', 'katri lylynpera')
	name_list['name'] = name_list['name'].str.replace('Julia Belger', 'julia preussger')
	name_list['name'] = name_list['name'].str.replace('Perttu Hyvaerinen', 'perttu hyvarinen')
	name_list['name'] = name_list['name'].str.replace('Kathrine Stewart-Jones', 'katherine stewart-jones')
	name_list['name'] = name_list['name'].str.replace('Ailja Iksanova', 'alija iksanova')
	name_list['name'] = name_list['name'].str.replace('Eric Silfver', 'erik silfver')
	name_list['name'] = name_list['name'].str.replace('Joni Maeki', 'joni maki')
	name_list['name'] = name_list['name'].str.replace('Emmi Laemsae', 'emmi lamsa')
	name_list['name'] = name_list['name'].str.replace('Anne Kylloenen', 'anne kyllonen')
	name_list['name'] = name_list['name'].str.replace('Finn O\'Connell', 'finn o connell')
	name_list['name'] = name_list['name'].str.replace('Viktoriya Olekh', 'viktoriia olekh')
	name_list['name'] = name_list['name'].str.lower()
	return name_list

def invent_relay():
	
	all_men = pd.read_pickle("~/ski/elo/python/ski/age/relay/excel365/varmen_distance_k.pkl")
	#print(all_men['date'])
	all_men = all_men.loc[all_men['date']==20230500]
	
	all_men = all_men.sort_values(by=['pelo'], ascending=False)
	all_men = df_to_name(all_men)
	#print(all_men)

	

	everyteam = pd.read_excel("~/ski/elo/knapsack/fantasy_api.xlsx")
	everyteam= everyteam.loc[everyteam['is_team']==True]
	everyteam= everyteam.loc[everyteam['country']!="RUS"]
	everyteam = everyteam.loc[everyteam['gender']=='m']
	
	country_codes = everyteam['country'].unique()
	df_countries = country_code_to_country(country_codes)
	men_teammates = []
	team_list = []
	for a in range(len(df_countries)):
		try:
			nat = all_men.loc[all_men['nation']==df_countries[a]]
			top4 = nat['name']
			top4 = top4[0:4]
			if(len(top4)<4):
				continue
			men_teammates.append(top4)
			team_list.append(df_countries[a])
		except:
			print(df_countries[a])
			continue
	team_list = country_to_team(team_list)
	print(team_list)
	men_teams = []
	for a in range(len(team_list)):
		print(team_list[a])
		men_teams.append('m'+team_list[a])

	#men_teammates = df_to_name(men_teammates)


	men_ids = []

	everyman = pd.read_excel("~/ski/elo/knapsack/fantasy_api.xlsx")
	everyman = everyman.loc[everyman['is_team']==False]
	everyman = everyman.loc[everyman['country']!='RUS']
	everyman = everyman.loc[everyman['gender']=='m']

	
	everyman_name = []
	for a in range(len(everyman['name'])):
		first_name = []
		last_name = []
		test_name = everyman['name'].iloc[a]
		test_name = test_name.split(" ")
		for word in test_name:
			if word.isupper():
				last_name.append(word)
			else:
				first_name.append(word)
		first_name = ' '.join(first_name)
		last_name = ' '.join(last_name)
		test_name = first_name + " " + last_name
		test_name = test_name.lower()
		everyman_name.append(test_name)
	everyman['name'] = everyman_name
	men_teammates = [item for sublist in men_teammates for item in sublist]
	print(men_teammates)

	for a in range(len(men_teammates)):
		#print(men_teammates[a])
		skier = everyman.loc[everyman['name']==men_teammates[a]]
		#print(skier)
		skier_id = skier['athlete_id']
		#print(skier_id)
		try:
			skier_id = skier_id.iloc[0]
		except:
			print(men_teammates[a])
			skier_id = 3040101
		men_ids.append(skier_id)
	#ids2 = men_teams+men_ids
	print(len(men_teams))
	print(len(men_ids))
	ids = []
	for a in range(len(men_teams)):
		ids.append(men_teams[a])
		index = a*4
		ids.append(men_ids[index])
		ids.append(men_ids[index+1])
		ids.append(men_ids[index+2])
		ids.append(men_ids[index+3])
	print(ids)
	return ids

def fis_relay():
	'''men_ids = [3420909, 3420605, 3420586, 3422819, 34819883, 3482119, 3482280, 3482277, 3180535, 3180861,
	3180557, 3180508, 3530882, 3530532, 3530902, 3530911, 3200802, 3200205, 3200356, 3200676,
	3501741, 3501223, 3500664, 3501010, 3510342, 3510023, 3510361, 3510351, 3150570, 3150519, 
	3150664, 3150637, 3100406, 3100409, 3100412, 1111111, 3190529, 3190111, 3190302, 3190345,
	3290379, 3290326, 3290407, 3290504, 1111111, 1111111, 3300494, 3300373, 3430249, 3430233,
	3430276, 3430103, 3390167, 3390240, 3390169, 3390207, 1111111, 1111111, 1111111, 1111111,
	3040101, 3040125, 1111111, 1111111, 1111111, 3560145, 3560101, 3560121]'''
	men_ids = []
	ids = []
	'''men_teams = ["mNORWAY I", "mRUSSIA I", "mFINLAND I", "mUNITED STATES OF AMERICA I", "mGERMANY I", "mSWEDEN I", "mSWITZERLAND I", "mCZECH REPUBLIC I", "mCANADA I", "mFRANCE I",
	"mITALY I", "mJAPAN I", "mPOLAND I", "mESTONIA I", "mPEOPLES REPUBLIC OF CHINA I", "mAUSTRALIA I", "mSLOVENIA I"]'''
	men_teams = []
	ladies_teams = []
	teams = []
	sex = []
	count = 0
	#ids = invent_relay()
	ids = []
	#start with the men
	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=44189',
'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=44188']
	for a in range(len(startlist_list)):

		startlist = BeautifulSoup(urlopen(startlist_list[a]), 'html.parser')
		'''if(a==0):
			count = 0
			for b in range(len(men_teams)):
				ids.append(men_teams[b])
				for c in range(4):
					ids.append(men_ids[count])
					count+=1
		else:
'''
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
		#count+=1

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
	API_df['name'] = API_df['name'].str.replace("CZECH REPUBLIC", "CZECHIA")

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






def elo_relay(fantasydf, menpkls, ladiespkls):
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [i*2 for i in wc]
	skier_elo = []
	team_elos = []
	fantasydf.loc[:, 'points'] = 0
	menrelaypkls = ["~/ski/elo/python/ski/relay/radar/varmen_distance_k.pkl"]
	ladiesrelaypkls = ["~/ski/elo/python/ski/relay/radar/varladies_distance_k.pkl"]


	for a in range(len(menpkls)):
		skier_elo = []
		df = pd.read_pickle(menpkls[a])
		df = df.loc[df['level']=="all"]
		
		
		ladiespkl = pd.read_pickle(ladiespkls[a])
		ladiespkl = ladiespkl.loc[ladiespkl['level']=="all"]
		df = df.append(ladiespkl, ignore_index = True)
		

		team_elos = []
	
		
		df['name'] = df['name'].str.replace('ø', 'oe')
		df['name'] = df['name'].str.replace('ä', 'ae')
		df['name'] = df['name'].str.replace('æ', 'ae')
		df['name']= df['name'].str.replace('ö', 'oe')
		df['name']= df['name'].str.replace('ü', 'ue')
		df['name']= df['name'].str.replace('å', 'aa')
		df['name']= df['name'].str.replace('Ã¸', 'oe')
		df['name']= df['name'].str.replace('Ã¤', 'ae')
		df['name']= df['name'].str.replace('Ã¼', 'ue')
		df['name'] = df['name'].str.replace('Ã¶', 'oe')
		df['name'] = df['name'].str.replace('Ã', 'oe')
		df['name'] = df['name'].str.replace('Ã¦', 'ae')
		df['name']= df['name'].str.replace('Ã¥', 'aa')
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
		df['name'] = df['name'].str.replace('Anne Kylloenen', 'anne kyllonen')
		df['name'] = df['name'].str.replace('Finn O\'Connell', 'finn o connell')
		df['name'] = df['name'].str.replace('Viktoriya Olekh', 'viktoriia olekh')

	teamsdf = fantasydf.iloc[::5, :]
	fantasydf = fantasydf[fantasydf.index % 5 !=0]

	#print(fantasydf)

	fantasy_names = fantasydf['name']
	fantasy_names = fantasy_names.str.lower()
	fantasy_names  = fantasy_names.tolist()
	count = 0
	team_elo = 0
	for b in range(len(fantasy_names)):

		skier = df.loc[df['name'].str.lower() == fantasy_names[b]]
		if(len(skier['name'])==0):
			print(fantasy_names[b])
		#print(skier)
		try:
			elo = skier['elo'].iloc[-1]
			team_elo+=elo
			#skier_elo.append(elo)
		#elo = (skier.loc[skier['date']==20210500]['elo'])
		except:
			print(fantasy_names[b])
			team_elo+=1300

		if(b%4==3):
			team_elos.append(team_elo)
			team_elo = 0
			#skier_elo.append(1300)
	print(team_elos)
	teamsdf['team_elo'] = team_elos
	fantasydf = teamsdf
	
	menrelaypkl = pd.read_pickle(menrelaypkls[a])
	menrelaypkl = menrelaypkl.loc[df['level']=="all"]
	menintslope = regress_relay(menrelaypkl)
	mendf = fantasydf.loc[fantasydf['sex']=='m']
	max_elo = max(mendf['team_elo'])
	mendf['elopct'] = mendf['team_elo'].apply(lambda x: 100*(x/max_elo))
	mendf = mendf.sort_values(by='team_elo', ascending = False)
	mendf = mendf.reset_index(drop=True)
	#Edit out these next three and the ladies three for pursuit.  One for actual
	
	#mendf = mendf.sort_values(by='elo', ascending=False)
	
	#mendf = mendf[:len(mendf)]
	mendf['points'] = mendf['elopct'].apply(lambda x: (menintslope[0]+menintslope[1]*x)**np.exp(1))
	mendf['points'] = mendf['points'].fillna(0)
	mendf['place'] = np.arange(1, len(mendf['name'])+1, 1)
	ladiesrelaypkl = pd.read_pickle(ladiesrelaypkls[a])
	ladiesrelaypkl = ladiesrelaypkl.loc[df['level']=="all"]
	ladiesintslope = regress_relay(ladiesrelaypkl)
	ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
	max_elo = max(ladiesdf['team_elo'])
	ladiesdf['elopct'] = ladiesdf['team_elo'].apply(lambda x: 100*(x/max_elo))
	ladiesdf = ladiesdf.sort_values(by='team_elo', ascending = False)
	ladiesdf = ladiesdf.reset_index(drop=True)
	#Edit out these next three and the ladies three for pursuit.  One for actual

	#ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
	print(len(ladiesdf))
	#ladiesdf = ladiesdf[:len(ladiesdf)]
	ladiesdf['points'] = ladiesdf['elopct'].apply(lambda x: (ladiesintslope[0]+ladiesintslope[1]*x)**np.exp(1))
	ladiesdf['points'] = ladiesdf['points'].fillna(0)
	ladiesdf['place'] = np.arange(1, len(ladiesdf['name'])+1, 1)
	fantasydf = mendf
	fantasydf = fantasydf.append(ladiesdf)

	return fantasydf

	
def fis_mixed_relay():
	'''men_ids = [3420909, 3420605, 3420586, 3422819, 34819883, 3482119, 3482280, 3482277, 3180535, 3180861,
	3180557, 3180508, 3530882, 3530532, 3530902, 3530911, 3200802, 3200205, 3200356, 3200676,
	3501741, 3501223, 3500664, 3501010, 3510342, 3510023, 3510361, 3510351, 3150570, 3150519, 
	3150664, 3150637, 3100406, 3100409, 3100412, 1111111, 3190529, 3190111, 3190302, 3190345,
	3290379, 3290326, 3290407, 3290504, 1111111, 1111111, 3300494, 3300373, 3430249, 3430233,
	3430276, 3430103, 3390167, 3390240, 3390169, 3390207, 1111111, 1111111, 1111111, 1111111,
	3040101, 3040125, 1111111, 1111111, 1111111, 3560145, 3560101, 3560121]'''
	
	ids = []
	'''men_teams = ["mNORWAY I", "mRUSSIA I", "mFINLAND I", "mUNITED STATES OF AMERICA I", "mGERMANY I", "mSWEDEN I", "mSWITZERLAND I", "mCZECH REPUBLIC I", "mCANADA I", "mFRANCE I",
	"mITALY I", "mJAPAN I", "mPOLAND I", "mESTONIA I", "mPEOPLES REPUBLIC OF CHINA I", "mAUSTRALIA I", "mSLOVENIA I"]'''
	#ladies_teams = []
	teams = []
	sex = []
	count = 0
	#start with the men
	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=44190']
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
				
				ids.append(team)
			#if(count==0):
			#	sex.append('M')
			#else:
			#	sex.append('L')
		
		

		#print(team)
		#count+=1

	print(ids)

	

	#now for the ladies
	
	return ids

def fantasy_mixed_relay(startlist):
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
			
			
				#print(startlist[a])
			country_name = startlist[a]
			
			
			if(country_name.endswith(" I") or country_name.endswith(" II")):
				pass
			else:
				country_name = country_name + " I"

			nation= API_df.loc[API_df['name']==country_name]
			nation = nation.loc[nation['gender']=='mixed']
			sex.append('mixed')
			name.append(country_name)
				#print(nation)
			
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

def elo_mixed_relay(fantasydf, menpkls, ladiespkls, relaypkls):
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [i*2 for i in wc]
	
	team_elos = []
	fantasydf.loc[:,'points'] =0

	for a in range(len(menpkls)):
		print(menpkls[a])
		df = pd.read_pickle(menpkls[a])
		df = df.loc[df['level']=="all"]
		ladiespkl = pd.read_pickle(ladiespkls[a])
		ladiespkl = ladiespkl.loc[ladiespkl['level']=="all"]
		df = df.append(ladiespkl, ignore_index = True)


		df['name'] = df['name'].str.replace('ø', 'oe')
		df['name'] = df['name'].str.replace('ä', 'ae')
		df['name'] = df['name'].str.replace('æ', 'ae')
		df['name']= df['name'].str.replace('ö', 'oe')
		df['name']= df['name'].str.replace('ü', 'ue')
		df['name']= df['name'].str.replace('å', 'aa')
		df['name']= df['name'].str.replace('Ã¸', 'oe')
		df['name']= df['name'].str.replace('Ã¤', 'ae')
		df['name']= df['name'].str.replace('Ã¼', 'ue')
		df['name'] = df['name'].str.replace('Ã¶', 'oe')
		df['name'] = df['name'].str.replace('Ã', 'oe')
		
		
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

		print(list(fantasydf['name']))
		

		fantasy_names = fantasydf['name']
		fantasy_names = fantasy_names.str.lower()
		fantasy_names  = fantasy_names.tolist()
		count = 0
		team_elo = 0
		for b in range(len(fantasy_names)):

			skier = df.loc[df['name'].str.lower() == fantasy_names[b]]
			if(len(skier['name'])==0):
				print(fantasy_names[b])
			#print(skier)
			try:
				elo = skier['elo'].iloc[-1]
				team_elo+=elo
				#print(team_elo)
				#skier_elo.append(elo)
			#elo = (skier.loc[skier['date']==20210500]['elo'])
			except:
				print("Name not registered", fantasy_names[b])
				team_elo+=1300

			if(b%4==3):
				team_elos.append(team_elo)
				team_elo = 0
				#skier_elo.append(1300)
		
		teamsdf['team_elo'] = team_elos
		fantasydf = teamsdf
		
		relaypkl = pd.read_pickle(relaypkls[a])
		relaypkl = relaypkl.loc[relaypkl['level']=="all"]
		
		intslope = regress_relay(relaypkl)

		mixeddf = fantasydf.loc[fantasydf['sex']=='mixed']
		max_elo = max(mixeddf['team_elo'])
		mixeddf['elopct'] = mixeddf['team_elo'].apply(lambda x: 100*(x/max_elo))
		mixeddf = mixeddf.sort_values(by='team_elo', ascending = False)
		mixeddf = mixeddf.reset_index(drop=True)
		#Edit out these next three and the ladies three for pursuit.  One for actual
		
		#mendf = mendf.sort_values(by='elo', ascending=False)
		
		#mendf = mendf[:len(mendf)]
		mixeddf['points'] = mixeddf['elopct'].apply(lambda x: (intslope[0]+intslope[1]*x)**np.exp(1))
		mixeddf['points'] = mixeddf['points'].fillna(0)


		#ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
		#ladiesdf = ladiesdf[:len(team_elos)]
		
		mixeddf['place'] = np.arange(1, len(mixeddf['name'])+1, 1)
		
		fantasydf = mixeddf
	

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

	everyone = pd.read_excel("~/ski/elo/knapsack/fantasy_api.xlsx")
	everyone = everyone.loc[everyone['is_team']==False]
	everyone = everyone.loc[everyone['active']==True]
	everyone = everyone.loc[everyone['country']!="RUS"]
	all_ladies = everyone.loc[everyone['gender']=='f']
	all_ladies_ids = all_ladies['athlete_id'].tolist()
	all_men = everyone.loc[everyone['gender']=='m']
	all_men_ids = all_men['athlete_id'].tolist()

	fuzz = pd.read_excel("~/ski/elo/knapsack/excel365/fuzzy_match.xlsx")
	men = fuzz.loc[fuzz['gender']=='m']
	ids_men = list((men['athlete_id']))
	ladies = fuzz.loc[fuzz['gender']=='f']
	ids_ladies = list(ladies['athlete_id'])
	print(ids_men)
	#ids_men = []
	#ids_ladies = []
	#ids_men =all_men_ids
	#ids_ladies=all_ladies_ids

	count = 0
	#start with the men
	startlist_list = ['https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=44233',
	'https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=44232']
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
	stage = [50, 47, 44, 41, 38, 35, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100,95,90,85,80,75,72,69,66,63,60,58,56,54,52,50,48,46,44,42,40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [300, 285, 270, 255, 240, 216, 207, 198, 189, 180, 174, 168, 162, 156, 150, 144, 138, 132, 126, 120, 114, 108, 102, 96, 90, 84, 78, 72, 66, 60, 57, 54, 51, 48, 45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3]
	mendfs = []
	ladiesdfs = []
	fantasydf.loc[:,'points'] =0
	
	#"~/ski/elo/python/ski/ladies/varladies_10_classic.pkl",
	#"~/ski/elo/python/ski/ladies/varladies_sprint_classic.pkl"]

	for a in range(len(menpkls)):
		skier_elo = []
		df = pd.read_pickle(menpkls[a])

		#It's all unless it's the first race of the years
		df = df.loc[df['level']=="all"]
		menintslope = regress(df)
		
		ladiespkl = pd.read_pickle(ladiespkls[a])
		ladiespkl = ladiespkl.loc[ladiespkl['level']=="all"]
		ladiesintslope = regress(ladiespkl)
		df = df.append(ladiespkl, ignore_index = True)
		df['name'] = df['name'].str.replace('ø', 'oe')
		df['name'] = df['name'].str.replace('ä', 'ae')
		df['name'] = df['name'].str.replace('æ', 'ae')
		df['name']= df['name'].str.replace('ö', 'oe')
		df['name']= df['name'].str.replace('ü', 'ue')
		df['name']= df['name'].str.replace('å', 'aa')
		df['name']= df['name'].str.replace('Ã¸', 'oe')
		df['name']= df['name'].str.replace('Ã¤', 'ae')
		df['name']= df['name'].str.replace('Ã¼', 'ue')
		df['name']= df['name'].str.replace('Ø', 'Oe')
		df['name'] = df['name'].str.replace('Ã¶', 'oe')
		df['name'] = df['name'].str.replace('Ã', 'oe')
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
				skier_elo.append(elo)
			#elo = (skier.loc[skier['date']==20210500]['elo'])
			except:
				print(fantasy_names[a])
				skier_elo.append(1300)
			
		fantasydf['elo'] = skier_elo
		mendf = fantasydf.loc[fantasydf['sex']=='m']
		print(mendf['elo'])
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
		mendf['points'] = mendf['elopct'].apply(lambda x: (menintslope[0]+menintslope[1]*x)**np.exp(1))
		mendf['points'] = mendf['points'].fillna(0)
		
		#print(mendf['points'])
		

		
		
		
		mendfs.append(mendf)
		
		ladiesdf = fantasydf.loc[fantasydf['sex']=='f']
		max_elo = max(ladiesdf['elo'])
		ladiesdf['elopct'] = ladiesdf['elo'].apply(lambda x: 100*(x/max_elo))
		ladiesdf = ladiesdf.sort_values(by='elo', ascending=False)
		ladiesdf = ladiesdf.reset_index(drop=True)

		#Next block is for regression
		
		ladiesdf['points'] = ladiesdf['elopct'].apply(lambda x: (ladiesintslope[0]+ladiesintslope[1]*x)**np.exp(1))
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



def regress_relay(df):#, pkl):
	

	stage = [50, 46, 43, 40, 37, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [i*2 for i in wc]
	#tour = [400, 320, 240, 200, 180, 160, 144, 128, 116, 104, 96, 88, 80, 72, 64, 60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20,20, 20, 20, 20]
	#points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	
	#Set what type of race it is
	points = wc
	df = df.loc[df['level']=="all"]
	print(df)
	max_elo = max(df['elo'])
	df = df.loc[df['season']>=2010]
	df = df.loc[df['elo']!=5200]
	df = df.loc[df['elo']!=2600]
	df2 = pd.DataFrame()
	seasons = pd.unique(df['season'])
	print(len(df['race']))
	

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
	
	#print(df2['pelopct'])
	#print(df2['points'])
	#print("total points", sum(df2['points']))

	#The next  line negates the block before.  It uses all elos ever as the pelopct
	#df2['pelopct'] = (df2['pelo']/max_elo)*100
	#print(df2['pelopct'])
	#print(len(df2['points']))
	df2['points'] = df2['points'].apply(lambda x: x**(1/np.exp(1)))
	#print("points", sum(df2['points']))
	
	x = df2['pelopct'].values.reshape((-1,1))
	#print(x)
	y = df2['points']
	lm = LinearRegression()
	#lm.fit(x,y)
	lm = LinearRegression().fit(x,y)
	print(lm.intercept_, lm.coef_[0])
	return [lm.intercept_, lm.coef_[0]]

#The point of regress is to take the pkl from pkl_setup, add a regression to it to get expected points for the current race
#So it should return an intercept and a coefficient
def regress(df):#, pkl):
	

	stage = [50, 47, 44, 41, 38, 35, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	wc = [100,95,90,85,80,75,72,69,66,63,60,58,56,54,52,50,48,46,44,42,40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	tour = [300, 285, 270, 255, 240, 225, 216, 207, 198, 189, 180, 174, 168, 162, 156, 150, 144, 138, 132, 126, 120, 114, 108, 102, 96, 90, 84, 78, 72, 66, 60, 57, 54, 51, 48, 45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3]

	#points = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	
	#Set what type of race it is
	points = wc
	df = df.loc[df['level']=="all"]
	max_elo = max(df['elo'])
	df = df.loc[df['season']>=2019]
	df = df.loc[df['pelo']!=1300]
	df2 = pd.DataFrame()
	seasons = pd.unique(df['season'])
	print(len(df['race']))
	

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
			if(len(racedf['place'])>50):
#			if(len(racedf['place'])>50):
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
	
	#print(df2['pelopct'])
	#print(df2['points'])
	#print("total points", sum(df2['points']))

	#The next  line negates the block before.  It uses all elos ever as the pelopct
	#df2['pelopct'] = (df2['pelo']/max_elo)*100
	#print(df2['pelopct'])
	#print(len(df2['points']))
	df2['points'] = df2['points'].apply(lambda x: x**(1/np.exp(1)))
	#print("points", sum(df2['points']))
	
	x = df2['pelopct'].values.reshape((-1,1))
	#print(x)
	y = df2['points']
	lm = LinearRegression()
	#lm.fit(x,y)
	lm = LinearRegression().fit(x,y)
	print(lm.intercept_, lm.coef_[0])
	return [lm.intercept_, lm.coef_[0]]

def mixed_combo(relaydf, tsdf):
	
	relaydf['ts_elo'] = tsdf['team_elo']
	relaydf['relay_points'] = relaydf['points']
	relaydf['ts_points'] = tsdf['points']
	relaydf['points'] = relaydf['relay_points'] + relaydf['ts_points']

	return relaydf

	
	#lm = 

		



	#WebDriverWait(driver, 30).until(EC.invisibility_of_element_located((By.XPATH,
	#	"//div[@class='js-off-canvas-overlay is-overlay-fixed']")))


#menpkls = ["~/ski/elo/python/ski/age/relay/excel365/varmen_sprint_classic_k.pkl"]
menpkls = ["~/ski/elo/python/ski/age/excel365/varmen_sprint_classic_k.pkl",
"~/ski/elo/python/ski/age/excel365/varmen_distance_classic_k.pkl",
"~/ski/elo/python/ski/age/excel365/varmen_distance_freestyle_k.pkl"]
	#"~/ski/elo/python/ski/age/excel365/varmen_distance_classic_k.pkl",
	#"~/ski/elo/python/ski/age/excel365/varmen_sprint_classic_k.pkl"]
#	"~/ski/elo/python/ski/age/excel365/varmen_distance_freestyle_k.pkl"]

#ladiespkls = ["~/ski/elo/python/ski/age/relay/excel365/varladies_sprint_classic_k.pkl"]
ladiespkls = ["~/ski/elo/python/ski/age/excel365/varladies_sprint_classic_k.pkl",
"~/ski/elo/python/ski/age/excel365/varladies_distance_classic_k.pkl",
"~/ski/elo/python/ski/age/excel365/varladies_distance_freestyle_k.pkl"]
#"~/ski/elo/python/ski/age/excel365/varladies_distance_classic_k.pkl",
#"~/ski/elo/python/ski/age/excel365/varladies_sprint_classic_k.pkl"]
#"~/ski/elo/python/ski/age/excel365/varladies_distance_freestyle_k.pkl"]
#ladiespkls = ["~/ski/elo/python/ski/age/excel365/varladies_sprint_freestyle_k.pkl"]
relaypkls = ["~/ski/elo/python/ski/age/relay/excel365/varmen_distance_k.pkl"]
#tspkls = ["~/ski/elo/python/ski/relay/radar/varmen_sprint_k.pkl"]


#df = pd.read_pickle(ladiespkls[0])
#regress(df)

startlist = fis()
#startlist = fis_relay()
#startlist = fis_mixed_relay()
#startlist = fis_team_sprint()

fantasydf = (fantasy(startlist))
#fantasydf = fantasy_relay(startlist)
#fantasydf = fantasy_mixed_relay(startlist)
#fantasydf = fantasy_team_sprint(startlist)
#with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
 #   print(fantasydf)
#print(fantasydf)
#print(fantasydf)

fantasydf = elo(fantasydf, menpkls, ladiespkls)
#fantasydf = pursuit(fantasydf)
#fantasydf = elo_relay(fantasydf, menpkls, ladiespkls)
#fantasydf = elo_mixed_relay(fantasydf, menpkls, ladiespkls, relaypkls)
#print(fantasydf)
#fantasydf = elo_team_sprint(fantasydf, menpkls, ladiespkls)
#print(fantasydf)

'''men_relay_skier_pkls = ["~/ski/elo/python/ski/relay/excel365/varmen_distance_k.pkl"]#,
ladies_relay_skier_pkls = ["~/ski/elo/python/ski/relay/excel365/varladies_distance_k.pkl"]#,
relaypkls = ["~/ski/elo/python/ski/relay/radar/varmen_distance_k.pkl"]
men_ts_skier_pkls = ["~/ski/elo/python/ski/relay/excel365/varmen_distance_k.pkl"]#,
ladies_ts_skier_pkls = ["~/ski/elo/python/ski/relay/excel365/varladies_distance_k.pkl"]#,
tspkls = ["~/ski/elo/python/ski/relay/radar/varmen_distance_k.pkl"]

mixed_relay_startlist = fis_mixed_relay()
mixed_relay_fantasydf = fantasy_mixed_relay(mixed_relay_startlist)
mixed_relay_fantasydf = elo_mixed_relay(mixed_relay_fantasydf, men_relay_skier_pkls, ladies_relay_skier_pkls, relaypkls)
mixed_ts_startlist = fis_mixed_ts()
mixed_ts_fantasydf = fantasy_mixed_ts(mixed_ts_startlist)
mixed_ts_fantasydf = elo_mixed_ts(mixed_ts_fantasydf, men_ts_skier_pkls, ladies_ts_skier_pkls, tspkls)
fantasydf = mixed_combo(mixed_relay_fantasydf, mixed_ts_fantasydf)
print(fantasydf)'''

fantasydf.to_pickle("~/ski/elo/knapsack/excel365/fantasydf_falun.pkl")
fantasydf.to_excel("~/ski/elo/knapsack/excel365/fantasydf_falun.xlsx")


print(time.time() - start_time)

