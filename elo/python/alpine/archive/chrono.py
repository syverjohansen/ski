import pandas as pd
import time
from functools import reduce
pd.options.mode.chained_assignment = None
start_time = time.time()


#Go through unique dates

def ladies():
	lady_all_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_downhill_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_downhill_k.xlsx', sheet_name="Sheet1", header=0)
	lady_downhill_k = lady_downhill_k.rename(columns = {'pelo':'downhill_pelo', 'elo':'downhill_elo'})
	lady_superg_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_superg_k.xlsx', sheet_name="Sheet1", header=0)
	lady_superg_k = lady_superg_k.rename(columns = {'pelo':'superg_pelo', 'elo':'superg_elo'})
	lady_gs_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_gs_k.xlsx', sheet_name="Sheet1", header=0)
	lady_gs_k = lady_gs_k.rename(columns = {'pelo':'gs_pelo', 'elo':'gs_elo'})
	lady_slalom_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_slalom_k.xlsx', sheet_name="Sheet1", header=0)
	lady_slalom_k = lady_slalom_k.rename(columns = {'pelo':'slalom_pelo', 'elo':'slalom_elo'})
	lady_combined_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_combined_k.xlsx', sheet_name="Sheet1", header=0)
	lady_combined_k = lady_combined_k.rename(columns = {'pelo':'combined_pelo', 'elo':'combined_elo'})
	lady_speed_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_speed_k.xlsx', sheet_name="Sheet1", header=0)
	lady_speed_k = lady_speed_k.rename(columns = {'pelo':'speed_pelo', 'elo':'speed_elo'})
	lady_tech_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varladies_tech_k.xlsx', sheet_name="Sheet1", header=0)
	lady_tech_k = lady_tech_k.rename(columns = {'pelo':'tech_pelo', 'elo':'tech_elo'})
	
	print("Done reading ladies files")

	'''dfs = [lady_all_k, lady_distance_k, lady_distance_classic_k, lady_distance_freestyle_k, 
	lady_downhill_k, lady_downhill_classic_k, lady_downhill_freestyle_k]'''
	#lady_all_k = lady_all_k.loc[lady_all_k['season']==1996]
	#lady_all_k = lady_all_k.drop("Unnamed: 0", axis=1)
	#cols_to_use = lady_distance_k.columns.difference(lady_all_k.columns)
	#print(cols_to_use)
	lady_all_k = lady_all_k[lady_all_k['city']!='Summer']
	distance_col = list(lady_all_k['distance'])
	
	lady_downhill_k = lady_downhill_k[lady_downhill_k['city']!='Summer']
	lady_superg_k = lady_superg_k[lady_superg_k['city']!='Summer']
	lady_gs_k = lady_gs_k[lady_gs_k['city']!='Summer']
	lady_slalom_k = lady_slalom_k[lady_slalom_k['city']!='Summer']
	lady_combined_k = lady_combined_k[lady_combined_k['city']!='Summer']
	lady_speed_k = lady_speed_k[lady_speed_k['city']!='Summer']
	lady_tech_k = lady_tech_k[lady_tech_k['city']!='Summer']
	

	lady_all_k = lady_all_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	'pelo', 'elo' ]]
	lady_downhill_k = lady_downhill_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"downhill_pelo", "downhill_elo"]]
	lady_superg_k = lady_superg_k[['Unnamed: 0','date','city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"superg_pelo", "superg_elo"]]
	lady_gs_k = lady_gs_k[['Unnamed: 0','date','city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"gs_pelo", "gs_elo"]]
	lady_slalom_k = lady_slalom_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"slalom_pelo", "slalom_elo"]]
	lady_combined_k = lady_combined_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"combined_pelo", "combined_elo"]]
	lady_speed_k = lady_speed_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"speed_pelo", "speed_elo"]]
	lady_tech_k = lady_tech_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"tech_pelo", "tech_elo"]]
	

	lady_all_k1 = lady_all_k.merge(lady_downhill_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k2 = lady_all_k1.merge(lady_superg_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k3 = lady_all_k2.merge(lady_gs_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k4 = lady_all_k3.merge(lady_slalom_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k5 = lady_all_k4.merge(lady_combined_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k6 = lady_all_k5.merge(lady_speed_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k7 = lady_all_k6.merge(lady_tech_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")


	print("Ladies files merged")
	lady_all_k7['distance'] = distance_col

	unique_ids = pd.unique(lady_all_k7['id'])
	newdf = pd.DataFrame()
	for a in range(len(unique_ids)):
		skierdf = lady_all_k7.loc[lady_all_k7['id']==unique_ids[a]]
		
		skierdf['age'] = skierdf['age'].ffill()
		skierdf['exp'] = skierdf['exp'].ffill()

		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['downhill_elo'] = skierdf['downhill_elo'].ffill()
		skierdf['superg_elo'] = skierdf['superg_elo'].ffill()
		skierdf['gs_elo'] = skierdf['gs_elo'].ffill()
		skierdf['slalom_elo'] = skierdf['slalom_elo'].ffill()
		skierdf['combined_elo'] = skierdf['combined_elo'].ffill()
		skierdf['speed_elo'] = skierdf['speed_elo'].ffill()
		skierdf['tech_elo'] = skierdf['tech_elo'].ffill()
		
		
		skierdf['pelo'] = skierdf['pelo'].fillna(skierdf['elo'])	
		skierdf['downhill_pelo'] = skierdf['downhill_pelo'].fillna(skierdf['downhill_elo'])
		skierdf['superg_pelo'] = skierdf['superg_pelo'].fillna(skierdf['superg_elo'])	
		skierdf['gs_pelo'] = skierdf['gs_pelo'].fillna(skierdf['gs_elo'])	
		skierdf['slalom_pelo'] = skierdf['slalom_pelo'].fillna(skierdf['slalom_elo'])
		skierdf['combined_pelo'] = skierdf['combined_pelo'].fillna(skierdf['combined_elo'])	
		skierdf['speed_pelo'] = skierdf['speed_pelo'].fillna(skierdf['speed_elo'])	
		skierdf['tech_pelo'] = skierdf['tech_pelo'].fillna(skierdf['tech_elo'])		
		

		newdf = newdf.append(skierdf)
	
	newdf = newdf.sort_values(by=['Unnamed: 0'])
	print("Ladies NA's Forward Filled")
	return newdf


def men():
	man_all_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_downhill_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_downhill_k.xlsx', sheet_name="Sheet1", header=0)
	man_downhill_k = man_downhill_k.rename(columns = {'pelo':'downhill_pelo', 'elo':'downhill_elo'})
	man_superg_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_superg_k.xlsx', sheet_name="Sheet1", header=0)
	man_superg_k = man_superg_k.rename(columns = {'pelo':'superg_pelo', 'elo':'superg_elo'})
	man_gs_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_gs_k.xlsx', sheet_name="Sheet1", header=0)
	man_gs_k = man_gs_k.rename(columns = {'pelo':'gs_pelo', 'elo':'gs_elo'})
	man_slalom_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_slalom_k.xlsx', sheet_name="Sheet1", header=0)
	man_slalom_k = man_slalom_k.rename(columns = {'pelo':'slalom_pelo', 'elo':'slalom_elo'})
	man_combined_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_combined_k.xlsx', sheet_name="Sheet1", header=0)
	man_combined_k = man_combined_k.rename(columns = {'pelo':'combined_pelo', 'elo':'combined_elo'})
	man_speed_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_speed_k.xlsx', sheet_name="Sheet1", header=0)
	man_speed_k = man_speed_k.rename(columns = {'pelo':'speed_pelo', 'elo':'speed_elo'})
	man_tech_k = pd.read_excel('~/ski/elo/python/alpine/age/excel365/varmen_tech_k.xlsx', sheet_name="Sheet1", header=0)
	man_tech_k = man_tech_k.rename(columns = {'pelo':'tech_pelo', 'elo':'tech_elo'})
	
	print("Done reading men files")

	'''dfs = [man_all_k, man_distance_k, man_distance_classic_k, man_distance_freestyle_k, 
	man_downhill_k, man_downhill_classic_k, man_downhill_freestyle_k]'''
	#man_all_k = man_all_k.loc[man_all_k['season']==1996]
	#man_all_k = man_all_k.drop("Unnamed: 0", axis=1)
	#cols_to_use = man_distance_k.columns.difference(man_all_k.columns)
	#print(cols_to_use)
	man_all_k = man_all_k[man_all_k['city']!='Summer']
	distance_col = list(man_all_k['distance'])
	
	man_downhill_k = man_downhill_k[man_downhill_k['city']!='Summer']
	man_superg_k = man_superg_k[man_superg_k['city']!='Summer']
	man_gs_k = man_gs_k[man_gs_k['city']!='Summer']
	man_slalom_k = man_slalom_k[man_slalom_k['city']!='Summer']
	man_combined_k = man_combined_k[man_combined_k['city']!='Summer']
	man_speed_k = man_speed_k[man_speed_k['city']!='Summer']
	man_tech_k = man_tech_k[man_tech_k['city']!='Summer']
	

	man_all_k = man_all_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	'pelo', 'elo' ]]
	man_downhill_k = man_downhill_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"downhill_pelo", "downhill_elo"]]
	man_superg_k = man_superg_k[['Unnamed: 0','date','city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"superg_pelo", "superg_elo"]]
	man_gs_k = man_gs_k[['Unnamed: 0','date','city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"gs_pelo", "gs_elo"]]
	man_slalom_k = man_slalom_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"slalom_pelo", "slalom_elo"]]
	man_combined_k = man_combined_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"combined_pelo", "combined_elo"]]
	man_speed_k = man_speed_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"speed_pelo", "speed_elo"]]
	man_tech_k = man_tech_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"tech_pelo", "tech_elo"]]
	

	man_all_k1 = man_all_k.merge(man_downhill_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k2 = man_all_k1.merge(man_superg_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k3 = man_all_k2.merge(man_gs_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k4 = man_all_k3.merge(man_slalom_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k5 = man_all_k4.merge(man_combined_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k6 = man_all_k5.merge(man_speed_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k7 = man_all_k6.merge(man_tech_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex',  'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	
	print("Men files merged")
	man_all_k7['distance'] = distance_col

	unique_ids = pd.unique(man_all_k7['id'])
	newdf = pd.DataFrame()
	for a in range(len(unique_ids)):
		skierdf = man_all_k7.loc[man_all_k7['id']==unique_ids[a]]
		
		skierdf['age'] = skierdf['age'].ffill()
		skierdf['exp'] = skierdf['exp'].ffill()

		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['downhill_elo'] = skierdf['downhill_elo'].ffill()
		skierdf['superg_elo'] = skierdf['superg_elo'].ffill()
		skierdf['gs_elo'] = skierdf['gs_elo'].ffill()
		skierdf['slalom_elo'] = skierdf['slalom_elo'].ffill()
		skierdf['combined_elo'] = skierdf['combined_elo'].ffill()
		skierdf['speed_elo'] = skierdf['speed_elo'].ffill()
		skierdf['tech_elo'] = skierdf['tech_elo'].ffill()
		
		
		skierdf['pelo'] = skierdf['pelo'].fillna(skierdf['elo'])	
		skierdf['downhill_pelo'] = skierdf['downhill_pelo'].fillna(skierdf['downhill_elo'])
		skierdf['superg_pelo'] = skierdf['superg_pelo'].fillna(skierdf['superg_elo'])	
		skierdf['gs_pelo'] = skierdf['gs_pelo'].fillna(skierdf['gs_elo'])	
		skierdf['slalom_pelo'] = skierdf['slalom_pelo'].fillna(skierdf['slalom_elo'])
		skierdf['combined_pelo'] = skierdf['combined_pelo'].fillna(skierdf['combined_elo'])	
		skierdf['speed_pelo'] = skierdf['speed_pelo'].fillna(skierdf['speed_elo'])	
		skierdf['tech_pelo'] = skierdf['tech_pelo'].fillna(skierdf['tech_elo'])		
		

		newdf = newdf.append(skierdf)
	
	newdf = newdf.sort_values(by=['Unnamed: 0'])
	
	print("Men NA's Forward Filled")
	return newdf


ladiesdf = ladies()
ladiesdf.to_pickle("/Users/syverjohansen/ski/elo/python/alpine/age/excel365/ladies_chrono.pkl")
ladiesdf.to_excel("/Users/syverjohansen/ski/elo/python/alpine/age/excel365/ladies_chrono.xlsx")

mendf = men()		
mendf.to_pickle("/Users/syverjohansen/ski/elo/python/alpine/age/excel365/men_chrono.pkl")
mendf.to_excel("/Users/syverjohansen/ski/elo/python/alpine/age/excel365/men_chrono.xlsx")

print(time.time() - start_time)