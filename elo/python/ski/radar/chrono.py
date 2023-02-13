import pandas as pd
import time
from functools import reduce
pd.options.mode.chained_assignment = None
start_time = time.time()


#Go through unique dates

def ladies():
	lady_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_k = lady_distance_k.rename(columns = {'pelo':'distance_pelo', 'elo':'distance_elo'})
	lady_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_classic_k = lady_distance_classic_k.rename(columns = {'pelo':'distance_classic_pelo', 'elo':'distance_classic_elo'})
	lady_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_freestyle_k = lady_distance_freestyle_k.rename(columns = {'pelo':'distance_freestyle_pelo', 'elo':'distance_freestyle_elo'})
	lady_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_k = lady_sprint_k.rename(columns = {'pelo':'sprint_pelo', 'elo':'sprint_elo'})
	lady_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_classic_k = lady_sprint_classic_k.rename(columns = {'pelo':'sprint_classic_pelo', 'elo':'sprint_classic_elo'})
	lady_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_freestyle_k = lady_sprint_freestyle_k.rename(columns = {'pelo':'sprint_freestyle_pelo', 'elo':'sprint_freestyle_elo'})
	lady_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_classic_k.xlsx', sheet_name="Sheet1", header=0)
	lady_classic_k = lady_classic_k.rename(columns = {'pelo':'classic_pelo', 'elo':'classic_elo'})
	lady_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	lady_freestyle_k = lady_freestyle_k.rename(columns = {'pelo':'freestyle_pelo', 'elo':'freestyle_elo'})
	print("Done reading ladies files")

	'''dfs = [lady_all_k, lady_distance_k, lady_distance_classic_k, lady_distance_freestyle_k, 
	lady_sprint_k, lady_sprint_classic_k, lady_sprint_freestyle_k]'''
	#lady_all_k = lady_all_k.loc[lady_all_k['season']==1996]
	#lady_all_k = lady_all_k.drop("Unnamed: 0", axis=1)
	#cols_to_use = lady_distance_k.columns.difference(lady_all_k.columns)
	#print(cols_to_use)
	lady_all_k = lady_all_k[lady_all_k['city']!='Summer']
	distance_col = list(lady_all_k['distance'])
	
	lady_distance_k = lady_distance_k[lady_distance_k['city']!='Summer']
	lady_distance_classic_k = lady_distance_classic_k[lady_distance_classic_k['city']!='Summer']
	lady_distance_freestyle_k = lady_distance_freestyle_k[lady_distance_freestyle_k['city']!='Summer']
	lady_sprint_k = lady_sprint_k[lady_sprint_k['city']!='Summer']
	lady_sprint_classic_k = lady_sprint_classic_k[lady_sprint_classic_k['city']!='Summer']
	lady_sprint_freestyle_k = lady_sprint_freestyle_k[lady_sprint_freestyle_k['city']!='Summer']
	lady_classic_k = lady_classic_k[lady_classic_k['city']!="Summer"]
	lady_freestyle_k = lady_freestyle_k[lady_freestyle_k['city']!="Summer"]

	lady_all_k = lady_all_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	'pelo', 'elo' ]]
	lady_distance_k = lady_distance_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"distance_pelo", "distance_elo"]]
	lady_distance_classic_k = lady_distance_classic_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"distance_classic_pelo", "distance_classic_elo"]]
	lady_distance_freestyle_k = lady_distance_freestyle_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"distance_freestyle_pelo", "distance_freestyle_elo"]]
	lady_sprint_k = lady_sprint_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"sprint_pelo", "sprint_elo"]]
	lady_sprint_classic_k = lady_sprint_classic_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"sprint_classic_pelo", "sprint_classic_elo"]]
	lady_sprint_freestyle_k = lady_sprint_freestyle_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"sprint_freestyle_pelo", "sprint_freestyle_elo"]]
	lady_classic_k = lady_classic_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"classic_pelo", "classic_elo"]]
	lady_freestyle_k = lady_freestyle_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"freestyle_pelo", "freestyle_elo"]]

	lady_all_k1 = lady_all_k.merge(lady_distance_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	lady_all_k2 = lady_all_k1.merge(lady_distance_classic_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	lady_all_k3 = lady_all_k2.merge(lady_distance_freestyle_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	lady_all_k4 = lady_all_k3.merge(lady_sprint_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	lady_all_k5 = lady_all_k4.merge(lady_sprint_classic_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	lady_all_k6 = lady_all_k5.merge(lady_sprint_freestyle_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	lady_all_k7 = lady_all_k6.merge(lady_classic_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	lady_all_k8 = lady_all_k7.merge(lady_freestyle_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	print("Ladies files merged")
	lady_all_k8['distance'] = distance_col

	unique_ids = pd.unique(lady_all_k8['id'])
	newdf = pd.DataFrame()
	for a in range(len(unique_ids)):
		skierdf = lady_all_k8.loc[lady_all_k8['id']==unique_ids[a]]
		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['distance_elo'] = skierdf['distance_elo'].ffill()
		skierdf['distance_classic_elo'] = skierdf['distance_classic_elo'].ffill()
		skierdf['distance_freestyle_elo'] = skierdf['distance_freestyle_elo'].ffill()
		skierdf['sprint_elo'] = skierdf['sprint_elo'].ffill()
		skierdf['sprint_classic_elo'] = skierdf['sprint_classic_elo'].ffill()
		skierdf['sprint_freestyle_elo'] = skierdf['sprint_freestyle_elo'].ffill()
		skierdf['classic_elo'] = skierdf['classic_elo'].ffill()
		skierdf['freestyle_elo'] = skierdf['freestyle_elo'].ffill()
		
		skierdf['pelo'] = skierdf['pelo'].fillna(skierdf['elo'])	
		skierdf['distance_pelo'] = skierdf['distance_pelo'].fillna(skierdf['distance_elo'])
		skierdf['distance_classic_pelo'] = skierdf['distance_classic_pelo'].fillna(skierdf['distance_classic_elo'])	
		skierdf['distance_freestyle_pelo'] = skierdf['distance_freestyle_pelo'].fillna(skierdf['distance_freestyle_elo'])	
		skierdf['sprint_pelo'] = skierdf['sprint_pelo'].fillna(skierdf['sprint_elo'])	
		skierdf['sprint_classic_pelo'] = skierdf['sprint_classic_pelo'].fillna(skierdf['sprint_classic_elo'])	
		skierdf['sprint_freestyle_pelo'] = skierdf['sprint_freestyle_pelo'].fillna(skierdf['sprint_freestyle_elo'])
		skierdf['classic_pelo'] = skierdf['classic_pelo'].fillna(skierdf['classic_elo'])	
		skierdf['freestyle_pelo'] = skierdf['freestyle_pelo'].fillna(skierdf['freestyle_elo'])

		newdf = newdf.append(skierdf)
	
	newdf = newdf.sort_values(by=['Unnamed: 0'])
	print("Ladies NA's Forward Filled")
	return newdf


def men():
	man_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_k = man_distance_k.rename(columns = {'pelo':'distance_pelo', 'elo':'distance_elo'})
	man_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_classic_k = man_distance_classic_k.rename(columns = {'pelo':'distance_classic_pelo', 'elo':'distance_classic_elo'})
	man_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_freestyle_k = man_distance_freestyle_k.rename(columns = {'pelo':'distance_freestyle_pelo', 'elo':'distance_freestyle_elo'})
	man_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_k = man_sprint_k.rename(columns = {'pelo':'sprint_pelo', 'elo':'sprint_elo'})
	man_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_classic_k = man_sprint_classic_k.rename(columns = {'pelo':'sprint_classic_pelo', 'elo':'sprint_classic_elo'})
	man_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_freestyle_k = man_sprint_freestyle_k.rename(columns = {'pelo':'sprint_freestyle_pelo', 'elo':'sprint_freestyle_elo'})
	man_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_classic_k.xlsx', sheet_name="Sheet1", header=0)
	man_classic_k = man_classic_k.rename(columns = {'pelo':'classic_pelo', 'elo':'classic_elo'})
	man_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_freestyle_k = man_freestyle_k.rename(columns = {'pelo':'freestyle_pelo', 'elo':'freestyle_elo'})
	print("Done reading men's files")

	'''dfs = [man_all_k, man_distance_k, man_distance_classic_k, man_distance_freestyle_k, 
	man_sprint_k, man_sprint_classic_k, man_sprint_freestyle_k]'''
	#man_all_k = man_all_k.loc[man_all_k['season']==1996]
	#man_all_k = man_all_k.drop("Unnamed: 0", axis=1)
	#cols_to_use = man_distance_k.columns.difference(man_all_k.columns)
	#print(cols_to_use)
	man_all_k = man_all_k[man_all_k['city']!='Summer']
	distance_col = list(man_all_k['distance'])
	#print(distance_col)
	man_distance_k = man_distance_k[man_distance_k['city']!='Summer']
	man_distance_classic_k = man_distance_classic_k[man_distance_classic_k['city']!='Summer']
	man_distance_freestyle_k = man_distance_freestyle_k[man_distance_freestyle_k['city']!='Summer']
	man_sprint_k = man_sprint_k[man_sprint_k['city']!='Summer']
	man_sprint_classic_k = man_sprint_classic_k[man_sprint_classic_k['city']!='Summer']
	man_sprint_freestyle_k = man_sprint_freestyle_k[man_sprint_freestyle_k['city']!='Summer']
	man_classic_k = man_classic_k[man_classic_k['city']!="Summer"]
	man_freestyle_k = man_freestyle_k[man_freestyle_k['city']!="Summer"]
	man_all_k = man_all_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	'pelo', 'elo' ]]
	man_distance_k = man_distance_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"distance_pelo", "distance_elo"]]
	man_distance_classic_k = man_distance_classic_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"distance_classic_pelo", "distance_classic_elo"]]
	man_distance_freestyle_k = man_distance_freestyle_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"distance_freestyle_pelo", "distance_freestyle_elo"]]
	man_sprint_k = man_sprint_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"sprint_pelo", "sprint_elo"]]
	man_sprint_classic_k = man_sprint_classic_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"sprint_classic_pelo", "sprint_classic_elo"]]
	man_sprint_freestyle_k = man_sprint_freestyle_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"sprint_freestyle_pelo", "sprint_freestyle_elo"]]
	man_classic_k = man_classic_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"classic_pelo", "classic_elo"]]
	man_freestyle_k = man_freestyle_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race',
	"freestyle_pelo", "freestyle_elo"]]

	man_all_k1 = man_all_k.merge(man_distance_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	man_all_k2 = man_all_k1.merge(man_distance_classic_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	man_all_k3 = man_all_k2.merge(man_distance_freestyle_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	man_all_k4 = man_all_k3.merge(man_sprint_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	man_all_k5 = man_all_k4.merge(man_sprint_classic_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	man_all_k6 = man_all_k5.merge(man_sprint_freestyle_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	man_all_k7 = man_all_k6.merge(man_classic_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	man_all_k8 = man_all_k7.merge(man_freestyle_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race'], how="left")
	print("Men files merged")

	man_all_k8['distance'] = distance_col

	unique_ids = pd.unique(man_all_k8['id'])
	newdf = pd.DataFrame()
	for a in range(len(unique_ids)):
		skierdf = man_all_k8.loc[man_all_k8['id']==unique_ids[a]]
		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['distance_elo'] = skierdf['distance_elo'].ffill()
		skierdf['distance_classic_elo'] = skierdf['distance_classic_elo'].ffill()
		skierdf['distance_freestyle_elo'] = skierdf['distance_freestyle_elo'].ffill()
		skierdf['sprint_elo'] = skierdf['sprint_elo'].ffill()
		skierdf['sprint_classic_elo'] = skierdf['sprint_classic_elo'].ffill()
		skierdf['sprint_freestyle_elo'] = skierdf['sprint_freestyle_elo'].ffill()
		skierdf['classic_elo'] = skierdf['classic_elo'].ffill()
		skierdf['freestyle_elo'] = skierdf['freestyle_elo'].ffill()
		
		skierdf['pelo'] = skierdf['pelo'].fillna(skierdf['elo'])	
		skierdf['distance_pelo'] = skierdf['distance_pelo'].fillna(skierdf['distance_elo'])
		skierdf['distance_classic_pelo'] = skierdf['distance_classic_pelo'].fillna(skierdf['distance_classic_elo'])	
		skierdf['distance_freestyle_pelo'] = skierdf['distance_freestyle_pelo'].fillna(skierdf['distance_freestyle_elo'])	
		skierdf['sprint_pelo'] = skierdf['sprint_pelo'].fillna(skierdf['sprint_elo'])	
		skierdf['sprint_classic_pelo'] = skierdf['sprint_classic_pelo'].fillna(skierdf['sprint_classic_elo'])	
		skierdf['sprint_freestyle_pelo'] = skierdf['sprint_freestyle_pelo'].fillna(skierdf['sprint_freestyle_elo'])
		skierdf['classic_pelo'] = skierdf['classic_pelo'].fillna(skierdf['classic_elo'])	
		skierdf['freestyle_pelo'] = skierdf['freestyle_pelo'].fillna(skierdf['freestyle_elo'])
	
		newdf = newdf.append(skierdf)


	
	newdf = newdf.sort_values(by=['Unnamed: 0'])
	print("Men NA's Forward Filled")
	return newdf


ladiesdf = ladies()
mendf = men()


		
ladiesdf.to_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/ladies_chrono.pkl")
ladiesdf.to_excel("/Users/syverjohansen/ski/elo/python/ski/radar/ladies_chrono.xlsx")

mendf.to_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/men_chrono.pkl")
mendf.to_excel("/Users/syverjohansen/ski/elo/python/ski/radar/men_chrono.xlsx")

print(time.time() - start_time)