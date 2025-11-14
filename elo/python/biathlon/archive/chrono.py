import pandas as pd
import time
from functools import reduce
pd.options.mode.chained_assignment = None
start_time = time.time()


#Go through unique dates

def ladies():
	lady_all_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_k = lady_sprint_k.rename(columns = {'pelo':'sprint_pelo', 'elo':'sprint_elo'})
	lady_pursuit_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varladies_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
	lady_pursuit_k = lady_pursuit_k.rename(columns = {'pelo':'pursuit_pelo', 'elo':'pursuit_elo'})
	lady_mass_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varladies_mass_k.xlsx', sheet_name="Sheet1", header=0)
	lady_mass_k = lady_mass_k.rename(columns = {'pelo':'mass_pelo', 'elo':'mass_elo'})
	lady_individual_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varladies_individual_k.xlsx', sheet_name="Sheet1", header=0)
	lady_individual_k = lady_individual_k.rename(columns = {'pelo':'individual_pelo', 'elo':'individual_elo'})
	
	print("Done reading ladies files")

	'''dfs = [lady_all_k, lady_distance_k, lady_distance_classic_k, lady_distance_freestyle_k, 
	lady_sprint_k, lady_sprint_classic_k, lady_sprint_freestyle_k]'''
	#lady_all_k = lady_all_k.loc[lady_all_k['season']==1996]
	#lady_all_k = lady_all_k.drop("Unnamed: 0", axis=1)
	#cols_to_use = lady_distance_k.columns.difference(lady_all_k.columns)
	#print(cols_to_use)
	lady_all_k = lady_all_k[lady_all_k['city']!='Summer']
	distance_col = list(lady_all_k['distance'])
	
	lady_sprint_k = lady_sprint_k[lady_sprint_k['city']!='Summer']
	lady_pursuit_k = lady_pursuit_k[lady_pursuit_k['city']!='Summer']
	lady_mass_k = lady_mass_k[lady_mass_k['city']!='Summer']
	lady_individual_k = lady_individual_k[lady_individual_k['city']!='Summer']
	

	lady_all_k = lady_all_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	'pelo', 'elo' ]]
	lady_sprint_k = lady_sprint_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"sprint_pelo", "sprint_elo"]]
	lady_pursuit_k = lady_pursuit_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"pursuit_pelo", "pursuit_elo"]]
	lady_mass_k = lady_mass_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"mass_pelo", "mass_elo"]]
	lady_individual_k = lady_individual_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"individual_pelo", "individual_elo"]]
	

	lady_all_k1 = lady_all_k.merge(lady_sprint_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k2 = lady_all_k1.merge(lady_pursuit_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k3 = lady_all_k2.merge(lady_mass_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	lady_all_k4 = lady_all_k3.merge(lady_individual_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	
	print("Ladies files merged")
	lady_all_k4['distance'] = distance_col

	unique_ids = pd.unique(lady_all_k4['id'])
	newdf = pd.DataFrame()
	for a in range(len(unique_ids)):
		skierdf = lady_all_k4.loc[lady_all_k4['id']==unique_ids[a]]
		
		skierdf['age'] = skierdf['age'].ffill()
		skierdf['exp'] = skierdf['exp'].ffill()

		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['sprint_elo'] = skierdf['sprint_elo'].ffill()
		skierdf['pursuit_elo'] = skierdf['pursuit_elo'].ffill()
		skierdf['mass_elo'] = skierdf['mass_elo'].ffill()
		skierdf['individual_elo'] = skierdf['individual_elo'].ffill()
		
		
		skierdf['pelo'] = skierdf['pelo'].fillna(skierdf['elo'])	
		skierdf['sprint_pelo'] = skierdf['sprint_pelo'].fillna(skierdf['sprint_elo'])
		skierdf['pursuit_pelo'] = skierdf['pursuit_pelo'].fillna(skierdf['pursuit_elo'])	
		skierdf['mass_pelo'] = skierdf['mass_pelo'].fillna(skierdf['mass_elo'])	
		skierdf['individual_pelo'] = skierdf['individual_pelo'].fillna(skierdf['individual_elo'])	
		

		newdf = newdf.append(skierdf)
	
	newdf = newdf.sort_values(by=['Unnamed: 0'])
	print("Ladies NA's Forward Filled")
	return newdf


def men():
	man_all_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_k = man_sprint_k.rename(columns = {'pelo':'sprint_pelo', 'elo':'sprint_elo'})
	man_pursuit_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varmen_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
	man_pursuit_k = man_pursuit_k.rename(columns = {'pelo':'pursuit_pelo', 'elo':'pursuit_elo'})
	man_mass_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varmen_mass_k.xlsx', sheet_name="Sheet1", header=0)
	man_mass_k = man_mass_k.rename(columns = {'pelo':'mass_pelo', 'elo':'mass_elo'})
	man_individual_k = pd.read_excel('~/ski/elo/python/biathlon/age/excel365/varmen_individual_k.xlsx', sheet_name="Sheet1", header=0)
	man_individual_k = man_individual_k.rename(columns = {'pelo':'individual_pelo', 'elo':'individual_elo'})
	
	print("Done reading men files")

	'''dfs = [man_all_k, man_distance_k, man_distance_classic_k, man_distance_freestyle_k, 
	man_sprint_k, man_sprint_classic_k, man_sprint_freestyle_k]'''
	#man_all_k = man_all_k.loc[man_all_k['season']==1996]
	#man_all_k = man_all_k.drop("Unnamed: 0", axis=1)
	#cols_to_use = man_distance_k.columns.difference(man_all_k.columns)
	#print(cols_to_use)
	man_all_k = man_all_k[man_all_k['city']!='Summer']
	distance_col = list(man_all_k['distance'])
	
	man_sprint_k = man_sprint_k[man_sprint_k['city']!='Summer']
	man_pursuit_k = man_pursuit_k[man_pursuit_k['city']!='Summer']
	man_mass_k = man_mass_k[man_mass_k['city']!='Summer']
	man_individual_k = man_individual_k[man_individual_k['city']!='Summer']
	

	man_all_k = man_all_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	'pelo', 'elo' ]]
	man_sprint_k = man_sprint_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"sprint_pelo", "sprint_elo"]]
	man_pursuit_k = man_pursuit_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"pursuit_pelo", "pursuit_elo"]]
	man_mass_k = man_mass_k[['Unnamed: 0','date','city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"mass_pelo", "mass_elo"]]
	man_individual_k = man_individual_k[['Unnamed: 0', 'date', 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp',
	"individual_pelo", "individual_elo"]]
	

	man_all_k1 = man_all_k.merge(man_sprint_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k2 = man_all_k1.merge(man_pursuit_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k3 = man_all_k2.merge(man_mass_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	man_all_k4 = man_all_k3.merge(man_individual_k, on=["Unnamed: 0","date", 'city', 'country', 'level', 'sex', 'discipline', 'place', 'name', 'nation', 'id', 'season', 'race', 'age', 'exp'], how="left")
	
	print("Men files merged")
	man_all_k4['distance'] = distance_col

	unique_ids = pd.unique(man_all_k4['id'])
	newdf = pd.DataFrame()
	for a in range(len(unique_ids)):
		skierdf = man_all_k4.loc[man_all_k4['id']==unique_ids[a]]
		
		skierdf['age'] = skierdf['age'].ffill()
		skierdf['exp'] = skierdf['exp'].ffill()

		skierdf['elo'] = skierdf['elo'].ffill()
		skierdf['sprint_elo'] = skierdf['sprint_elo'].ffill()
		skierdf['pursuit_elo'] = skierdf['pursuit_elo'].ffill()
		skierdf['mass_elo'] = skierdf['mass_elo'].ffill()
		skierdf['individual_elo'] = skierdf['individual_elo'].ffill()
		
		
		skierdf['pelo'] = skierdf['pelo'].fillna(skierdf['elo'])	
		skierdf['sprint_pelo'] = skierdf['sprint_pelo'].fillna(skierdf['sprint_elo'])
		skierdf['pursuit_pelo'] = skierdf['pursuit_pelo'].fillna(skierdf['pursuit_elo'])	
		skierdf['mass_pelo'] = skierdf['mass_pelo'].fillna(skierdf['mass_elo'])	
		skierdf['individual_pelo'] = skierdf['individual_pelo'].fillna(skierdf['individual_elo'])	
		

		newdf = newdf.append(skierdf)
	
	newdf = newdf.sort_values(by=['Unnamed: 0'])
	print("Men NA's Forward Filled")
	return newdf


ladiesdf = ladies()
ladiesdf.to_pickle("/Users/syverjohansen/ski/elo/python/biathlon/age/excel365/ladies_chrono.pkl")
ladiesdf.to_excel("/Users/syverjohansen/ski/elo/python/biathlon/age/excel365/ladies_chrono.xlsx")

mendf = men()		
mendf.to_pickle("/Users/syverjohansen/ski/elo/python/biathlon/age/excel365/men_chrono.pkl")
mendf.to_excel("/Users/syverjohansen/ski/elo/python/biathlon/age/excel365/men_chrono.xlsx")

print(time.time() - start_time)