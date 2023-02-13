import pandas as pd
import time
start_time = time.time()
'''lady_all = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
#print(max(lady_all['elo']))
lady_sprint = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
lady_pursuit = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
lady_individual = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_individual_k.xlsx', sheet_name="Sheet1", header=0)
lady_distance = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_distance_k.xlsx', sheet_name="Sheet1", header=0)
lady_mass = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_mass_k.xlsx', sheet_name="Sheet1", header=0)
lady_distance_freestyle = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
lady_maxes = [max(lady_all['elo']), max(lady_sprint['elo']), max(lady_pursuit['elo']), max(lady_individual['elo']),
max(lady_distance['elo']), max(lady_mass['elo']), max(lady_distance_freestyle['elo'])]


man_all = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
man_sprint = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
man_pursuit = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
man_individual = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_individual_k.xlsx', sheet_name="Sheet1", header=0)
man_distance = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_distance_k.xlsx', sheet_name="Sheet1", header=0)
man_mass = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_mass_k.xlsx', sheet_name="Sheet1", header=0)
man_distance_freestyle = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
man_maxes = [max(man_all['elo']), max(man_sprint['elo']), max(man_pursuit['elo']), max(man_individual['elo']),
max(man_distance['elo']), max(man_mass['elo']), max(man_distance_freestyle['elo'])]'''





def lady_radar():
	lady_all = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_all = lady_all.loc[lady_all['season']==2022]
	lady_all = lady_all.loc[lady_all['level']=="end"]


	lady_sprint = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	
	lady_sprint = lady_sprint.loc[lady_sprint['season']>=2019]
	lady_sprint = lady_sprint.loc[lady_sprint['level']=="end"]

	lady_pursuit = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
	#lady_pursuit_all = lady_pursuit_k
	lady_pursuit = lady_pursuit.loc[lady_pursuit['season']>-2020]
	lady_pursuit = lady_pursuit.loc[lady_pursuit['level']=="end"]

	lady_individual = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_individual_k.xlsx', sheet_name="Sheet1", header=0)
	#lady_individual_all - lady_individual_k
	lady_individual = lady_individual.loc[lady_individual['season']>=2020]
	lady_individual = lady_individual.loc[lady_individual['level']=="end"]



	lady_mass = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_mass_k.xlsx', sheet_name="Sheet1", header=0)
	#lady_mass_all = lady_mass_k
	lady_mass = lady_mass.loc[lady_mass['season']>=2020]
	lady_mass = lady_mass.loc[lady_mass['level']=="end"]



	lady_values = pd.DataFrame(columns=['Name',"Nation", "Overall", "Sprint", "Pursuit", "Individual", "Mass"])


	ski_ids_s = list(pd.unique(lady_all["id"]))

	for ids in ski_ids_s:
		#print(ids)
		skier = lady_all.loc[lady_all['id']==ids]
		name = skier['name'].iloc[-1]
		nation = skier['nation'].iloc[-1]
		try:
			alll = skier['pelo'].iloc[-1]
		except:
			alll = 1300
		

		try:
			skier_sp = lady_sprint.loc[lady_sprint['id']==ids]
			sp = skier_sp['pelo'].iloc[-1]
		except:
			sp = 1300

		print(name, sp)

		try:
			skier_sc = lady_pursuit.loc[lady_pursuit['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]
		except:
			sc = 1300

		try:
			skier_sf = lady_individual.loc[lady_individual['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]
		except:
			sf= 1300

		try:
			skier_dc = lady_mass.loc[lady_mass['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]
		except:
			dc = 1300
		lady_values = lady_values.append({'Name':name, 'Nation':nation, 'Overall':alll, 'Sprint':sp, 'Pursuit':sc, 'Individual':sf, 'Mass':dc}, ignore_index=True)
		#print(lady_values)
	#print(lady_values)
	#print(lady_values)
	return lady_values


def man_radar():
	man_all = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_all = man_all.loc[man_all['season']==2022]
	man_all = man_all.loc[man_all['level']=="end"]

	man_sprint = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint = man_sprint.loc[man_sprint['season']>=2020]
	man_sprint = man_sprint.loc[man_sprint['level']=="end"]

	man_pursuit = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
	man_pursuit = man_pursuit.loc[man_pursuit['season']>=2020]
	man_pursuit = man_pursuit.loc[man_pursuit['level']=="end"]

	man_individual = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_individual_k.xlsx', sheet_name="Sheet1", header=0)
	man_individual = man_individual.loc[man_individual['season']>=2020]
	man_individual = man_individual.loc[man_individual['level']=="end"]

	'''man_distance = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_distance_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance = man_distance.loc[man_distance['season']>=2020]
	man_distance = man_distance.loc[man_distance['level']=="end"]'''

	man_mass = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_mass_k.xlsx', sheet_name="Sheet1", header=0)
	man_mass = man_mass.loc[man_mass['season']>=2020]
	man_mass = man_mass.loc[man_mass['level']=="end"]

	'''man_distance_freestyle = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_freestyle = man_distance_freestyle.loc[man_distance_freestyle['season']>=2020]
	man_distance_freestyle = man_distance_freestyle.loc[man_distance_freestyle['level']=="end"]'''

	man_values = pd.DataFrame(columns=['Name',"Nation", "Overall", "Sprint", "Pursuit", "Individual", "Mass"])


	ski_ids_s = list(pd.unique(man_all["id"]))

	for ids in ski_ids_s:
		#print(ids)
		skier = man_all.loc[man_all['id']==ids]
		name = skier['name'].iloc[-1]
		nation = skier['nation'].iloc[-1]
		alll = skier['pelo'].iloc[-1]
		print(alll)

		try:
			skier_sp = man_sprint.loc[man_sprint['id']==ids]
			sp = skier_sp['pelo'].iloc[-1]
		except:
			sp = 1300

		try:
			skier_sc = man_pursuit.loc[man_pursuit['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]
		except:
			sc = 1300

		try:
			skier_sf = man_individual.loc[man_individual['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]
		except:
			sf= 1300


		try:
			skier_dc = man_mass.loc[man_mass['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]
		except:
			dc = 1300

		man_values = man_values.append({'Name':name,  'Nation':nation, 'Overall':alll, 'Sprint':sp, 'Pursuit':sc, 'Individual':sf,
			'Mass':dc}, ignore_index=True)
	print(man_values)	
	return man_values
		#print(man_values)
def decimals(df, sex):
	columns = list(df)
	for i in range(2,len(columns)):
		#try:
		col = columns[i]
		#print(col)

		#Below is for percentage among current
		max_col = float(max(df[columns[i]]))  
		#print(df[columns[i]])

		#Below is for percentage among all time
		'''if(sex=="L"):
			max_col = float(lady_maxes[i-2])
		else:
			max_col = float(man_maxes[i-2])'''

		#print(df[columns[i]])

		df[columns[i]] = df[columns[i]].map(lambda x: float(x)/max_col)
	
		#except:
			#print(i)
		#	continue
	return df



lady_valuesdf = lady_radar()
lady_valuesdf = decimals(lady_valuesdf, "L")
#print(lady_valuesdf)
lady_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/biathlon/radar/lady_values.pkl")
lady_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/biathlon/radar/lady_values_k.xlsx")

man_valuesdf = man_radar()
man_valuesdf = decimals(man_valuesdf, "M")
print(man_valuesdf)
man_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/biathlon/radar/man_values.pkl")
man_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/biathlon/radar/man_values_k.xlsx")
	#print(alll)
	
print(time.time() - start_time)

#Men Radar
