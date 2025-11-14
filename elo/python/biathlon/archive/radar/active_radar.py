import pandas as pd
import time
start_time = time.time()
'''lady_all_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
#print(max(lady_all_k['elo']))
lady_sprint_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
lady_pursuit_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
lady_individual_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_individual_k.xlsx', sheet_name="Sheet1", header=0)
lady_distance_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_distance_k.xlsx', sheet_name="Sheet1", header=0)
lady_mass_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_mass_k.xlsx', sheet_name="Sheet1", header=0)
lady_distance_freestyle_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
lady_maxes = [max(lady_all_k['elo']), max(lady_sprint_k['elo']), max(lady_pursuit_k['elo']), max(lady_individual_k['elo']),
max(lady_distance_k['elo']), max(lady_mass_k['elo']), max(lady_distance_freestyle_k['elo'])]


man_all_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
man_sprint_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
man_pursuit_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
man_individual_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_individual_k.xlsx', sheet_name="Sheet1", header=0)
man_distance_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_distance_k.xlsx', sheet_name="Sheet1", header=0)
man_mass_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_mass_k.xlsx', sheet_name="Sheet1", header=0)
man_distance_freestyle_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
man_maxes = [max(man_all_k['elo']), max(man_sprint_k['elo']), max(man_pursuit_k['elo']), max(man_individual_k['elo']),
max(man_distance_k['elo']), max(man_mass_k['elo']), max(man_distance_freestyle_k['elo'])]'''





def lady_radar():
	lady_all_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_all_max = max(lady_all_k['elo'])
	lady_all_k = lady_all_k.loc[lady_all_k['season']==2022]
	lady_all_k = lady_all_k.loc[lady_all_k['level']=="end"]


	lady_sprint_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_max = max(lady_sprint_k['elo'])
	lady_sprint_k = lady_sprint_k.loc[lady_sprint_k['season']>=2019]
	lady_sprint_k = lady_sprint_k.loc[lady_sprint_k['level']=="end"]

	lady_pursuit_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
	lady_pursuit_max = max(lady_pursuit_k['elo'])
	#lady_pursuit_all = lady_pursuit_k
	lady_pursuit_k = lady_pursuit_k.loc[lady_pursuit_k['season']>-2020]
	lady_pursuit_k = lady_pursuit_k.loc[lady_pursuit_k['level']=="end"]

	lady_individual_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_individual_k.xlsx', sheet_name="Sheet1", header=0)
	lady_individual_max = max(lady_individual_k['elo'])
	#lady_individual_all - lady_individual_k
	lady_individual_k = lady_individual_k.loc[lady_individual_k['season']>=2020]
	lady_individual_k = lady_individual_k.loc[lady_individual_k['level']=="end"]



	lady_mass_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varladies_mass_k.xlsx', sheet_name="Sheet1", header=0)
	lady_mass_max = max(lady_mass_k['elo'])
	#lady_mass_all = lady_mass_k
	lady_mass_k = lady_mass_k.loc[lady_mass_k['season']>=2020]
	lady_mass_k = lady_mass_k.loc[lady_mass_k['level']=="end"]

	

	lady_values = pd.DataFrame(columns=['Name',"Nation", "Overall", "Sprint", "Pursuit", "Individual", "Mass"])


	ski_ids_s = list(pd.unique(lady_all_k["id"]))

	for ids in ski_ids_s:
		#print(ids)
		skier = lady_all_k.loc[lady_all_k['id']==ids]
		name = skier['name'].iloc[-1]
		nation = skier['nation'].iloc[-1]
		try:
			alll = skier['pelo'].iloc[-1]/lady_all_max
		except:
			alll = 1300/lady_all_max
		

		try:
			skier_sp = lady_sprint_k.loc[lady_sprint_k['id']==ids]
			sp = skier_sp['pelo'].iloc[-1]/lady_sprint_max
		except:
			sp = 1300/lady_sprint_max

		print(name, sp)

		try:
			skier_sc = lady_pursuit_k.loc[lady_pursuit_k['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]/lady_pursuit_max
		except:
			sc = 1300/lady_pursuit_max

		try:
			skier_sf = lady_individual_k.loc[lady_individual_k['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]/lady_individual_max
		except:
			sf= 1300/lady_individual_max


		try:
			skier_dc = lady_mass_k.loc[lady_mass_k['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]/lady_mass_max
		except:
			dc = 1300/lady_mass_max

		

		lady_values = lady_values.append({'Name':name,  'Nation':nation, 'Overall':alll, 'Sprint':sp, 'Pursuit':sc, 'Individual':sf,
			 'Mass':dc}, ignore_index=True)
		#print(lady_values)
	#print(lady_values)
	#print(lady_values)
	return lady_values


def man_radar():
	man_all_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_all_max = max(man_all_k['elo'])
	man_all_k = man_all_k.loc[man_all_k['season']==2022]
	man_all_k = man_all_k.loc[man_all_k['level']=="end"]

	man_sprint_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_max = max(man_sprint_k['elo'])
	man_sprint_k = man_sprint_k.loc[man_sprint_k['season']>=2020]
	man_sprint_k = man_sprint_k.loc[man_sprint_k['level']=="end"]

	man_pursuit_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_pursuit_k.xlsx', sheet_name="Sheet1", header=0)
	man_pursuit_max = max(man_pursuit_k['elo'])
	man_pursuit_k = man_pursuit_k.loc[man_pursuit_k['season']>=2020]
	man_pursuit_k = man_pursuit_k.loc[man_pursuit_k['level']=="end"]

	man_individual_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_individual_k.xlsx', sheet_name="Sheet1", header=0)
	man_individual_max = max(man_individual_k['elo'])
	man_individual_k = man_individual_k.loc[man_individual_k['season']>=2020]
	man_individual_k = man_individual_k.loc[man_individual_k['level']=="end"]

	
	man_mass_k = pd.read_excel('~/ski/elo/python/biathlon/excel365/varmen_mass_k.xlsx', sheet_name="Sheet1", header=0)
	man_mass_max = max(man_mass_k['elo'])
	man_mass_k = man_mass_k.loc[man_mass_k['season']>=2020]
	man_mass_k = man_mass_k.loc[man_mass_k['level']=="end"]

	
	man_values = pd.DataFrame(columns=['Name',"Nation", "Overall", "Sprint", "Pursuit", "Individual", "Mass"])


	ski_ids_s = list(pd.unique(man_all_k["id"]))

	for ids in ski_ids_s:
		#print(ids)
		skier = man_all_k.loc[man_all_k['id']==ids]
		name = skier['name'].iloc[-1]
		nation = skier['nation'].iloc[-1]
		alll = skier['pelo'].iloc[-1]/man_all_max
		print(alll)

		try:
			skier_sp = man_sprint_k.loc[man_sprint_k['id']==ids]
			sp = skier_sp['pelo'].iloc[-1]/man_sprint_max
		except:
			sp = 1300/man_sprint_max

		try:
			skier_sc = man_pursuit_k.loc[man_pursuit_k['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]/man_pursuit_max
		except:
			sc = 1300/man_pursuit_max

		try:
			skier_sf = man_individual_k.loc[man_individual_k['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]/man_individual_max
		except:
			sf= 1300/man_individual_max


		try:
			skier_dc = man_mass_k.loc[man_mass_k['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]/man_mass_max
		except:
			dc = 1300/man_mass_max



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
#lady_valuesdf = decimals(lady_valuesdf, "L")
#print(lady_valuesdf)
lady_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/biathlon/radar/lady_active_values.pkl")
lady_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/biathlon/radar/lady_active_values.xlsx")

man_valuesdf = man_radar()
#man_valuesdf = decimals(man_valuesdf, "M")
print(man_valuesdf)
man_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/biathlon/radar/man_active_values.pkl")
man_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/biathlon/radar/man_active_values.xlsx")
	#print(alll)
	
print(time.time() - start_time)

#Men Radar
