import pandas as pd
import time
start_time = time.time()
'''lady_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
#print(max(lady_all_k['elo']))
lady_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
lady_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
lady_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
lady_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_k.xlsx', sheet_name="Sheet1", header=0)
lady_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
lady_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
lady_maxes = [max(lady_all_k['elo']), max(lady_sprint_k['elo']), max(lady_sprint_classic_k['elo']), max(lady_sprint_freestyle_k['elo']),
max(lady_distance_k['elo']), max(lady_distance_classic_k['elo']), max(lady_distance_freestyle_k['elo'])]


man_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
man_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
man_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
man_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
man_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_k.xlsx', sheet_name="Sheet1", header=0)
man_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
man_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
man_maxes = [max(man_all_k['elo']), max(man_sprint_k['elo']), max(man_sprint_classic_k['elo']), max(man_sprint_freestyle_k['elo']),
max(man_distance_k['elo']), max(man_distance_classic_k['elo']), max(man_distance_freestyle_k['elo'])]'''





def lady_radar():
	lady_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_all_max = max(lady_all_k['elo'])
	lady_all_k = lady_all_k.loc[lady_all_k['season']==2022]
	lady_all_k = lady_all_k.loc[lady_all_k['level']=="end"]


	lady_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_max = max(lady_sprint_k['elo'])
	lady_sprint_k = lady_sprint_k.loc[lady_sprint_k['season']>=2019]
	lady_sprint_k = lady_sprint_k.loc[lady_sprint_k['level']=="end"]

	lady_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_classic_max = max(lady_sprint_classic_k['elo'])
	#lady_sprint_classic_all = lady_sprint_classic_k
	lady_sprint_classic_k = lady_sprint_classic_k.loc[lady_sprint_classic_k['season']>-2020]
	lady_sprint_classic_k = lady_sprint_classic_k.loc[lady_sprint_classic_k['level']=="end"]

	lady_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_freestyle_max = max(lady_sprint_freestyle_k['elo'])
	#lady_sprint_freestyle_all - lady_sprint_freestyle_k
	lady_sprint_freestyle_k = lady_sprint_freestyle_k.loc[lady_sprint_freestyle_k['season']>=2020]
	lady_sprint_freestyle_k = lady_sprint_freestyle_k.loc[lady_sprint_freestyle_k['level']=="end"]

	lady_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_max = max(lady_distance_k['elo'])
	#lady_distance_all - lady_distance_k
	lady_distance_k = lady_distance_k.loc[lady_distance_k['season']>=2020]
	lady_distance_k = lady_distance_k.loc[lady_distance_k['level']=="end"]

	lady_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_classic_max = max(lady_distance_classic_k['elo'])
	#lady_distance_classic_all = lady_distance_classic_k
	lady_distance_classic_k = lady_distance_classic_k.loc[lady_distance_classic_k['season']>=2020]
	lady_distance_classic_k = lady_distance_classic_k.loc[lady_distance_classic_k['level']=="end"]

	lady_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_freestyle_max = max(lady_distance_freestyle_k['elo'])
	#lady_distance_freestyle_all - lady_distance_freestyle_k
	lady_distance_freestyle_k = lady_distance_freestyle_k.loc[lady_distance_freestyle_k['season']>=2020]
	lady_distance_freestyle_k = lady_distance_freestyle_k.loc[lady_distance_freestyle_k['level']=="end"]

	lady_values = pd.DataFrame(columns=['Name',"Nation", "Overall", "Sprint", "Sprint Classic", "Sprint Freestyle", "Distance", "Distance Classic", "Distance Freestyle"])


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
			skier_sc = lady_sprint_classic_k.loc[lady_sprint_classic_k['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]/lady_sprint_classic_max
		except:
			sc = 1300/lady_sprint_classic_max

		try:
			skier_sf = lady_sprint_freestyle_k.loc[lady_sprint_freestyle_k['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]/lady_sprint_freestyle_max
		except:
			sf= 1300/lady_sprint_freestyle_max

		try:
			skier_d = lady_distance_k.loc[lady_distance_k['id']==ids]
			d = skier_d['pelo'].iloc[-1]/lady_distance_max
		except:
			d = 1300/lady_distance_max

		try:
			skier_dc = lady_distance_classic_k.loc[lady_distance_classic_k['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]/lady_distance_classic_max
		except:
			dc = 1300/lady_distance_classic_max

		try:
			skier_df = lady_distance_freestyle_k.loc[lady_distance_freestyle_k['id']==ids]
			df = skier_df['pelo'].iloc[-1]/lady_distance_freestyle_max
		except:
			df = 1300/lady_distance_freestyle_max

		lady_values = lady_values.append({'Name':name,  'Nation':nation, 'Overall':alll, 'Sprint':sp, 'Sprint Classic':sc, 'Sprint Freestyle':sf,
			'Distance':d, 'Distance Classic':dc, 'Distance Freestyle':df}, ignore_index=True)
		#print(lady_values)
	#print(lady_values)
	#print(lady_values)
	return lady_values


def man_radar():
	man_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_all_max = max(man_all_k['elo'])
	man_all_k = man_all_k.loc[man_all_k['season']==2022]
	man_all_k = man_all_k.loc[man_all_k['level']=="end"]

	man_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_max = max(man_sprint_k['elo'])
	man_sprint_k = man_sprint_k.loc[man_sprint_k['season']>=2020]
	man_sprint_k = man_sprint_k.loc[man_sprint_k['level']=="end"]

	man_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_classic_max = max(man_sprint_classic_k['elo'])
	man_sprint_classic_k = man_sprint_classic_k.loc[man_sprint_classic_k['season']>=2020]
	man_sprint_classic_k = man_sprint_classic_k.loc[man_sprint_classic_k['level']=="end"]

	man_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_freestyle_max = max(man_sprint_freestyle_k['elo'])
	man_sprint_freestyle_k = man_sprint_freestyle_k.loc[man_sprint_freestyle_k['season']>=2020]
	man_sprint_freestyle_k = man_sprint_freestyle_k.loc[man_sprint_freestyle_k['level']=="end"]

	man_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_max = max(man_distance_k['elo'])
	man_distance_k = man_distance_k.loc[man_distance_k['season']>=2020]
	man_distance_k = man_distance_k.loc[man_distance_k['level']=="end"]

	man_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_classic_max = max(man_distance_classic_k['elo'])
	man_distance_classic_k = man_distance_classic_k.loc[man_distance_classic_k['season']>=2020]
	man_distance_classic_k = man_distance_classic_k.loc[man_distance_classic_k['level']=="end"]

	man_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_freestyle_max = max(man_distance_freestyle_k['elo'])
	man_distance_freestyle_k = man_distance_freestyle_k.loc[man_distance_freestyle_k['season']>=2020]
	man_distance_freestyle_k = man_distance_freestyle_k.loc[man_distance_freestyle_k['level']=="end"]

	man_values = pd.DataFrame(columns=['Name',"Nation", "Overall", "Sprint", "Sprint Classic", "Sprint Freestyle", "Distance", "Distance Classic", "Distance Freestyle"])


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
			skier_sc = man_sprint_classic_k.loc[man_sprint_classic_k['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]/man_sprint_classic_max
		except:
			sc = 1300/man_sprint_classic_max

		try:
			skier_sf = man_sprint_freestyle_k.loc[man_sprint_freestyle_k['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]/man_sprint_freestyle_max
		except:
			sf= 1300/man_sprint_freestyle_max

		try:
			skier_d = man_distance_k.loc[man_distance_k['id']==ids]
			d = skier_d['pelo'].iloc[-1]/man_distance_max
		except:
			d = 1300/man_distance_max

		try:
			skier_dc = man_distance_classic_k.loc[man_distance_classic_k['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]/man_distance_classic_max
		except:
			dc = 1300/man_distance_classic_max

		try:
			skier_df = man_distance_freestyle_k.loc[man_distance_freestyle_k['id']==ids]
			df = skier_df['pelo'].iloc[-1]/man_distance_freestyle_max
		except:
			df = 1300/man_distance_freestyle_max

		man_values = man_values.append({'Name':name, 'Nation':nation, 'Overall':alll, 'Sprint':sp, 'Sprint Classic':sc, 'Sprint Freestyle':sf,
			'Distance':d, 'Distance Classic':dc, 'Distance Freestyle':df}, ignore_index=True)
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
lady_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/lady_active_values.pkl")
lady_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/ski/radar/lady_active_values.xlsx")

man_valuesdf = man_radar()
#man_valuesdf = decimals(man_valuesdf, "M")
print(man_valuesdf)
man_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/ski/radar/man_active_values.pkl")
man_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/ski/radar/man_active_values.xlsx")
	#print(alll)
	
print(time.time() - start_time)

#Men Radar
