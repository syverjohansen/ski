import pandas as pd
import time
start_time = time.time()


def lady_radar():
	lady_all_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_all_k = lady_all_k.loc[lady_all_k['season']>=2021]
	lady_all_k = lady_all_k.loc[lady_all_k['level']=="end"]

	lady_tech_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_tech_k.xlsx', sheet_name="Sheet1", header=0)
	lady_tech_k = lady_tech_k.loc[lady_tech_k['season']>=2020]
	lady_tech_k = lady_tech_k.loc[lady_tech_k['level']=="end"]

	lady_speed_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_speed_k.xlsx', sheet_name="Sheet1", header=0)
	lady_speed_k = lady_speed_k.loc[lady_speed_k['season']>=2020]
	lady_speed_k = lady_speed_k.loc[lady_speed_k['level']=="end"]

	lady_slalom_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_slalom_k.xlsx', sheet_name="Sheet1", header=0)
	lady_slalom_k = lady_slalom_k.loc[lady_slalom_k['season']>=2020]
	lady_slalom_k = lady_slalom_k.loc[lady_slalom_k['level']=="end"]

	lady_gs_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_gs_k.xlsx', sheet_name="Sheet1", header=0)
	lady_gs_k = lady_gs_k.loc[lady_gs_k['season']>=2020]
	lady_gs_k = lady_gs_k.loc[lady_gs_k['level']=="end"]

	lady_superg_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_superg_k.xlsx', sheet_name="Sheet1", header=0)
	lady_superg_k = lady_superg_k.loc[lady_superg_k['season']>=2020]
	lady_superg_k = lady_superg_k.loc[lady_superg_k['level']=="end"]

	lady_downhill_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_downhill_k.xlsx', sheet_name="Sheet1", header=0)
	lady_downhill_k = lady_downhill_k.loc[lady_downhill_k['season']>=2020]
	lady_downhill_k = lady_downhill_k.loc[lady_downhill_k['level']=="end"]

	lady_ac_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varladies_ac_k.xlsx', sheet_name="Sheet1", header=0)
	lady_ac_k = lady_ac_k.loc[lady_ac_k['season']>=2020]
	lady_ac_k = lady_ac_k.loc[lady_ac_k['level']=="end"]

	lady_values = pd.DataFrame(columns=['Name',  "all", "tech", "speed", "slalom", "gs", "superg", "downhill", "ac"])


	ski_ids_s = list(pd.unique(lady_all_k["id"]))

	for ids in ski_ids_s:
		#print(ids)
		skier = lady_all_k.loc[lady_all_k['id']==ids]
		name = skier['name'].iloc[-1]
		alll = skier['pelo'].iloc[-1]

		try:
			skier_sp = lady_tech_k.loc[lady_tech_k['id']==ids]
			sp = skier_sp['pelo'].iloc[-1]
		except:
			sp = 1300

		try:
			skier_sc = lady_speed_k.loc[lady_speed_k['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]
		except:
			sc = 1300

		try:
			skier_sf = lady_slalom_k.loc[lady_slalom_k['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]
		except:
			sf= 1300

		try:
			skier_d = lady_gs_k.loc[lady_gs_k['id']==ids]
			d = skier_d['pelo'].iloc[-1]
		except:
			d = 1300

		try:
			skier_dc = lady_superg_k.loc[lady_superg_k['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]
		except:
			dc = 1300

		try:
			skier_df = lady_downhill_k.loc[lady_downhill_k['id']==ids]
			df = skier_df['pelo'].iloc[-1]
		except:
			df = 1300

		try:
			skier_ac = lady_ac_k.loc[lady_ac_k['id']==ids]
			ac = skier_ac['pelo'].iloc[-1]
		except:
			ac = 1300

		lady_values = lady_values.append({'Name':name, 'all':alll, 'tech':sp, 'speed':sc, 'slalom':sf,
			'gs':d, 'superg':dc, 'downhill':df, 'ac':ac}, ignore_index=True)
	return lady_values


def man_radar():
	man_all_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_all_k = man_all_k.loc[man_all_k['season']>=2021]
	man_all_k = man_all_k.loc[man_all_k['level']=="end"]

	man_tech_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_tech_k.xlsx', sheet_name="Sheet1", header=0)
	man_tech_k = man_tech_k.loc[man_tech_k['season']>=2020]
	man_tech_k = man_tech_k.loc[man_tech_k['level']=="end"]

	man_speed_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_speed_k.xlsx', sheet_name="Sheet1", header=0)
	man_speed_k = man_speed_k.loc[man_speed_k['season']>=2020]
	man_speed_k = man_speed_k.loc[man_speed_k['level']=="end"]

	man_slalom_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_slalom_k.xlsx', sheet_name="Sheet1", header=0)
	man_slalom_k = man_slalom_k.loc[man_slalom_k['season']>=2020]
	man_slalom_k = man_slalom_k.loc[man_slalom_k['level']=="end"]

	man_gs_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_gs_k.xlsx', sheet_name="Sheet1", header=0)
	man_gs_k = man_gs_k.loc[man_gs_k['season']>=2020]
	man_gs_k = man_gs_k.loc[man_gs_k['level']=="end"]

	man_superg_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_superg_k.xlsx', sheet_name="Sheet1", header=0)
	man_superg_k = man_superg_k.loc[man_superg_k['season']>=2020]
	man_superg_k = man_superg_k.loc[man_superg_k['level']=="end"]

	man_downhill_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_downhill_k.xlsx', sheet_name="Sheet1", header=0)
	man_downhill_k = man_downhill_k.loc[man_downhill_k['season']>=2020]
	man_downhill_k = man_downhill_k.loc[man_downhill_k['level']=="end"]

	man_ac_k = pd.read_excel('~/ski/elo/python/alpine/excel365/varmen_ac_k.xlsx', sheet_name="Sheet1", header=0)
	man_ac_k = man_ac_k.loc[man_ac_k['season']==2020]
	man_ac_k = man_ac_k.loc[man_ac_k['level']=="end"]


	man_values = pd.DataFrame(columns=['Name',  "all", "tech", "speed", "slalom", "gs", "superg", "downhill", "ac"])


	ski_ids_s = list(pd.unique(man_all_k["id"]))

	for ids in ski_ids_s:
		#print(ids)
		skier = man_all_k.loc[man_all_k['id']==ids]
		name = skier['name'].iloc[-1]
		alll = skier['pelo'].iloc[-1]

		try:
			skier_sp = man_tech_k.loc[man_tech_k['id']==ids]
			sp = skier_sp['pelo'].iloc[-1]
		except:
			sp = 1300

		try:
			skier_sc = man_speed_k.loc[man_speed_k['id']==ids]
			sc = skier_sc['pelo'].iloc[-1]
		except:
			sc = 1300

		try:
			skier_sf = man_slalom_k.loc[man_slalom_k['id']==ids]
			sf = skier_sf['pelo'].iloc[-1]
		except:
			sf= 1300

		try:
			skier_d = man_gs_k.loc[man_gs_k['id']==ids]
			d = skier_d['pelo'].iloc[-1]
		except:
			d = 1300

		try:
			skier_dc = man_superg_k.loc[man_superg_k['id']==ids]
			dc = skier_dc['pelo'].iloc[-1]
		except:
			dc = 1300

		try:
			skier_df = man_downhill_k.loc[man_downhill_k['id']==ids]
			df = skier_df['pelo'].iloc[-1]
		except:
			df = 1300

		try:
			skier_ac = man_ac_k.loc[man_ac_k['id']==ids]
			ac = skier_ac['pelo'].iloc[-1]
		except:
			ac = 1300

		man_values = man_values.append({'Name':name, 'all':alll, 'tech':sp, 'speed':sc, 'slalom':sf,
			'gs':d, 'superg':dc, 'downhill':df, 'ac':ac}, ignore_index=True)
	return man_values
		#print(man_values)

def decimals(df):
	columns = list(df)
	for i in range(1,len(columns)):
		#try:
		col = columns[i]
		print(col)
		max_col = float(max(df[columns[i]]))
		print(df[columns[i]])

		df[columns[i]] = df[columns[i]].map(lambda x: float(x)/max_col)
	
		#except:
			#print(i)
		#	continue
	return df


lady_valuesdf = lady_radar()
lady_valuesdf = decimals(lady_valuesdf)
print(lady_valuesdf)
lady_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/alpine/radar/lady_values.pkl")
lady_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/alpine/radar/lady_values.xlsx")

man_valuesdf = man_radar()
man_valuesdf = decimals(man_valuesdf)
print(man_valuesdf)
man_valuesdf.to_pickle("/Users/syverjohansen/ski/elo/python/alpine/radar/man_values.pkl")
man_valuesdf.to_excel("/Users/syverjohansen/ski/elo/python/alpine/radar/man_values.xlsx")
	#print(alll)
print(time.time() - start_time)

#Men Radar
