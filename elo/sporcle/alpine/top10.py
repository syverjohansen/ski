import pandas as pd
import numpy as np
import time

start_time = time.time()

#1. Read the PKL into a dataframe.
#2. Filter down to 0500s
#3. Filter down to season >=1982
#3a. Change characters in names to more accurately reflect an English keyboard
#4  Make an empty df
#5. For each season from 1982 to present
#6. Make mini-df of that season.  Sort by elo and add first 10 of season, name, nation to the newly made df


def top10(df):
	df = pd.read_pickle(df)
	df['date'] = df['date'].astype(str)
	df = df[df['date'].str.endswith('0500')]
	df = df.loc[df['season']>=1967]
	
	df['name'] = df['name'].str.replace('ä', 'a')
	df['name'] = df['name'].str.replace('Ä', 'A')
	df['name'] = df['name'].str.replace('å', 'a')
	df['name'] = df['name'].str.replace('Å', 'A')
	df['name'] = df['name'].str.replace('Æ', 'Ae')
	df['name'] = df['name'].str.replace('æ', 'ae')
	df['name'] = df['name'].str.replace('ø', 'o')
	df['name'] = df['name'].str.replace('Ø', 'O')
	df['name'] = df['name'].str.replace('ö', 'o')
	df['name'] = df['name'].str.replace('Ö', 'O')
	df['name'] = df['name'].str.replace('ü', 'u')
	df['name'] = df['name'].str.replace('Ü', 'U')

	ret_df = pd.DataFrame()
	for a in range(1967, 2024):
		df_small = df.loc[df['season']==a]
		df_small = df_small.sort_values(by='elo', ascending=False)
		df_small = df_small.head(10)
		df_small = df_small[["season", "name", "nation"]]
		ret_df = ret_df.append(df_small)
		
	print(ret_df)
	return ret_df


dfs = ['/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_all_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_downhill_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_superg_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_gs_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_slalom_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_combined_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_speed_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varmen_tech_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_all_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_downhill_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_superg_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_gs_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_slalom_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_combined_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_speed_k.pkl',
'/Users/syverjohansen/ski/elo/python/alpine/age/excel365/varladies_tech_k.pkl']
for a in range(len(dfs)):
	df = top10(dfs[a])
	if(len(df)>500):
		df1 = df.loc[df['season']<=2000]
		df2 = df.loc[df['season']>2000]
		filepath = dfs[a]
		filepath = filepath.split('/')[-1]
		filepath = filepath.split('.')[0]
		filepath1 = '/Users/syverjohansen/ski/elo/sporcle/alpine/excel365/top101_'+filepath+'_sporcle.xlsx'
		df1.to_excel(filepath1, index=False, header=False)
		filepath2 = '/Users/syverjohansen/ski/elo/sporcle/alpine/excel365/top102_'+filepath+'_sporcle.xlsx'
		df2.to_excel(filepath2, index=False, header=False)

	else:
		filepath = dfs[a]
		filepath = filepath.split('/')[-1]
		filepath = filepath.split('.')[0]
		filepath = '/Users/syverjohansen/ski/elo/sporcle/alpine/excel365/top10_'+filepath+'_sporcle.xlsx'
		df.to_excel(filepath, index=False, header=False)
	


