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
	df = df.loc[df['season']>=1982]
	
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
	df['name'] = df['name'].str.replace('Ã', 'Oe')

	ret_df = pd.DataFrame()
	for a in range(1982, 2024):
		df_small = df.loc[df['season']==a]
		df_small = df_small.sort_values(by='elo', ascending=False)
		df_small = df_small.head(10)
		df_small = df_small[["season", "name", "nation"]]
		ret_df = ret_df.append(df_small)
		
	print(ret_df)
	return ret_df


dfs = ['/Users/syverjohansen/ski/elo/python/biathlon/excel365/varmen_all_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varmen_sprint_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varmen_individual_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varmen_pursuit_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varmen_mass_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varladies_all_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varladies_sprint_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varladies_individual_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varladies_pursuit_k.pkl',
'/Users/syverjohansen/ski/elo/python/biathlon/excel365/varladies_mass_k.pkl']
for a in range(len(dfs)):
	df = top10(dfs[a])
	filepath = dfs[a]
	filepath = filepath.split('/')[-1]
	filepath = filepath.split('.')[0]
	filepath = '/Users/syverjohansen/ski/elo/sporcle/biathlon/excel365/top10_'+filepath+'_sporcle.xlsx'
	df.to_excel(filepath, index=False, header=False)

