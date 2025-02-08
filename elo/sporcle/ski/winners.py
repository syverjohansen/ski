import pandas as pd
import numpy as np
import time
import datetime
import operator

start_time = time.time()

def winners(df):
	df = pd.read_pickle(df)
	df = df[['season', 'name', 'nation', 'place']]
	df = df.loc[df['place']==1]
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
	#df['season'] = df.loc[df['season']>=1982]
	df2 = df.groupby(['season', 'name', 'nation'])['place'].count()
	df2 = df2.reset_index()
	df2 = df2.sort_values(by=['season', 'place'], ascending=[True, False])
	df2['nation'] = df2['nation']+" ("+df2['place'].astype(str)+")"
	
	df2 = df2.drop('place', axis=1)
	#print(df2)
	
	return df2


dfs = ['/Users/syverjohansen/ski/elo/python/ski/age/excel365/varmen_all_k.pkl',
'/Users/syverjohansen/ski/elo/python/ski/age/excel365/varladies_all_k.pkl']

for a in range(len(dfs)):
	df = winners(dfs[a])
	if(len(df)>500):
		df1 = df.loc[df['season']<=2000]
		df2 = df.loc[df['season']>2000]
		filepath = dfs[a]
		filepath = filepath.split('/')[-1]
		filepath = filepath.split('.')[0]
		filepath1 = '/Users/syverjohansen/ski/elo/sporcle/ski/excel365/winners1_'+filepath+'_sporcle.xlsx'
		df1.to_excel(filepath1, index=False, header=False)
		filepath2 = '/Users/syverjohansen/ski/elo/sporcle/ski/excel365/winners2_'+filepath+'_sporcle.xlsx'
		df2.to_excel(filepath2, index=False, header=False)

	else:
		filepath = dfs[a]
		filepath = filepath.split('/')[-1]
		filepath = filepath.split('.')[0]
		filepath = '/Users/syverjohansen/ski/elo/sporcle/ski/excel365/winners_'+filepath+'_sporcle.xlsx'
		df.to_excel(filepath, index=False, header=False)