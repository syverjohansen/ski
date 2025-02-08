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


def get_active(df, sex):
	df = pd.read_pickle(df)
	if(sex=="men"):
		all_skiers = pd.read_pickle('/Users/syverjohansen/ski/elo/python/ski/age/excel365/varmen_all_k.pkl')
	else:
		all_skiers = pd.read_pickle('/Users/syverjohansen/ski/elo/python/ski/age/excel365/varladies_all_k.pkl')
	ids = list(df['id'].unique())
	df['career'] = ''
	for a in range(len(ids)):
		small_df = all_skiers.loc[all_skiers['id']==ids[a]]
		#print(ids[a], small_df['season'])
		try:
			start = str(small_df['season'].iloc[0])

			end = str(small_df['season'].iloc[-1])
		except:
			start = "0"
			end="0"
		if(int(end)>=2023):
			end="Present"
		career = "("+start+"-"+end+")"
		df.loc[df['id']==ids[a], 'career'] = career
	return df

def top10(df):
	#df = pd.read_pickle(df)
	
	df['name']= df['name'].str.replace('Ã¸', 'ø')
	df['name']= df['name'].str.replace('Ã¤', 'ä')
	df['name']= df['name'].str.replace('Ã¼', 'ü')
	df['name'] = df['name'].str.replace('Ã¶', 'ö')
	df['name'] = df['name'].str.replace('Ã', 'Ø')
	df['name'] = df['name'].str.replace('Ã', 'Ø')
	df['name'] = df['name'].str.replace('Ã¦', 'æ')
	df['name']= df['name'].str.replace('Ã¥', 'å')
	df['name']= df['name'].str.replace('Ã', 'Å')
	
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
	
	ret_df['rank'] = range(1,101)
	ret_df['name'] = df['name'][0:100]
	ret_df['nation'] = df['nation'][0:100] + " " + df['career'][0:100]

		
	print(ret_df)
	return ret_df


dfs = ['/Users/syverjohansen/ski/ranks/ski/excel365/varmen_points.pkl',
'/Users/syverjohansen/ski/ranks/ski/excel365/varladies_points.pkl']
for a in range(len(dfs)):
	if(a==0):
		sex='men'
	else:
		sex = 'ladies'
	df = get_active(dfs[a], sex)
	df = top10(df)
	filepath = dfs[a]
	filepath = filepath.split('/')[-1]
	filepath = filepath.split('.')[0]
	filepath = '/Users/syverjohansen/ski/elo/sporcle/ski/excel365/alltime_100_'+filepath+'_sporcle.xlsx'
	df.to_excel(filepath, index=False, header=False)

