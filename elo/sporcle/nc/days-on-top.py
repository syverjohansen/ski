import pandas as pd
import numpy as np
import time
import datetime
import operator

start_time = time.time()


def on_top(df):
	df = pd.read_excel(df)
	ids = df['id'].unique()
	total_days = []
	names = []
	nations = []
	dates = []
	for a in range(len(ids)):
		skidf = df.loc[df['id']==ids[a]]
		print(skidf)
		total_days.append(sum(skidf['days']))
		date = ''
		for b in range(len(skidf['days'])):
			date = date+' '+skidf['dates'].iloc[b]
		dates.append(date)
		names.append(skidf['name'].iloc[-1])
		nations.append(skidf['nation'].iloc[-1])
	df = pd.DataFrame()
	df['name'] = names
	df['id'] = ids
	df['nation'] = nations
	df['dates'] = dates
	df['days'] = total_days 
	return df



dfs = ['/Users/syverjohansen/ski/elo/sporcle/nc/excel365/king-elo_sporcle.xlsx',
'/Users/syverjohansen/ski/elo/sporcle/nc/excel365/queen-elo_sporcle.xlsx']

for a in range(len(dfs)):
	if(a==0):
		gender = "men"
	else:
		gender = "ladies"
	
	df = on_top(dfs[a])

	
	filepath = '/Users/syverjohansen/ski/elo/sporcle/nc/excel365/'+gender+'_'+'top_sporcle.xlsx'
	df.to_excel(filepath, index=False, header=False)
	print(df)