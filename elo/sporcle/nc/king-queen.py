import pandas as pd
import numpy as np
import time
import datetime
import operator

start_time = time.time()

#1. Read in chrono_regress files
#2. Filter down to elo=100 (whatever)
#3. Sort by date, race
#4. Go through each date, grab name, nation
#5. If name equals last one from the names list, continue
#6. Else append name to list
#7. Start date appends date
#8. If len(start date) > 1, end date appends date
#9. Change dates to strings, change them so it is in the month, date, year format
#10. End date appends present
#11. Turn it into a data frame
#11. Save

def king_queen(df, col):
	df = pd.read_pickle(df)
	name = []
	ids = []
	start_date = []
	end_date = []
	top_elo = 0
	top_id = 0
	nation = []
	df = df.sort_values(['date', 'race', 'elo'], ascending=[True, True, False])
	seasons = df['season'].unique()
	for a in range(len(seasons)):
		seasondf = df.loc[df['season']==seasons[a]]

		id_list = list(seasondf['id'].unique())
		
		if(len(ids)==0):
			top_id = 0
		elif(top_id in id_list):
			pass
		else:

			top_id = 0

		races = seasondf['race'].unique()
		for b in range(len(races)):

			racedf = seasondf.loc[seasondf['race']==races[b]]
			max_elo = max(racedf['elo'])
			if(top_id!=0):
				top_id_df = seasondf.loc[seasondf['id']==top_id]
				top_id_df = top_id_df.loc[top_id_df['race']<=races[b]]
				
				
				try:
					top_elo = top_id_df['elo'].iloc[-1]
				except:
					if(len(top_id_df)==0):
						top_id_df = seasondf.loc[seasondf['id']==top_id]
						top_elo = top_id_df['pelo'].iloc[0]
			else:
				print(seasons[a])
				top_elo = 0
			if(max_elo>top_elo):
				row = racedf.loc[racedf['elo']==max_elo]
				ski_id = row['id'].iloc[0]
				
				if(len(ids)==0):
					ids.append(ski_id)
					name.append(row['name'].iloc[0])
					
					nation.append(row['nation'].iloc[0])
					start_date.append(row['date'].iloc[0])
					if(len(start_date)>1):
						end_date.append(row['date'].iloc[0])

				elif(ski_id!=ids[-1]):
					ids.append(ski_id)
					name.append(row['name'].iloc[0])
					nation.append(row['nation'].iloc[0])
					start_date.append(row['date'].iloc[0])
					if(len(start_date)>1):
						end_date.append(row['date'].iloc[0])
					
				top_id = ids[-1]
		top_id = ids[-1]
	#start_days_calc = start_date
	#end_days_calc = end_date

	#today = datetime.date.today()
	#end_days_calc.append(today)

	str_start_date = start_date
	str_end_date = end_date

	for a in range(len(str_start_date)):		
		str_start_date[a] = str(str_start_date[a])[0:10]
	for a in range(len(str_end_date)):
		str_end_date[a] = str(str_end_date[a])[0:10]
	str_end_date.append("Present")
	dates = []
	for a in range(len(str_start_date)):
		date = "("+str_start_date[a]+" - "+str_end_date[a]+")"
		dates.append(date)
	


	df = pd.DataFrame()

	df['start_date'] = str_start_date
	df['end_date'] = str_end_date
	df['name'] = name
	df['id'] = ids
	df['nation'] = nation
	df['dates'] = dates

	for a in range(len(start_date)):
		date = start_date[a].split("-")
		year = int(date[0])
		month = int(date[1])
		day = int(date[2])
		start_date[a] = datetime.date(year, month, day)
	end_date = end_date[0:(len(end_date)-1)]
	for a in range(len(end_date)):
		date = end_date[a].split("-")
		year = int(date[0])
		month = int(date[1])
		day = int(date[2])
		end_date[a] = datetime.date(year, month, day)
	end_date.append(datetime.date.today())
	df['start_date'] = start_date
	df['end_date'] = end_date
	df['days'] = df['end_date'] - df['start_date']






	return df


dfs = ['/Users/syverjohansen/ski/elo/python/nc/age/excel365/men_chrono.pkl',
'/Users/syverjohansen/ski/elo/python/nc/age/excel365/ladies_chrono.pkl']

for a in range(len(dfs)):
	if(a==0):
		gender = "king"
	else:
		gender = "queen"
	col = "elo"
	df = king_queen(dfs[a], col)
	
	filepath = '/Users/syverjohansen/ski/elo/sporcle/nc/excel365/'+gender+'-'+col+'_sporcle.xlsx'
	df.to_excel(filepath, index=False)
	print(df)

