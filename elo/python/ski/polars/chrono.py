import polars as pl
import time
from functools import reduce
pl.Config.set_tbl_cols(100)
#pl.options.mode.chained_assignment = None
start_time = time.time()


#Go through unique dates

def ladies():
	L = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L.feather')
	L_Distance = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_Distance.feather')
	L_Distance = L_Distance.rename({"Pelo":"Distance_Pelo", "Elo":"Distance_Elo"})
	L_Distance_C = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_Distance_C.feather')
	L_Distance_C = L_Distance_C.rename({'Pelo':'Distance_C_Pelo', 'Elo':'Distance_C_Elo'})
	L_Distance_F = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_Distance_F.feather')
	L_Distance_F = L_Distance_F.rename({'Pelo':'Distance_F_Pelo', 'Elo':'Distance_F_Elo'})
	L_Sprint = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_Sprint.feather')
	L_Sprint = L_Sprint.rename({'Pelo':'Sprint_Pelo', 'Elo':'Sprint_Elo'})
	L_Sprint_C = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_Sprint_C.feather')
	L_Sprint_C = L_Sprint_C.rename({'Pelo':'Sprint_C_Pelo', 'Elo':'Sprint_C_Elo'})
	L_Sprint_F = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_Sprint_F.feather')
	L_Sprint_F = L_Sprint_F.rename({'Pelo':'Sprint_F_Pelo', 'Elo':'Sprint_F_Elo'})
	L_C = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_C.feather')
	L_C = L_C.rename({'Pelo':'Classic_Pelo', 'Elo':'Classic_Elo'})
	L_F = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/L_F.feather')
	L_F = L_F.rename({'Pelo':'Freestyle_Pelo', 'Elo':'Freestyle_Elo'})
	print("Done reading ladies files")
	#common_columns = ['Date', 'City', 'Country', 'Event', 'Sex', 'Distance', 'MS', 'Technique', 'Place', 'Name', 'Nation', 'ID', 'Season', 'Race', 'Birthday', 'Age', 'Exp']
	common_columns = ['Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS', 'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season', 'Race', 'Birthday', 'Age', 'Exp']
	#common_columns = ['Date']
	dfs = [L, L_Distance, L_Distance_C, L_Distance_F, L_Sprint, L_Sprint_C, L_Sprint_F, L_C, L_F]
	for i in range(len(dfs)):
		dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))



	#print(L_Distance.join(L_Distance_C, on=common_columns, how="left"))
	# Start with the first DataFrame
	merged_df = dfs[0]

	# Merge each subsequent DataFrame
	for df in dfs[1:]:
  		merged_df = merged_df.join(df, on=common_columns, how="left")
	print(merged_df.filter(pl.col("Skier")=="Jessie Diggins"))

	def fill_nulls_forward(merged_df):
		elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo", "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo", "Classic_Elo", "Freestyle_Elo"] 
		for col in elo_cols:			 		
			merged_df = merged_df.with_columns(merged_df[col].fill_null(strategy="forward").alias(col))
		return merged_df

	merged_df = merged_df.groupby("ID").apply(fill_nulls_forward)

	elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo", "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo", "Classic_Elo", "Freestyle_Elo"] 
	pelo_cols = ["Pelo", "Distance_Pelo", "Distance_C_Pelo", "Distance_F_Pelo", "Sprint_Pelo", "Sprint_C_Pelo", "Sprint_F_Pelo", "Classic_Pelo", "Freestyle_Pelo"] 
	
	for a in range(len(elo_cols)):
		merged_df = merged_df.with_columns(pl.when(pl.col(pelo_cols[a]).is_null())
			.then(pl.col(elo_cols[a]))
			.otherwise(pl.col(pelo_cols[a]))
			.alias(pelo_cols[a])
			)

	print(merged_df.filter(pl.col("Skier")=="Jessie Diggins"))

	return merged_df




	

def men():
	M = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M.feather')
	M_Distance = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_Distance.feather')
	M_Distance = M_Distance.rename({"Pelo":"Distance_Pelo", "Elo":"Distance_Elo"})
	M_Distance_C = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_Distance_C.feather')
	M_Distance_C = M_Distance_C.rename({'Pelo':'Distance_C_Pelo', 'Elo':'Distance_C_Elo'})
	M_Distance_F = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_Distance_F.feather')
	M_Distance_F = M_Distance_F.rename({'Pelo':'Distance_F_Pelo', 'Elo':'Distance_F_Elo'})
	M_Sprint = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_Sprint.feather')
	M_Sprint = M_Sprint.rename({'Pelo':'Sprint_Pelo', 'Elo':'Sprint_Elo'})
	M_Sprint_C = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_Sprint_C.feather')
	M_Sprint_C = M_Sprint_C.rename({'Pelo':'Sprint_C_Pelo', 'Elo':'Sprint_C_Elo'})
	M_Sprint_F = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_Sprint_F.feather')
	M_Sprint_F = M_Sprint_F.rename({'Pelo':'Sprint_F_Pelo', 'Elo':'Sprint_F_Elo'})
	M_C = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_C.feather')
	M_C = M_C.rename({'Pelo':'Classic_Pelo', 'Elo':'Classic_Elo'})
	M_F = pl.read_ipc('~/ski/elo/python/ski/polars/excel365/M_F.feather')
	M_F = M_F.rename({'Pelo':'Freestyle_Pelo', 'Elo':'Freestyle_Elo'})
	print("Done reading mens files")
	#common_columns = ['Date', 'City', 'Country', 'Event', 'Sex', 'Distance', 'MS', 'Technique', 'Place', 'Name', 'Nation', 'ID', 'Season', 'Race', 'Birthday', 'Age', 'Exp']
	common_columns = ['Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS', 'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season', 'Race', 'Birthday', 'Age', 'Exp']
	#common_columns = ['Date']
	dfs = [M, M_Distance, M_Distance_C, M_Distance_F, M_Sprint, M_Sprint_C, M_Sprint_F, M_C, M_F]
	for i in range(len(dfs)):
		dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))



	#print(M_Distance.join(M_Distance_C, on=common_columns, how="left"))
	# Start with the first DataFrame
	merged_df = dfs[0]

	# Merge each subsequent DataFrame
	for df in dfs[1:]:
  		merged_df = merged_df.join(df, on=common_columns, how="left")
	print(merged_df.filter(pl.col("Skier")=="Gus Schumacher"))

	def fill_nulls_forward(merged_df):
		elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo", "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo", "Classic_Elo", "Freestyle_Elo"] 
		for col in elo_cols:			 		
			merged_df = merged_df.with_columns(merged_df[col].fill_null(strategy="forward").alias(col))
		return merged_df

	merged_df = merged_df.groupby("ID").apply(fill_nulls_forward)

	elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo", "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo", "Classic_Elo", "Freestyle_Elo"] 
	pelo_cols = ["Pelo", "Distance_Pelo", "Distance_C_Pelo", "Distance_F_Pelo", "Sprint_Pelo", "Sprint_C_Pelo", "Sprint_F_Pelo", "Classic_Pelo", "Freestyle_Pelo"] 
	
	for a in range(len(elo_cols)):
		merged_df = merged_df.with_columns(pl.when(pl.col(pelo_cols[a]).is_null())
			.then(pl.col(elo_cols[a]))
			.otherwise(pl.col(pelo_cols[a]))
			.alias(pelo_cols[a])
			)

	print(merged_df.filter(pl.col("Skier")=="Gus Schumacher"))
	return merged_df


ladiesdf = ladies()
mendf = men()

pl.Config.set_tbl_rows(100)
ladies_nation = ladiesdf.select("Nation").unique().sort(["Nation"])
men_nation = mendf.select("Nation").unique().sort(["Nation"])
print(ladies_nation)
print(men_nation)

ladiesdf.write_ipc("~/ski/elo/python/ski/polars/excel365/ladies_chrono.feather")
mendf.write_ipc("~/ski/elo/python/ski/polars/excel365/men_chrono.feather")
		


print(time.time() - start_time)