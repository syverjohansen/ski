
from scipy.stats import binom
import pandas as pd
import numpy as np
import math
import time
start_time = time.time()


ladiesdf = pd.read_pickle("~/ski/elo/python/ski/ladies/varladies_all.pkl")


def race_pred(ladiesdf):
	ladiesdf = ladiesdf.loc[ladiesdf['date']==20210500]
	ladiesdf = ladiesdf.sort_values(by='elo', ascending =False)
	ladiesdf = ladiesdf.reset_index(drop=True)
	
	#getting matrix of probabilities
	ladiesdf = ladiesdf.head(30)
	eloarr = np.array(ladiesdf['elo'])
	n = eloarr.size
	
	elocol = np.tile(eloarr,(n,1))
	elorow = np.transpose(elocol)
	elo_matrix = 1/(1+10**((elorow-elocol)/400))
	
	np.fill_diagonal(elo_matrix, 0)
	#elo_vector = np.sum(elo_matrix, axis=0)
	#elo_matrix = np.transpose(elo_matrix)
	#print(elo_matrix)
	print(elo_matrix)
	elo_vector = np.sum(elo_matrix, axis=0)/(n-1)
	n = n-1
	k = n-n
	for a in range(len(elo_vector)):
		bi = binom.pmf(k,n,elo_vector[a])
		print(bi)

race_pred(ladiesdf)