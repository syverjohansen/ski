import pandas as pd
import numpy as np

ladiesdf = pd.read_pickle("~/ski/elo/python/ski/ladiesdf.pkl")

mendf = pd.read_pickle("~/ski/elo/python/ski/mendf.pkl")

import numpy as np

def EA (pelos ,place):
	players = len(pelos)
	ra = pelos[place - 1]
	QA = 10**(ra/400) 
	QA = np.repeat(QA, players - 1)

	rb = np.delete(np.array(pelos), place - 1)
	QB = 10**(rb/400)
	EA = QA / (QA + QB)
	return EA

def SA (pelos,place):
	players = len(pelos)
	return (place - 1)*[0] + (players-place)*[1]



pelo = [1400,1200,1300,1100]
place = 4
K = 1
print(pelo[place -1] + K*(sum(SA(pelo,place)) - sum(EA(pelo,place))))