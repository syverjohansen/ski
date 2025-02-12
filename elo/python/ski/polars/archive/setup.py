import polars as pl
from bs4 import BeautifulSoup
import numpy as np
from urllib.request import urlopen
from datetime import datetime
import re
import time
import logging
import warnings
pl.Config.set_tbl_cols(100)
warnings.filterwarnings('ignore')



#Make sure that everything is in the right order.
def setup(df):
    #Column order is Date, City, Country, Sex, Distance, MS, Technique, Place, Name, Nation, ID, Season, Race 
    #Sort by Season (asc), race (asc), place (asc)
    df = df.select(["Date", "City", "Country","Event", "Sex", "Distance", "MS", "Technique", "Place", "Skier", "Nation", "ID",
            "Season", "Race", "Birthday", "Age", "Exp"])
    replacements = {
    'Ã¸': 'ø',
    'Ã¸': 'ø',
    'Ã¼': 'ü',
    'Ã¶': 'ö',
    'Ã': 'Ø',
    'Ã¦': 'æ',
    'Ã¥': 'å',
    'Ã': 'Å'
    }




    for old, new in replacements.items():
    	df = df.with_columns(pl.col('Skier').str.replace(old, new))
    df = df.sort(['Season', 'Race', 'Place'])
    df = df.with_columns(
    (pl.col('ID').cumcount().over('ID')).alias('Exp')
    )
    df = df.with_columns(pl.col("Technique").fill_null(""))
    return df



import warnings
warnings.filterwarnings('ignore')
start_time = time.time()
#0 is men, 1 is women
men_df = pl.read_ipc("~/ski/elo/python/ski/polars/excel365/men_age.feather")
ladies_df = pl.read_ipc("~/ski/elo/python/ski/polars/excel365/ladies_age.feather")

men_df = setup(men_df)
print(men_df.filter(pl.col('ID')=="10000"))
ladies_df = setup(ladies_df)
print(men_df)
print(ladies_df)

print("Writing to feather file")
men_df.write_ipc("~/ski/elo/python/ski/polars/excel365/men_setup.feather")
men_df.write_csv("~/ski/elo/python/ski/polars/excel365/men_setup.csv")
ladies_df.write_ipc("~/ski/elo/python/ski/polars/excel365/ladies_setup.feather")
print(time.time() - start_time)