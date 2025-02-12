from urllib.error import HTTPError
from urllib.error import URLError
import logging
from socket import timeout
import ssl
import re
ssl._create_default_https_context = ssl._create_unverified_context
from urllib.request import urlopen
from bs4 import BeautifulSoup
import xlsxwriter
import requests
import time
import pandas as pd
import polars as pl
import pyarrow
start_time = time.time()
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}




for a in range(1924, 1925):
	men_races_url = []
	men_calendar_url = "https://firstskisport.com/cross-country/calendar.php?y="+str(a)
	ladies_calendar_url = "https://firstskisport.com/cross-country/calendar.php?y="+str(a)+"&g=w"

	print(pd.read_html(men_calendar_url))
	men_calendar = urlopen(men_calendar_url)
	men_calendar_soup  = BeautifulSoup(men_calendar, 'html.parser')
	for i in range(0, len(men_calendar_soup.find_all('a', {'title':'Results'}, href = True)), 2):
		b = men_calendar_soup.find_all('a', {'title':'Results'}, href = True)[i]
		men_races_url.append('https://firstskisport.com/cross-country/'+b['href'])

	#print(pd.read_html(men_races_url[0]))		
	#for a in range(0,len(men_races_url))

	#polars_df = pl.from_pandas(pandas_df)

print(time.time() - start_time)
