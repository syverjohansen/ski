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
import multiprocessing
from multiprocessing import Pool
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from datetime import datetime

start_time = time.time()
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}

def get_table(link):
	season = link[1]
	race_num = link[2]
	link = link[0]

	page_html = urlopen(link)
	soup = BeautifulSoup(page_html, 'html.parser')
	race_city = soup.body.find('h1').text
	date_country = soup.body.find('h2').text.split(", ")
	event = date_country[1]
	date = str(date_country[2]).split(" ")[1:3]
	try:
		year = date[1]
		date = " ".join(date)
		#Now we need to get the date into yyyymmdd format
		date_obj = datetime.strptime(date, "%d.%b %Y")
		date = date_obj.strftime("%Y%m%d")
	except:
		year = "1980"
		date = "19800101"
	print(date)
	country = date_country[3]

	race_cty = str(race_city)
	race_city = race_city.split("- ")
	race = race_city[0]
	
	city = race_city[1]

	distance = race.split(" ")[0]

	if(("Mass Start" in race)):
		ms=1
	else:
		ms=0

	if(distance.startswith("4x") or distance.startswith("3x")):
		distance = "Rel"
	if(distance=="Team"):
		distance = "Ts"
	if(distance=="Duathlon"):
		technique = "P"
		table = [date, city, country, event, distance, 1, technique, season, race_num]
		return table
	if(distance=="Sprint"):
			#technique = body[3].text.split(" ")
			technique = race.split(" ")
			technique = technique[1][0]
			table = [date, city, country, event, distance, ms, technique, season, race_num]
			
			return table
	if(int(year)>1985 and distance!="Rel"):
		try:
			technique =race.split(" ")
			technique = technique[2][0]
			table = [date, city, country, event, distance, ms, technique, season, race_num]
			
			return table
		except:
			table = [date, city, country, event, distance, ms, "N/A", season, race_num]
			return table
	else:
		table = [date, city, country, event, distance, ms, "N/A", season, race_num]
		
		return table

#Getting the links from the calendar
def fetch_links(url):
	season = url[1]
	url = url[0]
	links = [] #list of links
	r = requests.get(url) #opening the url
	soup = BeautifulSoup(r.text, 'html.parser') #Html for the link
	for i in range(0, len(soup.find_all('a', {'title':'Results'}, href = True)), 2): #Getting the race urls
		b = soup.find_all('a', {'title':'Results'}, href = True)[i] #Finding them in the title
		race_num = i/2+1
		links.append(['https://firstskisport.com/cross-country/'+b['href'], season, race_num]) #Appending them to the list to return
	return links

#Gets the race results for the given links and returns it in a list of lists
def get_skier(link):
	link = link[0]
	page_html = urlopen(link)
	soup = BeautifulSoup(page_html, 'html.parser')
	body = soup.body.find_all('table', {'class':'tablesorter sortTabell'})[0].find_all('td')

	places = []
	skier = []
	nation = []
	ski_ids = []
	for a in range(0,len(body), 7):
		place = body[a].text
		if (place in {"DNF", "DSQ", "DNS", "DNQ", "OOT"}):
			break

		places.append(int(place))
		ski_id = str(body[a+2])
		ski_id = ski_id.split("id=")[1].split("&")[0]
		ski_ids.append(ski_id)
		skier_name = str(body[a+2])			
		skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
		nation.append(body[a+4].text.strip())

	
	return [places, skier, nation, ski_ids]	



def construct_men_calendar(year):
	print("men", year)
	return ["https://firstskisport.com/cross-country/calendar.php?y=" + str(year), year]


def construct_ladies_calendar(year):
	print("ladies", year)
	return ["https://firstskisport.com/cross-country/calendar.php?y="+str(year)+"&g=w", year]

def unnest_chunk(chunk_indices, df):
    chunk = df[chunk_indices]
    # Unnest the chunk
    unnested_rows = []
    for row in chunk.iter_rows(named=True):
    #for _, row in chunk.iter_rows(named=True):
        places = row['Place']
        skiers = row['Skier']
        nations = row['Nation']
        ids = row['ID']
        for i in range(len(places)):
            unnested_row = {
                'Date': row['Date'],
                'City': row['City'],
                'Country': row['Country'],
                'Event':row['Event'],
                'Sex': row['Sex'],
                'Distance': row['Distance'],
                'MS': row['MS'],
                'Technique': row['Technique'],
                'Season': row['Season'],
                'Race': row['Race'],
                'Place': places[i],
                'Skier': skiers[i],
                'Nation': nations[i],
                'ID': ids[i]
            }
            unnested_rows.append(unnested_row)
    '''
    exploded = chunk.with_columns([
        pl.col("Place").explode(),
        pl.col("Skier").explode(),
        pl.col("Nation").explode(),
        pl.col("ID").explode()
    ])
    
    return exploded
    '''
    return pl.DataFrame(unnested_rows)

def construct_df(tables, skiers, sex):
	print("Constructing df for " + sex)
	table_df = pl.DataFrame(tables, schema=["Date", "City", "Country","Event", "Distance", "MS", "Technique", "Season", "Race"])
	table_df = table_df.with_columns(pl.lit(sex).alias("Sex"))
	skier_df = pl.DataFrame(skiers, schema = ["Place", "Skier", "Nation", "ID"])

	df = pl.concat([table_df, skier_df], how="horizontal")
	#df = df.drop_nulls(subset=['Distance']).filter((pl.col('Distance') != "Rel") & (pl.col('Distance') != "Ts"))
	#df = df.reset_index(drop=True)
	empty_df = pl.DataFrame(schema=['Date', 'City', 'Country', 'Event', 'Distance', 'MS', 'Sex', 'Technique', 'Season', 'Race', 'Place', 'Skier', 'Nation', 'ID'])

	num_chunks = min(100, df.height)
	#chunks = np.array_split(df, num_chunks)
	chunk_indices = np.array_split(range(df.height), num_chunks)
	with ThreadPoolExecutor(max_workers=100) as pool:
    	# Map the unnest_chunk function to each chunk and concatenate the results
		result_chunks = list(pool.map(lambda indices: unnest_chunk(indices, df), chunk_indices))
	df = pl.concat(result_chunks)
	return df


def main():

	all_men_links = []
	all_ladies_links = []
	with ThreadPoolExecutor(max_workers=100) as p:
		years_range = range(1920, 2025)
		men_urls = p.map(construct_men_calendar, years_range)
		all_men_links = p.map(fetch_links, men_urls)

		ladies_urls = p.map(construct_ladies_calendar, years_range)
		all_ladies_links = p.map(fetch_links, ladies_urls)

	print("All urls gathered")
	all_men_links = [link for sublist in all_men_links for link in sublist]
	all_ladies_links = [link for sublist in all_ladies_links for link in sublist]
	print("All urls listed")

	with ThreadPoolExecutor(max_workers=100) as p:
		men_tables = p.map(get_table, all_men_links)
		print("Men tables gathered")
		men_skiers = p.map(get_skier, all_men_links)
		print("Men skiers gathered")

		ladies_tables = p.map(get_table, all_ladies_links)
		print("Ladies tables gathered")
		ladies_skiers = p.map(get_skier, all_ladies_links)
		print("Ladies skiers gathered")

	men_df = construct_df(men_tables, men_skiers, "M")

	print("Finished constructing df for men")
	ladies_df = construct_df(ladies_tables, ladies_skiers, "L")
	print("Finished constructing df for ladies")
	print(men_df)
	print(ladies_df)
	
	print("Writing to Feather")
	men_df.write_ipc("~/ski/elo/python/ski/polars/excel365/men_scrape.feather")
	ladies_df.write_ipc("~/ski/elo/python/ski/polars/excel365/ladies_scrape.feather")
	#print("Writing to excel file")
	#writer = pl.ExcelWriter("/Users/syverjohansen/ski/elo/python/ski/polars/excel365/multithread_demo_all.xlsx")
	#ladies_df.to_excel(writer, sheet_name='Ladies', index=False)
	#men_df.to_excel(writer, sheet_name='Men', index=False)
	#writer.save()


if __name__ == '__main__':
	main()


print(time.time() - start_time)










