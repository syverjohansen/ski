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
start_time = time.time()
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}

def convert_month(month):
	if(month=='Jan'):
		return '01'
	elif(month=='Feb'):
		return '02'
	elif(month=='Mar'):
		return '03'
	elif(month=='Apr'):
		return '04'
	elif(month=='May'):
		return '05'
	elif(month=='Jun'):
		return '06'
	elif(month=='Jul'):
		return '07'
	elif(month=='Aug'):
		return '08'
	elif(month=='Sep'):
		return '09'
	elif(month=='Oct'):
		return '10'
	elif(month=='Nov'):
		return '11'
	elif(month=='Dec'):
		return '12'

def get_table(link):
	page_html = urlopen(link)
	soup = BeautifulSoup(page_html, 'html.parser')
	race_city = soup.body.find('h1').text
	date_country = soup.body.find('h2').text
	date_country = date_country.split(", ")
	date = str(date_country[2])
	date = date.split(" ")
	try:
		year = date[2]
	except:
		print("No date")
		return ["19800101", "Örnsköldsvik", "Sweden", "5", 0, "N/A"]
	date = date[1].split(".")
	day = date[0]
	day = str(day.zfill(2))
	
	month = date[1]
	month = convert_month(month)
	date = (year+month+day)
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
		table = [date, city, country, distance, 1, technique]
		return table
	if(distance=="Sprint"):
			#technique = body[3].text.split(" ")
			technique = race.split(" ")
			technique = technique[1][0]
			table = [date, city, country, distance, ms, technique]
			
			return table
	if(int(year)>1985 and distance!="Rel"):
		try:
			technique =race.split(" ")
			technique = technique[2][0]
			table = [date, city, country, distance, ms, technique]
			
			return table
		except:
			table = [date, city, country, distance, ms, "N/A"]
			return table
	else:
		table = [date, city, country, distance, ms, "N/A"]
		
		return table




def get_skier(link):
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

		places.append(place)
		ski_id = str(body[a+2])
		ski_id = ski_id.split("id=")[1].split("&")[0]
		ski_ids.append(ski_id)
		skier_name = str(body[a+2])			
		skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
		nation.append(body[a+4].text.strip())

		#if(distance=="Rel" and people>12):
	#		break
	#	elif(distance!="Rel" and people>10):
	#		break
	
	
	return [places, skier, nation, ski_ids]	



def fetch_links(url):
	links = []
	r = requests.get(url)
	soup = BeautifulSoup(r.text, 'html.parser')
	for i in range(0, len(soup.find_all('a', {'title':'Results'}, href = True)), 2):
		b = soup.find_all('a', {'title':'Results'}, href = True)[i]
		links.append('https://firstskisport.com/cross-country/'+b['href'])
	return links


def construct_men_calendar(year):
	print("men", year)
	return "https://firstskisport.com/cross-country/calendar.php?y=" + str(year)


def construct_ladies_calendar(year):
	print("ladies", year)
	return "https://firstskisport.com/cross-country/calendar.php?y="+str(year)+"&g=w"

def unnest_chunk(chunk):
	    # Unnest the chunk
    unnested_rows = []
    for _, row in chunk.iterrows():
        for i in range(len(row['Place'])):
            unnested_row = {
                'Date': row['Date'],
                'City': row['City'],
                'Country': row['Country'],
                'Distance': row['Distance'],
                'MS': row['MS'],
                'Technique': row['Technique'],
                'Place': row['Place'][i],
                'Skier': row['Skier'][i],
                'Nation': row['Nation'][i],
                'ID': row['ID'][i]
            }
            unnested_rows.append(unnested_row)
    return pd.DataFrame(unnested_rows)

def construct_df(tables, skiers, sex):
	print("Constructing df for " + sex)
	table_df = pd.DataFrame(tables, columns=["Date", "City", "Country", "Distance", "MS", "Technique"])
	table_df['Sex'] = sex
	skier_df = pd.DataFrame(skiers, columns = ["Place", "Skier", "Nation", "ID"])

	df = pd.concat([table_df, skier_df], axis=1)
	df = df[df['Distance']!="Rel"]
	df = df[df['Distance']!="Ts"]
	df = df.reset_index(drop=True)
	empty_df = pd.DataFrame(columns=['Date', 'City', 'Country', 'Distance', 'MS', 'Sex', 'Technique', 'Place', 'Skier', 'Nation', 'ID'])

	num_chunks = multiprocessing.cpu_count()
	chunks = np.array_split(df, num_chunks)
	with multiprocessing.Pool(processes=num_chunks) as pool:
    	# Map the unnest_chunk function to each chunk and concatenate the results
		result_chunks = pool.map(unnest_chunk, chunks)
	df = pd.concat(result_chunks, ignore_index=True)
	return df

def construct_df2(tables, skiers, sex):
	print("Constructing df for " + sex)
	table_df = pd.DataFrame(tables, columns=["Date", "City", "Country", "Distance", "MS", "Technique"])
	table_df['Sex'] = sex
	skier_df = pd.DataFrame(skiers, columns = ["Place", "Skier", "Nation", "ID"])

	df = pd.concat([table_df, skier_df], axis=1)
	df = df[df['Distance']!="Rel"]
	df = df[df['Distance']!="Ts"]
	df = df.reset_index(drop=True)
	empty_df = pd.DataFrame(columns=['Date', 'City', 'Country', 'Distance', 'MS', 'Sex', 'Technique', 'Place', 'Skier', 'Nation', 'ID'])

	for index, row in df.iterrows():
    # Unpack the values from the lists
	    for i in range(len(row['Place'])):
	        empty_row = {
	            'Date': row['Date'],
	            'City': row['City'],
	            'Country': row['Country'],
	            'Distance': row['Distance'],
	            'MS': row['MS'],
	            'Sex':row['Sex'],
	            'Technique': row['Technique'],
	            'Place': row['Place'][i],
	            'Skier': row['Skier'][i],
	            'Nation': row['Nation'][i],
	            'ID': row['ID'][i]
	        }
	        empty_df = empty_df.append(empty_row, ignore_index=True)
	#men_df = pd.DataFrame(men_df, columns=['Date', 'City', 'Country', 'Distance', 'MS', 'Technique', 
	#	'Place', 'Skier', 'Nations', 'IDs'])
	df = empty_df
	return df

def main():
	coresNr = multiprocessing.cpu_count()

	all_men_links = []
	all_ladies_links = []
	with Pool(coresNr) as p:
		years_range = range(1924, 2025)
		men_urls = p.map(construct_men_calendar, years_range)
		all_men_links = p.map(fetch_links, men_urls)

		ladies_urls = p.map(construct_ladies_calendar, years_range)
		all_ladies_links = p.map(fetch_links, ladies_urls)

	print("All urls gathered")
	all_men_links = [link for sublist in all_men_links for link in sublist]
	all_ladies_links = [link for sublist in all_ladies_links for link in sublist]
	print("All urls listed")
	with Pool(coresNr) as p:
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

	print("Writing to excel file")
	writer = pd.ExcelWriter("/Users/syverjohansen/ski/elo/python/ski/excel365/all.xlsx")
	ladies_df.to_excel(writer, sheet_name='Ladies', index=False)
	men_df.to_excel(writer, sheet_name='Men', index=False)
	writer.save()


	#print(tables)
if __name__ == '__main__':
	main()


print(time.time() - start_time)