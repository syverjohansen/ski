import polars as pl
from bs4 import BeautifulSoup
import numpy as np
from urllib.request import urlopen
from datetime import datetime
import ssl
import re
ssl._create_default_https_context = ssl._create_unverified_context
import time
import multiprocessing
from multiprocessing import Pool
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import logging
import warnings
warnings.filterwarnings('ignore')


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()

#This function will get a skiers age and experience at a given race
def get_ages(df):
	sex = df['Sex'][0]
	logging.info(f"Processing chunk size: {len(df)}")

	#Set the birthday column to NA (default for people who don't have an age listed)
	df = df.with_columns(pl.lit(None).alias('Birthday'))

	#Set age/exp to 0
	df = df.with_columns(pl.lit(0).alias('Age'))
	df = df.with_columns(pl.lit(0).alias('Exp'))

	#Get the list of the unique IDs
	id_list = df['ID'].unique().to_list()
	#Create a list of the URLs based on their IDs
	if(sex=="M"):
		id_urls = list(map(lambda x: "https://firstskisport.com/cross-country/athlete.php?id="+str(x), id_list))

	else:
		id_urls = list(map(lambda x: "https://firstskisport.com/cross-country/athlete.php?id="+str(x)+"&g=w", id_list))
	#Get today's date (so it won't have to be recalculated several times)
	today = datetime.today()

	#Assign the date to the correct datetime format
	df = df.with_columns(pl.col('Date').str.strptime(pl.Date, format='%Y%m%d'))
	#df = df.with_columns(
    #    (pl.col('ID').cumcount().over('ID') + 1
    #).alias('Exp'))
	#print(df)
    
    #Go through the IDs and use BeautifulSoup to go to each skiers page to get their birthday
    #for a in range(10):
	for a in range(len(id_urls)):
		#print(id_list[a])
		if(a%10==0):
			print(f"Processing ID {a} in chunk")
        #Get the exp by separating the ids in the dataframe and assigning them 1 to length of their list
			
		#df = df.with_columns(
		#	pl.when(pl.col('ID')==id_list[a])
	#		.then(pl.arange(1, 1+pl.col('ID').count()).alias('Exp')))
		#print(df.filter(pl.col('ID')==id_list[a]))
		#Get the soup fired up
		#print("Opening url")
		id_soup = BeautifulSoup(urlopen(id_urls[a]), 'html.parser')

		#Get the birthday
		#print("Getting birthday")
		birthday = str(id_soup.body.find('h2').text)
		birthday = birthday.split(",")
		birthday = birthday[2].strip()
		#Convert it to the right format
		#Not all have dates, so we have to do some exceptions
		try:
			pattern = r"(\d{1,2})\.(\w{3}) (\d{4})"

			match = re.match(pattern, birthday)
			day, month, year = match.groups()
			#Create a month map
			month_map = {
	'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
	'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
	'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
	}
            #Get the month of the date using the map
			month_num = month_map[month]


			#Assign the birthday using the map
			birthday = datetime(int(year), month_num, int(day))
			

			 #Assign the birthday for all the skier with that id
			df = df.with_columns(pl.when(pl.col('ID') == id_list[a]).then(birthday).otherwise(pl.col('Birthday')).alias('Birthday'))        
			
			'''df = df.with_columns(
				pl.when(pl.col("ID")==id_list[a])
				.then(pl.lit(birthday).alias('Birthday'))
				.otherwise(pl.col('Birthday')))'''

			#Assign the age to that skier being the date of the race - their age
			'''df = df.with_columns(
				pl.when(pl.col('ID')==id_list[a])
				.then((pl.col('Date') - pl.lit(birthday)).cast(pl.Duration).dt.years().alias('Age'))
				.otherwise(pl.col('Age')))'''
			df = df.with_columns(pl.when(pl.col('ID') == id_list[a]).then(
				(pl.col('Date') - pl.col('Birthday')).dt.days() / 365.25
				).otherwise(pl.col('Age')).alias('Age'))
			

        #Not everyone has birthdays listed, we have to make exceptions    
		except Exception as e:
			logging.warning(f"Error parsing birthday for ID {id_list[a]}: {e}")
			#Some just have their age listed
			#Birthday will just be todays date minus the age
			try:
				age = re.search(r'\((\d+)\)', birthday)
				age = int(age.group(1))

				birthday = datetime(today.year-age, today.month, today.day)
				
				 #Assign the birthday for all the skier with that id        
				'''df = df.with_columns(
					pl.when(pl.col("ID")==id_list[a])
					.then(pl.lit(birthday).alias('Birthday'))
					.otherwise(pl.col('Birthday')))'''
				df = df.with_columns(pl.when(pl.col('ID') == id_list[a]).then(birthday).otherwise(pl.col('Birthday')).alias('Birthday'))
				
				df = df.with_columns(pl.when(pl.col('ID') == id_list[a]).then(
				(pl.col('Date') - pl.col('Birthday')).dt.days() / 365.25
				).otherwise(pl.col('Age')).alias('Age'))

				#Assign the age to that skier being the date of the race - their age
				'''df = df.with_columns(
					pl.when(pl.col('ID')==id_list[a])
					.then((pl.col('Date') - pl.lit(birthday)).cast(pl.Duration).dt.years().alias('Age'))
					.otherwise(pl.col('Age')))'''
            #For those who don't have anything listed, it will just have to be empty :(
			except Exception as e:
				logging.error(f"Error handling age for ID {id_list[a]}: {e}")
				birthday = ""
				age = ""
				'''df = df.with_columns(
					pl.when(pl.col('ID')==id_list[a])
					.then(pl.lit(age).alias('Age'))
					.otherwise(pl.col('Age')))'''
				df = df.with_columns(pl.when(pl.col('ID') == id_list[a]).then(None).otherwise(pl.col('Age')).alias('Age'))
				df = df.with_columns(pl.when(pl.col('ID') == id_list[a]).then(None).otherwise(pl.col('Birthday')).alias('Birthday'))
				
				'''df = df.with_columns(
					pl.when(pl.col('ID')==id_list[a])
					.then(pl.lit(birthday).alias('Birthday'))
					.otherwise(pl.col('Birthday')))'''
		#print(df.loc[df.ID==id_list[a]])
        #Assign the birthday for all the skier with that id        
	logging.info(f"Returning chunk size: {len(df)}")
	 
	return df


def get_chunks(df,max_threads=100):
	#Get the list of unique IDs
	id_list = df.select('ID').unique().to_series().to_list()

	#Split the IDs into chunks
	id_chunks = np.array_split(id_list, max_threads)
	
	#Create DataFrame chunks based on ID chunks
	df_chunks = [df.filter(pl.col('ID').is_in(id_chunk)) for id_chunk in id_chunks]
    
	with ThreadPoolExecutor(max_workers=max_threads) as p:
		results = p.map(get_ages, df_chunks)
		#results = list(p.map(lambda args: get_ages(*args), [(df_chunk, id_chunk) for df_chunk, id_chunk in zip(df_chunks, id_chunks)]))

	df = pl.concat(results)
	return df

def main():
	coresNr = multiprocessing.cpu_count()

	#Read the spreadsheets
	print("Reading feathers")
	men_df = pl.read_ipc("~/ski/elo/python/ski/polars/excel365/men_scrape.feather")
	ladies_df = pl.read_ipc("~/ski/elo/python/ski/polars/excel365/ladies_scrape.feather")

	
	print("Getting men ages")
	
	men_df = get_chunks(men_df)

	print("Getting lady ages")
	ladies_df = get_chunks(ladies_df)

	print(men_df)
	print(ladies_df)

	print("Writing to feather file")
	men_df.write_ipc("~/ski/elo/python/ski/polars/excel365/men_age.feather")
	ladies_df.write_ipc("~/ski/elo/python/ski/polars/excel365/ladies_age.feather")

if __name__ == '__main__':
    main()

print(time.time()- start_time)