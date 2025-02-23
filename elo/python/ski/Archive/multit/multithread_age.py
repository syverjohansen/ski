import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from urllib.request import urlopen
from datetime import datetime
import re
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
	sex = df['Sex'].iloc[1]
	logging.info(f"Processing chunk size: {len(df)}")

	#Set the birthday column to NA (default for people who don't have an age listed)
	df['Birthday'] =np.nan

	#Set age/exp to 0
	df['Age'] = 0
	df['Exp'] = 0

	#Get the list of the unique IDs
	id_list = list(df['ID'].unique())
	print(id_list)
	#Create a list of the URLs based on their IDs
	if(sex=="M"):
		id_urls = list(map(lambda x: "https://firstskisport.com/cross-country/athlete.php?id="+str(x), id_list))

	else:
		id_urls = list(map(lambda x: "https://firstskisport.com/cross-country/athlete.php?id="+str(x)+"&g=w", id_list))
	#Get today's date (so it won't have to be recalculated several times)
	today = datetime.today()

	#Assign the date to the correct datetime format
	df['Date'] = pd.to_datetime((df['Date']), format='%Y%m%d')
    
    
    #Go through the IDs and use BeautifulSoup to go to each skiers page to get their birthday
    #for a in range(10):
	for a in range(len(id_urls)):
		#print(id_list[a])
		if(a%10==0):
			print(f"Processing ID {a} in chunk")
        #Get the exp by separating the ids in the dataframe and assigning them 1 to length of their list
		#print("Getting exp")
		df.loc[df.ID==id_list[a], 'Exp'] = range(1,1+len(df.loc[df.ID==id_list[a], 'Exp']))
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
			df.loc[df.ID==id_list[a], 'Birthday'] = birthday

			#Assign the age to that skier being the date of the race - their age
			df.loc[df.ID==id_list[a], 'Age'] = df.loc[df.ID==id_list[a], 'Date'] - df.loc[df.ID==id_list[a], 'Birthday']
			df.loc[df.ID==id_list[a], 'Age'] = df.loc[df.ID==id_list[a], 'Age'] / np.timedelta64(1,'Y')
            
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
				df.loc[df.ID==id_list[a], 'Birthday'] = birthday

				#Assign the age to that skier being the date of the race - their age
				df.loc[df.ID==id_list[a], 'Age'] = df.loc[df.ID==id_list[a], 'Date'] - df.loc[df.ID==id_list[a], 'Birthday']
				df.loc[df.ID==id_list[a], 'Age'] = df.loc[df.ID==id_list[a], 'Age'] / np.timedelta64(1,'Y')
            #For those who don't have anything listed, it will just have to be empty :(
			except Exception as e:
				logging.error(f"Error handling age for ID {id_list[a]}: {e}")
				birthday = ""
				age = ""
				df.loc[df.ID==id_list[a], 'Age'] = ""
				df.loc[df.ID==id_list[a], 'Age'] = ""
		#print(df.loc[df.ID==id_list[a]])
        #Assign the birthday for all the skier with that id        
	logging.info(f"Returning chunk size: {len(df)}")
	 
	return df


def get_chunks(df,max_threads=100):
	id_list = list(df['ID'].unique())


	id_chunks = np.array_split(id_list, max_threads)
	

	df_chunks = [df[df['ID'].isin(id_chunk)] for id_chunk in id_chunks]
    
	with ThreadPoolExecutor(max_workers=max_threads) as p:
		results = p.map(get_ages, df_chunks)
		#results = list(p.map(lambda args: get_ages(*args), [(df_chunk, id_chunk) for df_chunk, id_chunk in zip(df_chunks, id_chunks)]))

	df = pd.concat(results)
	return df

def read_spreadsheets(sex):
	print(sex)
	df = pd.read_excel("~/ski/elo/python/ski/excel365/multithread_demo_all.xlsx", sheet_name = sex)
	return df


def main():
	coresNr = multiprocessing.cpu_count()

	#Read the spreadsheets
	print("Reading spreadsheets")
	men_df = read_spreadsheets("Men")
	ladies_df = read_spreadsheets("Ladies")

	
	print("Getting men ages")
	
	men_df = get_chunks(men_df)

	print("Getting lady ages")
	ladies_df = get_chunks(ladies_df)

	print("Writing to excel file")
	writer = pd.ExcelWriter("/Users/syverjohansen/ski/elo/python/ski/excel365/multithread_demo_age.xlsx")
	ladies_df.to_excel(writer, sheet_name='Ladies', index=False)
	men_df.to_excel(writer, sheet_name='Men', index=False)
	writer.save()

if __name__ == '__main__':
    main()

print(time.time()- start_time)