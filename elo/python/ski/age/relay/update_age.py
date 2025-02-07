#Get the age and the number of races for each athlete and 
#put it into the file after the setup

#Read the Varmen/Varladies Pkl

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
import pandas as pd
import numpy as np
import time
import datetime
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}
start_time = time.time()

men_pkl = pd.read_pickle("~/ski/elo/python/ski/age/relay/excel365/mendf.pkl")
update_mendf = pd.read_pickle("~/ski/elo/python/ski/age/relay/excel365/menupdate_setup.pkl")
print(update_mendf)

men_pkl = men_pkl.append(update_mendf, ignore_index=True)



ladies_pkl = pd.read_pickle("~/ski/elo/python/ski/age/relay/excel365/ladiesdf.pkl")

update_ladiesdf =  pd.read_pickle("~/ski/elo/python/ski/age/relay/excel365/ladiesupdate_setup.pkl")

ladies_pkl = ladies_pkl.append(update_ladiesdf, ignore_index=True)

def convert_month(month):
	if(month == "Jan"):
		return "1"
	elif(month == "Feb"):
		return "2"
	elif(month == "Mar"):
		return "3"
	elif(month == "Apr"):
		return "4"
	elif(month == "May"):
		return "5"
	elif(month == "Jun"):
		return "6"
	elif(month == "Jul"):
		return "7"
	elif(month == "Aug"):
		return "8"
	elif(month == "Sep"):
		return "9"
	elif(month == "Oct"):
		return "10"
	elif(month == "Nov"):
		return "11"
	elif(month == "Dec"):
		return "12"

def convert_date(date):
	date = list(map(lambda x: date[x][0:4])+"-"+date[x][4:6]+"-"+date[x][6:8])
	return date


def men_ages(mendf):
	#Get IDs
	

	mendf = mendf.loc[mendf['city']!="Summer"]

	big_df = mendf.loc[mendf['season']<2024]
	mendf = mendf.loc[mendf['season']==2024]
	
	mendf['birthday'] =np.nan
	#mendf['age'] = np.nan
	mendf['age'] = 0

	mendf['exp'] = 0
	
	#print(mendf['date'])
	mendf['date'] = pd.to_datetime((mendf['date']), format='%Y%m%d')
	#print(mendf['date'])

	id_list = list(mendf['id'].unique())
	big_id_list = list(big_df['id'].unique())
	id_urls = list(map(lambda x: "https://firstskisport.com/cross-country/athlete.php?id="+str(x), id_list))
	
	
	#Webscrape
	for a in range(len(id_urls)):
	#for a in range(187,188):
		print(a)
		if(id_list[a] in big_id_list):
			
			exp = int(list(big_df.loc[big_df.id==id_list[a], 'exp'])[-1])
			#print(exp)

			mendf.loc[mendf.id==id_list[a], 'exp'] = range(exp,exp+len(mendf.loc[mendf.id==id_list[a], 'exp']))

			birthday = list(big_df.loc[big_df.id==id_list[a], 'birthday'])[-1]

			mendf.loc[mendf.id==id_list[a], 'birthday'] = birthday
			mendf.loc[mendf.id==id_list[a], 'age'] = mendf.loc[mendf.id==id_list[a], 'date'] - mendf.loc[mendf.id==id_list[a], 'birthday']
			mendf.loc[mendf.id==id_list[a], 'age'] = mendf.loc[mendf.id==id_list[a], 'age'] / np.timedelta64(1,'Y')
	#	id_url_page0 = urlopen(id_urls[a], timeout=10)
	#for a in range(10):
		else:
			#print(mendf.loc[mendf.id==id_list[a], 'id'])
			mendf.loc[mendf.id==id_list[a], 'exp'] = range(1,1+len(mendf.loc[mendf.id==id_list[a], 'exp']))
			id_soup0 = BeautifulSoup(urlopen(id_urls[a]), 'html.parser')
			
			#print(id_soup0)
			#born = (id_soup0.body.find_all(text="Born"))
			born = str(id_soup0.body.find('h2').text)
			birthday = born.split(", ")[2]
			birthday = birthday.split(" (")[0]
			birthday = birthday.split(" ")
			try:
				year = birthday[1]
				day_month = birthday[0]
				day_month = day_month.split(".")
				
				day = day_month[0]
				month = day_month[1]
				month = convert_month(month)
				birthday = year+"-"+month+"-"+day
				birthdate = datetime.datetime(int(year), int(month), int(day))
			except:
				birthdate = ""
			


			
			'''if(len(born)==0):
				birthday = datetime.datetime(1000,1,1)
				continue
			#birthday = str((id_soup0.find("td", text="Born").find_next_sibling("td").text))
			#print(birthday)
			if("(" in birthday):
				birthday = birthday.split("(")[1]
				if(birthday==')'):
					continue
				else:
					birthday = birthday.split(")")[0]
					print(birthday)
			birthday = birthday.replace(".", " ")
			birthday = birthday.split(" ")
			day = birthday[0]
			if(day == "0"):
				day = "1"
			
			month = birthday[1]
			month = convert_month(month)
			year = birthday[2]'''
			
			
			mendf.loc[mendf.id==id_list[a], 'birthday'] = birthdate
			#print(birthdate)

			
			#mendf.loc[mendf.id==id_list[a], 'date']
			#print(type(mendf.loc[mendf.id==id_list[a], 'birthday']))
			try:
				mendf['birthday'] = pd.to_datetime(mendf['birthday'])
			except Exception as e:
				print("Error", e)

			mendf.loc[mendf.id==id_list[a], 'birthday'] = pd.to_datetime(mendf.loc[mendf.id==id_list[a], 'birthday'])

			
			mendf.loc[mendf.id==id_list[a], 'age'] = mendf.loc[mendf.id==id_list[a], 'date'] - mendf.loc[mendf.id==id_list[a], 'birthday']
			mendf.loc[mendf.id==id_list[a], 'age'] = mendf.loc[mendf.id==id_list[a], 'age'] / np.timedelta64(1,'Y')
	return mendf	#print(mendf.loc[mendf.id==id_list[a]])
		#print((mendf.loc[mendf.id==id_list[a], 'age']))

	#mendf['age'] = mendf['date']-mendf['birthday']
	#mendf['age'] = mendf['age']  / np.timedelta64(1,'Y')
	#print(mendf['age'])



		#print(id_list[a], birthday)
		



def ladies_ages(ladiesdf):
	#Get IDs
	

	ladiesdf = ladiesdf.loc[ladiesdf['city']!="Summer"]

	big_df = ladiesdf.loc[ladiesdf['season']<2024]
	ladiesdf = ladiesdf.loc[ladiesdf['season']==2024]
	
	ladiesdf['birthday'] =np.nan
	#ladiesdf['age'] = np.nan
	ladiesdf['age'] = 0

	ladiesdf['exp'] = 0
	
	#print(ladiesdf['date'])
	ladiesdf['date'] = pd.to_datetime((ladiesdf['date']), format='%Y%m%d')
	#print(ladiesdf['date'])

	id_list = list(ladiesdf['id'].unique())
	big_id_list = list(big_df['id'].unique())
	id_urls = list(map(lambda x: "https://skisport365.com/ski/utover.php?ID="+str(x)+"&k=F", id_list))
	
	
	#Webscrape
	for a in range(len(id_urls)):
	#for a in range(187,188):
		print(a)
		if(id_list[a] in big_id_list):
			
			exp = int(list(big_df.loc[big_df.id==id_list[a], 'exp'])[-1])
			#print(exp)

			ladiesdf.loc[ladiesdf.id==id_list[a], 'exp'] = range(exp,exp+len(ladiesdf.loc[ladiesdf.id==id_list[a], 'exp']))

			birthday = list(big_df.loc[big_df.id==id_list[a], 'birthday'])[-1]
			ladiesdf.loc[ladiesdf.id==id_list[a], 'birthday'] = birthday
			ladiesdf.loc[ladiesdf.id==id_list[a], 'age'] = ladiesdf.loc[ladiesdf.id==id_list[a], 'date'] - ladiesdf.loc[ladiesdf.id==id_list[a], 'birthday']
			ladiesdf.loc[ladiesdf.id==id_list[a], 'age'] = ladiesdf.loc[ladiesdf.id==id_list[a], 'age'] / np.timedelta64(1,'Y')
	#	id_url_page0 = urlopen(id_urls[a], timeout=10)
	#for a in range(10):
		else:
			ladiesdf.loc[ladiesdf.id==id_list[a], 'exp'] = range(1,1+len(ladiesdf.loc[ladiesdf.id==id_list[a], 'exp']))
			id_soup0 = BeautifulSoup(urlopen(id_urls[a]), 'html.parser')
			
			#print(id_soup0)
			#born = (id_soup0.body.find_all(text="Born"))
			born = str(id_soup0.body.find('h2').text)
			birthday = born.split(", ")[2]
			birthday = birthday.split(" (")[0]
			birthday = birthday.split(" ")
			try:
				year = birthday[1]
				day_month = birthday[0]
				day_month = day_month.split(".")
				
				day = day_month[0]
				month = day_month[1]
				month = convert_month(month)
				birthday = year+"-"+month+"-"+day
				birthdate = datetime.datetime(int(year), int(month), int(day))
			except:
				birthdate = ""
			#born = (id_soup0.body.find_all(text="Born"))
			'''born = str(id_soup0.body.find('h2').text)
			birthday = born.split(", ")[2]
			birthday = birthday.split(" (")[0]
			birthday = birthday.split(" ")
			year = birthday[1]
			day_month = birthday[0]
			day_month.split(".")
			day = day_month[0]
			month = day_month[1]
			month = convert_month(month)
			
			if(len(born)==0):
				birthday = datetime.datetime(1000,1,1)
				continue
			birthday = str((id_soup0.find("td", text="Born").find_next_sibling("td").text))
			#print(birthday)
			if("(" in birthday):
				birthday = birthday.split("(")[1]
				if(birthday==')'):
					continue
				else:
					birthday = birthday.split(")")[0]
					print(birthday)
			birthday = birthday.replace(".", " ")
			birthday = birthday.split(" ")
			day = birthday[0]
			if(day == "0"):
				day = "1"
			
			month = birthday[1]
			month = convert_month(month)
			year = birthday[2]
			birthday = year+"-"+month+"-"+day
			birthdate = datetime.datetime(int(year), int(month), int(day))
			'''
			ladiesdf.loc[ladiesdf.id==id_list[a], 'birthday'] = birthdate
			#print(birthdate)

			
			#ladiesdf.loc[ladiesdf.id==id_list[a], 'date']
			#print(type(ladiesdf.loc[ladiesdf.id==id_list[a], 'birthday']))
			ladiesdf['birthday'] = pd.to_datetime(ladiesdf['birthday'])

			ladiesdf.loc[ladiesdf.id==id_list[a], 'birthday'] = pd.to_datetime(ladiesdf.loc[ladiesdf.id==id_list[a], 'birthday'])

			
			ladiesdf.loc[ladiesdf.id==id_list[a], 'age'] = ladiesdf.loc[ladiesdf.id==id_list[a], 'date'] - ladiesdf.loc[ladiesdf.id==id_list[a], 'birthday']
			ladiesdf.loc[ladiesdf.id==id_list[a], 'age'] = ladiesdf.loc[ladiesdf.id==id_list[a], 'age'] / np.timedelta64(1,'Y')
	return ladiesdf	#print(ladiesdf.loc[ladiesdf.id==id_list[a]])

mendf = men_ages(men_pkl)
mendf.to_pickle("~/ski/elo/python/ski/age/relay/excel365/menupdate_setup.pkl")
mendf.to_excel("~/ski/elo/python/ski/age/relay/excel365/menupdate_setup.xlsx")


ladiesdf = ladies_ages(ladies_pkl)
ladiesdf.to_pickle("~/ski/elo/python/ski/age/relay/excel365/ladiesupdate_setup.pkl")
ladiesdf.to_excel("~/ski/elo/python/ski/age/relay/excel365/ladiesupdate_setup.xlsx")

print(time.time() - start_time)
