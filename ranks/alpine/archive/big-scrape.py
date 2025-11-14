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

def get_date(worldcup_page):
	worldcup_soup = BeautifulSoup(urlopen(worldcup_page), 'html.parser')
	body = worldcup_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	date = (body[-3].text)
	date = str(date)
	date = date.split(" ")
	year = date[2]
	date = "".join(date[1])
	
	date = date.split(".")
	month = convert_month(date[1])
	#print(month)
	day = date[0]
	day = day.zfill(2)
	date = (year+month+day)
	return date

def get_standings(standings_page, year):
	name = []
	nation = []
	place = []
	ski_ids = []
	##Get name, nation, id, place

	page0 = urlopen(standings_page)

	soup0 = BeautifulSoup(page0, 'html.parser')
	#print(soup0)

	body = soup0.body.find_all('table', {'class':'tablesorter sortTabell'})
	body = body[0]

	
	body = body.find_all("td")
	#print(body[13])
	

	
	for a in range(0, len(body), 6):
		#people+=1
		ski_id = str(body[a])
		ski_id = ski_id.split("id=")[1]
		ski_id = str(ski_id.split("&")[0])
		ski_id = str(ski_id.split("\"")[0])
		ski_ids.append(ski_id)
		skier_name = str(body[a])
		first_name = skier_name.split("/span> ")[1]
		first_name = first_name.split("</a")[0]
		last_name = skier_name.split("case;\">")[1]
		last_name = last_name.split("</span")[0]
		skier_name = first_name + " " + last_name		
		name.append(skier_name)
		#name.append((skier_name.split("title=\"")[1]).split("\">")[0])
		place.append(body[a+2].text)
		nation.append(body[a+1].text.strip())
			#if(people>10):
			#	break
		

	
	temp = [place, name, nation, ski_ids]
	
	return temp


def get_table(worldcup_page):
	w0 = 0
	to_worldcup_page = []
	to_worldcup_page.append(worldcup_page)
	while(len(to_worldcup_page)>0):
		if(w0>=len(to_worldcup_page)):
			w0 = 0
		try:
			worldcup_page = urlopen(to_worldcup_page[w0], timeout=10)
			to_worldcup_page.remove(to_worldcup_page[w0])
		except:
			w0+=1
			pass

	worldcup_soup = BeautifulSoup(worldcup_page, 'html.parser')
	body = worldcup_soup.body.find_all('table', {'class':'tablesorter'})

	race_city = worldcup_soup.body.find('h1').text
	date_country = worldcup_soup.body.find('h2').text
	date_country = date_country.split(", ")
	category = date_country[1]

	if(category.startswith("Olympic")):
		category="Olympics"
	elif(category.startswith("WSC")):
		category="WSC"
	
	else:
		category = "WC"
	
	try:
		date = str(date_country[2])
		date = date.split(" ")
		year = date[2]
		date = date[1].split(".")
		day = date[0]
		day = str(day.zfill(2))		
		month = date[1]
		month = convert_month(month)
		date = (year+month+day)
	except:
		date = str(date_country[2])
		date = date.split(" ")
		year = date[2]
		date = year+"0101"
		print("No date")
	country = date_country[3]

	race_cty = str(race_city)
	race_city = race_city.split(" - ")
	race = race_city[0]
	
	city = race_city[1]
	distance = race.split(" ")
	enddistance = distance[-1]
	distance = distance[0]




	#distance = distance[0]

	

	#distance = distance[0]

	if(race == "Downhill"):
		distance = "Downhill"
	elif("Combined" in race):
		distance = "Combined"
	elif("Parallel" in race):
		distance = "Parallel"
	elif("Giant Slalom" in race):
		distance = "Giant Slalom"
	elif (race == "Super G"):
		distance = "Super G"

	elif(("Slalom" in race)  or (race=="City Event")):
		distance = "Slalom"
	
	else:
		print(distance)

		
	table = [date, city, country, category, distance]
	return table
	
		
	#return worldcup_date
def get_skier(worldcup_page, distance):
	w0 = 0
	to_worldcup_page = []
	to_worldcup_page.append(worldcup_page)
	while(len(to_worldcup_page)>0):
		if(w0>=len(to_worldcup_page)):
			w0 = 0
		try:
			worldcup_page = urlopen(to_worldcup_page[w0], timeout=10)
			to_worldcup_page.remove(to_worldcup_page[w0])
		except:
			w0+=1
			pass
	worldcup_soup = BeautifulSoup(worldcup_page, 'html.parser')
	body = worldcup_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
	body = body[0]
	body = body.find_all('td')
	places = []
	skier = []
	nation = []
	ski_ids = []
	people = 1
	highest_place = 0
	OOT_places = []
	OOT_skier = []
	OOT_nation = []
	OOT_ski_ids = []
	OOT_time = []
	if(len(body)<=10):
		return -1
	
	if(body[10].text=="1" or body[10].text=="2"):
	
		for a in range(len(body)):
			
			if(a%10==0):
				if(str(body[a].text)!="DNS"
					and str(body[a].text)!="DNQ" and str(body[a].text)!="DSQ" and str(body[a].text)!="OOT" and ("DNF" not in str(body[a].text))):
					places.append(body[a].text)

					ski_id = str(body[a+2])
					
					ski_id = ski_id.split("id=")[1]
					ski_id = str(ski_id.split("&")[0])
					ski_ids.append(ski_id)
					skier_name = str(body[a+2])
					first_name = skier_name.split("/span> ")[1]
					first_name = first_name.split("</a")[0]
					last_name = skier_name.split("case;\">")[1]
					last_name = last_name.split("</span")[0]
					skier_name = first_name + " " + last_name		
					skier.append(skier_name)		
					
					#skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
					nation.append(body[a+4].text.strip())
					
				elif(str(body[a].text)=="OOT"):
					
					highest_place+=1
					OOT_places.append(highest_place)
					OOT_ski_id = str(body[a+2])
					OOT_ski_id = OOT_ski_id.split("id=")[1]
					OOT_ski_id = str(OOT_ski_id.split("&")[0])
					OOT_ski_ids.append(OOT_ski_id)
					#OOT_ski_id = OOT_ski_id[1]
					
					OOT_skier_name = str(body[a+2])	
					first_name = OOT_skier_name.split("/span> ")[1]
					first_name = first_name.split("</a")[0]
					last_name = OOT_skier_name.split("case;\">")[1]
					last_name = last_name.split("</span")[0]
					OOT_skier_name = first_name + " " + last_name
					OOT_skier.append(OOT_skier_name)					
					#OOT_skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
					OOT_nation.append(body[a+4].text.strip())
					
					OOT_time.append(body[a+6].text)
				else:
					continue

			if(distance=="Rel" and people>12):
				break
			elif(distance!="Rel" and people>10):
				break
	else:
	
		for a in range(len(body)):
			if(a%8==0):
				if(str(body[a].text)!="DNS"
					and str(body[a].text)!="DNQ" and str(body[a].text)!="DSQ" and str(body[a].text)!="OOT" and ("DNF" not in str(body[a].text))):
					places.append(body[a].text)

					ski_id = str(body[a+2])
					
					ski_id = ski_id.split("id=")[1]
					ski_id = str(ski_id.split("&")[0])
					ski_ids.append(ski_id)
					skier_name = str(body[a+2])
					first_name = skier_name.split("/span> ")[1]
					first_name = first_name.split("</a")[0]
					last_name = skier_name.split("case;\">")[1]
					last_name = last_name.split("</span")[0]
					skier_name = first_name + " " + last_name		
					skier.append(skier_name)		
					
					#skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
					nation.append(body[a+4].text.strip())
				elif(str(body[a].text)=="OOT"):
					
					#highest_place+=1
					#OOT_places.append(highest_place)
					highest_place+=1
					OOT_places.append(highest_place)
					OOT_ski_id = str(body[a+2])
					OOT_ski_id = OOT_ski_id.split("id=")[1]
					OOT_ski_id = str(OOT_ski_id.split("&")[0])
					OOT_ski_ids.append(OOT_ski_id)
					#OOT_ski_id = OOT_ski_id[1]
					
					OOT_skier_name = str(body[a+2])	
					first_name = OOT_skier_name.split("/span> ")[1]
					first_name = first_name.split("</a")[0]
					last_name = OOT_skier_name.split("case;\">")[1]
					last_name = last_name.split("</span")[0]
					OOT_skier_name = first_name + " " + last_name
					OOT_skier.append(OOT_skier_name)					
					#OOT_skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
					OOT_nation.append(body[a+4].text.strip())
					
					OOT_time.append(body[a+6].text)
				else:
					continue

			if(distance=="Rel" and people>12):
				break
			elif(distance!="Rel" and people>10):
				break
	OOT = pd.DataFrame()
	OOT['ski_ids'] = OOT_ski_ids
	OOT['skier'] = OOT_skier
	OOT['nation'] = OOT_nation
	highest_place = int(places[len(places)-1])+1
	highest_highest_place = highest_place+len(OOT_ski_ids)
	OOT['time'] = OOT_time
	OOT = OOT.sort_values('time')
	OOT['places'] = list(range(highest_place,highest_highest_place))
	#print(OOT)
	OOT_ski_ids = OOT['ski_ids'].values.tolist()
	OOT_skier = OOT['skier'].values.tolist()
	OOT_nation = OOT['nation'].values.tolist()
	OOT_places = OOT['places'].values.tolist()
	places = places+OOT_places
	skier = skier+OOT_skier
	nation = nation+OOT_nation
	ski_ids=ski_ids+OOT_ski_ids
	
	
	return [places, skier, nation, ski_ids]


def get_team(worldcup_page, distance):
	worldcup_soup = BeautifulSoup(urlopen(worldcup_page), 'html.parser')
	body = worldcup_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
	body = body[0]
	body = body.find_all('td')
	places = []
	skier = []
	nation = []
	people = 1
	for a in range(len(body)):
		
		if(a%7==0):
			people+=1
			places.append(body[a].text)
			skier.append(body[a+2].text.strip('\n'))
			nation.append(body[a+4].text)

		
	return [places, skier, nation]


def get_worldcup():
	men_worldcup_page1 = []
	ladies_worldcup_page1 = []
	menwc_standings = []
	ladieswc_standings = []
	#for a in range(2021, 2022):
	for a in range(1931, 2024):
		print(a)
		men_worldcup_page0 = "https://firstskisport.com/alpine/calendar.php?y="+str(a)
		ladies_worldcup_page0 = "https://firstskisport.com/alpine/calendar.php?y="+str(a)+"&g=w"
		

		try:
			men_worldcup_page0 = urlopen(men_worldcup_page0, timeout=10)	
		except:
			m0 = 0
			to_men_worldcup_page0 = []
			to_men_worldcup_page0.append(men_worldcup_page0)
			while(len(to_men_worldcup_page0)>0):
				if(m0>=len(to_men_worldcup_page0)):
					m0=0
				try:
					men_worldcup_page0 = urlopen(to_men_worldcup_page0[m0], timeout=10)	
					to_men_worldcup_page0.remove(to_men_worldcup_page0[m0])
				except:
					m0+=1
					pass
		
		try:
			ladies_worldcup_page0 = urlopen(ladies_worldcup_page0, timeout=10)	
		except:
			l0 = 0
			to_ladies_worldcup_page0 = []
			to_ladies_worldcup_page0.append(ladies_worldcup_page0)
			while(len(to_ladies_worldcup_page0)>0):
				if(l0>=len(to_ladies_worldcup_page0)):
					l0=0
				try:
					ladies_worldcup_page0=urlopen(to_ladies_worldcup_page0[l0], timeout=19)
					to_ladies_worldcup_page0.remove(to_ladies_worldcup_page0[l0])
				except:
					l0+=1
					pass



		#ladies_worldcup_page0 = urlopen(ladies_worldcup_page0)
		men_worldcup_soup0 = BeautifulSoup(men_worldcup_page0, 'html.parser')
		ladies_worldcup_soup0 = BeautifulSoup(ladies_worldcup_page0, 'html.parser')
		

		title_results_count=0
		for b in men_worldcup_soup0.find_all('a', {'title':'Results'}, href = True):
			if(title_results_count%2==0):
				men_worldcup_page1.append('https://firstskisport.com/alpine/'+b['href'])
			title_results_count+=1
		
		title_results_count = 0
		for b in ladies_worldcup_soup0.find_all('a', {'title':'Results'}, href=True):
			if(title_results_count%2==0):
				ladies_worldcup_page1.append('https://firstskisport.com/alpine/'+b['href'])
			title_results_count+=1
		
		if(a>=1967):
			men_standings_page0 = "https://firstskisport.com/alpine/ranking.php?y="+str(a)
			ladies_standings_page0 = "https://firstskisport.com/alpine/ranking.php?y="+str(a)+"&hva=&g=w"
			
			men_standings = get_standings(men_standings_page0, a)

			menwcs = [str(a)+'0500', "WC", "Standings", "table", "M", 0, men_standings[0], men_standings[1],men_standings[2], men_standings[3]]
			menwc_standings.append(menwcs)
			ladies_standings = get_standings(ladies_standings_page0,a )
			ladieswcs = [str(a)+'0500', "WC", "Standings", "table", "L", 0, ladies_standings[0], ladies_standings[1],ladies_standings[2], ladies_standings[3]]
			ladieswc_standings.append(ladieswcs)

	


	men_worldcup_page3 = []
	ladies_worldcup_page3 = []
	men_worldcup = []
	ladies_worldcup = []
	worldcup = []
	for a in range(len(men_worldcup_page1)):
	

		table = get_table(men_worldcup_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = table[3]
		sex = "M"
		distance = table[4]
		
		
		
		skiers = get_skier(men_worldcup_page1[a], distance)
		if(skiers==-1):
			continue
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		ski_ids = skiers[3]
		menwc = [date, city, country, category, sex, distance, places, ski, nation, ski_ids]
		men_worldcup.append(menwc)
		

	for a in range(len(ladies_worldcup_page1)):
		
		table = get_table(ladies_worldcup_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = table[3]
		sex = "L"
		distance = table[4]
		
		
		skiers = get_skier(ladies_worldcup_page1[a], distance)
		if(skiers==-1):
			continue
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		ski_ids = skiers[3]
		ladieswc = [date, city, country, category, sex, distance,  places, ski, nation, ski_ids]
		ladies_worldcup.append(ladieswc)
		
	#worldcup = ladies_worldcup
	#worldcup.extend(men_worldcup)
	ladies_worldcup.extend(ladieswc_standings)
	men_worldcup.extend(menwc_standings)
	return [ladies_worldcup, men_worldcup]
		
			

		#print(table)
	

#date, city, country, category, sex, distance, discipline, places, name
worldcup = get_worldcup()

workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/ranks/alpine/excel365/all.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")




for g in range(len(worldcup)):
	#print(worldcup[g])
	if(g==0):
		row = 0
		col = 0
		for a in range(len(worldcup[g])):
			print(worldcup[g][a][0])
			for b in range(len(worldcup[g][a][8])):
				ladies.write(row, col, worldcup[g][a][0])
				ladies.write(row, col+1, worldcup[g][a][1])
				ladies.write(row, col+2, worldcup[g][a][2])
				ladies.write(row, col+3, worldcup[g][a][3])
				ladies.write(row, col+4, worldcup[g][a][4])
				ladies.write(row, col+5, worldcup[g][a][5])
				
		
				ladies.write(row, col+6, worldcup[g][a][6][b])
				#print(worldcup[g][a][0])
				#print(worldcup[g][a][7][b])
				ladies.write(row, col+7, worldcup[g][a][7][b])
				#print(worldcup[g][a][8][b])
				ladies.write(row, col+8, worldcup[g][a][8][b])
				ladies.write(row, col+9, worldcup[g][a][9][b])
				#print(worldcup[g][a][9][b])
				row+=1
	else:
		row = 0
		col = 0
		for a in range(len(worldcup[g])):
			print(worldcup[g][a][0])
			for b in range(len(worldcup[g][a][8])):
				men.write(row, col, worldcup[g][a][0])
				men.write(row, col+1, worldcup[g][a][1])
				men.write(row, col+2, worldcup[g][a][2])
				men.write(row, col+3, worldcup[g][a][3])
				men.write(row, col+4, worldcup[g][a][4])
				men.write(row, col+5, worldcup[g][a][5])
			
				men.write(row, col+6, worldcup[g][a][6][b])
				men.write(row, col+7, worldcup[g][a][7][b])
				men.write(row, col+8, worldcup[g][a][8][b])
				men.write(row, col+9, worldcup[g][a][9][b])
				row+=1


workbook.close()
print(time.time() - start_time)







