import ssl
import re
ssl._create_default_https_context = ssl._create_unverified_context
from urllib.request import urlopen
from bs4 import BeautifulSoup
import xlsxwriter

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

def get_date(olympic_page):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter'})
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
	#print(date)
	return date

def get_city(olympic_page):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	city = body[5].text
	return(city)

def get_country(olympic_page):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	country = body[7].text.strip()
	return(country)

def get_distance(olympic_page):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	distance = body[3].text.split(" ")
	distance = distance[0]
	if(distance.startswith("4x" or "3x")):
		distance = "Rel"
	if(distance=="Team"):
		distance = "Ts"
	return(distance)

def get_technique(olympic_page):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	technique = body[3].text.split(" ")
	technique = technique[2]
	print(technique)

def get_table(olympic_page):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	
	date = (body[-3].text)
	date = str(date)
	date = date.split(" ")
	year = date[2]
	date = "".join(date[1])
	date = date.split(".")
	month = convert_month(date[1])
	day = date[0]
	day = day.zfill(2)
	date = (year+month+day)

	city = body[5].text

	country = body[7].text.strip()

	distance = body[3].text.split(" ")
	distance = distance[0]
	if(distance.startswith("4x" or "3x")):
		distance = "Rel"
	if(distance=="Team"):
		distance = "Ts"
	if(distance=="Duathlon"):
		technique = "P"
		table = [date, city, country, distance, technique]
		return table
	if(distance=="Sprint"):
			technique = body[3].text.split(" ")
			technique = technique[1]
			table = [date, city, country, distance, technique]
			return table
	if(int(year)>1985 and distance!="Rel"):
		try:
			technique = body[3].text.split(" ")
			technique = technique[2]
			table = [date, city, country, distance, technique]
			return table
		except:
			table = [date, city, country, distance, "N/A"]
			return table
	else:
		table = [date, city, country, distance, "N/A"]
		return table
	#return olympic_date
def get_skier(olympic_page, distance):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
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

		if(distance=="Rel" and people>12):
			break
		elif(distance!="Rel" and people>10):
			break
	
	
	return [places, skier, nation]


def get_team(olympic_page, distance):
	olympic_soup = BeautifulSoup(urlopen(olympic_page), 'html.parser')
	body = olympic_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
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


def get_olympic():
	men_olympic_page1 = []
	ladies_olympic_page1 = []
	olympics_years = [1924, 1928, 1932, 1936, 1948, 1952, 1956, 1960, 1964, 1968, 1972, 1976, 1980, 1984, 1988, 1992, 1994, 1998, 2002, 2006, 2010, 2014, 2018]
	for a in range(len(olympics_years)):

		men_olympic_page0 = "https://skisport365.com/ski/rennkalender.php?aar="+str(olympics_years[a])+"&hva=OLVM"
		ladies_olympic_page0 = "https://skisport365.com/ski/rennkalender.php?aar="+str(olympics_years[a])+"&hva=OLVM&k=F"
		men_olympic_page0 = urlopen(men_olympic_page0)
		ladies_olympic_page0 = urlopen(ladies_olympic_page0)
		men_olympic_soup0 = BeautifulSoup(men_olympic_page0, 'html.parser')

		ladies_olympic_soup0 = BeautifulSoup(ladies_olympic_page0, 'html.parser')

		menbody = men_olympic_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		ladiesbody = ladies_olympic_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		menbody = menbody[0]
		ladiesbody = ladiesbody[0]
		menbody = menbody.find_all('td')
		ladiesbody = ladiesbody.find_all('td')
		num = 0
		men_wcevents = []
		ladies_wcevents = []
		for a in range(2, len(menbody), 8):
			if(menbody[a].text==""):
				men_wcevents.append(num)
				#men_olympic_page1.append('https://skisport365.com/ski/'+menbody[a+2]['href'])
			num+=1


		num = 0
		for a in range(2, len(ladiesbody), 8):
			if(ladiesbody[a].text==""):
				ladies_wcevents.append(num)
			num+=1
		
		it = 0
		for b in men_olympic_soup0.find_all('a', {'class':'ablue'}, href = True):
			if(it in men_wcevents):
				men_olympic_page1.append('https://skisport365.com/ski/'+b['href'])
			it+=1
			
		num = 0
		it = 0
		for b in ladies_olympic_soup0.find_all('a', {'class':'ablue'}, href=True):
			if(it in ladies_wcevents):
				ladies_olympic_page1.append('https://skisport365.com/ski/'+b['href'])
			it+=1

	print(men_olympic_page1)
	print(ladies_olympic_page1)
	
	men_olympic_page3 = []
	ladies_olympic_page3 = []
	men_olympic = []
	ladies_olympic = []
	olympic = []
	for a in range(len(men_olympic_page1)):
	
		'''men_olympic_soup1 = BeautifulSoup(urlopen(men_olympic_page1[a]), 'html.parser')
		date = get_date(men_olympic_page1[a])
		year  = int(date[0:4])
		city = get_city(men_olympic_page1[a])
		country = get_country(men_olympic_page1[a])
		category = "olympic Cup"
		sex = "M"
		distance = get_distance(men_olympic_page1[a])
		if(year>1985 and distance!="Rel"):
			technique = get_technique(men_olympic_page1[a])'''
		table = get_table(men_olympic_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = "olympic"
		sex = "M"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(men_olympic_page1[a], distance)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		menwc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		#print(menwc)
		men_olympic.append(menwc)

	for a in range(len(ladies_olympic_page1)):
		table = get_table(ladies_olympic_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = "olympic"
		sex = "L"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(ladies_olympic_page1[a], distance)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		ladieswc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		#print(ladieswc)
		ladies_olympic.append(ladieswc)

	#olympic = ladies_olympic
	#olympic.extend(men_olympic)
	return [ladies_olympic, men_olympic]
		
			

		#print(table)
	

#date, city, country, category, sex, distance, discipline, places, name
	

workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/excel365/olympics/olympic.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")


olympic = get_olympic()

for g in range(len(olympic)):
	#print(olympic[g])
	if(g==0):
		row = 0
		col = 0
		for a in range(len(olympic[g])):
			for b in range(len(olympic[g][a][7])):
				ladies.write(row, col, olympic[g][a][0])
				ladies.write(row, col+1, olympic[g][a][1])
				ladies.write(row, col+2, olympic[g][a][2])
				ladies.write(row, col+3, olympic[g][a][3])
				ladies.write(row, col+4, olympic[g][a][4])
				ladies.write(row, col+5, olympic[g][a][5])
				ladies.write(row, col+6, olympic[g][a][6])
				ladies.write(row, col+7, olympic[g][a][7][b])
				#print(olympic[g][a][0])
				#print(olympic[g][a][7][b])
				ladies.write(row, col+8, olympic[g][a][8][b])
				#print(olympic[g][a][8][b])
				ladies.write(row, col+9, olympic[g][a][9][b])
				#print(olympic[g][a][9][b])
				row+=1
	else:
		row = 0
		col = 0
		for a in range(len(olympic[g])):
			for b in range(len(olympic[g][a][7])):
				men.write(row, col, olympic[g][a][0])
				men.write(row, col+1, olympic[g][a][1])
				men.write(row, col+2, olympic[g][a][2])
				men.write(row, col+3, olympic[g][a][3])
				men.write(row, col+4, olympic[g][a][4])
				men.write(row, col+5, olympic[g][a][5])
				men.write(row, col+6, olympic[g][a][6])
				men.write(row, col+7, olympic[g][a][7][b])
				men.write(row, col+8, olympic[g][a][8][b])
				men.write(row, col+9, olympic[g][a][9][b])
				row+=1


workbook.close()








