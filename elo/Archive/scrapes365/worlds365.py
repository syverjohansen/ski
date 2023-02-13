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

def get_date(world_page):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter'})
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

def get_city(world_page):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	city = body[5].text
	return(city)

def get_country(world_page):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	country = body[7].text.strip()
	return(country)

def get_distance(world_page):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	distance = body[3].text.split(" ")
	distance = distance[0]
	if(distance.startswith("4x" or "3x")):
		distance = "Rel"
	if(distance=="Team"):
		distance = "Ts"
	return(distance)

def get_technique(world_page):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter'})
	body = body[1]
	body = body.find_all('td')
	technique = body[3].text.split(" ")
	technique = technique[2]
	print(technique)

def get_table(world_page):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter'})
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
	#return world_date
def get_skier(world_page, distance):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
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


def get_team(world_page, distance):
	world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	body = world_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
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


def get_world():
	men_world_page1 = []
	ladies_world_page1 = []
	worlds_years = [1925, 1926, 1927, 1929, 1930, 1931, 1933, 1934, 1935, 1937, 1939, 1950, 1954, 1958, 1962, 1966, 1970, 1974, 1978, 1982, 1985, 1987, 1989, 1991, 1993, 1995, 1997, 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019]
	for a in range(len(worlds_years)):

		men_world_page0 = "https://skisport365.com/ski/rennkalender.php?aar="+str(worlds_years[a])+"&hva=OLVM"
		ladies_world_page0 = "https://skisport365.com/ski/rennkalender.php?aar="+str(worlds_years[a])+"&hva=OLVM&k=F"
		men_world_page0 = urlopen(men_world_page0)
		ladies_world_page0 = urlopen(ladies_world_page0)
		men_world_soup0 = BeautifulSoup(men_world_page0, 'html.parser')

		ladies_world_soup0 = BeautifulSoup(ladies_world_page0, 'html.parser')
		menbody = men_world_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		ladiesbody = ladies_world_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		menbody = menbody[0]
		ladiesbody = ladiesbody[0]
		menbody = menbody.find_all('td')
		ladiesbody = ladiesbody.find_all('td')
		num = 0
		men_wcevents = []
		ladies_wcevents = []
		for a in range(2, len(menbody), 8):
			if(menbody[a].text=="WSC"):
				men_wcevents.append(num)
				#men_world_page1.append('https://skisport365.com/ski/'+menbody[a+2]['href'])
			num+=1


		num = 0
		for a in range(2, len(ladiesbody), 8):
			if(ladiesbody[a].text=="WSC"):
				ladies_wcevents.append(num)
			num+=1
		
		it = 0
		for b in men_world_soup0.find_all('a', {'class':'ablue'}, href = True):
			if(it in men_wcevents):
				men_world_page1.append('https://skisport365.com/ski/'+b['href'])
			it+=1
			
		num = 0
		it = 0
		for b in ladies_world_soup0.find_all('a', {'class':'ablue'}, href=True):
			if(it in ladies_wcevents):
				ladies_world_page1.append('https://skisport365.com/ski/'+b['href'])
			it+=1

	print(men_world_page1)
	print(ladies_world_page1)
	
	men_world_page3 = []
	ladies_world_page3 = []
	men_world = []
	ladies_world = []
	world = []
	for a in range(len(men_world_page1)):

		table = get_table(men_world_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = "world"
		sex = "M"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(men_world_page1[a], distance)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		menwc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		#print(menwc)
		men_world.append(menwc)

	for a in range(len(ladies_world_page1)):
		table = get_table(ladies_world_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = "world"
		sex = "L"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(ladies_world_page1[a], distance)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		ladieswc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		#print(ladieswc)
		ladies_world.append(ladieswc)

	#world = ladies_world
	#world.extend(men_world)
	return [ladies_world, men_world]
		
			

		#print(table)
	

#date, city, country, category, sex, distance, discipline, places, name
	

workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/excel365/worlds/world.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")


world = get_world()

for g in range(len(world)):
	#print(world[g])
	if(g==0):
		row = 0
		col = 0
		for a in range(len(world[g])):
			for b in range(len(world[g][a][7])):
				ladies.write(row, col, world[g][a][0])
				ladies.write(row, col+1, world[g][a][1])
				ladies.write(row, col+2, world[g][a][2])
				ladies.write(row, col+3, world[g][a][3])
				ladies.write(row, col+4, world[g][a][4])
				ladies.write(row, col+5, world[g][a][5])
				ladies.write(row, col+6, world[g][a][6])
				ladies.write(row, col+7, world[g][a][7][b])
				#print(world[g][a][0])
				#print(world[g][a][7][b])
				ladies.write(row, col+8, world[g][a][8][b])
				#print(world[g][a][8][b])
				ladies.write(row, col+9, world[g][a][9][b])
				#print(world[g][a][9][b])
				row+=1
	else:
		row = 0
		col = 0
		for a in range(len(world[g])):
			for b in range(len(world[g][a][7])):
				men.write(row, col, world[g][a][0])
				men.write(row, col+1, world[g][a][1])
				men.write(row, col+2, world[g][a][2])
				men.write(row, col+3, world[g][a][3])
				men.write(row, col+4, world[g][a][4])
				men.write(row, col+5, world[g][a][5])
				men.write(row, col+6, world[g][a][6])
				men.write(row, col+7, world[g][a][7][b])
				men.write(row, col+8, world[g][a][8][b])
				men.write(row, col+9, world[g][a][9][b])
				row+=1


workbook.close()








