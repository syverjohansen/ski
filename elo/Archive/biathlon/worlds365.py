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
	try:
		world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	except:
		table =  ["", "", "", "", ""]
		return table
	body = world_soup.find_all("tr")
	body = body[-5:-1]
	td = world_soup.find_all('td')
	body = td[-10:-1]
	date = body[-2].text
	date = str(date)
	date = date.split(' ')
	year = date[2]
	date = "".join(date[1])
	date = date.split(".")
	month = convert_month(date[1])
	day = date[0]
	day = day.zfill(2)
	date = (year+month+day)
	city = body[3].text.strip()
	print(date)
	country = body[5].text.strip()
	distance = td[-9].text.split(" ")
	enddistance = distance[-1]
	distance = distance[0]
	if(distance.startswith("4x") or distance.startswith("3x")):
		distance = "Rel"
	if(distance=="Team" or distance=="Single"):
		distance = "Ts"
	if(enddistance=="Pursuit"):
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
			technique = body[1].text.split(" ")
			if(technique[0]=="Mixed"):
				distance = "Mixed Relay"
				technique = "N/A"
			elif(technique[0]=="Single"):
				distance = "Single Mixed Relay"
				technique = "N/A"
			else:
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
def get_skier(world_page, distance, sex2):
	try:
		world_soup = BeautifulSoup(urlopen(world_page), 'html.parser')
	except:
		places = ""
		skier = ""
		nation = ""
		return [places, skier, nation]
	body = world_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
	
	body = body[0]
	body = body.find_all('td')
	places = []
	skier = []
	nation = []
	sex = []
	people = 1
	if(distance == "Mixed Relay"):
		for a in range(len(body)):
			if(a%10==0):
				people+=1
				male = "../img/mini/male.png"
				female = "../img/mini/female.png"
				
				if("female" in str(body[a+3])):
					sex.append("L")
				else:
					sex.append("M")
				places.append(body[a].text)
				skier.append(body[a+3].text.strip('\n').strip(" "))
				nation.append(body[a+5].text.strip('\n'))
				if (people>12):
					break
		return [places, skier, nation, sex]
	elif(distance == "Single Mixed Relay"):
		for a in range(len(body)):
			if(a%10==0):
				people+=1
				male = "../img/mini/male.png"
				female = "../img/mini/female.png"
				
				if("female" in str(body[a+3])):
					sex.append("L")
				else:
					sex.append("M")
				places.append(body[a].text)
				skier.append(body[a+3].text.strip('\n').strip(" "))
				nation.append(body[a+5].text.strip('\n'))
				if (people>10):
					break
		return [places, skier, nation, sex]
	else:
		for a in range(len(body)):
		
			if(distance=="Rel"):
				if(a%10==0):
					people+=1
					#print(body[a].text)
					places.append(body[a].text)
					skier.append(body[a+3].text.strip('\n'))
					nation.append(body[a+5].text.strip('\n'))
					sex.append(sex2)
					if (people>12):
						break
			elif(a%9==0):
				people+=1
				
				places.append(body[a].text)

				skier.append(body[a+2].text.strip('\n'))

				nation.append(body[a+4].text.strip('\n'))
				sex.append(sex2)
				if(people>10):
					break	
		return [places, skier, nation, sex]


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
	
	for a in range(1958, 2021):
		print(a)
		men_world_page0 = "https://skisport365.com/skiskyting/rennkalender.php?aar="+str(a)+"&hva=OLVM"
		ladies_world_page0 = "https://skisport365.com/skiskyting/rennkalender.php?aar="+str(a)+"&hva=OLVM&k=F"
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
			elif(menbody[a+2].text==""):
				num-=1

				#men_world_page1.append('https://skisport365.com/ski/'+menbody[a+2]['href'])
			num+=1
		num = 0
		for a in range(2, len(ladiesbody), 8):
			#print(a, ladiesbody[a+2])
			if(ladiesbody[a].text=="WSC"):
				ladies_wcevents.append(num)
			elif(ladiesbody[a+2].text==""):
				num-=1
			#	ladies_world_page1.append('https://skisport365.com/skiskyting/'+ladiesbody[a+2]['href'])
			num+=1
		#print(ladies_wcevents)
		it = 0
		for b in men_world_soup0.find_all('a', {'class':'ablue'}, href = True):
			if(it in men_wcevents):
				men_world_page1.append('https://skisport365.com/skiskyting/'+b['href'])
			it+=1
			
		num = 0
		it = 0
		for b in ladies_world_soup0.find_all('a', {'class':'ablue'}, href=True):
			#print(b)
			if(it in ladies_wcevents):
				ladies_world_page1.append('https://skisport365.com/skiskyting/'+b['href'])
			it+=1

		
	
	men_world_page3 = []
	ladies_world_page3 = []
	men_world = []
	ladies_world = []
	world = []
	for a in range(len(men_world_page1)):
	
		table = get_table(men_world_page1[a])
		date = table[0]
		city = table[1]
		country = table[2]
		category = "world"
		sex = "M"
		distance = table[3]
		technique = table[4]
		skiers = get_skier(men_world_page1[a], distance, sex)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		sex = skiers[3]
		menwc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		#print(menwc)
		men_world.append(menwc)

	for a in range(len(ladies_world_page1)):
		table = get_table(ladies_world_page1[a])
		date = table[0]
		#print(date)
		city = table[1]
		country = table[2]
		category = "world"
		sex = "L"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(ladies_world_page1[a], distance, sex)
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
	

workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/excel365/biathlon/worlds/world.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")


world = get_world()
print('yo')
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
				men.write(row, col+4, world[g][a][4][b])
				men.write(row, col+5, world[g][a][5])
				men.write(row, col+6, world[g][a][6])
				men.write(row, col+7, world[g][a][7][b])
				men.write(row, col+8, world[g][a][8][b])
				men.write(row, col+9, world[g][a][9][b])
				row+=1


workbook.close()








