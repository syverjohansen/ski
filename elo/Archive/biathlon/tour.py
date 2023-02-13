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


def get_tour():
	men_tour_page1= []
	ladies_tour_page1 = []
	for a in range(2007,2020):
		print(a)
		men_tour_page0 = "https://skisport365.com/ski/rennkalender.php?aar="+str(a)+"&hva=WC"
		ladies_tour_page0 = "https://skisport365.com/ski/rennkalender.php?aar="+str(a)+"&hva=WC&k=F"
		men_tour_page0 = urlopen(men_tour_page0)
		ladies_tour_page0 = urlopen(ladies_tour_page0)
		men_tour_soup0 = BeautifulSoup(men_tour_page0, 'html.parser')
		ladies_tour_soup0 = BeautifulSoup(ladies_tour_page0, 'html.parser')


		menbody = men_tour_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		ladiesbody = ladies_tour_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		menbody = menbody[0]
		ladiesbody = ladiesbody[0]
		menbody = menbody.find_all('td')
		ladiesbody = ladiesbody.find_all('td')
		num = 0
		men_wcevents = []
		ladies_wcevents = []
		for a in range(1, len(menbody), 8):
			
			if(menbody[a].text==" Tour de Ski"):
				#print(menbody[a].text)
				men_wcevents.append(num)
				break
				#men_tour_page1.append('https://skisport365.com/ski/'+menbody[a+2]['href'])
			num+=1


		num = 0
		for a in range(1, len(ladiesbody), 8):
			if(ladiesbody[a].text==" Tour de Ski"):
				#print(ladiesbody[a].text)
				ladies_wcevents.append(num)
				break
			num+=1


		it = 0
		for b in men_tour_soup0.find_all('a', {'class':'ablue'}, href = True):
			if(it in men_wcevents):
				men_tour_page1.append('https://skisport365.com/ski/'+b['href'])
			it+=1
			
		num = 0
		it = 0
		for b in ladies_tour_soup0.find_all('a', {'class':'ablue'}, href=True):
			if(it in ladies_wcevents):
				ladies_tour_page1.append('https://skisport365.com/ski/'+b['href'])
			it+=1

	print(men_tour_page1)
	print(ladies_tour_page1)

	men_tour_page3 = []
	ladies_tour_page3 = []
	men_tour = []
	ladies_tour = []
	tour = []

	for a in range(len(men_tour_page1)):

		table = get_table(men_tour_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = "tour"
		sex = "M"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(men_tour_page1[a], distance)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		menwc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		#print(menwc)
		men_tour.append(menwc)

	for a in range(len(ladies_tour_page1)):
		table = get_table(ladies_tour_page1[a])
		date = table[0]
		print(date)
		city = table[1]
		country = table[2]
		category = "tour"
		sex = "L"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(ladies_tour_page1[a], distance)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		ladieswc = [date, city, country, category, sex, distance, technique, places, ski, nation]

		ladies_tour.append(ladieswc)


	return[ladies_tour, men_tour]

workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/excel365/tour/tour.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")


tour = get_tour()

for g in range(len(tour)):
	#print(tour[g])
	if(g==0):
		row = 0
		col = 0
		for a in range(len(tour[g])):
			for b in range(len(tour[g][a][7])):
				ladies.write(row, col, tour[g][a][0])
				ladies.write(row, col+1, tour[g][a][1])
				ladies.write(row, col+2, tour[g][a][2])
				ladies.write(row, col+3, tour[g][a][3])
				ladies.write(row, col+4, tour[g][a][4])
				ladies.write(row, col+5, "N/A")
				ladies.write(row, col+6, tour[g][a][6])
				ladies.write(row, col+7, tour[g][a][7][b])
				#print(tour[g][a][0])
				#print(tour[g][a][7][b])
				ladies.write(row, col+8, tour[g][a][8][b])
				#print(tour[g][a][8][b])
				ladies.write(row, col+9, tour[g][a][9][b])
				#print(tour[g][a][9][b])
				row+=1
	else:
		row = 0
		col = 0
		for a in range(len(tour[g])):
			for b in range(len(tour[g][a][7])):
				men.write(row, col, tour[g][a][0])
				men.write(row, col+1, tour[g][a][1])
				men.write(row, col+2, tour[g][a][2])
				men.write(row, col+3, tour[g][a][3])
				men.write(row, col+4, tour[g][a][4])
				men.write(row, col+5, "N/A")
				men.write(row, col+6, tour[g][a][6])
				men.write(row, col+7, tour[g][a][7][b])
				men.write(row, col+8, tour[g][a][8][b])
				men.write(row, col+9, tour[g][a][9][b])
				row+=1

workbook.close()








