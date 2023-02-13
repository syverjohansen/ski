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



def get_table(worldcup_page):
	try:
		worldcup_soup = BeautifulSoup(urlopen(worldcup_page), 'html.parser')
	except:
		table =  ["", "", "", "", ""]
		return table
	body = worldcup_soup.find_all("tr")
	body = body[-5:-1]
	td = worldcup_soup.find_all('td')
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
	country = body[5].text.strip()
	distance = td[-9].text.split(" ")
	enddistance = distance[-1]
	distance = distance[0]
	if(distance.startswith("4x") or distance.startswith("3x") or enddistance.startswith("Team")):
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
	#return worldcup_date
def get_skier(worldcup_page, distance, sex2):
	try:
		worldcup_soup = BeautifulSoup(urlopen(worldcup_page), 'html.parser')
	except:
		places = ""
		skier = ""
		nation = ""
		return [places, skier, nation]
	body = worldcup_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
	
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
		
			if(distance=="Rel" or distance=="Mixed Relay" or distance=="Single Mixed Relay"):
				break
			elif(a%9==0):
				people+=1
				
				if(str(body[a].text)!="DNF" and str(body[a].text)!="DNS" and str(body[a].text)!="DSQ"):
					places.append(body[a].text)

					skier.append(body[a+2].text.strip('\n'))

					nation.append(body[a+4].text.strip('\n'))
					sex.append(sex2)
		return [places, skier, nation, sex]


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
	
	for a in range(2021, 2022):
		print(a)
		men_worldcup_page0 = "https://skisport365.com/skiskyting/rennkalender.php?aar="+str(a)
		ladies_worldcup_page0 = "https://skisport365.com/skiskyting/rennkalender.php?aar="+str(a)+"&k=F"
		men_worldcup_page0 = urlopen(men_worldcup_page0)
		ladies_worldcup_page0 = urlopen(ladies_worldcup_page0)
		men_worldcup_soup0 = BeautifulSoup(men_worldcup_page0, 'html.parser')
		ladies_worldcup_soup0 = BeautifulSoup(ladies_worldcup_page0, 'html.parser')
		menbody = men_worldcup_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		ladiesbody = ladies_worldcup_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		menbody = menbody[0]
		ladiesbody = ladiesbody[0]
		menbody = menbody.find_all('td')
		ladiesbody = ladiesbody.find_all('td')
		num = 0
		men_wcevents = []
		ladies_wcevents = []

		#print(ladies_wcevents)
		for b in men_worldcup_soup0.find_all('a', {'class':'ablue'}, href = True):
			men_worldcup_page1.append('https://skisport365.com/skiskyting/'+b['href'])
			
		for b in ladies_worldcup_soup0.find_all('a', {'class':'ablue'}, href=True):
			ladies_worldcup_page1.append('https://skisport365.com/skiskyting/'+b['href'])
		
		
	
	men_worldcup_page3 = []
	ladies_worldcup_page3 = []
	men_worldcup = []
	ladies_worldcup = []
	worldcup = []
	for a in range(len(men_worldcup_page1)):
	
		table = get_table(men_worldcup_page1[a])
		date = table[0]
		city = table[1]
		country = table[2]
		category = "all"
		sex = "M"
		distance = table[3]
		technique = table[4]
		skiers = get_skier(men_worldcup_page1[a], distance, sex)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		sex = skiers[3]
		menwc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		print(date)
		if(distance!="Rel" and distance!="Mixed Relay" and distance!="Single Mixed Relay"):
			men_worldcup.append(menwc)

	for a in range(len(ladies_worldcup_page1)):
		table = get_table(ladies_worldcup_page1[a])
		date = table[0]
		#print(date)
		city = table[1]
		country = table[2]
		category = "all"
		sex = "L"
		distance = table[3]
		technique = table[4]
		
		skiers = get_skier(ladies_worldcup_page1[a], distance, sex)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		ladieswc = [date, city, country, category, sex, distance, technique, places, ski, nation]
		print(date)
		if(distance!="Rel" and distance!="Mixed Relay" and distance!="Single Mixed Relay"):
			ladies_worldcup.append(ladieswc)

	#worldcup = ladies_worldcup
	#worldcup.extend(men_worldcup)
	return [ladies_worldcup, men_worldcup]
		
			

		#print(table)
	

#date, city, country, category, sex, distance, discipline, places, name
	

workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/elo/biathlon/excel365/all2021.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")


worldcup = get_worldcup()
print("yo")

for g in range(len(worldcup)):
	#print(worldcup[g])
	if(g==0):
		row = 0
		col = 0
		for a in range(len(worldcup[g])):
			for b in range(len(worldcup[g][a][7])):
				ladies.write(row, col, worldcup[g][a][0])
				ladies.write(row, col+1, worldcup[g][a][1])
				ladies.write(row, col+2, worldcup[g][a][2])
				ladies.write(row, col+3, worldcup[g][a][3])
				ladies.write(row, col+4, worldcup[g][a][4])
				ladies.write(row, col+5, worldcup[g][a][5])
				ladies.write(row, col+6, worldcup[g][a][6])
				ladies.write(row, col+7, worldcup[g][a][7][b])
				#print(worldcup[g][a][0])
				#print(worldcup[g][a][7][b])
				ladies.write(row, col+8, worldcup[g][a][8][b])
				#print(worldcup[g][a][8][b])
				ladies.write(row, col+9, worldcup[g][a][9][b])
				#print(worldcup[g][a][9][b])
				row+=1
	else:
		row = 0
		col = 0
		for a in range(len(worldcup[g])):
			for b in range(len(worldcup[g][a][7])):
				men.write(row, col, worldcup[g][a][0])
				men.write(row, col+1, worldcup[g][a][1])
				men.write(row, col+2, worldcup[g][a][2])
				men.write(row, col+3, worldcup[g][a][3])
				men.write(row, col+4, worldcup[g][a][4][b])
				men.write(row, col+5, worldcup[g][a][5])
				men.write(row, col+6, worldcup[g][a][6])
				men.write(row, col+7, worldcup[g][a][7][b])
				men.write(row, col+8, worldcup[g][a][8][b])
				men.write(row, col+9, worldcup[g][a][9][b])
				row+=1


workbook.close()








