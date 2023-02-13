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

	body = soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
	body = body[0]

	
	body = body.find_all("td")
	#print(body[13])
	if(year<=1991):
		for a in range(0, len(body)-1, 13):
			#people+=1
			ski_id = str(body[a])
			
			#print(ski_id.split("ID="))
			ski_id = ski_id.split("ID=")[1]
			ski_id = str(ski_id.split("\"")[0])
			ski_ids.append(ski_id)
			place.append(body[a+3].text)
			name.append(body[a].text.strip('\n'))
			nation.append(body[a+2].text.strip('\n'))
	elif(year<=2006):
		#people = 1
		for a in range(0, len(body), 13):
			#people+=1
			ski_id = str(body[a])
			ski_id = ski_id.split("ID=")[1]
			ski_id = str(ski_id.split("\"")[0])
			ski_ids.append(ski_id)
			place.append(body[a+3].text)
			name.append(body[a].text.strip('\n'))
			nation.append(body[a+2].text.strip('\n'))


	else:	
		#people = 1		
		for a in range(0, len(body), 13):
			#people+=1
			ski_id = str(body[a])
			ski_id = ski_id.split("ID=")[1]
			ski_id = str(ski_id.split("\"")[0])
			ski_ids.append(ski_id)
			place.append(body[a+3].text)
			name.append(body[a].text.strip('\n'))
			nation.append(body[a+2].text.strip('\n'))
			#if(people>10):
			#	break
		

	
	temp = [place, name, nation, ski_ids]
	
	return temp
	
	

def get_table(worldcup_page):
	if("2resultat" in worldcup_page):
		return -1
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
	
	body = body[1]
	
	
	#Finding the category.  Olympics, World Cup, World Championship, Tour de Ski
	body = body.find_all('td')
	
	if(body[1].text.startswith("Olympic")):
		category="Olympics"
	elif(body[1].text.startswith("WSC")):
		category="WSC"
	else:
		category = "WC"
	try:
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
	except:
		date = (body[-1].text)
		date = str(date).split("/")
		year = date[1]
		date = year+"0101"
		

	city = body[5].text

	country = body[7].text.strip()

	event = body[3].text
	if(event=="Single Mixed Relay"):
		technique = "Single Mixed Relay"
		distance = "N/A"
	elif(event == "Mixed Relay"):
		technique = "Mixed Relay"
		distance = "N/A"
	elif(event.endswith("Team")):
		technique = "Team"
		distance = (event.split(" ")[0])
	elif(event.endswith("Relay")):
		technique = "Rel"
		distance = "N/A"
	elif(event.endswith("Individual")):
		distance = float(event.split(" ")[0])
		technique = "Individual"
	elif(event.endswith("Sprint")):
		distance = (event.split(" ")[0])
		technique = "Sprint"
	elif(event.endswith("Pursuit")):
		distance = (event.split(" ")[0])
		technique = "Pursuit"
	elif(event.endswith("Mass Start")):
		distance = (event.split(" ")[0])
		technique = "Mass Start"
	else:
		print(event)

	table = [date, city, country, category, distance, technique]
	return table
	

	
	

	#Distances are the first word unless it is Single Mixed Relay or Mixed Relay, then it's NA
	#Technique is relay, individual, single mixed relay, sprint, pursuit and mass start
	
	'''if(distance.startswith("4x") or distance.startswith("3x") or enddistance.startswith("Team")):
		distance = "Rel"
	if(enddistance=="Pursuit"):
		technique = "P"
		table = [date, city, country, category, distance, technique]
		return table
	if(distance=="Sprint"):
			technique = body[3].text.split(" ")
			technique = technique[1]
			table = [date, city, country, category, distance, technique]
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
			table = [date, city, country, category, distance, technique]
			return table
		except:
			table = [date, city, country, category, distance, "N/A"]
			return table
	else:
		table = [date, city, country, category, distance, "N/A"]
		return table'''
	#return worldcup_date
def get_skier(worldcup_page, distance, sex2):
	
	try:
		worldcup_soup = BeautifulSoup(urlopen(worldcup_page), 'html.parser')
	except:
		return -1
	body = worldcup_soup.body.find_all('table', {'class':'tablesorter sortTabell'})
	
	body = body[0]
	body = body.find_all('td')
	places = []
	skier = []
	nation = []
	ski_ids = []
	sex = []
	people = 1
	if(distance == "Mixed Relay"):
		for a in range(len(body)):
			if(a%9==0):
				people+=1
				male = "../img/mini/male.png"
				female = "../img/mini/female.png"
				
				if("female" in str(body[a+3])):
					sex.append("L")
				else:
					sex.append("M")
				places.append(body[a].text)
				
				
				skier.append(body[a+2].text.strip('\n').strip(" "))
				ski_id = str(body[a+2])
				#print(ski_id)
				ski_id = ski_id.split("ID=")[1]
				ski_id = str(ski_id.split("\"")[0])
				
				ski_ids.append(ski_id)
				nation.append(body[a+4].text.strip('\n'))
				#print(nation)
				#if (people>12):
				#	break
		return [places, skier, nation, sex, ski_ids]
	elif(distance == "Single Mixed Relay"):
		
		for a in range(len(body)):
			if(a%9==0):
				people+=1
				male = "../img/mini/male.png"
				female = "../img/mini/female.png"
				
				if("female" in str(body[a+3])):
					sex.append("L")
				else:
					sex.append("M")
				places.append(body[a].text)
				skier.append(body[a+2].text.strip('\n').strip(" "))
				ski_id = str(body[a+2])
				ski_id = ski_id.split("ID=")[1]
				ski_id = str(ski_id.split("\"")[0])
				
				ski_ids.append(ski_id)
				nation.append(body[a+4].text.strip('\n'))
				#if (people>10):
				#	break
		return [places, skier, nation, sex, ski_ids]
	elif(distance == "Rel"):
		for a in range(len(body)):
			if(a%9==0):
				people+=1
				
				
				
				places.append(body[a].text)
				
				sex.append(sex2)
				
				skier.append(body[a+2].text.strip('\n').strip(" "))
				ski_id = str(body[a+2])
				
				
				ski_id = ski_id.split("ID=")[1]

				ski_id = str(ski_id.split("\"")[0])
				
				ski_ids.append(ski_id)
				nation.append(body[a+4].text.strip('\n'))
				#if (people>12):
				#	break
		return [places, skier, nation, sex, ski_ids]
	else:
		for a in range(len(body)):
		
			if(distance=="Rel" or distance=="Mixed Relay" or distance=="Single Mixed Relay"):
				break
			elif(a%9==0):
				people+=1
				
				if(str(body[a].text)!="DNF" and str(body[a].text)!="DNS" and str(body[a].text)!="DSQ"):
					places.append(body[a].text)
					sex.append(sex2)

					skier.append(body[a+2].text.strip('\n'))
					ski_id = str(body[a+2])
					
					ski_id = ski_id.split("ID=")[1]
					ski_id = str(ski_id.split("\"")[0])
					
					ski_ids.append(ski_id)

					nation.append(body[a+4].text.strip('\n'))
					sex.append(sex2)
		return [places, skier, nation, sex, ski_ids]


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
	
	#for a in range(2003, 2004):
	for a in range(2023, 2024):
		print(a)
		men_worldcup_page0 = "https://skisport365.com/skiskyting/rennkalender.php?aar="+str(a)
		ladies_worldcup_page0 = "https://skisport365.com/skiskyting/rennkalender.php?aar="+str(a)+"&k=F"
		

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
		

		for b in men_worldcup_soup0.find_all('a', {'class':'ablue'}, href = True):
			men_worldcup_page1.append('https://skisport365.com/skiskyting/'+b['href'])
		

		for b in ladies_worldcup_soup0.find_all('a', {'class':'ablue'}, href=True):
			ladies_worldcup_page1.append('https://skisport365.com/skiskyting/'+b['href'])
	

		men_standings_page0 = "https://skisport365.com/skiskyting/ranking.php?aar="+str(a)
		ladies_standings_page0 = "https://skisport365.com/skiskyting/ranking.php?aar="+str(a)+"&&k=F"

		if(a>=1983):
			men_standings = get_standings(men_standings_page0, a)
			len(men_standings[0])
			menwcs = [str(a)+'0500', "WC", "Standings", "table", ["M"]*len(men_standings[0]), 0,None, men_standings[0], men_standings[1],men_standings[2], men_standings[3]]
			menwc_standings.append(menwcs)
			ladies_standings = get_standings(ladies_standings_page0,a )
			ladieswcs = [str(a)+'0500', "WC", "Standings", "table", ["L"]*len(ladies_standings[0]), 0,None, ladies_standings[0], ladies_standings[1],ladies_standings[2], ladies_standings[3]]
			ladieswc_standings.append(ladieswcs)
		elif(a<1983 and a>=1978):
			men_standings = get_standings(men_standings_page0, a)
			menwcs = [str(a)+'0500', "WC", "Standings", "table", ["M"]*len(men_standings[0]), 0,None, men_standings[0], men_standings[1],men_standings[2], men_standings[3]]
			menwc_standings.append(menwcs)

			
		

		

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
		discipline = table[5]
		
		
		
		skiers = get_skier(men_worldcup_page1[a], discipline, sex)
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		sex = skiers[3]
		
		ski_ids = skiers[4]
		

		menwc = [date, city, country, category, sex, distance,  discipline, places, ski, nation, ski_ids]
		
		#if(distance!="Rel" and distance!="Ts"):
		men_worldcup.append(menwc)

	for a in range(len(ladies_worldcup_page1)):
		
		
		table = get_table(ladies_worldcup_page1[a])
		if(table==-1):
			continue
		date = table[0]
		print(date)

		city = table[1]
		country = table[2]
		category = table[3]
		sex = "L"
		distance = table[4]
		discipline = table[5]
		
		
		skiers = get_skier(ladies_worldcup_page1[a], discipline, sex)
		if(skiers==-1):
			continue
		places = skiers[0]
		ski = skiers[1]
		nation = skiers[2]
		sex = skiers[3]
		ski_ids = skiers[4]
		
		ladieswc = [date, city, country, category, sex, distance, discipline, places, ski, nation, ski_ids]
		#if(distance!="Rel" and distance!="Ts"):
		ladies_worldcup.append(ladieswc)

	#worldcup = ladies_worldcup
	#worldcup.extend(men_worldcup)
	ladies_worldcup.extend(ladieswc_standings)
	men_worldcup.extend(menwc_standings)
	return [ladies_worldcup, men_worldcup]
		
			

		
	

#date, city, country, category, sex, distance, discipline, places, name
worldcup = get_worldcup()


workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/ranks/biathlon/excel365/update_scrape.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")




for g in range(len(worldcup)):
	
	
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
				ladies.write(row, col+4, worldcup[g][a][4][b])
				ladies.write(row, col+5, worldcup[g][a][5])
				ladies.write(row, col+6, worldcup[g][a][6])

				ladies.write(row, col+7, worldcup[g][a][7][b])

				ladies.write(row, col+8, worldcup[g][a][8][b])

				ladies.write(row, col+9, worldcup[g][a][9][b])
				ladies.write(row, col+10, worldcup[g][a][10][b])
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
				men.write(row, col+4, worldcup[g][a][4][b])
				men.write(row, col+5, worldcup[g][a][5])
				men.write(row, col+6, worldcup[g][a][6])
				men.write(row, col+7, worldcup[g][a][7][b])
				men.write(row, col+8, worldcup[g][a][8][b])
				men.write(row, col+9, worldcup[g][a][9][b])
				men.write(row, col+10, worldcup[g][a][10][b])
				row+=1


workbook.close()
print(time.time() - start_time)







