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
	









	race_city = worldcup_soup.body.find('h1').text
	date_country = worldcup_soup.body.find('h2').text

	date_country = date_country.split(", ")
	date = str(date_country[2])
	date = date.split(" ")
	try:
		year = date[2]
	except:
		print("No date")
		return ["19800101", "Örnsköldsvik", "Sweden", "5", 0, "N/A"]
	date = date[1].split(".")
	day = date[0]
	day = str(day.zfill(2))
	
	month = date[1]
	month = convert_month(month)
	date = (year+month+day)
	country = date_country[3]

	race_cty = str(race_city)
	race_city = race_city.split(" - ")
	race = race_city[0]
	
	city = race_city[1]
	distance = race.split(" ")
	enddistance = distance[-1]
	distance = distance[0]
	if(distance.startswith("4x") or distance.startswith("3x") or enddistance.startswith("Team")):
		distance = "Rel"
	if(distance=="Team"):
		distance = "Ts"
	if(enddistance=="Pursuit"):
		technique = "Pursuit"
		table = [date, city, country, distance, technique]
		return table
	elif(enddistance=="Start"):
			technique = "Mass Start"
			table = [date, city, country, distance, technique]
			return table
	elif(enddistance=="Sprint"):
			technique = enddistance
			table = [date, city, country, distance, technique]
			return table
	if(int(year)>1985 and distance!="Rel"):
		try:
			
			if(distance=="Mixed"):
				distance = "Mixed Relay"
				technique = "N/A"
			elif(distance=="Single"):
				distance = "Single Mixed Relay"
				technique = "N/A"
			
			else:
				technique = enddistance
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
	ski_ids = []
	sex = []
	people = 1
	
	if(distance == "Mixed Relay"):
		for a in range(len(body)):
			if(a%8==0):
				people+=1
				male = "../img/mini/male.png"
				female = "../img/mini/female.png"
				
				if("female" in str(body[a+2])):
					sex.append("L")
				else:
					sex.append("M")
				places.append(body[a].text)
				skier_name = str(body[a+2])	
				if("female" in skier_name):
					first_name = skier_name.split("/span> ")[1]
					first_name = first_name.split("</a")[0]
					last_name = skier_name.split("case;\">")[1]
					last_name = last_name.split("</span")[0]
					skier_name = first_name + " " + last_name
				
					skier.append(skier_name)
				else:			
					skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
				nation.append(body[a+4].text.strip('\n'))
				#if (people>12):
					#break
		return [places, skier, nation, sex, ski_ids]
	elif(distance == "Single Mixed Relay"):
		for a in range(len(body)):
			if(a%8==0):
				people+=1
				male = "../img/mini/male.png"
				female = "../img/mini/female.png"
				
				if("female" in str(body[a+2])):
					sex.append("L")
				else:
					sex.append("M")
				places.append(body[a].text)
				skier_name = str(body[a+2])	
				if("female" in skier_name):
					first_name = skier_name.split("/span> ")[1]
					first_name = first_name.split("</a")[0]
					last_name = skier_name.split("case;\">")[1]
					last_name = last_name.split("</span")[0]
					skier_name = first_name + " " + last_name
					#print(skier_name)
					skier.append(skier_name)
				else:			
					skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
				nation.append(body[a+4].text.strip('\n'))
				#if (people>10):
					#break
		return [places, skier, nation, sex, ski_ids]
	else:
		for a in range(len(body)):
		
			if(distance=="Rel" or distance=="Mixed Relay" or distance=="Single Mixed Relay"):
				break
			elif(a%8==0):
				people+=1
				
				if(str(body[a].text)!="DNF" and str(body[a].text)!="DNS" and str(body[a].text)!="DSQ"):
					places.append(body[a].text)

					
					ski_id = str(body[a+2])
					ski_id = ski_id.split("id=")[1]
					ski_id = str(ski_id.split("&")[0])
					ski_ids.append(ski_id)
					skier_name = str(body[a+2])			
					skier.append((skier_name.split("title=\"")[1]).split("\"><span")[0])		
					nation.append(body[a+4].text.strip())
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
	
	for a in range(1958, 2024):
	#for a in range(1924, 2024):
		print(a)
		men_worldcup_page0 = "https://firstskisport.com/biathlon/calendar.php?y="+str(a)
		ladies_worldcup_page0 = "https://firstskisport.com/biathlon/calendar.php?y="+str(a)+"&g=w"
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
		title_results_count=0
		for b in men_worldcup_soup0.find_all('a', {'title':'Results'}, href = True):
			if(title_results_count%2==0):
				men_worldcup_page1.append('https://firstskisport.com/biathlon/'+b['href'])
			title_results_count+=1
		
		title_results_count = 0
		for b in ladies_worldcup_soup0.find_all('a', {'title':'Results'}, href=True):
			if(title_results_count%2==0):
				ladies_worldcup_page1.append('https://firstskisport.com/biathlon/'+b['href'])
			title_results_count+=1
		
	

	#print(ladies_worldcup_page1)
	
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
		#sex = skiers[3]
		try:
			ski_ids = skiers[4]
		except:
			continue
		menwc = [date, city, country, category, sex, distance, technique, places, ski, nation, ski_ids]
		print(date)
		if(distance!="Rel" and distance!="Mixed Relay" and distance!="Single Mixed Relay"):
			men_worldcup.append(menwc)

	for a in range(len(ladies_worldcup_page1)):
		table = get_table(ladies_worldcup_page1[a])
		date = table[0]
		print(date)
		#print(ladies_worldcup_page1[a])
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
		#print(skiers[4])
		try:
			ski_ids = skiers[4]
		except:
			continue
		ladieswc = [date, city, country, category, sex, distance, technique, places, ski, nation, ski_ids]
		#print(date)
		if(distance!="Rel" and distance!="Mixed Relay" and distance!="Single Mixed Relay"):
			ladies_worldcup.append(ladieswc)

	#worldcup = ladies_worldcup
	#worldcup.extend(men_worldcup)
	return [ladies_worldcup, men_worldcup]
		
			

		#print(table)
	

#date, city, country, category, sex, distance, discipline, places, name
	

workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/elo/python/biathlon/excel365/all.xlsx")
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
				ladies.write(row, col+10, worldcup[g][a][10][b])
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
				men.write(row, col+4, worldcup[g][a][4])
				men.write(row, col+5, worldcup[g][a][5])
				men.write(row, col+6, worldcup[g][a][6])
				men.write(row, col+7, worldcup[g][a][7][b])
				men.write(row, col+8, worldcup[g][a][8][b])
				men.write(row, col+9, worldcup[g][a][9][b])
				men.write(row, col+10, worldcup[g][a][10][b])
				row+=1


workbook.close()








