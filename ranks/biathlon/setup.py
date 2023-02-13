import pandas as pd
import time
start_time = time.time()

xlsx = pd.ExcelFile('~/ski/ranks/biathlon/excel365/all.xlsx')
update = pd.ExcelFile('~/ski/ranks/biathlon/excel365/update_scrape.xlsx')


#ladies_places = list(int(ladiesdf['place']))

def ladies_setup():
	#Step 1 is to read the sheet and assign column names
	ladiesdf = pd.read_excel(xlsx, sheet_name="Ladies", header=None)
	ladies_updatedf = pd.read_excel(update, sheet_name="Ladies", header=None)
	#mendf = pd.read_excel(xlsx, sheet_name="Men", header=None)
	#men_updatedf = pd.read_excel(update, sheet_name="Men", header=None)
	#ladiesdf = ladiesdf.append(mendf, ignore_index=True)

	ladiesdf = ladiesdf.append(ladies_updatedf, ignore_index=True)
	

	
	#ladiesdf = ladiesdf.append(men_updatedf, ignore_index=True)
	ladiesdf.columns = ['date', 'city', 'country', 'category', 'sex', 'distance', 'discipline', 'place', 'name', 'nation', 'id']
	lady_seasons = []

	#This is to get rid of the space before the nations
	ladiesdf['nation'] = ladiesdf['nation'].str.lstrip()
	ladiesdf = ladiesdf.loc[ladiesdf['place']!="DNF"]
	ladiesdf = ladiesdf.loc[ladiesdf['place']!="DNS"]
	ladiesdf = ladiesdf.loc[ladiesdf['place']!="DSQ"]
	ladiesdf['place'] = ladiesdf['place'].astype(int)
	ladiesdf = ladiesdf.loc[ladiesdf['place']<=3]
	ladiesdf = ladiesdf.loc[ladiesdf['sex']=="L"]
	ladiesdf = ladiesdf.reset_index(drop=True)

	#This is to assign seasons in the data frame
	for a in range(len(ladiesdf['date'])):
		date = str(ladiesdf['date'][a])
		year = date[0:4]
		day = date[4:8]
		if(day>'0800'):
			season = int(year)+1
		else:
			season = int(year)
		lady_seasons.append(season)

	ladiesdf['season'] = lady_seasons


	#This next part is to number the races in each season
	ladies_race = []
	ladies_points = []
	race = 1

	for a in range(len(ladiesdf['place'])):
		point = 1
		if(ladiesdf['category'][a]=='Olympics'):
			point*=5
			if(ladiesdf['discipline'][a]=='Rel' or ladiesdf['discipline'][a]=='Team' 
				or ladiesdf['discipline'][a]=='Mixed Relay'):
				point*=1
			elif(ladiesdf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(ladiesdf['place'][a])=='1'):
				point*=4
			if(str(ladiesdf['place'][a])=='2'):
				point*=2
			if(str(ladiesdf['place'][a])=='3'):
				point*=1
		elif(ladiesdf['category'][a]=='WSC'):
			point*=2.5
			if(ladiesdf['discipline'][a]=='Rel' or ladiesdf['discipline'][a]=='Team' 
				or ladiesdf['discipline'][a]=='Mixed Relay'):
				point*=1
			elif(ladiesdf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(ladiesdf['place'][a])=='1'):
				point*=4
			if(str(ladiesdf['place'][a])=='2'):
				point*=2
			if(str(ladiesdf['place'][a])=='3'):
				point*=1
		elif(ladiesdf['category'][a]=='WSC'):
			point*=2.5
			if(ladiesdf['discipline'][a]=='Rel' or ladiesdf['discipline'][a]=='Team' 
				or ladiesdf['discipline'][a]=='Mixed Relay'):
				point*=1
			elif(ladiesdf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(ladiesdf['place'][a])=='1'):
				point*=4
			if(str(ladiesdf['place'][a])=='2'):
				point*=2
			if(str(ladiesdf['place'][a])=='3'):
				point*=1
		
		elif(ladiesdf['category'][a]=='table'):
			if(str(ladiesdf['place'][a])=='1'):
				point*=80
			if(str(ladiesdf['place'][a])=='2'):
				point*=40
			if(str(ladiesdf['place'][a])=='3'):
				point*=20
		else:
			if(ladiesdf['discipline'][a]=='Rel' or ladiesdf['discipline'][a]=='Team' 
				or ladiesdf['discipline'][a]=='Mixed Relay'):
				point*=1
			elif(ladiesdf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(ladiesdf['place'][a])=='1'):
				point*=2
			elif(str(ladiesdf['place'][a])=='2' or str(ladiesdf['place'][a])=='3'):
				point*=1
				
		ladies_points.append(point)
		#If it is the first one, it'll be a race
		if (a==0):
			ladies_race.append(race)
			continue
		#If it is not the same season, reset the race back to one
		
		if(str(ladiesdf['date'][a]).endswith('0500')):
			race = 0
			ladies_race.append(race)
		elif (lady_seasons[a]!=lady_seasons[a-1]):
			race=1
			ladies_race.append(race)
		#If it is not the same date, it's a new race
		elif(ladiesdf['date'][a]!=ladiesdf['date'][a-1]):
			race+=1
			ladies_race.append(race)
		#If the place is 1
		elif(str(ladiesdf['place'][a])=='1'):
			#If the one before it is greater than one, it's a new race.  
			#Other wise it is the same race
			if(str(ladiesdf['place'][a-1])>'1'):
				race+=1
				ladies_race.append(race)
			else:
				ladies_race.append(race)
		#Else it is the same race
		else:
			ladies_race.append(race)
		




	ladiesdf['id'] = ladiesdf['id'].str.split("&")
	ladiesdf['id'] = ladiesdf['id'].str[0]
	ladiesdf['id'] = ladiesdf['id'].astype(int)

	#ladiesdf['name'][24330] = "Tatjana Kuznetsova2"

	


	#ladiesdf.loc[(ladiesdf['name']=="Lilia Vasilieva") & (ladiesdf['season']>2010), 
	#"name"] = "Lilia Vasilieva2"

	ladiesdf['race'] = ladies_race
	ladiesdf['points'] = ladies_points
	return ladiesdf

def men_setup():
	mendf = pd.read_excel(xlsx, sheet_name="Men", header=None)
	men_updatedf = pd.read_excel(update, sheet_name="Men", header=None)
	mendf = mendf.append(men_updatedf, ignore_index=True)
	mendf.columns = ['date', 'city', 'country', 'category', 'sex', 'distance',  'discipline', 'place', 'name', 'nation', 'id']
	mendf['nation'] = mendf['nation'].str.lstrip()
	mendf = mendf.loc[mendf['place']!="DNF"]
	mendf = mendf.loc[mendf['place']!="DNS"]
	mendf = mendf.loc[mendf['place']!="DSQ"]
	mendf['place'] = mendf['place'].astype(int)
	mendf = mendf.loc[mendf['place']<=3]
	mendf = mendf.loc[mendf['sex']=="M"]
	mendf= mendf.reset_index(drop=True)

	male_seasons = []
	for a in range(len(mendf['date'])):
		date = str(mendf['date'][a])
		year = date[0:4]
		day = date[4:8]
		if(day>'0800'):
			season = int(year)+1
		else:
			season = int(year)
		male_seasons.append(season)

	mendf['season'] = male_seasons



	men_race = []
	men_points = []
	race = 1

	for a in range(len(mendf['place'])):
		
		point = 1
		if(mendf['category'][a]=='Olympics'):
			point*=5
			if(mendf['discipline'][a]=='Rel' or mendf['discipline'][a]=='Mixed Relay'
				or mendf['discipline'][a]=='Team'):
				point*=1
			elif(mendf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(mendf['place'][a])=='1'):
				point*=4
			if(str(mendf['place'][a])=='2'):
				point*=2
			if(str(mendf['place'][a])=='3'):
				point*=1
		elif(mendf['category'][a]=='WSC'):
			point*=2.5
			if(mendf['discipline'][a]=='Rel' or mendf['discipline'][a]=='Mixed Relay'
				or mendf['discipline'][a]=='Team'):
				point*=1
			elif(mendf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(mendf['place'][a])=='1'):
				point*=4
			if(str(mendf['place'][a])=='2'):
				point*=2
			if(str(mendf['place'][a])=='3'):
				point*=1
		elif(mendf['category'][a]=='WSC'):
			point*=2.5
			if(mendf['discipline'][a]=='Rel' or mendf['discipline'][a]=='Mixed Relay'
				or mendf['discipline'][a]=='Team'):
				point*=1
			elif(mendf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(mendf['place'][a])=='1'):
				point*=4
			if(str(mendf['place'][a])=='2'):
				point*=2
			if(str(mendf['place'][a])=='3'):
				point*=1
		
		elif(mendf['category'][a]=='table'):
			if(str(mendf['place'][a])=='1'):
				point*=80
			if(str(mendf['place'][a])=='2'):
				point*=40
			if(str(mendf['place'][a])=='3'):
				point*=20
		else:
			if(mendf['discipline'][a]=='Rel' or mendf['discipline'][a]=='Mixed Relay'
				or mendf['discipline'][a]=='Team'):
				point*=1
			elif(mendf['discipline'][a]=="Single Mixed Relay"):
				point*=2
			else:
				point*=4
			if(str(mendf['place'][a])=='1'):
				point*=2
			elif(str(mendf['place'][a])=='2' or str(mendf['place'][a])=='3'):
				point*=1
		men_points.append(point)
		if (a==0):
			men_race.append(race)
			continue
		if(str(mendf['date'][a]).endswith('0500')):
			race = 0
			men_race.append(race)
		elif (male_seasons[a]!=male_seasons[a-1]):
			race=1
			men_race.append(race)
		elif(mendf['date'][a]!=mendf['date'][a-1]):
			race+=1
			men_race.append(race)
		elif(str(mendf['place'][a])=='1'):
			if(str(mendf['place'][a-1])>'1'):
				race+=1
				men_race.append(race)
			else:
				men_race.append(race)
		else:
			men_race.append(race)
		

	
	#print(mendf.loc[[2072]])

	#mendf['name'][2072] = "Oddmund Jensen2"
	#mendf.loc[(mendf['name']=="David Rees") & (mendf['nation'] =="Canada"), "name"] = "David Rees2"
	#print(mendf.loc[mendf['name']=="David Rees2"])
	#mendf.loc[(mendf['name']=="Gunnar Eriksson") & (mendf['nation']=="Sweden") , "name"] = "Gunnar Eriksson2"
	#print(mendf.loc[mendf['nation']=="Finland"])
	#mendf.loc[(mendf['name']=="Alexander Schwarz") & (mendf['nation']=='Finland'), "name"] = "Alexander Schwarz2"
	#mendf.loc[(mendf['name']=="Peter Klofutar") & (mendf['season']>1989), "name"] = "Peter Klofutar2"

	#mendf.loc[(mendf['name']=="Lilia Vasilieva") & (mendf['season']>2010), 
	#"name"] = "Lilia Vasilieva2"

	mendf['race'] = men_race
	mendf['points'] = men_points
	return mendf

ladiesdf = ladies_setup()
ladiesdf.to_pickle("~/ski/ranks/biathlon/excel365/ladiesdf.pkl")
ladiesdf.to_excel("~/ski/ranks/biathlon/excel365/ladiesdf.xlsx")



mendf = men_setup()
mendf.to_pickle("~/ski/ranks/biathlon/excel365/mendf.pkl")
mendf.to_excel("~/ski/ranks/biathlon/excel365/mendf.xlsx")

print(time.time() - start_time)


