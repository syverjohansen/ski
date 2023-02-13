import ssl
import re
ssl._create_default_https_context = ssl._create_unverified_context
from urllib.request import urlopen
from bs4 import BeautifulSoup
import xlsxwriter


def get_standings():
	mentable = []
	ladiestable = []
	for a in range(1978, 2021):
		yar = a
		
		year = (str(a)+"9999")
		men_table_page0 = "https://skisport365.com/skiskyting/ranking.php?aar="+str(a)
		ladies_table_page0 = "https://skisport365.com/skiskyting/ranking.php?aar="+str(a)+"&hva=1&k=F"
		men_table_page0 = urlopen(men_table_page0)
		ladies_table_page0 = urlopen(ladies_table_page0)
		men_table_soup0 = BeautifulSoup(men_table_page0, 'html.parser')
		ladies_table_soup0 = BeautifulSoup(ladies_table_page0, 'html.parser')

		menbody = men_table_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
		menbody = menbody[0]
		menbody = menbody.find_all('td')
		menplaces = []
		menskier = []
		mennation = []
		if(a>1982):
			yar = a
			ladiesbody = ladies_table_soup0.body.find_all('table', {'class':'sortTabell tablesorter'})
			
			ladiesbody = ladiesbody[0]
			
			ladiesbody = ladiesbody.find_all('td')

			ladiesplaces = []
			ladiesskier = []
			ladiesnation = []
			people = 1
			for a in range(0, len(ladiesbody), 13):
				people+=1
				ladiesplaces.append(ladiesbody[a+3].text)
				ladiesskier.append(ladiesbody[a].text.strip())
				ladiesnation.append(ladiesbody[a+2].text.strip())
				if(a==1983):
					if(people==9):
						break
				if(people>10):
					break
			people = 1
			for a in range(0, len(menbody), 13):
				people+=1
				
				menplaces.append(menbody[a+3].text)
				menskier.append(menbody[a].text.strip('\n').strip())
				mennation.append(menbody[a+2].text.strip('\n').strip())

				if(people>10):
					break

		else:
			people = 1
			for a in range(0, len(menbody), 13):
				people+=1
				
				menplaces.append(menbody[a+3].text)
				menskier.append(menbody[a].text.strip('\n').strip())
				mennation.append(menbody[a+2].text.strip('\n').strip())
				if(people>10):
					break
	
			

		
		
			

			
		
		temp = [year, menplaces, menskier, mennation]
		mentable.append(temp)

		if(yar>1982):
			
			temp = [year, ladiesplaces, ladiesskier, ladiesnation]
			ladiestable.append(temp)
			
		else:
			ladiestable.append(['','', '', ''])

	table = [mentable, ladiestable]
	return table


workbook = xlsxwriter.Workbook("/Users/syverjohansen/ski/excel365/biathlon/table/wctable.xlsx")
ladies = workbook.add_worksheet("Ladies")
men = workbook.add_worksheet("Men")


table = get_standings()

for g in range(len(table)):
	if(g==1):
		row = 0
		col = 0
		for a in range(len(table[g])):
			for b in range(len(table[g][a][1])):
				ladies.write(row, col, table[g][a][0])
				ladies.write(row, col+1, "N/A")
				ladies.write(row, col+2, "N/A")
				ladies.write(row, col+3, "table")
				ladies.write(row, col+4, "L")
				ladies.write(row, col+5, "N/A")
				ladies.write(row, col+6, "N/A")
				ladies.write(row, col+7, table[g][a][1][b])
				ladies.write(row, col+8, table[g][a][2][b])
				ladies.write(row, col+9, table[g][a][3][b])
				row+=1
	else:
		row = 0
		col = 0
		for a in range(len(table[g])):
			for b in range(len(table[g][a][1])):
				men.write(row, col, table[g][a][0])
				men.write(row, col+1, "N/A")
				men.write(row, col+2, "N/A")
				men.write(row, col+3, "table")
				men.write(row, col+4, "M")
				men.write(row, col+5, "N/A")
				men.write(row, col+6, "N/A")
				men.write(row, col+7, table[g][a][1][b])
				men.write(row, col+8, table[g][a][2][b])
				men.write(row, col+9, table[g][a][3][b])
				row+=1

workbook.close()







