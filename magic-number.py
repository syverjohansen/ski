from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import pandas as pd
import time
import json
from pandas.io.json import json_normalize
pd.options.mode.chained_assignment = None

import time
start_time = time.time()
wc = 1
stage_races = 0
relay = 0
stage =0
tds = 0
races = wc+stage_races+relay+stage+tds


def ladies_magic():
	names = []
	points = []
	leads = []
	magic_numbers = []
	magic_avg = []
	ppr = []
	url = 'https://www.fis-ski.com/DB/cross-country/cup-standings.html?sectorcode=CC&seasoncode=2024&cupcode=WC&disciplinecode=ALL&gendercode=W&nationcode='
	standings = BeautifulSoup(urlopen(url), 'html.parser')
	namessoup = standings.find_all('div', {'g-xs-10 g-sm-9 g-md-4 g-lg-4 justify-left bold align-xs-top'})
	pointssoup = standings.find_all('div', {'pl-xs-1 pl-sm-1 g-xs-10 g-sm-7 g-md-9 g-lg-8 justify-right bold'})
	for a in range(len(namessoup)):
		names.append(namessoup[a].text.strip())
		point = pointssoup[a].text.strip()
		try:
			point = float(point)
			points.append(point)
		except:
			point = point.replace("\'", "")
			point = float(point)
			points.append(point)
	#print(points)
	points_remaining = wc*100+stage_races*50+stage*200+relay*25+400*tds
	print(points_remaining)
	for a in range(len(points)):
		lead = points[0]-points[a]
		if(lead>points_remaining):
			break
		magic_numbers.append(points_remaining-lead)
		magic_avg.append(magic_numbers[a]/(races))
		ppr.append(lead/races)

	names = names[0:len(magic_numbers)]
	points = points[0:len(magic_numbers)]
	magicdf = pd.DataFrame(list(zip(names, points, ppr, magic_numbers, magic_avg)),
		columns=["name", "points", "ppr", "magic number", "magic avg"])
	print(magicdf)
	return magicdf


def men_magic():
	names = []
	points = []
	leads = []
	magic_numbers = []
	magic_avg = []
	ppr = []
	url = 'https://www.fis-ski.com/DB/cross-country/cup-standings.html?sectorcode=CC&seasoncode=2024&cupcode=WC&disciplinecode=ALL&gendercode=M&nationcode='
	standings = BeautifulSoup(urlopen(url), 'html.parser')
	namessoup = standings.find_all('div', {'g-xs-10 g-sm-9 g-md-4 g-lg-4 justify-left bold align-xs-top'})
	pointssoup = standings.find_all('div', {'pl-xs-1 pl-sm-1 g-xs-10 g-sm-7 g-md-9 g-lg-8 justify-right bold'})
	for a in range(len(namessoup)):
		names.append(namessoup[a].text.strip())
		point = pointssoup[a].text.strip()
		try:
			point = float(point)
			points.append(point)
		except:
			point = point.replace("\'", "")
			point = float(point)
			points.append(point)
	#print(points)
	points_remaining = wc*100+stage_races*50+stage*200+relay*25+400*tds
	print(points_remaining)
	for a in range(len(points)):
		lead = points[0]-points[a]
		if(lead>points_remaining):
			break
		magic_numbers.append(points_remaining-lead)
		magic_avg.append(magic_numbers[a]/(races))
		ppr.append(lead/races)

	names = names[0:len(magic_numbers)]
	points = points[0:len(magic_numbers)]
	magicdf = pd.DataFrame(list(zip(names, points, ppr, magic_numbers, magic_avg)), 
		columns=["name", "points", "ppr", "magic number", "magic avg"])
	print(magicdf)
	return magicdf


ladies = ladies_magic()
men = men_magic()

ladies.to_excel("~/ski/excel365/magicladies20240316.xlsx")
men.to_excel("~/ski/excel365/magicmen20240316.xlsx")

