import pandas as pd
import time
start_time = time.time()
from itertools import chain


andorra_men = ["Irineu Esteve Altimiras"]
argentina_men = ["Franco Dal Farra"]
armenia_men = ["Mikayel Mikayelyan"]
australia_men = ["Philip Bellingham", "Seve De Campo", "Hugo Hinckfuss", "Lars Young Vik"]
austria_men = ["Michael Föttinger", "Benjamin Moser", "Mika Vermeulen"]
belarus_men = ["Yahor Shpuntau", "Aliaksandr Voranau"]
belgium_men = ["Thibaut De Marre"]
bolivia_men = ["Timo Juhani Gronlund"]
bosnia_men = ["Strahinja Eric"]
brazil_men = ["Manex Silva"]
bulgaria_men = ["Simeon Deyanov"]
canada_men = ["Antoine Cyr", "Remi Drolet", "Olivier Leveille", "Graham Ritchie"]
chile_men = ["Yonathan Jesus Fernandez"]
china_men = ["Jincai Shang", "Qiang Wang"]
colombia_men = ["Carlos Andres Quintana"]
croatia_men = ["Marko Skender"]
czech_men = ["Ondrej Cerny", "Adam Fellner", "Petr Knop", "Michal Novak", "Jan Pechousek", "Ludek Seller"]
estonia_men = ["Alvar Johannes Alev", "Martin Himma", "Marko Kilp", "Henri Roos"]
finland_men = ["Ristomatti Hakola", "Perttu Hyvärinen", "Remi Lindholm", "Joni Mäki", "Iivo Niskanen", "Lauri Vuorinen"]
france_men = ["Adrien Backscheider", "Lucas Chanavat", "Renaud Jay", "Richard Jouve", "Hugo Lapalus", "Jules Lapierre", "Maurice Manificat", "Clement Parisse"]
germany_men = ["Lucas Bögl", "Janosch Brugger", "Jonas Dobler", "Albert Kuchler", "Friedrich Moch", "Florian Notz"]
gb_men = ["James Clugnet", "Andrew Musgrave", "Andrew Young"]
greece_men = ["Apostolos Angelis"]
hungary_men = ["Adam Konya"]
iceland_men = ["Snorri Einarsson", "Isak Pedersen"]
ireland_men = ["Thomas Hjalmar Westgård"]
iran_men = ["Seyed Sattar Seyd"]
italy_men = ["Francesco De Fabiani", "Davide Graz", "Federico Pellegrino", "Maicol Rastelli", "Giandomenico Salvadori", "Paolo Ventura"]
japan_men = ["Naoto Baba", "Ryo Hirose", "Hiroyuki Miyazawa", "Haruki Yamashita"]
kazakhstan_men = ["Vitaliy Pukhkalo", "Yevgeniy Velichko"]
korean_men = ["Jong-Won Jeong", "Min-Woo Kim"]
latvia_men = ["Roberts Slotins", "Raimo Vigants"]
lebanon_men = ["Elie Tawk"]
lithuania_men = ["Tautvydas Strolia", "Modestas Vaiciulis"]
mongolia_men = ["Achbadrakh Batmunkh"]
montenegro_men = ["Aleksandar Grbovic"]
nigeria_men = ["Samuel Uduigowme Ikpefan"]
macedonia_men = ["Stavre Jada"]
norway_men = ["Pål Golberg", "Hans Christer Holund", "Emil Iversen", "Johannes Høsflot Klæbo", "Simen Hegstad Krüger", "Sjur Røthe", "Håvard Solås Taugbøl", "Erik Valnes"]
poland_men = ["Dominik Bury", "Kamil Bury", "Mateusz Haratyk", "Maciej Starega"]
portugal_men = ["Jose Cabeca"]
russia_men = ["Alexander Bolshunov", "Alexey Chervotkin", "Artem Maltsev", "Ilia Semikov", "Denis Spitsov", "Aleksandr Terentev", "Sergey Ustiugov", "Ivan Yakimushkin"]
romania_men = ["Paul Constantin Pepene", "Raul Mihai Popa"]
slovakia_men = ["Jan Koristek", "Peter Mlynar"]
slovenia_men = ["Vili Crv", "Janez Lampic", "Miha Licef", "Miha Simenc"]
spain_men = ["Jaume Pueyo", "Imanol Rojo"]
sweden_men = ["Jens Burman", "Marcus Grate", "Johan Häggström", "Calle Halfvarsson", "Leo Johansson", "Anton Persson", "William Poromaa", "Oskar Svensson"]
switzerland_men = ["Jonas Baumann", "Dario Cologna", "Roman Furger", "Valerio Grond", "Jovian Hediger", "Candide Pralong", "Jason Rüesch", "Roman Schaad"]
thailand_men = ["Mark Chanloung"]
turkey_men = ["Yusuf Emre Firat"]
ukraine_men = ["Oleksii Krasovskyi", "Ruslan Perekhoda"]
usa_men = ["Kevin Bolger", "Luke Jager", "Ben Ogden", "Scott Patterson", "James Clinton Schoonmaker", "Gus Schumacher"]

men_names = [andorra_men, argentina_men, armenia_men, australia_men, austria_men,
belarus_men, belgium_men, bolivia_men, bosnia_men, brazil_men,
bulgaria_men, canada_men, chile_men, china_men, colombia_men,
croatia_men, czech_men, estonia_men, finland_men, france_men,
germany_men, gb_men, greece_men, hungary_men, iceland_men,
ireland_men, iran_men, italy_men, japan_men, kazakhstan_men,
korean_men, latvia_men, lebanon_men, lithuania_men, mongolia_men,
montenegro_men, nigeria_men, macedonia_men, norway_men, poland_men,
portugal_men, russia_men, romania_men, slovakia_men, slovenia_men,
spain_men, sweden_men, switzerland_men, thailand_men, turkey_men,
ukraine_men, usa_men]


andorra_ladies = ["Carola Vila Obiols"]
armenia_ladies = ["Katya Galstyan"]
australia_ladies = ["Casey Wright", "Jessica Yeaton"]
austria_ladies = ["Teresa Stadlober", "Lisa Unterweger"]
belarus_ladies = ["Hanna Karaliova", "Anastasia Kirillova"]
bosnia_ladies = ["Sanja Kusmuk"]
brazil_ladies = ["Bruna Moura", "Jaqueline Mourao"]
canada_ladies = ["Dahria Beatty", "Olivia Bouffard-Nesbitt", "Cendrine Browne", "Laura Leclair", "Kathrine Stewart-Jones"]
china_ladies = ["Chunxue Chi", "Bayani Jialin", "Xin Li", "Qinghua Ma", "Dinigeer Yilamujiang"]
croatia_ladies = ["Tena Hadzic", "Vedrana Malec"]
czech_ladies = ["Tereza Beranova", "Zuzana Holikova", "Petra Hyncicova", "Katerina Janatova", "Petra Novakova"]
estonia_ladies = ["Kaidy Kaasiku", "Keidy Kaasiku","Tatjana Mannima", "Mariel Merlii Pulles", "Aveli Uustalu"]
finland_ladies = ["Jasmi Joensuu", "Jasmin Kähärä", "Anne Kyllönen", "Katri Lylynperä", "Johanna Matintalo", "Kerttu Niskanen", "Krista Pärmäkoski"]
france_ladies = ["Coralie Bentz", "Delphine Claudel", "Flora Dolci", "Melissa Gal", "Lena Quintin"]
germany_ladies = ["Victoria Carl", "Pia Fink", "Antonia Fräbel", "Laura Gimmler", "Katharina Hennig", "Sofie Krehl", "Coletta Rydzek", "Katherine Sauerbrey"]
greece_ladies = ["Maria Ntanou", "Nefeli Tita"]
hungary_ladies = ["Sara Ponya"]
iceland_ladies = ["Kristrun Gudnadottir"]
italy_ladies = ["Anna Comarella", "Martina di Centa", "Caterina Ganz", "Greta Laurent", "Cristina Pittin", "Lucia Scardoni"]
japan_ladies = ["Masako Ishida", "Chika Kobayashi", "Miki Kodama", "Masae Tsuchiya"]
kazakhstan_ladies = ["Irina Bykova", "Kseniya Shalygina", "Angelina Shuryga", "Nadezhda Stepashkina", "Valeriya Tyuleneva"]
korea_ladies = ["Da-Som Han", "Chae-Won Lee", "Eui Jin Lee"]
latvia_ladies = ["Kitija Auzina", "Baiba Bendika", "Patricijia Eiduka", "Samanta Krampe", "Estere Volfa"]
liechtenstein_ladies = ["Nina Riedener"]
lithuania_ladies = ["Ieva Dainyte", "Egle Savickaite"]
mongolia_ladies = ["Enkhtuul Ariunsanaa"]
macedonia_ladies = ["Ana Cvetanovska"]
norway_ladies = ["Maiken Caspersen Falla", "Helene Marie Fossesholm", "Therese Johaug", "Anne Kjersti Kalvå", "Mathilde Myhrvold", "Heidi Weng", "Lotta Udnes Weng", "Tiril Udnes Weng"]
poland_ladies = ["Weronika Kaleta", "Magdalena Kobielusz", "Karolina Kukuczka", "Izabela Marcisz", "Monika Skinder"]
russia_ladies = ["Mariya Istomina", "Hristina Matsokina", "Natalia Nepryaeva", "Anastasia Rygalina", "Tatiana Sorina", "Veronika Stepanova", "Yulia Stupak", "Lilia Vasilieva"]
romania_ladies = ["Timea Lorincz"]
slovakia_ladies = ["Barbora Klementova", "Alena Prochazkova"]
slovenia_ladies = ["Anita Klemencic", "Anja Mandeljc", "Eva Urevc", "Anamarija Lampic"]
sweden_ladies = ["Ebba Andersson", "Maja Dahlqvist", "Anna Dyvik", "Charlotte Kalla", "Frida Karlsson", "Moa Olsson", "Emma Ribom", "Jonna Sundling"]
switzerland_ladies = ["Nadine Fähndrich", "Lydia Hiernickel", "Nadja Kälin", "Alina Meier", "Laurien van der Graaff", "Anja Weber"]
turkey_ladies = ["Aysenur Duman"]
ukraine_ladies = ["Maryna Antsybor", "Valentyna Kaminska", "Yuliia Krol", "Viktoriya Olekh"]
usa_ladies = ["Rosie Brennan", "Jessie Diggins", "Hannah Halvorsen", "Julia Kern", "Sophia Laukli", "Novie McCabe", "Caitlin Patterson", "Hailey Swirbul"]

ladies_names = [andorra_ladies, armenia_ladies, australia_ladies, austria_ladies, belarus_ladies,
bosnia_ladies, brazil_ladies, canada_ladies, china_ladies, croatia_ladies,
czech_ladies, estonia_ladies, finland_ladies, france_ladies, germany_ladies,
greece_ladies, hungary_ladies, iceland_ladies, italy_ladies, japan_ladies,
kazakhstan_ladies, korea_ladies, latvia_ladies, liechtenstein_ladies, lithuania_ladies,
mongolia_ladies, macedonia_ladies, norway_ladies, poland_ladies, russia_ladies,
romania_ladies, slovakia_ladies, slovenia_ladies, sweden_ladies, switzerland_ladies,
turkey_ladies, ukraine_ladies, usa_ladies]

def ladies():

	lady_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_all_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
	lady_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
	lady_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varladies_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	lady_radar = pd.read_excel('~/ski/elo/python/ski/radar/lady_values.xlsx', sheet_name="Sheet1", header=0)

	names = list(chain.from_iterable(ladies_names))
	all_names = lady_all_k.name.unique()
	'''for a in range(len(names)):
		if(names[a] not in all_names):
			print(names[a])'''
	lady_all_k = lady_all_k[lady_all_k.name.isin(names)]
	lady_distance_k = lady_distance_k[lady_distance_k.name.isin(names)]
	lady_distance_classic_k = lady_distance_classic_k[lady_distance_classic_k.name.isin(names)]
	lady_distance_freestyle_k = lady_distance_freestyle_k[lady_distance_freestyle_k.name.isin(names)]
	lady_sprint_k = lady_sprint_k[lady_sprint_k.name.isin(names)]
	lady_sprint_classic_k = lady_sprint_classic_k[lady_sprint_classic_k.name.isin(names)]
	lady_sprint_freestyle_k = lady_sprint_freestyle_k[lady_sprint_freestyle_k.name.isin(names)]
	lady_radar = lady_radar[lady_radar.Name.isin(names)]

	#print(names)
	return [lady_all_k, lady_distance_k, lady_distance_classic_k, lady_distance_freestyle_k,
	lady_sprint_k, lady_sprint_classic_k, lady_sprint_freestyle_k, lady_radar]




def men():
	man_all_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_all_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_classic_k.xlsx', sheet_name="Sheet1", header=0)
	man_distance_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_distance_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_classic_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_classic_k.xlsx', sheet_name="Sheet1", header=0)
	man_sprint_freestyle_k = pd.read_excel('~/ski/elo/python/ski/excel365/varmen_sprint_freestyle_k.xlsx', sheet_name="Sheet1", header=0)
	man_radar = pd.read_excel('~/ski/elo/python/ski/radar/man_values.xlsx', sheet_name="Sheet1", header=0)

	names = list(chain.from_iterable(men_names))
	all_names = man_all_k.name.unique()
	for a in range(len(names)):
		if(names[a] not in all_names):
			print(names[a])
	man_all_k = man_all_k[man_all_k.name.isin(names)]
	man_distance_k = man_distance_k[man_distance_k.name.isin(names)]
	man_distance_classic_k = man_distance_classic_k[man_distance_classic_k.name.isin(names)]
	man_distance_freestyle_k = man_distance_freestyle_k[man_distance_freestyle_k.name.isin(names)]
	man_sprint_k = man_sprint_k[man_sprint_k.name.isin(names)]
	man_sprint_classic_k = man_sprint_classic_k[man_sprint_classic_k.name.isin(names)]
	man_sprint_freestyle_k = man_sprint_freestyle_k[man_sprint_freestyle_k.name.isin(names)]
	man_radar = man_radar[man_radar.Name.isin(names)]

	#print(names)
	return [man_all_k, man_distance_k, man_distance_classic_k, man_distance_freestyle_k,
	man_sprint_k, man_sprint_classic_k, man_sprint_freestyle_k, man_radar]


lady_dfs = ladies()
man_dfs = men()

for a in range(len(lady_dfs)):
	if(a==0):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_all_olympics_k.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_all_olympics_k.xlsx")
	elif(a==1):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_distance_olympics_k.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_distance_olympics_k.xlsx")
	elif(a==2):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_distance_classic_olympics_k.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_distance_classic_olympics_k.xlsx")
	elif(a==3):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_distance_freestyle_olympics_k.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_distance_freestyle_olympics_k.xlsx")
	elif(a==4):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_sprint_olympics_k.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_sprint_olympics_k.xlsx")
	elif(a==5):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_sprint_classic_olympics_k.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_sprint_classic_olympics_k.xlsx")
	elif(a==6):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_sprint_freestyle_olympics_k.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_sprint_freestyle_olympics_k.xlsx")	
	elif(a==7):
		lady_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_values_olympics.pkl")
		lady_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/lady_values_olympics.xlsx")	

for a in range(len(man_dfs)):
	if(a==0):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_all_olympics_k.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_all_olympics_k.xlsx")
	elif(a==1):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_distance_olympics_k.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_distance_olympics_k.xlsx")
	elif(a==2):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_distance_classic_olympics_k.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_distance_classic_olympics_k.xlsx")
	elif(a==3):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_distance_freestyle_olympics_k.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_distance_freestyle_olympics_k.xlsx")
	elif(a==4):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_sprint_olympics_k.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_sprint_olympics_k.xlsx")
	elif(a==5):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_sprint_classic_olympics_k.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_sprint_classic_olympics_k.xlsx")
	elif(a==6):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_sprint_freestyle_olympics_k.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_sprint_freestyle_olympics_k.xlsx")	
	elif(a==7):
		man_dfs[a].to_pickle("/Users/syverjohansen/ski/elo/python/ski/olympics/man_values_olympics.pkl")
		man_dfs[a].to_excel("/Users/syverjohansen/ski/elo/python/ski/olympics/man_values_olympics.xlsx")	


print(time.time() - start_time)








