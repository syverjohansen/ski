import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import Levenshtein as lev
df0 = pd.read_excel("~/ski/elo/knapsack/fantasy_api.xlsx")

df0 = df0[df0['is_team']!=True]

names = list(df0['name'])

headers = {
    'authority': 'www.google.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    # Add more headers as needed
}
#"https://nitter.no-logs.com/fantasyxc"
url = ["https://nitter.privacydev.net/fantasyxc"]

text = []
skiers = []
df = pd.DataFrame()
for a in range(len(url)):

	response = requests.get(url[a], headers=headers)
	print(response)

	if response.status_code == 200:
	    soup = BeautifulSoup(response.text, "html.parser")
	    tweets = soup.find_all("div", class_="timeline-item")

	    for tweet in tweets:
	        tweet_text = tweet.find("div", class_="tweet-content")
	        tweet_date = tweet.find("span", class_="tweet-date")

	        if tweet_text and tweet_date:
	            tweet_text = tweet_text.get_text(strip=True)
	            tweet_date = tweet_date.a["title"]

	            # Convert the tweet date string to a datetime object
	            tweet_datetime = datetime.strptime(tweet_date, "%b %d, %Y · %I:%M %p %Z")

	            # Check if the tweet falls between November 19, 2023, and November 23, 2023
	            start_date = datetime(2024, 3, 12)
	            end_date = datetime(2024, 3, 15)

	            if start_date <= tweet_datetime <= end_date:
	                print(f"Tweet Date: {tweet_date}")
	                print(f"Tweet Text: {tweet_text}")
	                print("---")
	                text.append(tweet_text)
df['text'] = text
df['text'] = df['text'].str.lower()
df['text'] = df['text'].str.replace('ø', 'oe')
df['text'] = df['text'].str.replace('ä', 'ae')
df['text'] = df['text'].str.replace('æ', 'ae')
df['text']= df['text'].str.replace('ö', 'oe')
df['text']= df['text'].str.replace('ü', 'ue')
df['text']= df['text'].str.replace('å', 'aa')
df = df.reset_index()


data = []

for a in range(len(df['text'])):
	athletes = (df['text'][a].split("\n"))
	for b in range(len(athletes)):
		fuzzz = (process.extractOne(athletes[b], names, scorer=fuzz.token_sort_ratio))
		#print(fuzzz)
		if(fuzzz[1]>=60):
			skier = fuzzz[0]
			athlete = df0.loc[df0['name']==skier]['name'].values[0]

			print(athlete, fuzzz[1])
			athlete_id = df0.loc[df0['name']==skier]['athlete_id'].values[0]
			
			gender = df0.loc[df0['name']==skier]['gender'].values[0]
			country = df0.loc[df0['name']==skier]['country'].values[0]
			data.append([athlete, athlete_id, gender, country])

fuzz_df = pd.DataFrame(data, columns=["name", "athlete_id", "gender", "country"])

fuzz_df.to_excel("~/ski/elo/knapsack/excel365/fuzzy_match.xlsx")




