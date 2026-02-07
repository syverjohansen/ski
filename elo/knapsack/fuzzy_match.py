import requests
import json
import tweepy
import pandas as pd
from credentials import *
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import Levenshtein as lev
df0 = pd.read_excel("~/ski/elo/knapsack/fantasy_api.xlsx")

df0 = df0[df0['is_team']!=True]

names = list(df0['name'])


bearer_token = "AAAAAAAAAAAAAAAAAAAAANo1ZwEAAAAAUt%2B2R4SIW6GAEVmZK8ws9kt8n64%3DBs8NUmmosiMVEUhGYoehItc2UKkfDRS9yoiCzUuFbSeITQOcLO"
access_token = "918462179820621824-8uvoYoK902HTnxrX1pJidtjQESD8oZ1"
access_token_secret = "hW32uiTLLM3A9pnA3GDXRbJGWhg4Nt1jdcwKJLZ2iZsZP"
consumer_key = "QtmjeIgcHFZzbQSZMqtojjXrL"
consumer_secret = "eqVimFq2CboFxV6wr05u2CHLJtrxHDK7znmyFv9pxGIosFpvuT"




client = tweepy.Client( bearer_token=bearer_token, 
                        consumer_key=consumer_key, 
                        consumer_secret=consumer_secret, 
                        access_token=access_token, 
                        access_token_secret=access_token_secret, 
                        return_type = requests.Response,
                        wait_on_rate_limit=True)
query = 'from:fantasyxc'
tweets = client.search_recent_tweets(query=query, 
                                    tweet_fields=['author_id', 'created_at'],
                                     max_results=100)

tweets_dict = tweets.json()
tweets_data = tweets_dict['data']
df = pd.json_normalize(tweets_data)
print(df)
start_time = '2023-11-19T01:01:01.000Z'
end_time = '2023-11-24T23:52:04.000Z '
df['text'] = df['text'].str.lower()
df['text'] = df['text'].str.replace('ø', 'oe')
df['text'] = df['text'].str.replace('ä', 'ae')
df['text'] = df['text'].str.replace('æ', 'ae')
df['text']= df['text'].str.replace('ö', 'oe')
df['text']= df['text'].str.replace('ü', 'ue')
df['text']= df['text'].str.replace('å', 'aa')
df = df[df['created_at']>=start_time]
df = df.reset_index()
df = df[df['created_at']<=end_time]
df = df.reset_index()

data = []

for a in range(len(df['text'])):
	athletes = (df['text'][a].split("\n"))
	for b in range(len(athletes)):
		fuzzz = (process.extractOne(athletes[b], names, scorer=fuzz.token_sort_ratio))
		#print(fuzzz)
		if(fuzzz[1]>=75):
			skier = fuzzz[0]
			athlete = df0.loc[df0['name']==skier]['name'].values[0]

			print(athlete, fuzzz[1])
			athlete_id = df0.loc[df0['name']==skier]['athlete_id'].values[0]
			
			gender = df0.loc[df0['name']==skier]['gender'].values[0]
			country = df0.loc[df0['name']==skier]['country'].values[0]
			data.append([athlete, athlete_id, gender, country])

fuzz_df = pd.DataFrame(data, columns=["name", "athlete_id", "gender", "country"])

fuzz_df.to_excel("~/ski/elo/knapsack/excel365/fuzzy_match.xlsx")













