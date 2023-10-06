#-------------------------------------------------------
#Modules imported for pulling tweets and market data!
#-------------------------------------------------------
import tweepy
import requests
import pandas as pd
import re

from dateutil import parser
from datetime import timedelta
from datetime import datetime

from nltk.sentiment.vader import SentimentIntensityAnalyzer
#import snscrape.modules.twitter as sntwitter
import nltk
#nltk.download()

#exactly matching with live data
#!pip install yfinance
import yfinance as yf
import datetime as dt

from twarc.client2 import Twarc2
from twarc.expansions import ensure_flattened
from datetime import timezone

#-------------------------------------------------------

no_days=int(input('enter the number of days to pull the market data before and after:'))
print(no_days)
# Create an empty DataFrame
data = {'Tweet': [], 'Created_at': [], 'Positive': [], 'Negative': [], 'Neutral': [], 'Currencies': [],
        'Tweeted_day_Open': 0,'Tweeted_day_High': 0,'Tweeted_day_Low': 0,'Tweeted_day_Close': 0,'Tweeted_day_Volume': 0,
        'Prev_30days_Open': 0,'Prev_30days_High': 0,'Prev_30days_Low': 0,'Prev_30days_Close': 0,'Prev_30days_Volume': 0,
        'Next_7days_Open': 0,'Next_7days_High': 0,'Next_7days_Low': 0,'Next_7days_Close': 0,'Next_7days_Volume': 0,
        'Next_30days_Open': 0,'Next_30days_High': 0,'Next_30days_Low': 0,'Next_30days_Close': 0,'Next_30days_Volume': 0}
if no_days>7:
    data = {'Tweet': [], 'Created_at': [], 'Positive': [], 'Negative': [], 'Neutral': [], 'Currencies': [],
        'Tweeted_day_Open': 0,'Tweeted_day_High': 0,'Tweeted_day_Low': 0,'Tweeted_day_Close': 0,'Tweeted_day_Volume': 0,
        'Prev_30days_Open': 0,'Prev_30days_High': 0,'Prev_30days_Low': 0,'Prev_30days_Close': 0,'Prev_30days_Volume': 0,
        'Next_7days_Open': 0,'Next_7days_High': 0,'Next_7days_Low': 0,'Next_7days_Close': 0,'Next_7days_Volume': 0,
        'Next_30days_Open': 0,'Next_30days_High': 0,'Next_30days_Low': 0,'Next_30days_Close': 0,'Next_30days_Volume': 0,
        'Prev_'+str(no_days)+'days_Open': 0,'Prev_'+str(no_days)+'days_High': 0,'Prev_'+str(no_days)+'days_Low': 0,'Prev_'+str(no_days)+'days_Close': 0,'Prev_'+str(no_days)+'days_Volume': 0,
        'Next_'+str(no_days)+'days_Open': 0,'Next_'+str(no_days)+'days_High': 0,'Next_'+str(no_days)+'days_Low': 0,'Next_'+str(no_days)+'days_Close': 0,'Next_'+str(no_days)+'days_Volume': 0

            }
df = pd.DataFrame(data)
print(df)


def get_market_data(list_currency,dt_created, d_type):  #'2023-07-19T13:16:31.000Z'

    date_time_start = parser.parse(dt_created)
    temp=date_time_start
    #date_time_end=date_time_start+timedelta(days=1)
    
    time_difference = datetime.now(timezone.utc) - date_time_start
    days_difference = time_difference.days

    if d_type=='prev_30days':
        date_time_start = temp - timedelta(days=30) 
        date_time_end = date_time_start+timedelta(days=1)
        
        day_name = date_time_start.strftime("%A")
        if day_name=='Saturday':
            date_time_start=date_time_start+timedelta(days=2)
            date_time_end = date_time_start+timedelta(days=1)
        elif day_name=='Sunday':
            date_time_start=date_time_start+timedelta(days=1)
            date_time_end = date_time_start+timedelta(days=1)
        else:
            print()
            
    elif d_type=='next_7days':
        if days_difference>=7:
            date_time_start = temp + timedelta(days=7)
            date_time_end = date_time_start + timedelta(days=1)
            
            day_name = date_time_start.strftime("%A")
            if day_name=='Saturday':
                date_time_start=date_time_start-timedelta(days=1)
                date_time_end = date_time_start+timedelta(days=1)
            elif day_name=='Sunday':
                date_time_start=date_time_start-timedelta(days=2)
                date_time_end = date_time_start+timedelta(days=1)
            else:
                print()
            
        else:
            date_time_end = temp
            date_time_start = temp
            print()   
    elif d_type=='next_30days':
        if days_difference>=30: 
            date_time_start = temp+timedelta(days=30)
            date_time_end = date_time_start + timedelta(days=1)
            day_name = date_time_start.strftime("%A")
            if day_name=='Saturday':
                date_time_start=date_time_start-timedelta(days=1)
                date_time_end = date_time_start+timedelta(days=1)
            elif day_name=='Sunday':
                date_time_start=date_time_start-timedelta(days=2)
                date_time_end = date_time_start+timedelta(days=1)
            else:
                print()
        else:
            date_time_end = temp
            date_time_start = temp
            print()
            
    elif d_type=='prev_ndays':
        date_time_start = temp-timedelta(days=no_days)
        date_time_end = date_time_start+timedelta(days=1)
        day_name = date_time_start.strftime("%A")
        if day_name=='Saturday':
            date_time_start=date_time_start+timedelta(days=2)
            date_time_end = date_time_start+timedelta(days=1)
        elif day_name=='Sunday':
            date_time_start=date_time_start+timedelta(days=1)
            date_time_end = date_time_start+timedelta(days=1)
        else:
            print()
        
    elif d_type=='next_ndays':
        if days_difference>=no_days:     
            date_time_start = temp+timedelta(days=no_days)
            date_time_end = date_time_start+timedelta(days=1)
            day_name = date_time_start.strftime("%A")
            if day_name=='Saturday':
                date_time_start=date_time_start-timedelta(days=1)
                date_time_end = date_time_start+timedelta(days=1)
            elif day_name=='Sunday':
                date_time_start=date_time_start-timedelta(days=2)
                date_time_end = date_time_start+timedelta(days=1)
            else:
                print()
            
        else:
            date_time_end = temp
            date_time_start = temp
        
    elif d_type=='same_day':
        date_time_start = temp
        date_time_end = temp + timedelta(days=1)
    else:
        print()
    
    ticks=list_currency #['F','AAPL','AMZN','NFLX','GOOG','BTC']
    start = dt.datetime(date_time_start.year,date_time_start.month,date_time_start.day)
    end = dt.datetime(date_time_end.year,date_time_end.month,date_time_end.day)
   # end=temp+timedelta(days=1) #dt.datetime(2023,7,18)
    market_data_dict = {}
    
    for currency in list_currency:
        currency_data = yf.download(currency+"-USD", start, end)  #'BTC-USD'
        print("\nMarket data for the currency-"+currency)
        print(currency_data)
        market_data_dict[currency] = currency_data
    return market_data_dict

# Your bearer token here
t = Twarc2(bearer_token="AAAAAAAAAAAAAAAAAAAAAGmooAEAAAAAQujDMWrR%2Bx58Ya6Vl2TIftvNoYs%3DbH5vIsD4xci2eNHfi3oasY46Y0lxYKUpAAJmkdpKVydnUZxAKr")

# Start and end times must be in UTC
#inputs start time & end time to pull the tweets.
start_time = dt.datetime(2023, 8, 1, 0, 0, 0, 0, dt.timezone.utc)
end_time = dt.datetime(2023, 8, 14, 0, 0, 0, 0, dt.timezone.utc)

# search_results is a generator, max_results is max tweets per page, 500 max for full archive search.
search_results = t.search_all(query="from:CryptoWizardd lang:en -is:retweet has:cashtags", start_time=start_time, end_time=end_time)#, max_results=10)

counter=1
positive=0
negative=0
neutral=0

# Get all results page by page:
for page in search_results:
    # Do something with the whole page of results:
    # print(page)
    # or alternatively, "flatten" results returning 1 tweet at a time, with expansions inline:
    
    for tweet in ensure_flattened(page):
        # Do something with the tweet
        #print(counter)
        #print(tweet['created_at']+"\n"+tweet['text'])
        
        if '$' in tweet['text']:
            analyzer = SentimentIntensityAnalyzer().polarity_scores(tweet['text'])
            negative = analyzer['neg']
            neutral = analyzer['neu']
            positive = analyzer['pos']
            comp = analyzer['compound']
            
            print("Tweet no:"+str(counter))
            print("Date tweeted: "+tweet['created_at']+"\nTweet:\n"+tweet['text'])
            print("\nSentiment analysis on the tweet:")
            print("positive={0}, negative={1}, neutral={2}".format(positive,negative,neutral))
            print("\ncurrencies in the tweet:")
            txt=tweet['text']
            
            lst_currency=[ t for t in txt.split() if t.startswith('$') and not re.search("[0-9]",t)]
            lst_currency_symbol=[(i.strip('$').upper().strip('.').strip(',')) for i in lst_currency]

            unique_currencies = list(set(lst_currency_symbol))
            
            counter=counter+1
            num_currency_symbol=len(unique_currencies)

            market_data = {}
            
            if num_currency_symbol>0:
                print(unique_currencies)
                #market_data=get_market_data(lst_currency_symbol,tweet['created_at'])
                
                market_data1=get_market_data(unique_currencies,tweet['created_at'],'same_day')
                market_data2=get_market_data(unique_currencies,tweet['created_at'],'prev_30days')
                market_data3=get_market_data(unique_currencies,tweet['created_at'],'next_7days')
                market_data4=get_market_data(unique_currencies,tweet['created_at'],'next_30days')
                market_data5=get_market_data(unique_currencies,tweet['created_at'],'prev_ndays')
                market_data6=get_market_data(unique_currencies,tweet['created_at'],'next_ndays')
                
                for cur in unique_currencies:
                    new_row = {'Tweet': tweet['text'], 'Created_at': tweet['created_at'],
               'Positive': positive, 'Negative': negative, 'Neutral': neutral,
               'Currencies': cur, 'Tweeted_day_Open': market_data1[cur]['Open'].iloc[0] if not market_data1[cur]['Open'].empty else 0,
                               'Tweeted_day_High': market_data1[cur]['High'].iloc[0] if not market_data1[cur]['High'].empty else 0,
                               'Tweeted_day_Low': market_data1[cur]['Low'].iloc[0] if not market_data1[cur]['Low'].empty else 0,
                               'Tweeted_day_Close': market_data1[cur]['Close'].iloc[0] if not market_data1[cur]['Close'].empty else 0,
                               'Tweeted_day_Volume': market_data1[cur]['Volume'].iloc[0] if not market_data1[cur]['Volume'].empty else 0,
                               
                               'Prev_30days_Open': market_data2[cur]['Open'].iloc[0] if not market_data2[cur]['Open'].empty else 0,
                               'Prev_30days_High': market_data2[cur]['High'].iloc[0] if not market_data2[cur]['High'].empty else 0,
                               'Prev_30days_Low': market_data2[cur]['Low'].iloc[0] if not market_data2[cur]['Low'].empty else 0,
                               'Prev_30days_Close': market_data2[cur]['Close'].iloc[0] if not market_data2[cur]['Close'].empty else 0,
                               'Prev_30days_Volume': market_data2[cur]['Volume'].iloc[0] if not market_data2[cur]['Volume'].empty else 0,
                               
                               
                               'Next_7days_Open': market_data3[cur]['Open'].iloc[0] if not market_data3[cur]['Open'].empty else 0,
                               'Next_7days_High': market_data3[cur]['High'].iloc[0] if not market_data3[cur]['Open'].empty else 0,
                               'Next_7days_Low': market_data3[cur]['Low'].iloc[0] if not market_data3[cur]['Open'].empty else 0,
                               'Next_7days_Close': market_data3[cur]['Close'].iloc[0] if not market_data3[cur]['Close'].empty else 0,
                               'Next_7days_Volume': market_data3[cur]['Volume'].iloc[0] if not market_data3[cur]['Volume'].empty else 0,
                               
                               'Next_30days_Open': market_data4[cur]['Open'].iloc[0] if not market_data4[cur]['Open'].empty else 0,
                               'Next_30days_High': market_data4[cur]['High'].iloc[0] if not market_data4[cur]['High'].empty else 0,
                               'Next_30days_Low': market_data4[cur]['Low'].iloc[0] if not market_data4[cur]['Low'].empty else 0,
                               'Next_30days_Close': market_data4[cur]['Close'].iloc[0] if not market_data4[cur]['Close'].empty else 0,
                               'Next_30days_Volume': market_data4[cur]['Volume'].iloc[0] if not market_data4[cur]['Volume'].empty else 0,

                               'Prev_'+str(no_days)+'days_Open': market_data5[cur]['Open'].iloc[0] if not market_data5[cur]['Open'].empty else 0,
                               'Prev_'+str(no_days)+'days_High': market_data5[cur]['High'].iloc[0] if not market_data5[cur]['High'].empty else 0,
                               'Prev_'+str(no_days)+'days_Low': market_data5[cur]['Low'].iloc[0] if not market_data5[cur]['Low'].empty else 0,
                               'Prev_'+str(no_days)+'days_Close': market_data5[cur]['Close'].iloc[0] if not market_data5[cur]['Close'].empty else 0,
                               'Prev_'+str(no_days)+'days_Volume': market_data5[cur]['Volume'].iloc[0] if not market_data5[cur]['Volume'].empty else 0,

                               'Next_'+str(no_days)+'days_Open': market_data6[cur]['Open'].iloc[0] if not market_data6[cur]['Open'].empty else 0,
                               'Next_'+str(no_days)+'days_High': market_data6[cur]['High'].iloc[0] if not market_data6[cur]['High'].empty else 0,
                               'Next_'+str(no_days)+'days_Low': market_data6[cur]['Low'].iloc[0] if not market_data6[cur]['Low'].empty else 0,
                               'Next_'+str(no_days)+'days_Close': market_data6[cur]['Close'].iloc[0] if not market_data6[cur]['Close'].empty else 0,
                               'Next_'+str(no_days)+'days_Volume': market_data6[cur]['Volume'].iloc[0] if not market_data6[cur]['Volume'].empty else 0
                               
                               } 
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

                
                print("-------------------------\n")


df.to_csv("market_data_test2.csv")
#print(df)
