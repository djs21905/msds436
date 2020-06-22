import tweepy
import pandas as pd

# Using Twitter API to form tabular data set of n = 50

 
# Connect to Twitter API 
consumer_key = ''
consumer_secret = ''
access_key= ''
access_secret = ''

#pass twitter credentials to tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

search_words = 'to:realDonaldTrump'
data_since = "2020-05-1"

tweets = tweepy.Cursor(api.search,
              q=search_words,
              lang="en",
              since=data_since).items(50)

# Past 50 tweets to @Trump
r = [[tweet.text,tweet.user.location,tweet.user.screen_name,tweet.place] for tweet in tweets]

table = pd.DataFrame(data=r, 
                    columns=['Text', "User Location","Screen Name","Coordinates"])


print(table)