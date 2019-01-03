#######################################################################
#                            IMPORT SECTION                           #
#######################################################################

import cx_Oracle as con
import tweepy as tw
import simplejson as json
from tweepy.streaming import StreamListener
import time
import json
import unicodedata
from time import mktime
from datetime import datetime
import re
from textblob import TextBlob


#######################################################################
#                               LISTENER                              #
#######################################################################


class MyListener(StreamListener):

    def on_data(self, data):
        try:
            #Connecting to ADW
            cur = con.connect('username/password#@<db_name>_low')
            cursors = cur.cursor()
            
            #Saving the tweets as JSON
            with open('ipltweets.json','a') as f:
                f.write(data)

            # Get all the tweet data
            all_data = json.loads(data)
           
             
            #######################################################################
            #              EXTRACTING THE TWEET INFORMATION                       #
            #######################################################################
            #Get the tweet text and convert to ascii from unicode
            tweet = all_data["text"]
            tweet = unicodedata.normalize('NFKD', tweet).encode('ascii','ignore')
            
            #Get the screen name of the tweeter and convert to ascii
            username = all_data["user"]["screen_name"]
	    username = unicodedata.normalize('NFKD', username).encode('ascii','ignore')
            
            #Get the tweet time
            tweet_time = all_data["created_at"]
	    tweet_time = unicodedata.normalize('NFKD', tweet_time).encode('ascii','ignore')
            tweet_time = time.strptime(tweet_time,"%a %b %d %H:%M:%S +0000 %Y")
            tweet_time = datetime.fromtimestamp(mktime(tweet_time))

            #Get the weekday
            tweet_weekday = tweet_time.strftime('%A')

            #Get the retweets
            retweeted = all_data['retweeted']
 
            # Get the source 
            source = all_data['source']
            if source!=None:
                source = source.encode('utf-8')

            #Get the Location of the user
            location = all_data['user']['location']
            if location!=None:
                location = location.encode('utf-8')
            else:
                location = '0'

            #Get the place attribute
            place = all_data['place']
            if place!=None:
                place = all_data['place']['full_name']
                if place!=None:
                  place = place.encode('utf-8')
                else:
                  place = all_data['place']
            else:
                place = '0'
            
            #Get the Number of Retweets
            retweet_count = all_data['retweet_count']
            
            #Insert the extracted data into ADW
            cursors.execute('INSERT INTO TWEETSDATA (TIME, USERNAME, TWEET, TWEET_TIME, RETWEETED, SOURCE, RETWEET_COUNT, PLACE, TWEET_WEEKDAY, LOCATION) VALUES (SYSTIMESTAMP,:2,:3,:4,:5,:6,:7,:8,:9,:10)',{"2":str(username),"3":str(tweet),"4":tweet_time,"5":retweeted,"6":str(source),"7":int(retweet_count),"8":str(place),"9":tweet_weekday, "10":location})

            #######################################################################
            #               EXTRACTING THE TWEETER INFORMATION                    #
            #######################################################################
             
            # Get the tweeter username
            name = all_data["user"]["name"]
            name = unicodedata.normalize('NFKD', name).encode('ascii','ignore')
    
            #Get the time when tweeter joined twitter
            user_join_time = all_data["user"]["created_at"]
            user_join_time = unicodedata.normalize('NFKD', user_join_time).encode('ascii','ignore')
            user_join_time = time.strptime(user_join_time,"%a %b %d %H:%M:%S +0000 %Y")
            user_join_time = datetime.fromtimestamp(mktime(user_join_time))
    
            #Get the day of the week when user joined
            joining_weekday = user_join_time.strftime('%A')
            #print tweet_time
       
            
            verified = all_data["user"]["verified"]
    
            statuses_count = all_data["user"]["statuses_count"]
            
            friends_count = all_data["user"]["friends_count"]
    
            followers_count = all_data["user"]["followers_count"]
    
            following = all_data["user"]["following"]
    
            geo_enabled = all_data["user"]["geo_enabled"]
      
            language = all_data["user"]["lang"]
            if language!=None:
                language = language.encode('utf-8')
    
            location = all_data["user"]["location"]
            if location!=None:
                location = location.encode('utf-8')
    
            time_zone = all_data["user"]["time_zone"]
            if time_zone!=None:
                time_zone = time_zone.encode('utf-8')
            
            favourites_count = all_data["user"]["favourites_count"]

            #Insert the data into the table
            cursors.execute('INSERT INTO USERDATA (user_name, account_created, name, verified, statuses_count , friends_count, followers_count, following, geo_enabled, language, location, 
            time_zone, favourites_count, joining_weekday) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)',{"1":str(username),"2":user_join_time,"3":str(name),"4":str(verified),
            "5":int(statuses_count),"6":int(friends_count),"7":int(followers_count),"8":str(following),"9":str(geo_enabled),"10":str(language),"11":str(location),"12":str(time_zone),
            "13":int(favourites_count),"14":joining_weekday})  

            #######################################################################
            #               CALCULATING THE TWEET SENTIMENTS                      #
            #######################################################################
            cleaned_tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(tweet)).split())

            sentiment = ""            

            #Calculating the sentiment of the Tweet
            sentiment = TextBlob(cleaned_tweet)
            if sentiment.sentiment.polarity > 0:
                sentiment='positive'
            elif sentiment.sentiment.polarity == 0:
                sentiment='neutral'
            else:
                sentiment='negative'
            
            # Inserting the tweets sentiment into ADW
            cursors.execute('INSERT INTO SENTIMENTS (TWEET, SENTIMENT) VALUES (:2,:3)',{"2":str(cleaned_tweet),"3":str(sentiment)})
            cur.commit()
            cursors.close()
            cur.close()
            return True
        except BaseException as e:
            print "Error on_data: %s" % str(e)
        return True

    def on_error(self, status):
        print(status)
        return True


#######################################################################
#               CONNECTING TO ORACLE AUTONOMOUS DATABASE              #
#######################################################################
def connectToADW(name):
    #Creating a connection
    cur = con.connect('username/password#@<db_name>_low')
    flag = ""
    cursors = cur.cursor()
    
    #for result in cursors:
    #    print result
    
    #Printing the DB version
    print cur.version 
    
    #Creating a table for storing tweets in the database
    try:
        cursors.execute('CREATE TABLE TweetsData (tweet_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) PRIMARY KEY, time TIMESTAMP, username VARCHAR2(100), tweet VARCHAR2(300), tweet_time TIMESTAMP, retweeted VARCHAR2(20), source VARCHAR2(1000), retweet_count NUMBER(38), place VARCHAR2(500), tweet_weekday VARCHAR2(15), location VARCHAR2(1000))')
        flag = "Table 1 Created"
    except BaseException as de:
        print "DB Name already taken, please select a different name"
        print "Error while creating the table: %s" % str(de)
        flag = flag+"Table 1 already exists"

    # Creating a table for storing user data
    try:
        cursors.execute('CREATE TABLE UserData (user_name VARCHAR2(70), account_created TIMESTAMP, name VARCHAR2(100), verified VARCHAR2(5), statuses_count NUMBER(38), friends_count NUMBER(38), followers_count NUMBER(38), following VARCHAR2(5), geo_enabled VARCHAR2(5), language VARCHAR2(40), location VARCHAR2(1000), time_zone VARCHAR2(80), favourites_count NUMBER(38), joining_weekday VARCHAR(15))')
        flag = flag + ", Table1 Created"
    except BaseException as de:
        print "DB Name already taken, please select a different name"
        print "Error while creating the table: %s" % str(de)
        flag = flag + ", Table 2 already exists"

    #Closing the connection
    cursors.close()
    cur.close()
    
    return flag


#######################################################################
#               SOME API'S TO EXTRACT USER SPECIFIC INFORMATION       #
#######################################################################
def setAccess(cons_key,cons_sec,access_tok,access_sec):
    auth  = tw.OAuthHandler(cons_key, cons_sec)
    auth.set_access_token(access_tok, access_sec)
    return auth

def connectToTwitter(auth):
    api = tw.API(auth)
    return api

def getTimelineInfo(api):    
    for status in tw.Cursor(api.home_timeline).items(10):
        #Process a single status
        print(status.text)
        #process_or_store(status._json)

def getFriends(api):

    print "My friend List"
    for friend in tw.Cursor(api.friends).items():
        print friend.screen_name
        #process_or_store(friend._json)


def getTweets(api):
  
    print "My Tweets"
    for tweets in tw.Cursor(api.user_timeline).items():
        #print(tweets)
        process_or_store(tweets._json)
    

def process_or_store(tweet):
    for key, value in tweet.iteritems():
        print key, value
    #print(json.dumps(tweet))

def streamData(auth, term):
    #twitter_stream = tw.Stream(auth, MyListener())
    twitter_stream = tw.Stream(auth, MyListener())
    twitter_stream.filter(track=[term])


#MAIN FUNCTION
def main():

    #Authentication and Authorization tokens for twitter application
    consumer_key = '<consumer_key>'
    consumer_secret = '<consumer_secret>'

    access_token = '<access_token>'
    access_secret = '<access_secret>'


    #Configuration
    tablename = 'TweetData'
    auth = setAccess(consumer_key,consumer_secret,access_token,access_secret)
    
    #Connect to Twitter API
    #api = connectToTwitter(auth)
    
    #Connect to Oracle Autonomous Database
    message = connectToADW(tablename)
    
    #Get most 10 recent tweets from my feed
    #getTimelineInfo(api)
    #Get the list of all the followers
    #getFriends(api)
    # Get the list of all of my tweets
    #getTweets(api)
   
    # Streaming Tweets
    streamData(auth, 'ipl')
    
    
main()
