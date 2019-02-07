#######################################################################
#                            IMPORT SECTION                           #
#######################################################################

import argparse
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
#from nltk.tokenize import word_tokenize
from collections import Counter
import nltk
#nltk.download('stopwords')
from nltk.corpus import stopwords
import string
import nltk
from collections import defaultdict
import operator
import ast
#######################################################################
#                               LISTENER STARTS                       #
#######################################################################


class MyListener(StreamListener):

    def on_data(self, data):
        try:
            #Connecting to ADW
            cur = con.connect(connect_string)
            cursors = cur.cursor()
            
            #Saving the tweets as JSON
            with open('ipltweets.json','a') as f:
                f.write(data)

            # Get all the tweet data
            all_data = json.loads(data)
            
            #Creating a Counter to calculate term frequencies
            #hash_freq = Counter()
            #term_freq = Counter()
            #bigrams_freq = Counter()
             
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
                location = 'Unknown'

            #Get the place attribute
            place = all_data['place']
            if place!=None:
                place = all_data['place']['full_name']
                if place!=None:
                  place = place.encode('utf-8')
                else:
                  place = all_data['place']
            else:
                place = 'Unknown'
            
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
            cursors.execute('INSERT INTO USERDATA (user_name, account_created, name, verified, statuses_count , friends_count, followers_count, following, geo_enabled, language, location, time_zone, favourites_count, joining_weekday) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)',{"1":str(username),"2":user_join_time,"3":str(name),"4":str(verified),"5":int(statuses_count),"6":int(friends_count),"7":int(followers_count),"8":str(following),"9":str(geo_enabled),"10":str(language),"11":str(location),"12":str(time_zone),"13":int(favourites_count),"14":joining_weekday})  

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

            normal_tweet = tweet
            tweet_set = set(tweet.split(" "))
            matches = list(tweet_set.intersection(curse_words))
            for term in matches:
                normal_tweet = normal_tweet.replace(term,mask_char*len(term))
            
            mid_tweet = normal_tweet
            tweet_set_racial = set(mid_tweet.split(" "))
            matches = list(tweet_set_racial.intersection(curse_words))
            for term in matches:
                normal_tweet = normal_tweet.replace(term,mask_char*len(term))
            cursors.execute('INSERT INTO NORMAL_TWEETS (NORMAL_TWEETS, SENTIMENT, TWEET_TIME, LOCATION) VALUES (:2,:3,:4,:5)',{"2":str(normal_tweet),"3":str(sentiment),"4":tweet_time,"5":location})

            flag_debug = 0
            try:
                #Pre-Processing the tweet
                punctuation = list(string.punctuation)
                stop_words = stopwords.words('english')+punctuation+['rt','via']
                #print "1",tweet,"-----",preprocess(tweet)
                flag_debug = 1
                hash_tags = [term for term in preprocess(tweet) if term.startswith('#')]
                #print "2",tweet
                flag_debug = 11
                tweet_terms = [term for term in preprocess(tweet) if term not in stop_words and not term.startswith(('#','@'))]
                
                #print "3",tweet
                flag_debug = 111
                
                
                #Updating the tweet term and hashtag frequencies after every tweet is processed
                hash_freq.update(hash_tags)
                term_freq.update(tweet_terms)
                #Build co-occurrence matrix
                for i in range(len(tweet_terms)-1):
                    for j in range(i+1, len(tweet_terms)):
                        w1,w2 = sorted([tweet_terms[i], tweet_terms[j]])
                        if w1 != w2:
                            com[w1][w2] += 1
                flag_debug = 2
                
                for term1 in com:
                    term1_tt = sorted(com[term1].items(), key=operator.itemgetter(1), reverse=True)[:5]
                    for term2,term2_count in term1_tt:
                        key = term1.strip(" ")+" , "+term2.strip(" ")
                        com_topten[key] = term2_count
        
                flag_debug = 3
                #com_tt = str(sorted(com_topten, key=operator.itemgetter(1), reverse=True)[:5]).strip("[]")
                com_tt = dict(sorted(com_topten.items(), key=operator.itemgetter(1), reverse=True)[:10])
                flag_debug = 4
                '''topTen_CoM = ""
                for i in com_tt:
                    topTen_CoM.append()                
                '''
                flag_debug = 5
                term_bigrams = nltk.bigrams(tweet_terms)
                bigrams_freq.update(term_bigrams)
                
                flag_debug = 6
                # Getting Top Ten stats
                #print "Top ten hashtags-",hash_freq.most_common(5)
                #print "Top ten bigrams-",term_freq.most_common(5)
                #print "Top ten terms-",bigrams_freq.most_common(5)
                top_ten_hashtags = str(hash_freq.most_common(10)).strip("[]")
                top_ten_terms = str(term_freq.most_common(10)).strip("[]")
                top_ten_bigrams = str(bigrams_freq.most_common(10)).strip("[]")
                flag_debug = 7
                #print "Top ten hashtags",top_ten_hashtags
                #print "Top ten bigrams",top_ten_bigrams
                #print "Top ten terms",top_ten_terms
                #print "Top Ten Co Terms",str(com_tt.items())
                #Uploading the Top Ten Statistics
                flag_debug = 8
                cursors.execute('UPDATE TOPTEN SET VALUES_TT = :2 WHERE STAT_NAME=:1',{"1":'Hashtag',"2":top_ten_hashtags})
                cursors.execute('UPDATE TOPTEN SET VALUES_TT = :2 WHERE STAT_NAME=:1',{"1":'Term',"2":top_ten_terms})
                cursors.execute('UPDATE TOPTEN SET VALUES_TT = :2 WHERE STAT_NAME=:1',{"1":'Bigram',"2":top_ten_bigrams})
                cursors.execute('UPDATE TOPTEN SET VALUES_TT = :2 WHERE STAT_NAME=:1',{"1":'Co-occurring Terms',"2":str(com_tt.items())})
                
            except BaseException as e:
                print "Error while pre-processing the tweet: %s" % str(e)
                print "Flag Debug : %s" % str(flag_debug)
                                
                                


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
#                               LISTENER ENDS                         #
#######################################################################

#######################################################################
#      CONNECTING AND CONFIGURING ORACLE AUTONOMOUS DATABASE          #
#######################################################################
def connectToADW(name):
    #Creating a connection
    cur = con.connect(connect_string)
    flag = ""
    cursors = cur.cursor()
    
    #for result in cursors:
    #    print result
    
    #Printing the DB version
    print "Database Version : ",cur.version 
    
    #Creating a table for storing tweets in the database
    try:
        cursors.execute('CREATE TABLE TweetsData (tweet_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) PRIMARY KEY, time TIMESTAMP, username VARCHAR2(100), tweet VARCHAR2(300), tweet_time TIMESTAMP, retweeted VARCHAR2(20), source VARCHAR2(1000), retweet_count NUMBER(38), place VARCHAR2(500), tweet_weekday VARCHAR2(15), location VARCHAR2(1000))')
        flag = "Table TweetsData Created"
    except BaseException as de:
        #print "DB Name already taken, please select a different name"
        #print "Error while creating the table: %s" % str(de)
        print "Table TweetsData already created"
        flag = flag+"Table TweetsData already exists"

    # Creating a table for storing user data
    try:
        cursors.execute('CREATE TABLE UserData (user_name VARCHAR2(70), account_created TIMESTAMP, name VARCHAR2(100), verified VARCHAR2(5), statuses_count NUMBER(38), friends_count NUMBER(38), followers_count NUMBER(38), following VARCHAR2(5), geo_enabled VARCHAR2(5), language VARCHAR2(40), location VARCHAR2(1000), time_zone VARCHAR2(80), favourites_count NUMBER(38), joining_weekday VARCHAR(15))')
        flag = flag + ", \nTable UserData Created"
    except BaseException as de:
        #print "DB Name already taken, please select a different name"
        #print "Error while creating the table: %s" % str(de)
        print "Table USerData already created"
        flag = flag + ", \nTable UserData already exists"

    # Creating a table for storing sentiment data
    try:
        cursors.execute('CREATE TABLE sentiments (tweet VARCHAR2(300), sentiment VARCHAR2(100))')
        flag = flag + ", \nTable Sentiments Created"
    except BaseException as de:
        #print "DB Name already taken, please select a different name"
        #print "Error while creating the table: %s" % str(de)
        print "Table Sentiments already created"
        flag = flag + ", \nTable Sentiments already exists"

    # Creating a table for storing statistics data
    try:
        cursors.execute('CREATE TABLE TopTen (stat_name VARCHAR2(70),values_tt VARCHAR2(1000))')
        flag = flag + ", \nTable TopTen Created"
    except BaseException as de:
        #print "DB Name already taken, please select a different name"
        #print "Error while creating the table: %s" % str(de)
        print "Table TopTen already created"
        flag = flag + ", \nTable TopTen already exists"

    # Creating a table for storing normalized tweet data
    try:
        cursors.execute('CREATE TABLE NORMAL_TWEETS (normal_tweets VARCHAR2(300), sentiment VARCHAR2(100), tweet_time TIMESTAMP, location VARCHAR2(1000))')
        flag = flag + ", \nTable Normal Tweets Created"
    except BaseException as de:
        #print "DB Name already taken, please select a different name"
        #print "Error while creating the table: %s" % str(de)
        print "Table Normal Tweets already created"
        flag = flag + ", \nTable Normal Tweets already exists"
            
    # Inserting rows with stat names in table for storing statistics data
    try:
        cursors.execute("SELECT COUNT(*) FROM TOPTEN")
        numberOfRows = int(cursors.fetchone()[0])
        #print numberOfRows
        if numberOfRows == 0:
            cursors.execute("INSERT INTO TOPTEN (STAT_NAME, VALUES_TT) VALUES ('Term','T')")
            cursors.execute("INSERT INTO TOPTEN (STAT_NAME, VALUES_TT) VALUES ('Hashtag','H')")
            cursors.execute("INSERT INTO TOPTEN (STAT_NAME, VALUES_TT) VALUES ('Bigram','B')")
            cursors.execute("INSERT INTO TOPTEN (STAT_NAME, VALUES_TT) VALUES ('Co-occurring Terms','CT')")
            flag = flag + ",Rows inserted in Table TopTen"
        else:
            cursors.execute('SELECT VALUES_TT FROM TOPTEN WHERE STAT_NAME=:1',{"1":'Term'})
            terms = cursors.fetchall()[0][0]
            terms = ast.literal_eval("["+terms+"]")
            print terms
            for i in terms:
                if term_freq[i[0]] != None:
                    term_freq[i[0]] += int(i[1])
                else:
                    term_freq[i[0]] = int(i[1])
            cursors.execute('SELECT VALUES_TT FROM TOPTEN WHERE STAT_NAME=:1',{"1":'Hashtag'})
            hash = cursors.fetchall()[0][0]
            hash = ast.literal_eval("["+hash+"]")
            print hash
            for i in hash:
                if hash_freq[i[0]] != None:
                    hash_freq[i[0]] += int(i[1])
                else:
                    hash_freq[i[0]] = int(i[1])
            cursors.execute('SELECT VALUES_TT FROM TOPTEN WHERE STAT_NAME=:1',{"1":'Bigram'})
            big = cursors.fetchall()[0][0]
            big = ast.literal_eval("["+big+"]")
            print big
            for i in big:
                if bigrams_freq[i[0]] != None:
                    bigrams_freq[i[0]] += int(i[1])
                else:
                    bigrams_freq[i[0]] = int(i[1])
            cursors.execute('SELECT VALUES_TT FROM TOPTEN WHERE STAT_NAME=:1',{"1":'Co-occurring Terms'})
            cot = cursors.fetchall()[0][0]
            cot = ast.literal_eval(cot)
            print cot
            for i in cot:
                terms = i[0].split(",")
                if com[terms[0].strip()][terms[1].strip()] != None:
                    com[terms[0].strip()][terms[1].strip()] += int(i[1])
                else:
                    com[terms[0].strip()][terms[1].strip()] = int(i[1])
            flag = flag + ",\nRows already inserted in Table TopTen"
            
    except BaseException as de:
        print "Error while inserting values into the tables: %s" % str(de)
    cur.commit()
    #Closing the connection
    cursors.close()
    cur.close()
    
    return flag



#######################################################################
#                          TWEET PROCESSING API's                     #
#######################################################################

def tokenizeTweets(tw):
    emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""
 
    regex_str = [
        emoticons_str,
        r'<[^>]+>', # HTML tags
        r'(?:@[\w_]+)', # @-mentions
        r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
        r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
 
        r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
        r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
        r'(?:[\w_]+)', # other words
        r'(?:\S)' # anything else
    ]

    tokens_regex = re.compile(r'('+'|'.join(regex_str)+')',re.VERBOSE | re.IGNORECASE)
    emoticon_regex = re.compile(r'^'+emoticons_str+'$',re.VERBOSE | re.IGNORECASE)
    return tokens_regex.findall(tw)
    
def preprocess(tweet, lowercase=False):
    tokens = tokenizeTweets(tweet)
    if lowercase:
        tokens = [tokens if emoticon_re.search(token) else token.lower() for token in tokens]  
    return tokens

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

def configure_parameters(file_name):
    config_fp = open(file_name,'r')

    config_par = config_fp.readlines()

    config_params = dict()
    for i in config_par:
        key_value = i.strip('\n').split(":")
        config_params[key_value[0]]=key_value[1]

    key = config_params['keyword']
    twitter_cons_key = config_params['consumer_key']
    twitter_cons_sec = config_params['consumer_secret']
    twitter_access_tok = config_params['access_token']
    twitter_access_sec = config_params['access_secret']
    connect_string = config_params['connect_string']
    curse_words_dict = config_params['curse_words_dict']
    racial_slurs_dict = config_params['racial_slurs_dict']

    curse_fp = open(curse_words_dict,'r')
    curse_words = set(ast.literal_eval(curse_fp.read()))

    racial_fp = open(racial_slurs_dict,'r')
    racial_words = set(ast.literal_eval(racial_fp.read()))
    return key,twitter_cons_key,twitter_cons_sec,twitter_access_tok,twitter_access_sec,connect_string,curse_words_dict,racial_slurs_dict



#MAIN FUNCTION
def main():

    #Authentication and Authorization tokens for twitter application
    consumer_key = consumer_key_tw
    consumer_secret = consumer_secret_tw

    access_token = access_token_tw
    access_secret = access_secret_tw

    
    
    auth = setAccess(consumer_key,consumer_secret,access_token,access_secret)
    
    
    #Connect to Twitter API
    #api = connectToTwitter(auth)
    
    #Connect to Oracle Autonomous Database
    message = connectToADW(tablename)
   
    # Streaming Tweets
    streamData(auth, keyword)


parser = argparse.ArgumentParser(description='Configure the parameters.')
parser.add_argument('config_file', metavar='config_file', help='The file containing the twitter auth tokens and other configuration parameters.')

args = parser.parse_args()

config = args.config_file
keyword,consumer_key_tw,consumer_secret_tw,access_token_tw,access_secret_tw,connect_string,curse_words,racial_slurs=configure_parameters(config)

#Creating a Counter to calculate term frequencies
hash_freq = Counter()
term_freq = Counter()
bigrams_freq = Counter()
com = defaultdict(lambda : defaultdict(int))
com_topten=dict()

#Configuration
mask_char = '*'
tablename = 'TweetData'

main()
