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
#               CONNECTING TO ORACLE AUTONOMOUS DATABASE              #
#######################################################################
def connectToADW(name):
    #Creating a connection
    cur = con.connect(connect_string)
    flag = ""
    cursors = cur.cursor()
    
    #for result in cursors:
    #    print result
    
    #Printing the DB version
    print cur.version 
    
    #Deleting the tweet data
    try:
        cursors.execute('DROP TABLE TweetsData')
        flag = "Table TweetsData deleted"
    except BaseException as de:
        print "Table TweetsData does not exist"
        print "Error while deleting TweetsData table: %s" % str(de)
        flag = flag+"Table TweetsData does not exist"

    # Deleting user data
    try:
        cursors.execute('DROP TABLE UserData')
        flag = flag + ", Table UserData Dropped"
    except BaseException as de:
        print "Table UserData does not exist"
        print "Error while dropping UserData table: %s" % str(de)
        flag = flag + ", Table UserData already deleted"

    
    # Deleting sentiment data
    try:
        cursors.execute('DROP TABLE SENTIMENTS')
        flag = flag + ", Table Sentiments Dropped"
    except BaseException as de:
        print "Table SENTIMENTS does not exist"
        print "Error while dropping the SENTIMENTS table: %s" % str(de)
        flag = flag + ", Table Sentiments already deleted"
    
    # Deleting sentiment data
    try:
        cursors.execute('DROP TABLE TOPTEN')
        flag = flag + ", Table Top Ten Dropped"
    except BaseException as de:
        print "Table TopTen does not exist"
        print "Error while dropping the TopTen table: %s" % str(de)
        flag = flag + ", Table TopTen already deleted"


    # Deleting sentiment data
    try:
        cursors.execute('DROP TABLE NORMAL_TWEETS')
        flag = flag + ", Table NORMAL_TWEETS Dropped"
    except BaseException as de:
        print "Table NORMAL_TWEETS does not exist"
        print "Error while dropping the NORMAL_TWEETS table: %s" % str(de)
        flag = flag + ", Table NORMAL_TWEETS already deleted"
    
    #Closing the connection
    cursors.close()
    cur.close()
    
    return flag

def configure_parameters(file_name):
    config_fp = open(file_name,'r')

    config_par = config_fp.readlines()

    config_params = dict()
    for i in config_par:
        key_value = i.strip('\n').strip(' ').split(":")
        config_params[key_value[0]]=key_value[1]

    connect_string = config_params['connect_string']
    return connect_string


#MAIN FUNCTION
def main():

    #Connect to Oracle Autonomous Database
    message = connectToADW(tablename)

parser = argparse.ArgumentParser(description='Configure the parameters.')
parser.add_argument('config_file', metavar='config_file', help='The file containing the twitter auth tokens and other configuration parameters.')

args = parser.parse_args()

config = args.config_file
connect_string=configure_parameters(config)

main()
