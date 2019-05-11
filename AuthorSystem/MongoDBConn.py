from bson import SON
import pymongo
import re
from bson.code import Code
import json


class MongoDB:
    #Constructor
    def __init__(self, dbname):
        self._conn = pymongo.Connection("localhost", 27017)
        self._db = self._conn[dbname]

    #Create Collection
    def createCollection(self, name=""):
        return self._db[name]

    #Select * Collection
    def selectCollection(self, name=""):
        list = []
        for item in self._db[name].find():
            list.append(item)
        return list

    #Select * Collection
    def selectSimscoreTrending(self, name=""):
        list = []
        for item in self._db[name].find({"simscore" : { "$gt" : 0 }}):
            list.append(item)
        return list


    #Remove item from collection
    def removeCollection(self, removeItem, name=""):
        self._db[name].remove(removeItem)

    #Insert to collection
    def insertCollection(self, additem, name=""):
        self._db[name].insert(additem)

    #Get TweetId from Collection
    def get_AllTweetId(self, name=""):
        ids = []
        for item in self._db[name].find():
            ids.append(item["id"])
        return ids

    #Get Tweettext from Collection
    def get_AllTweettext(self, name=""):
        tweets = []
        for item in self._db[name].find():
            tweets.append(item["text"])
        return tweets

    #Get tweets with Hashtag Candidate TweetSet
    def store_AllCandidateTweetSet(self, name="", newcollec=""):
        for tweet in self._db[name].find({"entities.hashtags.text": {"$exists": 1}}):
            self.insertCollection(tweet, newcollec)

    #Get Tweettextwithout@mentions from Collection
    def get_AllParsedTweeTextAndIdMap(self, name=""):
        finaloutput = {}
        atPattern = re.compile(r'@([A-Za-z0-9_]+)')
        hPattern = re.compile(r'#(\w+)')
        urlPattern = re.compile(r"(http://[^ ]+)")
        notLetterPattern = re.compile('[^A-Za-z0-9]+')
        httpPattern = re.compile("^(http|https)://")
        for item in self._db[name].find():
            tweetMap = []
            id = {}
            twttext = {}
            text = (item["text"])
            parsedText = re.sub(httpPattern, "", re.sub(notLetterPattern, " ", re.sub(atPattern, "",
                                                                                      re.sub(hPattern, "",
                                                                                             re.sub(urlPattern, "",
                                                                                                    text))))).strip()
            finaloutput[item["id"]] = parsedText
        return finaloutput

    #Get Tweettextwithout@mentions from Collection
    def store_AllParsedTweeTextAndIdMap(self, name="", newcollec=""):
        atPattern = re.compile(r'@([A-Za-z0-9_]+)')
        hPattern = re.compile(r'#(\w+)')
        urlPattern = re.compile(r"(http://[^ ]+)")
        notLetterPattern = re.compile('[^A-Za-z0-9]+')
        httpPattern = re.compile("^(http|https)://")
        for item in self._db[name].find():
            dbtweetmap = {}
            twttext = {}
            text = (item["text"])
            parsedText = re.sub(httpPattern, "", re.sub(notLetterPattern, " ", re.sub(atPattern, "",
                                                                                      re.sub(hPattern, "",
                                                                                             re.sub(urlPattern, "",
                                                                                                    text))))).strip()
            dbtweetmap["summary"] = parsedText
            self._db[newcollec].insert(dbtweetmap)


    #Select * Collection
    def queryCollection(self, searchquerydict, name=""):
        list = []
        for item in self._db[name].find(searchquerydict):
            list.append(item)
        return list

