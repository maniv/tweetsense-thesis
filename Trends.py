from collections import Set
import datetime
import math
import pymongo

from MongoDBConn import MongoDB

import json
from Utils import Utils
import pickle
from sets import Set

from dateutil.parser import *



class Trends:

    @staticmethod
    def construct_hashtagpopularity_collec_mapreduce(self, name="", newcollec=""):
        count = 0
        for item in self._db[name].find():
            count += 1
            entities = item["entities"]
            hashtag = entities["hashtags"]
            for txt in hashtag:
                dbtweetmap = {}
                print(txt["text"])
                dbtweetmap["summary"] = txt["text"]
                self._db[newcollec].insert(dbtweetmap)
        print("Total Tweets: %d" % count)

    @staticmethod
    def constructRecencyCollection(mongodbObj, collec_query,collec_candidateset,collec_outputCollec ):
        query = mongodbObj.selectCollection(collec_query)
        jsontweets = mongodbObj.selectCollection(collec_candidateset)

        for tweets in jsontweets:
            diffInSeconds = (parse(query[0]["created_at"]) - parse(tweets["created_at"])).total_seconds()

            if(diffInSeconds>=0): # Allow only tweets created beofre the orphan tweet was created.
                hashtagList = tweets["entities"]["hashtags"]
                for hashtag in hashtagList:
                    recencymap = {"id": hashtag["text"],
                                  "secdiff": diffInSeconds }
                    print(recencymap)
                    mongodbObj.insertCollection(recencymap, collec_outputCollec)

        print("Done")


    @staticmethod
    def computeAuthorAllRecencyScores(mongodbObj, collec_query,collec_candidateset,collec_secdiff,collec_minrecencyScore,collec_avgUsageScore ):
        query = mongodbObj.selectCollection(collec_query)
        jsontweets = mongodbObj.selectCollection(collec_candidateset)
        tagcountmap = {d['id'] : d['score'] for d in mongodbobject.selectCollection("uk_candidatesettrend_score_rawcount")}
        sumscoremap = {}
        minrecencyscoremap = {}
        for tweets in jsontweets:
            diffInSeconds = (parse(query[0]["created_at"]) - parse(tweets["created_at"])).total_seconds()

            if(diffInSeconds>=0): # Allow only tweets created before the orphan tweet was created.
                hashtagList = tweets["entities"]["hashtags"]

                for hashtag in hashtagList:
                    recencymap = {"id": hashtag["text"],
                                  "secdiff": diffInSeconds }
                    #print(recencymap)
                    mongodbObj.insertCollection(recencymap, collec_secdiff) # Raw Sec Diff diff

                    if hashtag["text"] in minrecencyscoremap:
                        oldscore = minrecencyscoremap[hashtag["text"]]
                        sumscoremap[hashtag["text"]] = sumscoremap[hashtag["text"]] + diffInSeconds
                        if oldscore > diffInSeconds:
                            minrecencyscoremap[hashtag["text"]] = diffInSeconds
                    else:
                        minrecencyscoremap[hashtag["text"]] = diffInSeconds
                        sumscoremap[hashtag["text"]] = diffInSeconds


        for key , value in minrecencyscoremap.iteritems():
            recencyminscore = {"id" : key,
                            "score" : value}
            mongodbobject.insertCollection(recencyminscore, collec_minrecencyScore) # min(now - createdat)

        for hashtag, sumscore in sumscoremap.iteritems():
            avgscore = sumscore / tagcountmap[hashtag]
            avgscoremap = {"id" : hashtag,
                           "score" : avgscore}
            mongodbobject.insertCollection(avgscoremap,collec_avgUsageScore) # avg (min - createdat)

        print("Done")



    @staticmethod
    def computeRecencyScore(mongodbObj,collec_query,collec_candidateset,collec_outputCollec ):
        query = mongodbObj.selectCollection(collec_query)
        jsontweets = mongodbObj.selectCollection(collec_candidateset)
        scoremap = {}

        for tweets in jsontweets:
            diffInSeconds = (parse(query[0]["created_at"]) - parse(tweets["created_at"])).total_seconds()
            recencymins =  diffInSeconds / 600000  # as per experiment  minutes comparision is best. - 100mins sensitivity
            score = float(10**4 * (math.exp(- recencymins)))

            if(diffInSeconds>=0):
                 hashtagList = tweets["entities"]["hashtags"]
                 for hashtag in hashtagList:
                     if(hashtag["text"] in scoremap):
                        oldvalue = scoremap.get(hashtag["text"])
                        scoremap[hashtag["text"]] = oldvalue + score
                     else:
                        scoremap[hashtag["text"]] = score

        for key , value in scoremap.iteritems():
            recencyscore = {"id" : key,
                            "score" : value}
            mongodbobject.insertCollection(recencyscore, collec_outputCollec)

        print("Done! Computing Recency Score")



    @staticmethod
    def computeRecencyScoreExponentialReadFromSecDiff(mongodbobject,inputcollec,outputcollec):

        hashtagPopularities = mongodbobject.selectCollection(inputcollec)
        hashtagset = Set()
        scoremap = {}
        for hashtagmap in hashtagPopularities:
            hashtagset.add(hashtagmap["id"])
            recencymins = hashtagmap["secdiff"] / 60000 # as per experiment  minutes comparision is best. - 1000mins sensitivity

            score = float(10**4 * (math.exp(- recencymins)))

            if(hashtagmap["id"] in scoremap):
                oldvalue  = scoremap.get(hashtagmap["id"])
                scoremap[hashtagmap["id"]] = oldvalue + score
            else:
                scoremap[hashtagmap["id"]] = score

        for key , value in scoremap.iteritems():
            recencyscore = {"id" : key,
                            "score" : value}
            mongodbobject.insertCollection(recencyscore, outputcollec)

        #Store Unique set of hashtag
        setmap  = { "id" : "*hashtag",
                    "set" : list(hashtagset)}
        mongodbobject.insertCollection(setmap, "hashtagset")
        print("Done")

    @staticmethod
    def computeCandidateSetSocialTrendingScoreNorm(mongodbobject,inputcollec,outputcollec):
        totalsum = 0.0
        dbtweetmap = {}
        globalhashtagcount =0.0
        for item in mongodbobject.selectCollection(inputcollec):
            entities = item["entities"]
            hashtag = entities["hashtags"]

            for txt in hashtag:
                if(txt["text"] in dbtweetmap):
                    dbtweetmap[txt["text"]] = dbtweetmap.get(txt["text"]) +1.0
                    globalhashtagcount+=1.0
                else:
                    dbtweetmap[txt["text"]] = 1.0
                    globalhashtagcount+=1.0

        for tag, score in  dbtweetmap.iteritems():
            finalnormpopularity  = {"id" : tag,
                                    "score" : score/globalhashtagcount}
            mongodbobject.insertCollection(finalnormpopularity, outputcollec)
        print "Done"

    @staticmethod
    def computeCandidateSetSocialTrendingScoreRaw(mongodbobject,inputcollec,outputcollec):
        totalsum = 0.0
        dbtweetmap = {}
        for item in mongodbobject.selectCollection(inputcollec):
            entities = item["entities"]
            hashtag = entities["hashtags"]

            for txt in hashtag:
                if(txt["text"] in dbtweetmap):
                    dbtweetmap[txt["text"]] = dbtweetmap.get(txt["text"]) +1.0
                else:
                    dbtweetmap[txt["text"]] = 1.0

        for tag, score in  dbtweetmap.iteritems():
            finalnormpopularity  = {"id" : tag,
                                    "score" : score}
            print(finalnormpopularity)
            mongodbobject.insertCollection(finalnormpopularity, outputcollec)
        print "Done"

    @staticmethod
    def computeSimScoreTrending(mongodbobject,inputcollec,outputcollec):
        dbsimtrendsmap = {}
        for item in mongodbobject.selectSimscoreTrending(inputcollec):
            hashtag = item["id"]

            if hashtag in dbsimtrendsmap:
                dbsimtrendsmap[hashtag] = dbsimtrendsmap[hashtag] + 1.0
            else:
                dbsimtrendsmap[hashtag] = 1.0

        maxval = max(dbsimtrendsmap.values())
        for tag, score in  dbsimtrendsmap.iteritems():
            finalpopularity  = {"id" : tag,
                                    "count" : score / maxval}
            mongodbobject.insertCollection(finalpopularity, outputcollec)

        print "Done! SimScore trending Hashtags"

    @staticmethod
    def computeSimScoreTrendingRaw(mongodbobject,inputcollec,outputcollec):
        dbsimtrendsmap = {}
        for item in mongodbobject.selectSimscoreTrending(inputcollec):
            hashtag = item["id"]

            if hashtag in dbsimtrendsmap:
                dbsimtrendsmap[hashtag] = dbsimtrendsmap[hashtag] + 1.0
            else:
                dbsimtrendsmap[hashtag] = 1.0

        for tag, score in  dbsimtrendsmap.iteritems():
            finalpopularity  = {"id" : tag,
                                    "count" : score }
            mongodbobject.insertCollection(finalpopularity, outputcollec)

        print "Done! SimScore trending Hashtags"

    @staticmethod
    def NormalizeRecencyExpoScore(mongodbobject,inputcollec, outputcollec):

        hashtagPopularities = {d['id']: d['score'] for d in mongodbobject.selectCollection(inputcollec)}
        scores = hashtagPopularities.values()
        sumscore = sum(scores)
        for id, score in  hashtagPopularities.iteritems():
            normscoremap = {"id" : id,
                            "score" : score/sumscore }
            mongodbobject.insertCollection(normscoremap,outputcollec)


mongodbobject = MongoDB("ctxbot")
inputcollec1 =  "ukcandidateset"
inputcollec2 =  "uk_secdiff_score"
cossimscoretweetid = "author_cossimscore_tweetid"
recencyexpscoreop = "recencyexpo_score_60000"


outputcollec1 =  "author_secdiff_score"
outputcollec2 =  "uk_recencyx_score"
querycollec = "query"
author_cossimscoretrending = "author_cossimscoretrending"
author_cossimscoretrendingraw = "author_cossimscoretrendingraw"
collec_minsecdiff = "author_minmostrecency_score"
collec_avgUsageScore = "author_avgusagescore"

collec_normtrendind = "uk_candidiatetrend_normalizescore"


uk_recencyx_score_60000 = "uk_recencyx_score_60000"
uk_recencyx_60000_normz = "uk_recencyx_60000_normz"


Trends.NormalizeRecencyExpoScore(mongodbobject)

#Trends.computeCandidateSetSocialTrendingScoreNorm(mongodbobject,inputcollec1,collec_normtrendind)

#Trends.computeAuthorAllRecencyScores(mongodbobject,querycollec,inputcollec1,outputcollec1,collec_minsecdiff,collec_avgUsageScore)

# Trends.computeSimScoreTrending(mongodbobject,cossimscoretweetid,author_cossimscoretrending)

# Trends.computeSimScoreTrendingRaw(mongodbobject,cossimscoretweetid,author_cossimscoretrendingraw)

#Trends.computeRecencyScore(mongodbobject,querycollec,inputcollec1,outputcollec2)

#Trends.constructRecencyCollection(mongodbobject,querycollec,inputcollec1,outputcollec1)
#Trends.computeRecencyScoreExponential(mongodbobject,inputcollec2,outputcollec2)

#Trends.computeCandidateSetSocialTrendingScoreRaw(mongodbobject,inputcollec,outputcollec)
#Trends.computeCandidateSetSocialTrendingScore(mongodbobject,inputcollec,outputcollec)
#Trends.parseDateTime()
#Trends.computePopularityExponential(mongodbobject, inputcollec, outputcollec)
#Trends.parseDateTime()


    # db_ctxbot = "ctxbot"
    #
    # collec_ctxbot_uk = "uk"
    # collec_ctxbot_ukcandidateset = "ukcandidateset"
    # collec_ctxbot_uktext = "uk_text"
    # collec_ctxbot_query = "query"
    # collec_ctxbot_recency = "uk_recency"
    # collec_ctxbot_recencynofuture = "uk_recency_nofuture"
    # ctxbot_object = MongoDB(db_ctxbot)


    # db_tfidf = "tfidf"
    # tfidf_object = MongoDB(db_tfidf)
    #
    # collec_tfidf_termfreq = "term_freq"
    # collec_tfidf_docs_text = "docs_text"
    # collec_tfidf_query = "query"
    # collec_tfidf_recency = "hashtag_recency"

    # query = tfidf_object.selectCollection(collec_tfidf_query)
    # jsontweets = tfidf_object.selectCollection(collec_tfidf_docs_text)

# mongodbobject = MongoDB("ctxbot")
# inputcollec =  "uk_recency"
# outputcollec =  "uk_recency_expscore"

# mongodbobject = MongoDB("tfidf")
# inputcollec =  "hashtag_recency"
# outputcollec =  "hashtag_exposcore"
#
# mongodbobject = MongoDB("tfidf")
# inputcollec =  "docs_text"
# outputcollec =  "hashtag_socialtrending"


# scoremap.add({"id": hashtagmap["id"],
            #             "score": float(10**4 * (math.exp(- recencymins)) )
            #             })
