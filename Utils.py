from collections import Counter
import pickle
import lucene
import json
from heapq import nlargest

import pickle
from sets import Set

from dateutil.parser import *
from MongoDBConn import MongoDB


class Utils:

    @staticmethod
    def writepickle(self, object, filename):
        with open(filename, 'wb') as handle:
            pickle.dump(object, handle)
    @staticmethod
    def readpickle(self, filename):
        with open(filename, 'rb') as handle:
           return pickle.load(handle)

    @staticmethod
    def generatequerydata():
        mongodbobj = MongoDB("ctxbot")
        expmongo = MongoDB("expdataset")
        tweetcollec = mongodbobj.GetTweetWithOneHashtagNoRetweet("ukcandidateset")
        count = 0
        for tweet in tweetcollec:
            if count ==100:
                break
            else:
                expmongo.insertCollection(tweet,"query")
            count+=1
        print("Done")

    @staticmethod
    def BuildQueryCandidateset(expmongo):
        ctxmongo = MongoDB("ctxbot")
        orphanuserid = 137252812
        querylist =  expmongo.selectCollection("query")
        userids = [ d["fids"] for d in ctxmongo.selectCollection("uk_orphauserfriends") ]

        for query in querylist:
            collectioname = query["id_str"]
            counter = 0
            #Friends tweets
            for uid in userids[0]:
                tweetcollec = expmongo.selectUserTweets(uid,collectioname)
                for tweets in tweetcollec:
                    diffInSeconds = (parse(query["created_at"]) - parse(tweets["created_at"])).total_seconds()
                    if diffInSeconds <= 0:
                        counter+=1
                        expmongo.removeItemFromCollection(tweets, collectioname)
                    elif diffInSeconds > 0:
                        break
            #Orphan user tweets
            tweetcollec = expmongo.selectUserTweets(orphanuserid,collectioname)
            for tweets in tweetcollec:
                diffInSeconds = (parse(query["created_at"]) - parse(tweets["created_at"])).total_seconds()
                if diffInSeconds <= 0:
                    counter+=1
                    expmongo.removeItemFromCollection(tweets, collectioname)
                elif diffInSeconds > 0:
                    break

            print("Total Removed: ", counter)
            print("Done for Query:",query["id"])

        print("Done")

    @staticmethod
    def BuildFavCandidateset():
        expmongo = MongoDB("expdataset")
        querylist =  expmongo.selectCollection("query")

        for query in querylist:
            collectioname = str(query["id"]) + "_fav"
            tweetcollec = expmongo.selectCollection(collectioname)
            for tweets in tweetcollec:
                diffInSeconds = (parse(query["created_at"]) - parse(tweets["created_at"])).total_seconds()
                if diffInSeconds <= 0:
                    expmongo.removeItemFromCollection(tweets, collectioname)
                elif diffInSeconds > 0:
                    break

            print("Done for Query:",query["id"])

        print("Done")

    @staticmethod
    def BuildFrndFavCandidateset():
        expmongo = MongoDB("expdataset")
        querylist =  expmongo.selectCollection("query")
        userids = [ d["fids"] for d in expmongo.selectCollection("uk_orphauserfriends") ]


        for query in querylist:
            collectioname = str(query["id"]) + "_frndfav"
            #tweetcollec = expmongo.selectCollection(collectioname)
            for uid in userids[0]:
                tweetcollec = expmongo.selectFidTweets(uid,collectioname)
                for tweets in tweetcollec:
                    diffInSeconds = (parse(query["created_at"]) - parse(tweets["created_at"])).total_seconds()
                    if diffInSeconds <= 0:
                        expmongo.removeItemFromCollection(tweets, collectioname)
                    elif diffInSeconds > 0:
                        break

            print("Done for Query:",query["id"])

        print("Done")

    @staticmethod
    def PrintResuts():

        expmongo = MongoDB("expdataset")
        querylist =  expmongo.selectCollection("query")
        for query in querylist:
            collectioname = query["id_str"]
            resultdb = MongoDB(collectioname)
            print("MyScore: " , collectioname )
            sorteditems = {d['id']: d['regscore'] for d in resultdb.SortedCollection("author_regscore_final") }

            for k , v in sorteditems.iteritems():
                print k, v

            # results  = {d['id']: d['regscore'] for d in resultdb.selectCollection("uk_reg_score")}
            # for key in nlargest(20, results, key=results.get):
            #     print key
            # print("==========================================")
            print("AuthorScore: " , collectioname )
            # authresults  = {d['id']: d['regscore'] for d in resultdb.selectCollection("author_regscore_final")}
            #
            # t = sorted(authresults.iteritems(), key=lambda x: float(-x[1]))[:20]
            # for x in t:
            #      print "{0}: {1}".format(*x)

            # d = Counter(authresults)
            # d.most_common()
            # for k, v in d.most_common(20):
            #     print '%s: %f' % (k, v)

            # for key in nlargest(20, results, key=authresults.__getitem__):
            #     print key , results[key]
            # print("==========================================")

            #results.sort (cmp=lambda x,y: cmp(x['regscore'],y['regscore']),reverse=True)
        #for x in range(20):




expmongo = MongoDB("expdataset")

Utils.PrintResuts()

# Utils.BuildFrndFavCandidateset()
# Utils.BuildFavCandidateset()
# Utils.BuildQueryCandidateset(expmongo)
#Utils.generatequerydata()