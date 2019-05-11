import math

from dateutil.parser import *

class RecencyRanks:

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
                    #print(recencymap)
                    mongodbObj.insertCollection(recencymap, collec_outputCollec)

        print("Done")

    @staticmethod
    def computeAuthorAllRecencyScores(ouput_db, query, candidatesetTweetSet ):
        jsontweets = candidatesetTweetSet

        sumscoremap = {}
        minrecencyscoremap = {}
        for tweets in jsontweets:
            diffInSeconds = (parse(query[0]["created_at"]) - parse(tweets["created_at"])).total_seconds()

            if(diffInSeconds>0): # Allow only tweets created before the orphan tweet was created.
                hashtagList = tweets["entities"]["hashtags"]

                for hashtag in hashtagList:
                    recencymap = {"id": hashtag["text"],
                                  "secdiff": diffInSeconds }
                    #print(recencymap)
                    ouput_db.insertCollection(recencymap, "author_recency_raw_secdiff") # Raw Sec Diff diff

                    if hashtag["text"] in minrecencyscoremap:
                        oldscorelist = sumscoremap.get(hashtag["text"])
                        oldscorelist.append(diffInSeconds)
                        sumscoremap[hashtag["text"]] = oldscorelist

                        oldscore = minrecencyscoremap[hashtag["text"]]
                        if oldscore > diffInSeconds:
                            minrecencyscoremap[hashtag["text"]] = diffInSeconds
                    else:
                        minrecencyscoremap[hashtag["text"]] = diffInSeconds
                        scorelist = []
                        scorelist.append(diffInSeconds)
                        sumscoremap[hashtag["text"]] = scorelist

        normsumtotal = sum(minrecencyscoremap.values())
        for key , value in minrecencyscoremap.iteritems():
            recencyminscore = {"id" : key,
                            "score" : value}
            ouput_db.insertCollection(recencyminscore, "author_minmostrecency_score") # min(now - createdat)
            normrecencyscore = {"id" : key,
                                "score" : value/normsumtotal}
            ouput_db.insertCollection(normrecencyscore,"author_recencymin_norm")




        for hashtag, sumscorevals in sumscoremap.iteritems():
            avgscore = sum(sumscorevals) / len(sumscorevals)
            avgscoremap = {"id" : hashtag,
                           "score" : avgscore}
            ouput_db.insertCollection(avgscoremap,"author_avgusagescore") # avg (min - createdat)

        print("Done All recency Score")


