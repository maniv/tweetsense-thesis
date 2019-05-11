

class RecCountRank:
    @staticmethod
    def computeSimScoreTrending(mongodbobject,inputcollec,outputcollec):
        dbsimtrendsmap = {}
        for item in mongodbobject.selectSimscoreTrending(inputcollec):
            hashtag = item["id"]

            if hashtag in dbsimtrendsmap:
                dbsimtrendsmap[hashtag] = dbsimtrendsmap[hashtag] + 1.0
            else:
                dbsimtrendsmap[hashtag] = 1.0

        sumval = sum(dbsimtrendsmap.values())
        for tag, score in  dbsimtrendsmap.iteritems():
            finalpopularity  = {"id" : tag,
                                    "count" : score / sumval}
            mongodbobject.insertCollection(finalpopularity, outputcollec)

        print "Done! SimScore trending Hashtags"

    @staticmethod
    def computeSimScoreTrendingRaw(ouput_db,inputcollec):
        dbsimtrendsmap = {}
        for item in ouput_db.selectSimscoreTrending(inputcollec):
            hashtag = item["id"]

            if hashtag in dbsimtrendsmap:
                dbsimtrendsmap[hashtag] = dbsimtrendsmap[hashtag] + 1.0
            else:
                dbsimtrendsmap[hashtag] = 1.0

        for tag, score in  dbsimtrendsmap.iteritems():
            finalpopularity  = {"id" : tag,
                                    "count" : score }
            ouput_db.insertCollection(finalpopularity, "author_cossimscoretrending")

        print "Done! SimScore trending Hashtags"

    @staticmethod
    def computeSimScoreTrendingNorm(ouput_db,inputcollec):
        dbsimtrendsmap = {}
        for item in ouput_db.selectSimscoreTrending(inputcollec):
            hashtag = item["id"]

            if hashtag in dbsimtrendsmap:
                dbsimtrendsmap[hashtag] = dbsimtrendsmap[hashtag] + 1.0
            else:
                dbsimtrendsmap[hashtag] = 1.0

        sumvalue = sum(dbsimtrendsmap.values())

        for tag, score in  dbsimtrendsmap.iteritems():
            finalpopularity  = {"id" : tag,
                                    "count" : score / sumvalue }
            ouput_db.insertCollection(finalpopularity, "author_cossimscoretrending")

        print "Done! SimScore trending Hashtags"