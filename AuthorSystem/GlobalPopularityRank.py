

class GlobalPopularityRank:
      @staticmethod
      def computeCandidateSetSocialTrendingScoreNorm(mongodbobject,inputcollec,outputcollec):
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

            maxval = max(dbtweetmap.values())

            for tag, score in  dbtweetmap.iteritems():
                finalnormpopularity  = {"id" : tag,
                                        "score" : score/maxval}
                print(finalnormpopularity)
                mongodbobject.insertCollection(finalnormpopularity, outputcollec)
            print "Done"

      @staticmethod
      def computeCandidateSetSocialTrendingScoreRaw(ouput_db,candidatesetTweetSet):
            totalsum = 0.0
            dbtweetmap = {}
            for item in candidatesetTweetSet:
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
                #print(finalnormpopularity)
                ouput_db.insertCollection(finalnormpopularity, "author_candidatesettrend_score_rawcount")
            print "Done Candidate Trend Score"