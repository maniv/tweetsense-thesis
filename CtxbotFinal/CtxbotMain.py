from CtxbotFinal.FinalRank import Result
from CtxbotFinal.SocialGraphScore import SocialGraphScore
from CtxbotFinal.MongoDBConn import MongoDB
from CtxbotFinal.RecencyScore import Trends
from CtxbotFinal.SimScore import CosineSim


class CtxbotMain:

#====================================================================
    @staticmethod
    def FindContext(db_expdatasetobj,ouput_db,collec_candidatedataset,orphanuserid,friendsids,collec_utovfav,collec_vtoufav,query_set,query_tweetobj):
        # #Declare database
        # db_expdatasetobj = MongoDB("expdataset")
        # ouput_db = MongoDB("430803574512431104")
        # collec_candidatedataset = "430803574512431104"
        # orphanuserid = 137252812
        # friendsids = [ d["fids"] for d in db_expdatasetobj.selectCollection("uk_orphauserfriends") ][0]
        # collec_utovfav = "430803574512431104_fav"
        # collec_vtoufav = "430803574512431104_frndfav"

        candidatesetTweetSet = db_expdatasetobj.selectCollection(collec_candidatedataset)

        #Compute Sim Score
        tweettextmap = CosineSim.get_AllParsedTweeTextAndHashTagMap(candidatesetTweetSet)
        CosineSim.computeCosineSimForTagBasedText(ouput_db,query_set,tweettextmap)

        #Compute Recency Expo Smoothing Score
        Trends.computeRecencyScore(ouput_db,query_tweetobj,candidatesetTweetSet)

        #Hashtag Popularity
        Trends.computeCandidateSetSocialTrendingScoreNorm(ouput_db,candidatesetTweetSet)

        #SocialGraphScore
        SocialGraphScore.ComputeAllSocialGraphScore(db_expdatasetobj,orphanuserid,friendsids,ouput_db,collec_candidatedataset,collec_utovfav,collec_vtoufav)

        #Final Search Ranking
        Result.ComputeRegressionModel(ouput_db)


#Main.FindContext()

#====================================================================
#
# #-----------------------Given---------------------------------------:
# orphantweet = "I would love a go at Luge Or Ski Jump Looks awesome"
# orphanuser = "DeeCeeGee89"
# #-----------------------Given---------------------------------------:
# orphantweet = "Donachie suspended for striking a player When you thought those in charge at Newcastle couldn't get any worse"
# orphanuser = "DeeCeeGee89"
# #-----------------------Given---------------------------------------:
 #query_set = ["Ok Not gonna lie I'm pleased with this"] #Query
 # #-----------------------Given---------------------------------------:
#query_set = ["MUT Choice D Bronze Mark Sanchez"] #Query
# #-----------------------Given---------------------------------------: