from AuthorSystem.RecencyRanks import RecencyRanks
from AuthorSystem.AuthorRecCountRank import RecCountRank
from AuthorSystem.AuthorSimRank import AuthorSimRank
from AuthorSystem.GlobalPopularityRank import GlobalPopularityRank
from AuthorSystem.HybridRank import HybridRanking
from AuthorSystem.MongoDBConn import MongoDB


class AuthorMain:

 #====================================================================
    @staticmethod
    def AuthorContext(db_expdatasetobj,ouput_db,collec_candidatedataset,query_set,query_tweetobj):
        #Declare database
        # db_expdatasetobj = MongoDB("expdataset")
        # ouput_db = MongoDB("430803574512431104")
        # collec_candidatedataset = "430803574512431104"
        # orphanuserid = 137252812
        # friendsids = [ d["fids"] for d in db_expdatasetobj.selectCollection("uk_orphauserfriends") ][0]
        # collec_utovfav = "430803574512431104_fav"
        # collec_vtoufav = "430803574512431104_frndfav"

        candidatesetTweetSet = db_expdatasetobj.selectCollection(collec_candidatedataset)

        #Compute Sim Score
        tweettextmap = AuthorSimRank.get_ParsedTextTweeAndIdMap(candidatesetTweetSet)
        AuthorSimRank.computeCosineSimBasedonTweetId(ouput_db,query_set,tweettextmap,candidatesetTweetSet)

        #RecCountRank - Simscore Popular Hashtag
        inputSimscoreCollec = "author_cossim_tweetid_max"
        RecCountRank.computeSimScoreTrendingRaw(ouput_db,inputSimscoreCollec)

        #GlobalPopularityRank
        GlobalPopularityRank.computeCandidateSetSocialTrendingScoreRaw(ouput_db,candidatesetTweetSet)

        #MostRecencyUsedRank & AvgusageAgeRank - Min Recency Rank and AVg Recency Rank
        RecencyRanks.computeAuthorAllRecencyScores(ouput_db, query_tweetobj, candidatesetTweetSet )

        #Final Rank
        HybridRanking.ComputeFinalRank(ouput_db)
     #====================================================================

#Main.AuthorContext()