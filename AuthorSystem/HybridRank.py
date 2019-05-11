import math

from AuthorSystem.MongoDBConn import MongoDB


class HybridRanking:

    @staticmethod
    def ComputeFinalRank(ouput_db):
        Simrank =  "author_cossim_tweetid_max"
        SimRecCount =  "author_cossimscoretrending"
        Avgrecencyscore = "author_avgusagescore"
        Minrecencyscore = "author_recencymin_norm"
        Globaltrendng = "author_candidatesettrend_score_rawcount"

        simscore = {d['id']: d['simscore'] for d in ouput_db.selectCollection(Simrank)}
        simreccountscore = {d['id']: d['count'] for d in ouput_db.selectCollection(SimRecCount)}
        avgrecencyscore = {d['id']: d['score'] for d in ouput_db.selectCollection(Avgrecencyscore)}
        minrecencyscore = {d['id']: d['score'] for d in ouput_db.selectCollection(Minrecencyscore)}
        globalrecencyscore = {d['id']: d['score'] for d in ouput_db.selectCollection(Globaltrendng)}

        HybridRanking.SimRecencyCountRank(simscore, minrecencyscore, ouput_db)


    @staticmethod
    def SimRecencyCountRank(simscore, minrecencyscore, ouput_db):
        alpha = 0.6
        outputcollec = "Author_SimRecCountRank_" +  str(alpha)
        for hahstag, recencyscore in minrecencyscore.iteritems():
            regscore = alpha * minrecencyscore.get(hahstag,0.0) + (1-alpha) * simscore.get(hahstag,0.0)
            regressionscoremap = { "id" : hahstag, "regscore" : regscore }
            ouput_db.insertCollection(regressionscoremap,"author_regscore_final")
        print("Done Computing SimRecCount")

    def SimTimeRank(self):
        pass

    def SimPopRank(self):
        pass

