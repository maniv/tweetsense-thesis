from collections import Set
import datetime
import math
import pymongo
import json
import math

from MongoDBConn import MongoDB

import json
from Utils import Utils
import pickle
from sets import Set

class Result:

    @staticmethod
    def ComputeRegressionModel(ouput_db):

        simscoreCollec =  "tag_cossimscore"
        recencyCollec =  "recency_exposcore_60000"
        candidateTrendCollec = "tagpopularity_norm"
        socialscore = "uk_hashtag_socialgraph_authhubs"


        simscores = {d['id']: d['simscore'] for d in ouput_db.selectCollection(simscoreCollec)}
        recencyscore = {d['id']: d['score'] for d in ouput_db.selectCollection(recencyCollec)}
        candidateTrendingNormscore = {d['id']: d['score'] for d in ouput_db.selectCollection(candidateTrendCollec)}
        socialgraphscoreforhashtagbasedonuser = {d['id']: d['score'] for d in ouput_db.selectCollection(socialscore)}

        alpha = 0.4 #Recency
        beta = 0.3 #Social
        gamma = 0.2 #SimScore
        teta = 0.1  #CandidateTrendind

        for tag , trendscore in candidateTrendingNormscore.iteritems():
            finalrankforhashtag = alpha * recencyscore.get(tag,0.0) + beta * socialgraphscoreforhashtagbasedonuser.get(tag,0.0) + gamma * simscores.get(tag,0.0) + teta * candidateTrendingNormscore.get(tag,0.0)
            regressionscoremap = { "id" : tag,
                                   "regscore" : finalrankforhashtag }
            ouput_db.insertCollection(regressionscoremap,"uk_reg_score")
        print("Done")