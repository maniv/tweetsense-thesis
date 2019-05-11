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
    def computeRegressionModel( mongodbobject, simscores, recencyscore, candidateTrendingNormscore, socialgraphscoreforhashtagbasedonuser, outputcollec):

        alpha = 0.4 #Recency
        beta = 0.3 #Social
        gamma = 0.2 #SimScore
        teta = 0.1  #CandidateTrendind

        for tag , trendscore in candidateTrendingNormscore.iteritems():
            finalrankforhashtag = alpha * recencyscore.get(tag,0.0) + beta * socialgraphscoreforhashtagbasedonuser.get(tag,0.0) + gamma * simscores.get(tag,0.0) + teta * candidateTrendingNormscore.get(tag,0.0)
            regressionscoremap = { "id" : tag,
                                   "regscore" : finalrankforhashtag }
            mongodbobject.insertCollection(regressionscoremap,outputcollec)
        print("Done")


mongodbobject = MongoDB("ctxbot")
simscoreCollec =  "uk_cossimscore"
recencyCollec =  "uk_recencyx_score_60000"
candidateTrendCollec = "uk_candidiatetrendnorm_score"
socialscore = "uk_hashtag_socialgraph_authhubs"
outputcollec = "uk_reg_score"



simscore = {d['id']: d['simscore'] for d in mongodbobject.selectCollection(simscoreCollec)}
recencyscore = {d['id']: d['score'] for d in mongodbobject.selectCollection(recencyCollec)}
candidateTrends = {d['id']: d['score'] for d in mongodbobject.selectCollection(candidateTrendCollec)}
socialgraphscoreforhashtagbasedonuser = {d['id']: d['score'] for d in mongodbobject.selectCollection(socialscore)}

Result.computeRegressionModel(mongodbobject, simscore,recencyscore,candidateTrends,socialgraphscoreforhashtagbasedonuser,outputcollec)


