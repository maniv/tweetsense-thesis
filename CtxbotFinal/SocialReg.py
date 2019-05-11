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

class SocialReg:


    @staticmethod
    def ComputeSocialRegScore(outputdb,friendscore,mutualfriendscore,mutualfollowersscore,utovatmentionscore,vtouatmentionscore,utovfavscore,vtoufavscore,commontagscore):
        w1 = 0.25  #CommonTag
        w2 = 0.20  #Friend Score
        w3 = 0.20  #Mutual Friend Score
        w4 = 0.10  #utov atmentions
        w5 = 0.10  #utov fav
        w6 = 0.05  #vtou mentions
        w7 = 0.05  #vtou fav
        w8 = 0.05  #Mutual Followers Score
        for vid , fscore in friendscore.iteritems():
            regscore = w1 * (commontagscore.get(vid,0.0)) + w2 * (fscore) +  w3 * (mutualfriendscore.get(vid,0.0)) + w4 * (utovatmentionscore.get(vid,0.0)) + w5 * (utovfavscore.get(vid,0.0)) + w6 * (vtouatmentionscore.get(vid,0.0)) + w7 * (vtoufavscore.get(vid,0.0)) + w8 * (mutualfollowersscore.get(vid,0.0))
            userscoremap = {"vid" : vid,
                            "socialscore" : regscore}
            #print(userscoremap)
            outputdb.insertCollection(userscoremap,"uk_socialscore_final")
        print("Done Social Score Regression")

    @staticmethod
    def ComputeHashtagSocialScoreSum(mongodbobject):

        usersocialscoremap = {d['vid']: d['socialscore'] for d in mongodbobject.selectCollection("uk_socialscore_final")}
        usertagmap = {d['vid']: d['originaltagset'] for d in mongodbobject.selectCollection("uk_commontag_scores")}
        #alltagset = [d['id'] for d in mongodbobject.selectCollection("uk_candidiatetrendnorm_score")]
        tagsocialscoremap ={}
        for vid , tagitem in usertagmap.iteritems():
            usersocialscore = usersocialscoremap.get(vid,0.0)
            #Propogate socre to tag
            for tag in tagitem:
                if tag in tagsocialscoremap:
                    oldscore = tagsocialscoremap.get(tag)
                    tagsocialscoremap[tag] = usersocialscore + oldscore
                else:
                    tagsocialscoremap[tag] = usersocialscore

        for tag , score in tagsocialscoremap.iteritems():
            mongoobjmap = {"id" : tag,
                           "score" : score}
            mongodbobject.insertCollection(mongoobjmap,"uk_hashtag_socialgraph_score")
        print("Done HashtagCompute")

    @staticmethod
    def ComputeHashtagSocialScoreMax(mongodbobject):

        usersocialscoremap = {d['vid']: d['socialscore'] for d in mongodbobject.selectCollection("uk_socialscore_final")}
        usertagmap = {d['vid']: d['originaltagset'] for d in mongodbobject.selectCollection("uk_commontag_scores")}
        #alltagset = [d['id'] for d in mongodbobject.selectCollection("uk_candidiatetrendnorm_score")]
        tagsocialscoremap ={}
        for vid , tagitem in usertagmap.iteritems():
            usersocialscore = usersocialscoremap.get(vid,0.0)
            #Propogate score to tag
            for tag in tagitem:
                if tag in tagsocialscoremap:
                    oldscore = tagsocialscoremap.get(tag)
                    if oldscore < usersocialscore:
                        tagsocialscoremap[tag] = usersocialscore
                else:
                    tagsocialscoremap[tag] = usersocialscore

        for tag , score in tagsocialscoremap.iteritems():
            mongoobjmap = {"id" : tag,
                           "score" : score}
            mongodbobject.insertCollection(mongoobjmap,"uk_hashtag_socialgraph_maxscore")
        print("Done HashtagCompute")

    @staticmethod
    def PropogateUserTagScore(outputdb):

        usersocialscoremap = {d['vid']: d['socialscore'] for d in outputdb.selectCollection("uk_socialscore_final")}
        usertagmap = {d['vid']: d['originaltagset'] for d in outputdb.selectCollection("uk_commontag_scores")}
        #alltagset = [d['id'] for d in mongodbobject.selectCollection("uk_candidiatetrendnorm_score")]
        socialscorevalues = usersocialscoremap.values()
        theta = sum(socialscorevalues) / len(socialscorevalues)

        tagsocialscoremap ={}
        tagusersetmap ={}

        for vid , tagitem in usertagmap.iteritems():
            usersocialscore = 0.0
            if vid in usersocialscoremap:
                usersocialscore = usersocialscoremap.get(vid)

            #Propogate score to tag
            for tag in set(tagitem):
                if tag in tagusersetmap:
                    oldlist = tagusersetmap.get(tag)
                    oldlist.append(usersocialscore)
                    tagusersetmap[tag] = oldlist
                else:
                    valset = []
                    valset.append(usersocialscore)
                    tagusersetmap[tag] = valset

        #histogram algo - Auth & Hubs
        for tag , userscoreset in tagusersetmap.iteritems():
            lowset = []
            highset = []
            for val in userscoreset:
                if val <= theta:
                    lowset.append(val)
                elif val > theta:
                    highset.append(val)

            highavgscore = 0.0
            lowavgscore = 0.0
            if len(highset) > 0:
                highavgscore = float(sum(highset) / len(highset))
            if len(lowset) > 0:
                lowavgscore = float(sum(lowset) / len(lowset))

            finalscorefortag = 0.6 * highavgscore + 0.4 * lowavgscore

            tagsocialscoremap[tag] = finalscorefortag

        for tag , score in tagsocialscoremap.iteritems():
            mongoobjmap = {"id" : tag,
                           "score" : score}
            outputdb.insertCollection(mongoobjmap,"uk_hashtag_socialgraph_authhubs")
        print("Done HashtagCompute")

