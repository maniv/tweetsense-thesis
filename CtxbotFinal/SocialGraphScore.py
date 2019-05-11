from twython import Twython, TwythonError
from CtxbotFinal.SocialReg import SocialReg
from MongoDBConn import MongoDB


class SocialGraphScore:

    @staticmethod
    def ComputeAllSocialGraphScore(mongodbobj,orphanuserid,friendsids,outputdb,collec_candidatedataset,collec_utovfav,collec_vtoufav):



        SocialGraphScore.ComputeOrphanUserMentions(mongodbobj, outputdb, orphanuserid, friendsids, collec_candidatedataset)     #1
        SocialGraphScore.ComputeFriendUserMentions(mongodbobj, outputdb, orphanuserid, friendsids, collec_candidatedataset)     #2
        SocialGraphScore.ComputeFavCount(mongodbobj,outputdb, friendsids, orphanuserid, collec_utovfav, collec_vtoufav)         #3 & 4
        SocialGraphScore.ComputeMutualFollowingNormScore(mongodbobj, outputdb)                                                  #5
        SocialGraphScore.ComputeMutualFriendsNormScore(mongodbobj, outputdb)                                                    #6
        SocialGraphScore.ConstructCommonHashtagScore(mongodbobj, outputdb, friendsids, orphanuserid , collec_candidatedataset)  #7
        SocialGraphScore.ComputeFriendScore(mongodbobj,outputdb)                                                                #8

        collec_friendinfo =  "uk_friendscore" #1
        collec_mutualfriendscore = "uk_mutualfriend_score_norm" #2
        collec_mutualfollowersscore = "uk_mutualfollowers_score_norm" #3
        collec_atmentions_utov = "uk_mentioninfo_norm_utov" #4
        collec_atmentions_vtou = "uk_mentioninfo_norm_vtou" #5
        collec_favcount_utov = "uk_favcount_normscore_utov" #6
        collec_favcount_vtou = "uk_favcount_normscore_vtou" #7
        collec_commonhashtag = "uk_commontag_scores" #8
        collec_rtcount_utov = "" #9
        collec_rtcount_vtou = "" #10

        friendscore = {d['vid']: d['fscore'] for d in outputdb.selectCollection(collec_friendinfo)}
        mutualfriendscore = {d['id']: d['mutualscore'] for d in outputdb.selectCollection(collec_mutualfriendscore)}
        mutualfollowersscore = {d['id']: d['mutualscore'] for d in outputdb.selectCollection(collec_mutualfollowersscore)}
        utovatmentionscore = {d['vid']: d['mentioncount'] for d in outputdb.selectCollection(collec_atmentions_utov)}
        vtouatmentionscore = {d['vid']: d['mentioncount'] for d in outputdb.selectCollection(collec_atmentions_vtou)}
        utovfavscore = {d['vid']: d['favcount'] for d in outputdb.selectCollection(collec_favcount_utov)}
        vtoufavscore = {d['vid']: d['favcount'] for d in outputdb.selectCollection(collec_favcount_vtou)}
        commontagscore = {d['vid']: d['tagnormscore'] for d in outputdb.selectCollection(collec_commonhashtag)}


        SocialReg.ComputeSocialRegScore(outputdb,friendscore,mutualfriendscore,mutualfollowersscore,utovatmentionscore,vtouatmentionscore,utovfavscore,vtoufavscore,commontagscore)
        SocialReg.PropogateUserTagScore(outputdb)

        print("Done- Social Score Computation")


    @staticmethod
    def ComputeFavCount(mongodbobj,outputdb, friendsids, orphanuserid, collec_utovfav, collec_vtoufav):
        set_orphauserfriendsid = set(friendsids)
        orphanfavtweets = mongodbobj.selectCollection(collec_utovfav)
        friendsfavtweets = mongodbobj.selectCollection(collec_vtoufav)

        userfavcountmap = {}
        frndfavcountmap = {}
        globalcount = 0.0
        globalfrndfavcount = 0.0

        #Check for orphan user
        for favtweet in orphanfavtweets:
            favuserid = favtweet["user"]["id"]
            if favuserid in set_orphauserfriendsid:
                if favuserid in userfavcountmap:
                    userfavcountmap[favuserid] = userfavcountmap.get(favuserid) + 1
                    globalcount += 1
                else:
                    userfavcountmap[favuserid] = 1
                    globalcount += 1

        #Check for orphan Friends
        for frndfavtweet in friendsfavtweets:
            frndid = frndfavtweet["fid"]
            if frndfavtweet["user"]["id"] == orphanuserid:
                if frndid in frndfavcountmap:
                    frndfavcountmap[frndid] = frndfavcountmap.get(frndid) + 1
                    globalfrndfavcount += 1
                else:
                    frndfavcountmap[frndid] = 1
                    globalfrndfavcount += 1

        for uid, favcount in userfavcountmap.iteritems():
            favcountmap = {"vid": uid,
                           "favcount": favcount,
                           "uid": orphanuserid
            }
            outputdb.insertCollection(favcountmap, "uk_favcount_rawscore_utov")

        for uid, favcount in userfavcountmap.iteritems():
            favcountmap = {"vid": uid,
                           "favcount": float(favcount) / globalcount,
                           "uid": orphanuserid
            }
            outputdb.insertCollection(favcountmap, "uk_favcount_normscore_utov")

        for uid, favcount in frndfavcountmap.iteritems():
            favcountmap = {"vid": uid,
                           "favcount": favcount,
                           "uid": orphanuserid
            }
            outputdb.insertCollection(favcountmap, "uk_favcount_rawscore_votu")

        for uid, favcount in frndfavcountmap.iteritems():
            favcountmap = {"vid": uid,
                           "favcount": float(favcount) / globalfrndfavcount,
                           "uid": orphanuserid
            }
            outputdb.insertCollection(favcountmap, "uk_favcount_normscore_vtou")
        print("Done")

    @staticmethod
    def ComputeOrphanUserMentions(mongodbobj, outputdb, orphanuserid, friendsids, collec_candidatedataset):
        set_orphauserfriendsid = set(friendsids)

        atmentiosntweetsofuser = mongodbobj.selectatmentionsforspecificuser(orphanuserid, collec_candidatedataset)

        atmentionusermap = {}
        globalmentioncount = 0.0

        for tweet in atmentiosntweetsofuser:
            usercollec = tweet["entities"]["user_mentions"]
            mentionuserids = set([d['id'] for d in usercollec])
            for uid in mentionuserids:
                if uid in set_orphauserfriendsid:
                    if uid in atmentionusermap:
                        atmentionusermap[uid] = atmentionusermap.get(uid) + 1
                        globalmentioncount += 1
                    else:
                        atmentionusermap[uid] = 1
                        globalmentioncount += 1

        for vid, mentioncount in atmentionusermap.iteritems():
            mentionmap = {"vid": vid,
                          "mentioncount": mentioncount,
                          "uid": orphanuserid}
            outputdb.insertCollection(mentionmap, "uk_mentioninfo_raw_utov")

        for vid, mentioncount in atmentionusermap.iteritems():
            mentionmap = {"vid": vid,
                          "mentioncount": mentioncount / globalmentioncount,
                          "uid": orphanuserid}
            outputdb.insertCollection(mentionmap, "uk_mentioninfo_norm_utov")


    @staticmethod
    def ComputeFriendUserMentions(mongodbobj, outputdb, orphanuserid, friendsids, collec_candidatedataset):
        set_orphauserfriendsid = set(friendsids)
        atmentionampausermap = {}
        globalmentioncount = 0.0
        for uid in set_orphauserfriendsid:
            atmentiosntweetsofuser = mongodbobj.selectatmentionsforspecificuser(uid, collec_candidatedataset)
            for tweet in atmentiosntweetsofuser:
                usercollec = tweet["entities"]["user_mentions"]
                mentionuserids = set([d['id'] for d in usercollec])

                for uidobj in mentionuserids:
                    if uidobj == orphanuserid:
                        if uid in atmentionampausermap:
                            atmentionampausermap[uid] = atmentionampausermap.get(uid) + 1
                            globalmentioncount += 1
                        else:
                            atmentionampausermap[uid] = 1
                            globalmentioncount += 1

        for vid, mentioncount in atmentionampausermap.iteritems():
            mentionmap = {"vid": vid,
                          "mentioncount": mentioncount,
                          "uid": orphanuserid}
            outputdb.insertCollection(mentionmap, "uk_mentioninfo_raw_vtou")

        for vid, mentioncount in atmentionampausermap.iteritems():
            mentionmap = {"vid": vid,
                          "mentioncount": mentioncount / globalmentioncount,
                          "uid": orphanuserid}
            outputdb.insertCollection(mentionmap, "uk_mentioninfo_norm_vtou")
        print("Done")

    @staticmethod
    def ComputeRT(twitter, mongodbobj, orphanuserid):
        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])
        rtcheckinfo = [d['id'] for d in mongodbobj.selectCollection("uk_rtinfo")]
        for uid in set_orphauserfriendsid:
            if uid in rtcheckinfo:
                continue
            else:
                try:
                    rttweets = mongodbobj.selectReTweetOfUser(orphanuserid, "ukcandidateset")
                    rtcount = 0
                    rtuserstatusidmap = {}
                    for statusid in rttweets:
                        rtusers = set(twitter.get_retweeters_ids(id=statusid["id"]))
                        rtuserstatusidmap[statusid["id"]] = rtusers
                        if orphanuserid in rtusers:
                            rtcount += 1
                    rtcountmaptoorphanuser = {"id": uid,
                                              "rtcount": rtcount,
                                              "totaltweets": len(rttweets),
                                              "rtusercollec": rtuserstatusidmap}
                    mongodbobj.insertCollection(rtcountmaptoorphanuser, "uk_rtinfo")

                except TwythonError as e:
                    print(e)
                    print("Skippping ID %d:", uid)

    @staticmethod
    def ComputeMutualFriendsNormScore(mongodbobj, outputdb):
        friendsinfo = mongodbobj.selectmutualcountinfo("uk_friendsinfo")
        mutualsum = 0.0
        for frnd in friendsinfo:
            mutualcount = frnd["info"][6]["mutualcount"]
            mutualsum += mutualcount

        for frnd in friendsinfo:
            score = frnd["info"][6]["mutualcount"] / mutualsum
            mutualfriendmap = {"id": frnd["id"],
                               "mutualscore": score}
            outputdb.insertCollection(mutualfriendmap, "uk_mutualfriend_score_norm")

        print("Done")

    @staticmethod
    def ComputeMutualFollowingNormScore(mongodbobj, outputdb):
        followinfo = mongodbobj.selectmutualcountinfo("uk_followersinfo")
        mutualsum = 0.0
        for frnd in followinfo:
            mutualcount = frnd["info"][4]["mutualcount"]
            mutualsum += mutualcount

        for frnd in followinfo:
            score = frnd["info"][4]["mutualcount"] / mutualsum
            mutualfriendmap = {"id": frnd["id"],
                               "mutualscore": score}
            outputdb.insertCollection(mutualfriendmap, "uk_mutualfollowers_score_norm")

        print("Done")

    @staticmethod
    def ConstructCommonHashtagScore(mongodbobj, outputdb, friendsids, orphanuserid , collec_candidatedataset):
        set_orphauserfriendsid = set(friendsids)
        orphanusertweets = mongodbobj.selectUserTweets(orphanuserid, collec_candidatedataset)
        set_orphanhashtaglist = SocialGraphScore.getuniquehashtagsetforauser(orphanusertweets)
        frndtag_map = {}
        commontagset = {}
        commontagcount = {}
        useroriginaltagset = {}
        globalcount = 0.0
        for frndid in set_orphauserfriendsid:
            frndusertweets = mongodbobj.selectUserTweets(frndid, collec_candidatedataset)
            set_uniquehashtaglist = SocialGraphScore.getuniquehashtagsetforauser(frndusertweets)
            frndtag_map[frndid] = set_uniquehashtaglist

        for vid, tagset in frndtag_map.iteritems():
            commonhashtagset = tagset.intersection(set_orphanhashtaglist)
            commontagset[vid] = list(commonhashtagset)
            useroriginaltagset[vid] = list(tagset)
            tcount = len(commonhashtagset)
            commontagcount[vid] = tcount
            globalcount += tcount

        for vid, ctagset in commontagset.iteritems():
            commontagmap = {"uid": orphanuserid,
                            "vid": vid,
                            "originaltagset" : useroriginaltagset[vid],
                            "mutualtagset": commontagset[vid],
                            "tagrawcount": commontagcount[vid],
                            "tagnormscore": commontagcount[vid] / globalcount
            }
            outputdb.insertCollection(commontagmap, "uk_commontag_scores")
        print("Done")

    @staticmethod
    def getuniquehashtagsetforauser(usertweets):
        set_hashtag = []
        for item in usertweets:
            entities = item["entities"]
            hashtag = entities["hashtags"]
            for txt in hashtag:
                set_hashtag.append(txt["text"])
        return set(set_hashtag)

    @staticmethod
    def ComputeRTUtoV(twitter, mongodbobj, orphanuserid):
        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])
        orphanrttweets = mongodbobj.selectReTweetOfUser(orphanuserid, "ukcandidateset")
        usrrtcountmap = {}
        globalrtcount = 0.0
        globalvtoucont = 0.0
        vtoucountmap = {}
        for rt in orphanrttweets:
            rtuserid = rt["retweeted_status"]["user"]["id"]
            if rtuserid in set_orphauserfriendsid:
                if rtuserid in usrrtcountmap:
                    usrrtcountmap[rtuserid] = usrrtcountmap.get(rtuserid) + 1.0
                    globalrtcount += 1.0
                else:
                    usrrtcountmap[rtuserid] = 1.0
                    globalrtcount += 1.0

        for fid in set_orphauserfriendsid:
            frndrttweets = mongodbobj.selectReTweetOfUser(fid, "ukcandidateset")
            for frt in frndrttweets:
                rtuserid = frt["retweeted_status"]["user"]["id"]
                if rtuserid == orphanuserid:
                    if fid in vtoucountmap:
                        vtoucountmap[fid] = vtoucountmap.get(fid) + 1.0
                        globalvtoucont += 1.0
                    else:
                        vtoucountmap[fid] = 1.0
                        globalvtoucont += 1.0

        for vid, count in usrrtcountmap.iteritems():
            rtscoremap = {"vid": vid,
                          "uid": orphanuserid,
                          "rtraw": count,
                          "rtnorm": count / globalrtcount}
            mongodbobj.insertCollection(rtscoremap, "uk_rtscore_utov")

        for vid, count in vtoucountmap.iteritems():
            rtscoremap = {"vid": vid,
                          "uid": orphanuserid,
                          "rtraw": count,
                          "rtnorm": count / globalvtoucont}
            mongodbobj.insertCollection(rtscoremap, "uk_rtscore_vtou")

    @staticmethod
    def ComputeFriendScore(mongodbobj,outputdb):
        friendsinfocollec = {d['id']: d['info'] for d in mongodbobj.selectCollection("uk_friendsinfo")}
        frndscoremap = {}
        for vid, info in friendsinfocollec.iteritems():
            if info[2]["bi"]:
                frndscoremap[vid] = 1.0
            elif info[3]["uni"]:
                frndscoremap[vid] = 0.5
        for vid, score in frndscoremap.iteritems():
            scoremap = {"vid": vid,
                        "fscore": score}
            outputdb.insertCollection(scoremap, "uk_friendscore")
        print("Done")






