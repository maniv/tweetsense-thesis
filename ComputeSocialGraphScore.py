from twython import Twython, TwythonError
from MongoDBConn import MongoDB


class EntityCrawl:
    @staticmethod
    def ComputeFavCount(twitter, mongodbobj, orphanuserid):
        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])
        orphanfavtweets = mongodbobj.selectCollection("uk_orphan_favs")
        friendsfavtweets = mongodbobj.selectCollection("uk_orphanfriends_favs")
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
            mongodbobj.insertCollection(favcountmap, "uk_favcount_rawscore_utov")

        for uid, favcount in userfavcountmap.iteritems():
            favcountmap = {"vid": uid,
                           "favcount": float(favcount) / globalcount,
                           "uid": orphanuserid
            }
            mongodbobj.insertCollection(favcountmap, "uk_favcount_normscore_utov")

        for uid, favcount in frndfavcountmap.iteritems():
            favcountmap = {"vid": uid,
                           "favcount": favcount,
                           "uid": orphanuserid
            }
            mongodbobj.insertCollection(favcountmap, "uk_favcount_rawscore_votu")

        for uid, favcount in frndfavcountmap.iteritems():
            favcountmap = {"vid": uid,
                           "favcount": float(favcount) / globalfrndfavcount,
                           "uid": orphanuserid
            }
            mongodbobj.insertCollection(favcountmap, "uk_favcount_normscore_vtou")
        print("Done")


    @staticmethod
    def ComputeOrphanUserMentions(twitter, mongodbobj, orphanuserid):
        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])

        atmentiosntweetsofuser = mongodbobj.selectatmentionsforspecificuser(orphanuserid, "ukcandidateset")

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
            mongodbobj.insertCollection(mentionmap, "uk_mentioninfo_raw_utov")

        for vid, mentioncount in atmentionusermap.iteritems():
            mentionmap = {"vid": vid,
                          "mentioncount": mentioncount / globalmentioncount,
                          "uid": orphanuserid}
            mongodbobj.insertCollection(mentionmap, "uk_mentioninfo_norm_utov")


    @staticmethod
    def ComputeFriendUserMentions(twitter, mongodbobj, orphanuserid):
        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])
        atmentionampausermap = {}
        globalmentioncount = 0.0
        for uid in set_orphauserfriendsid:
            atmentiosntweetsofuser = mongodbobj.selectatmentionsforspecificuser(uid, "ukcandidateset")
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
            mongodbobj.insertCollection(mentionmap, "uk_mentioninfo_raw_vtou")

        for vid, mentioncount in atmentionampausermap.iteritems():
            mentionmap = {"vid": vid,
                          "mentioncount": mentioncount / globalmentioncount,
                          "uid": orphanuserid}
            mongodbobj.insertCollection(mentionmap, "uk_mentioninfo_norm_vtou")
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
    def ComputeMutualFriendsNormScore(mongodbobj):
        friendsinfo = mongodbobj.selectmutualcountinfo("uk_friendsinfo")
        mutualsum = 0.0
        for frnd in friendsinfo:
            mutualcount = frnd["info"][6]["mutualcount"]
            mutualsum += mutualcount

        for frnd in friendsinfo:
            score = frnd["info"][6]["mutualcount"] / mutualsum
            mutualfriendmap = {"id": frnd["id"],
                               "mutualscore": score}
            mongodbobj.insertCollection(mutualfriendmap, "uk_mutualfriend_score_norm")

        print("Done")

    @staticmethod
    def ComputeMutualFollowingNormScore(mongodbobj):
        followinfo = mongodbobj.selectmutualcountinfo("uk_followersinfo")
        mutualsum = 0.0
        for frnd in followinfo:
            mutualcount = frnd["info"][4]["mutualcount"]
            mutualsum += mutualcount

        for frnd in followinfo:
            score = frnd["info"][4]["mutualcount"] / mutualsum
            mutualfriendmap = {"id": frnd["id"],
                               "mutualscore": score}
            mongodbobj.insertCollection(mutualfriendmap, "uk_mutualfollowers_score_norm")

        print("Done")

    @staticmethod
    def ComputeMutualFollowingNormScore(mongodbobj):
        followinfo = mongodbobj.selectmutualcountinfo("uk_followersinfo")
        mutualsum = 0.0
        for frnd in followinfo:
            mutualcount = frnd["info"][4]["mutualcount"]
            mutualsum += mutualcount

        for frnd in followinfo:
            score = frnd["info"][4]["mutualcount"] / mutualsum
            mutualfriendmap = {"id": frnd["id"],
                               "mutualscore": score}
            mongodbobj.insertCollection(mutualfriendmap, "uk_mutualfollowers_score_norm")

        print("Done")

    @staticmethod
    def ConstructCommonHashtagScore(twitter, mongodbobj, orphanuserid):
        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])
        orphanusertweets = mongodbobj.selectUserTweets(orphanuserid, "ukcandidateset")
        set_orphanhashtaglist = EntityCrawl.getuniquehashtagsetforauser(orphanusertweets)
        frndtag_map = {}
        commontagset = {}
        commontagcount = {}
        useroriginaltagset = {}
        globalcount = 0.0
        for frndid in set_orphauserfriendsid:
            frndusertweets = mongodbobj.selectUserTweets(frndid, "ukcandidateset")
            set_uniquehashtaglist = EntityCrawl.getuniquehashtagsetforauser(frndusertweets)
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
            mongodbobj.insertCollection(commontagmap, "uk_commontag_scores")
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
    def ComputeFriendScore(mongodbobj):
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
            mongodbobj.insertCollection(scoremap, "uk_friendscore")
        print("Done")


APP_KEY_1 = "pJGu7bueIMz3WM4BzhuC6g"
APP_SECRET_1 = "HbNmWfztTzjGBF6VfR6IsSanjvYbPXBmeZVyPe7BO8w"
OAUTH_TOKEN_1 = "16890316-bo9Ar5XDopYygFA0tPHosWTyZ9f0jempL1dy2peI"
OAUTH_TOKEN_SECRET_1 = "fIpVC7RPq8KQfW142aEr1rBPZnRroT7mqVhImECb4s"

APP_KEY_2 = "ccREBIh5hIOkMpWzXeYQeQ"
APP_SECRET_2 = "7qRJACSiphOfIZlpEfxcS2di5vMNCE5l40E0zrAI"
OAUTH_TOKEN_2 = "16890316-R4rqbEd9xH5eAiWHj5i4IpLuesRvSuO2YQ6fWB5G2"
OAUTH_TOKEN_SECRET_2 = "iRLd4oDQQVtCt86VueJ3G8daHzYMFLr1r14oa5EuM2pCR"

APP_KEY_3 = "nkYxBn24bGJz6snjvMMA"
APP_SECRET_3 = "ApnxeK92mavewD910yzEYeSPsT6c15YHDGzQkFqlXxc"
OAUTH_TOKEN_3 = "16890316-moXLOsVKUgxhgwLCidYIFyhnSvh2g1iEfoLCXQPzf"
OAUTH_TOKEN_SECRET_3 = "u7RRKzYVtlPQiI3vJXHvWgBeqRoCXZvV24UWoX3x6JYAf"

APP_KEY_4 = "Px34tV0ogI9p5FaBIv5UrQ"
APP_SECRET_4 = "zD03Fl4HEetUnoX2WcyKqbxe874mwgWtzK40wOx0QM"
OAUTH_TOKEN_4 = "16890316-vrocohEez5gojkyndXwdd2eZvbbvJTkCYJSjBzqhM"
OAUTH_TOKEN_SECRET_4 = "jLRG7apFlwJ0PNuRdO4o3Aq1jjX39Bj5k5yPtmfHVSnn7"

APP_KEY_5 = "kVPhCQY3GLO9L0MwZ7Cz0w"
APP_SECRET_5 = "EFasci4tGJ6r2MF6KxS2jIRZg0mcQm9p5ZvBNBr2k"
OAUTH_TOKEN_5 = "16890316-5C7aEMfA20aqhocpW4VX1uZBaI7wx3A6xr6Lv8JCl"
OAUTH_TOKEN_SECRET_5 = "eOULuu1BNq6mTb1PMuFHErEm6o4CCgXXmBPzJRIhG2nV3"

APP_KEY_6 = "1ooHnbcKeIWszBTgHoRgQ"
APP_SECRET_6 = "esXszQ7ON7t9qEAKDBDQzjO6OmNvURc7447wHSsyY"
OAUTH_TOKEN_6 = "16890316-agsMAe14foN42utBt40SxrKHoqYvvbUvwhFAXvBXR"
OAUTH_TOKEN_SECRET_6 = "brRn6MwerkezLAlPiokNppP0YRFRsHUhyTi1NI67qbgPw"

APP_KEY_7 = "gKDylzFMa4Zk3TRJLdUXQ"
APP_SECRET_7 = "YPSqoVJnGHIcbxd6vGvSCO6m5evIZxmrfYdV31i6pag"
OAUTH_TOKEN_7 = "16890316-xLwya48GgpEw63yPNa56w83NLKA3xabacQF0NC3Jl"
OAUTH_TOKEN_SECRET_7 = "xLwya48GgpEw63yPNa56w83NLKA3xabacQF0NC3Jl"

APP_KEY_8 = "YWTEhC6oO9VWlvA5mIGjg"
APP_SECRET_8 = "kdmEOMfFGBlR57Rzuhlh3xkWRfH8HWH11QaZ9YuOBWg"
OAUTH_TOKEN_8 = "16890316-vYDuw5rHSk4kQ7a0ySk0AdUvg6QD3zzlBNVpRQhMi"
OAUTH_TOKEN_SECRET_8 = "CVW3IiBknoZH2K7XAF3BSK1GauAry5FO3nKul7qSOyInd"

APP_KEY_9 = "gbywTkKEyDyiUv9ApVJeg"
APP_SECRET_9 = "dkHgsGOnYw0pWYq2AcV1SaaiAKItu3SDB7TR2ukRPGw"
OAUTH_TOKEN_9 = "16890316-F5YsLyxcSuflLwyJtHSCKoYntBUT6ub6VbRXoakjQ"
OAUTH_TOKEN_SECRET_9 = "FgMM6DqGGAc6hCT8kjUbmgnVDOn2pgmE5DlyxvaSw3rRx"

APP_KEY_10 = "dDW6eiSBUZt0OenpBUyKgw"
APP_SECRET_10 = "91QLqrnZsD440jFsf5CLqxJB1I6ucNBSX0nVyZIg1JU"
OAUTH_TOKEN_10 = "16890316-5YVu9ZA8rVaHFtGi8rWEjIj4InCwfzvbG8cFBr0Z3"
OAUTH_TOKEN_SECRET_10 = "6JuJg7t4ZcbdUZA9kiMOUgq6ihSj371r1RKRjov1woUPI"

twitter_1 = Twython(APP_KEY_1, APP_SECRET_1, OAUTH_TOKEN_1, OAUTH_TOKEN_SECRET_1)
#twitter_2 = Twython(APP_KEY_2, APP_SECRET_2, OAUTH_TOKEN_2, OAUTH_TOKEN_SECRET_2)
#twitter_3 = Twython(APP_KEY_3, APP_SECRET_3, OAUTH_TOKEN_3, OAUTH_TOKEN_SECRET_3)
#twitter_4 = Twython(APP_KEY_4, APP_SECRET_4, OAUTH_TOKEN_4, OAUTH_TOKEN_SECRET_4)
#twitter_5 = Twython(APP_KEY_5, APP_SECRET_5, OAUTH_TOKEN_5, OAUTH_TOKEN_SECRET_5)
#twitter_6 = Twython(APP_KEY_6, APP_SECRET_6, OAUTH_TOKEN_6, OAUTH_TOKEN_SECRET_6)
#twitter_8 = Twython(APP_KEY_8, APP_SECRET_8, OAUTH_TOKEN_8, OAUTH_TOKEN_SECRET_8)
#twitter_9 = Twython(APP_KEY_9, APP_SECRET_9, OAUTH_TOKEN_9, OAUTH_TOKEN_SECRET_9)
#twitter_10 = Twython(APP_KEY_10, APP_SECRET_10, OAUTH_TOKEN_10, OAUTH_TOKEN_SECRET_10)

# twitter_7 = Twython(APP_KEY_7, APP_SECRET_7, OAUTH_TOKEN_7, OAUTH_TOKEN_SECRET_7)



mongodbobj = MongoDB("ctxbot")

orphanuserid = 137252812

#EntityCrawl.ComputeFriendScore(mongodbobj)

#EntityCrawl.ComputeRTUtoV(twitter_1,mongodbobj,orphanuserid)

EntityCrawl.ConstructCommonHashtagScore(twitter_1,mongodbobj,orphanuserid)

#EntityCrawl.ComputeMutualFollowingNormScore(mongodbobj)

#EntityCrawl.ComputeMutualFriendsNormScore(mongodbobj)

#EntityCrawl.ComputeOrphanUserMentions(twitter_1,mongodbobj,orphanuserid)

#EntityCrawl.ComputeFavCount(twitter_1,mongodbobj,orphanuserid)
#EntityCrawl.ComputeFavCount(twitter_1,mongodbobj,orphanuserid)

#EntityCrawl.CrawlRT(twitter_1,mongodbobj,orphanuserid)

#EntityCrawl.CrawlFav(twitter_9,mongodbobj,orphanuserid)

#EntityCrawl.CrawlFriendsFav(twitter_1,mongodbobj,orphanuserid)

#EntityCrawl.CrawlUserMentions(twitter_9,mongodbobj,orphanuserid)

#EntityCrawl.CrawlFriendUserMentions(twitter_9,mongodbobj,orphanuserid)

# statusid = 430956118463221760
# uid = 52274819
#
# candidatetweets = mongodbobj.selectReTweetOfUser(orphanuserid, "ukcandidateset")
# rtusrs = twitter_1.get_retweeters_ids(id=statusid)
# rttweets = twitter_1.get_retweets(id=statusid)
#
# print(rtusrs)
# print(rttweets)
# print(candidatetweets)


