from twython import Twython, TwythonError
from MongoDBConn import MongoDB


class Socialgraph:

    @staticmethod
    def storeOrphanUserFollowers(twitter,mongodbobject,orphanuserid):
         set_orphauserfollowersid = twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"]
         mongodbobject.insertCollection({"id" : orphanuserid,
                                         "fids" : set_orphauserfollowersid},"uk_orphauserfriends")

    @staticmethod
    def getFriendsIdDict(twitter, mongodbobject,orphanuserid):

        friendsinfocollec = {d['id'] : d['info'] for d in mongodbobject.selectCollection("uk_friendsinfo")}

        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])

        for uid in set_orphauserfriendsid:

            biflag = False
            uniFlag = False
            mutualFlag = False
            if uid in friendsinfocollec:
                continue
            else:
                try:
                    followersids = set(twitter.get_friends_ids(id=uid, count=5000)["ids"])
                    commonfollowers = followersids.intersection(set_orphauserfriendsid)

                    if orphanuserid in followersids:
                        biflag = True
                    else:
                        uniFlag =True

                    if len(commonfollowers) > 0:
                        mutualFlag = True

                    mongo_userfollowermap = {"id": uid,
                                              "info" : [{"followersset": list(followersids)},{ "mutual" : list(commonfollowers)}, {"bi" :  biflag}, {"uni" : uniFlag},
                                                        {"mutual" : mutualFlag}, {"followcount" : len(followersids)} , {"mutualcount" : len(commonfollowers)}]
                                             }
                    mongodbobject.insertCollection(mongo_userfollowermap,"uk_friendsinfo")

                except TwythonError as e:
                    print(e)
                    print("Skippping ID %d:", uid)
                    continue


    @staticmethod
    def getFollowersIdDict(twitter, mongodbobject,orphanuserid):

        followersinfocollec = {d['id'] : d['info'] for d in mongodbobject.selectCollection("uk_followersinfo")}

        set_orphauserfollowersid = set(twitter.get_followers_ids(id=orphanuserid, count=5000)["ids"])

        set_orphauserfriendsid = set(twitter.get_friends_ids(id=orphanuserid, count=5000)["ids"])

        for uid in set_orphauserfriendsid:

            mutualFlag = False

            if uid in followersinfocollec:
                continue
            else:
                try:
                    followingids = set(twitter.get_followers_ids(id=uid, count=5000)["ids"])
                    commonfollowing = followingids.intersection(set_orphauserfollowersid)

                    if len(commonfollowing) > 0:
                        mutualFlag = True

                    mongo_userfollowingmap = {"id": uid,
                                              "info" : [{"followingset": list(followingids)},{ "mutual" : list(commonfollowing)},
                                                        {"mutual" : mutualFlag}, {"followingcount" : len(followingids)} , {"mutualcount" : len(commonfollowing)}]
                                             }
                    mongodbobject.insertCollection(mongo_userfollowingmap,"uk_followersinfo")

                except TwythonError as e:
                    print(e)
                    print("Skippping ID %d:", uid)
                    continue

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

#twitter_1 = Twython(APP_KEY_1, APP_SECRET_1, OAUTH_TOKEN_1, OAUTH_TOKEN_SECRET_1)
#twitter_2 = Twython(APP_KEY_2, APP_SECRET_2, OAUTH_TOKEN_2, OAUTH_TOKEN_SECRET_2)
#twitter_3 = Twython(APP_KEY_3, APP_SECRET_3, OAUTH_TOKEN_3, OAUTH_TOKEN_SECRET_3)
#twitter_4 = Twython(APP_KEY_4, APP_SECRET_4, OAUTH_TOKEN_4, OAUTH_TOKEN_SECRET_4)
#twitter_5 = Twython(APP_KEY_5, APP_SECRET_5, OAUTH_TOKEN_5, OAUTH_TOKEN_SECRET_5)
twitter_6 = Twython(APP_KEY_6, APP_SECRET_6, OAUTH_TOKEN_6, OAUTH_TOKEN_SECRET_6)
#twitter_7 = Twython(APP_KEY_7, APP_SECRET_7, OAUTH_TOKEN_7, OAUTH_TOKEN_SECRET_7)
#twitter_8 = Twython(APP_KEY_8, APP_SECRET_8, OAUTH_TOKEN_8, OAUTH_TOKEN_SECRET_8)
#twitter_9 = Twython(APP_KEY_9, APP_SECRET_9, OAUTH_TOKEN_9, OAUTH_TOKEN_SECRET_9)
#twitter_10 = Twython(APP_KEY_10, APP_SECRET_10, OAUTH_TOKEN_10, OAUTH_TOKEN_SECRET_10)
orphanuserid = 137252812

mongodbobj = MongoDB("ctxbot")

#Socialgraph.getFollowersIdDict(twitter_1,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_2,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_3,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_4,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_5,mongodbobj,orphanuserid)
Socialgraph.getFollowersIdDict(twitter_6,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_7,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_8,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_9,mongodbobj,orphanuserid)
#Socialgraph.getFollowersIdDict(twitter_10,mongodbobj,orphanuserid)

# Socialgraph.getFriendsIdDict(twitter_1,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_2,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_3,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_4,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_5,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_6,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_7,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_8,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_9,mongodbobj,orphanuserid)
# Socialgraph.getFriendsIdDict(twitter_10,mongodbobj,orphanuserid)

# #sochi2014
# 157798886 - 3 - Mutual and uni
# 175378976 -  2 - Mutual and uni
# 489429626 - 2 - Bi and mutual - ranked Four
# 50745212 - 3 - Bi and mutual - rank 9
# 73571504 - 4 - uni and mutual -
# 47820222
#382642818

#Socialgraph.storeOrphanUserFollowers(twitter_3,mongodbobj,orphanuserid)
