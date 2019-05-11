import re
from AuthorSystem.AuthorMain import AuthorMain
from CtxbotFinal.CtxbotMain import CtxbotMain

from MongoDBConn import MongoDB


class ClassMain:

    @staticmethod
    def CallBothSystems():

        db_expdatasetobj = MongoDB("expdataset")
        ouput_db = MongoDB("431440193640738816")
        collec_candidatedataset = "431440193640738816" # tweetid
        orphanuserid = 137252812
        friendsids = [ d["fids"] for d in db_expdatasetobj.selectCollection("uk_orphauserfriends") ][0]
        collec_utovfav = "431440193640738816_fav"
        collec_vtoufav = "431440193640738816_frndfav"

        query_set = ["MUT Choice D Bronze Mark Sanchez"] #Query
        query_tweetobj = db_expdatasetobj.selectCollection("query")

        #My System
        CtxbotMain.FindContext(db_expdatasetobj,ouput_db,collec_candidatedataset,orphanuserid,friendsids,collec_utovfav,collec_vtoufav,query_set,query_tweetobj)

        #Author System
        AuthorMain.AuthorContext(db_expdatasetobj,ouput_db,collec_candidatedataset)
    #==========================================#==========================================#==========================================#==========================================
    @staticmethod
    def AutomatedExperimentSystems():
        print("Starting Automated Proceess")
        db_expdatasetobj = MongoDB("expdataset")
        query_tweetobj = db_expdatasetobj.selectCollection("query")
        for query in query_tweetobj:
            ouput_db = MongoDB(query["id_str"])
            collec_candidatedataset = query["id_str"] # tweetid
            orphanuserid = 137252812
            friendsids = [ d["fids"] for d in db_expdatasetobj.selectCollection("uk_orphauserfriends") ][0]
            collec_utovfav = query["id_str"] + "_fav"
            collec_vtoufav = query["id_str"] + "_frndfav"
            qtext = ClassMain.ParseQueryText(query["text"])
            query_set = [qtext] #Query

            #My System
            print("Starting my system")
            CtxbotMain.FindContext(db_expdatasetobj,ouput_db,collec_candidatedataset,orphanuserid,friendsids,collec_utovfav,collec_vtoufav,query_set,query_tweetobj)

            #Author System
            print("Starting Author System")
            AuthorMain.AuthorContext(db_expdatasetobj,ouput_db,collec_candidatedataset,query_set,query_tweetobj)
            print("Done", query["id_str"] )
        print("Done- All 20 Expeirments")

    @staticmethod
    def ParseQueryText(text):
        atPattern = re.compile(r'@([A-Za-z0-9_]+)')
        hPattern = re.compile(r'#(\w+)')
        urlPattern = re.compile(r"(http://[^ ]+)")
        notLetterPattern = re.compile('[^A-Za-z0-9]+')
        httpPattern = re.compile("^(http|https)://")
        parsedText = re.sub(httpPattern, "", re.sub(notLetterPattern, " ", re.sub(atPattern, "",
                                                                                      re.sub(hPattern, "",
                                                                                             re.sub(urlPattern, "",
                                                                                                    text))))).strip()
        return parsedText


#ClassMain.AutomatedExperimentSystems()
#ClassMain.CallBothSystems()