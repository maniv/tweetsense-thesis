import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from sklearn.metrics.pairwise import cosine_similarity

import numpy as np
import numpy.linalg as LA
from MongoDBConn import MongoDB

class CosineSim:

    #Compute SimScore for Hashtag and tweet text data structure for my system
    @staticmethod
    def computeCosineSimForTagBasedText(ouput_db,query_set,tweettextmap):

        doc_set = tweettextmap.values()
        stopWords = stopwords.words('english')

        vectorizer = CountVectorizer(stop_words = stopWords)

        docVectorizerArray = vectorizer.fit_transform(doc_set).toarray()
        queryVectorizerArray = vectorizer.transform(query_set).toarray()

        cx = lambda a, b: (np.inner(a, b)/(LA.norm(a)*LA.norm(b)))

        cosscore = []
        for vector in docVectorizerArray:
            #print vector
            for queryV in queryVectorizerArray:
                #print queryV
                cosine = cx(vector, queryV)
                #print cosine
                cosscore.append(cosine)
            #print("=================")

        counter = 0

        for hashtag , document in tweettextmap.iteritems():
            simscoremap = {"id" : hashtag,
                           "simscore" : cosscore[counter]}
            ouput_db.insertCollection(simscoremap,"tag_cossimscore")
            counter+=1

        print ("Total Hashtags SimScore Computed",counter)

    #Get Tweettextwithout@mentions from Collection
    @staticmethod
    def get_AllParsedTweeTextAndHashTagMap(collec_candidatedataset):
        atPattern = re.compile(r'@([A-Za-z0-9_]+)')
        hPattern = re.compile(r'#(\w+)')
        urlPattern = re.compile(r"(http://[^ ]+)")
        notLetterPattern = re.compile('[^A-Za-z0-9]+')
        httpPattern = re.compile("^(http|https)://")
        dbhashtagmap = {}

        for item in collec_candidatedataset:
            entities = item["entities"]
            hashtag = entities["hashtags"]

            text = (item["text"])
            parsedText = re.sub(httpPattern, "", re.sub(notLetterPattern, " ", re.sub(atPattern, "",
                                                                                      re.sub(hPattern, "",
                                                                                             re.sub(urlPattern, "",
                                                                                                    text))))).strip()
            for txt in hashtag:
                if(txt["text"] in dbhashtagmap):
                    testlist = dbhashtagmap.get(txt["text"])
                    testlist.append(" " + parsedText + " ")
                    dbhashtagmap[txt["text"]] = testlist
                else:
                    testlist = []
                    testlist.append(parsedText)
                    dbhashtagmap[txt["text"]] = testlist

        newdbhashtagmap = {}
        for hashtag , list in dbhashtagmap.iteritems():
            newdbhashtagmap[hashtag] = "".join(list)
        print(newdbhashtagmap)

        return newdbhashtagmap

    #========================================Not Used========================================
    @staticmethod
    def computeHashtagSimScoreSummation(mongodbobject,inputcollec,outputcollec):
        totalsum = 0.0
        scoremap = {}
        simscorejson = mongodbobject.selectSimscoreTrending(inputcollec)
        for item in simscorejson:

            if(item["id"] in scoremap):
                oldvalue  = scoremap.get(item["id"])
                scoremap[item["id"]] = oldvalue + item["simscore"]
            else:
                scoremap[item["id"]] = item["simscore"]

        for tag, score in  scoremap.iteritems():
            finalnormpopularity  = {"id" : tag,
                                    "score" : score}
            mongodbobject.insertCollection(finalnormpopularity, outputcollec)
        print "Done"


#====================================================================================================================================================================================================================#

#Author's Approach on Finding SimScore
    @staticmethod
    def computeCosineSimBasedonTweetId(mongodbobject,query_set,tweettextmap,candidatesetTweetSet):

        #-------------------------------- Cosine Sim Algo ----------------------------------#
        doc_set = tweettextmap.values()

        stopWords = stopwords.words('english')

        vectorizer = CountVectorizer(stop_words = stopWords)

        docVectorizerArray = vectorizer.fit_transform(doc_set).toarray()
        queryVectorizerArray = vectorizer.transform(query_set).toarray()

        #print 'Fit Vectorizer to train set', docVectorizerArray
        #print 'Transform Vectorizer to test set', queryVectorizerArray

        #cx = lambda a, b: round(np.inner(a, b)/(LA.norm(a)*LA.norm(b)), 5)
        cx = lambda a, b: (np.inner(a, b)/(LA.norm(a)*LA.norm(b)))

        cosscore = []
        for vector in docVectorizerArray:
            #print vector
            for queryV in queryVectorizerArray:
                #print queryV
                cosine = cx(vector, queryV)
                #print cosine
                cosscore.append(cosine)
            #print("=================")

        counter = 0
        simscoremap = {}
        maxscoremap = {}
        for tweetid , document in tweettextmap.iteritems():
            simscoremap[tweetid] = cosscore[counter]
            counter+=1
        #-------------------------------- Cosine Sim Algo ----------------------------------#

        for tweets in candidatesetTweetSet:
            simscore = simscoremap.get(tweets["id"])

            hashtagList = tweets["entities"]["hashtags"]
            for hashtag in hashtagList:
                tagsimscore = {"id": hashtag["text"],
                               "simscore": simscore}
                mongodbobject.insertCollection(tagsimscore,"")
                if hashtag["text"] in maxscoremap:
                    oldscore = maxscoremap[hashtag["text"]]
                    if oldscore < simscore:
                        maxscoremap[hashtag["text"]] = simscore
                else:
                    maxscoremap[hashtag["text"]] = simscore

        for hashtag, maxscore in maxscoremap.iteritems():
            collecmap = {"id" : hashtag,
                         "maxsimscore" : maxscore}
            mongodbobject.insertCollection(collecmap,"uk_cossim_tweetid_max")


        print "Done"

    #Get Tweettextwithout@mentions from Collection
    @staticmethod
    def get_AllParsedTweeTextAndIdMap(tweetcollec):
        atPattern = re.compile(r'@([A-Za-z0-9_]+)')
        hPattern = re.compile(r'#(\w+)')
        urlPattern = re.compile(r"(http://[^ ]+)")
        notLetterPattern = re.compile('[^A-Za-z0-9]+')
        httpPattern = re.compile("^(http|https)://")
        dbtweetmap = {}
        for item in tweetcollec:
            text = (item["text"])
            parsedText = re.sub(httpPattern, "", re.sub(notLetterPattern, " ", re.sub(atPattern, "",
                                                                                      re.sub(hPattern, "",
                                                                                             re.sub(urlPattern, "",
                                                                                                    text))))).strip()
            dbtweetmap[item["id"]] = parsedText
        return dbtweetmap

    @staticmethod
    def constructMaxSimScoreCollec(mongodbobject,simscorecollec,outputcollec):
        tagsimscoremap =  mongodbobject.selectCollection(simscorecollec)
        maxscoremap = {}
        for hashtag, score in tagsimscoremap.iteritems():
            if hashtag in maxscoremap:
                oldscore = maxscoremap[hashtag]
                if oldscore < score:
                    maxscoremap[hashtag] = score
            else:
                maxscoremap[hashtag] = score
        for hashtag, maxscore in maxscoremap.iteritems():
            collecmap = {"id" : hashtag,
                         "maxsimscore" : maxscore}
            mongodbobject.insertCollection(collecmap,outputcollec)
        print("Done! Finding Max Cosine Sim Score")

    @staticmethod
    def computeSimScoreTrending(mongodbobject,inputcollec,outputcollec):
        scoremap = {}
        simscorejson = mongodbobject.selectSimscoreTrending(inputcollec)
        for item in simscorejson:

            if(item["id"] in scoremap):
                oldvalue  = scoremap.get(item["id"])
                scoremap[item["id"]] = oldvalue + 1.0
            else:
                scoremap[item["id"]] = 1.0

        for tag, score in  scoremap.iteritems():
            finalnormpopularity  = {"id" : tag,
                                    "score" : score}
            mongodbobject.insertCollection(finalnormpopularity, outputcollec)
        print "Done"
