import re
import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import numpy.linalg as LA


class AuthorSimRank:

     def __init__(self):
         pass

     @staticmethod
     def get_ParsedTextTweeAndIdMap(candidatesetTweetSet): # returns tweet-id map
        atPattern = re.compile(r'@([A-Za-z0-9_]+)')
        hPattern = re.compile(r'#(\w+)')
        urlPattern = re.compile(r"(http://[^ ]+)")
        notLetterPattern = re.compile('[^A-Za-z0-9]+')
        httpPattern = re.compile("^(http|https)://")
        dbtweetmap = {}
        for item in candidatesetTweetSet:
            text = (item["text"])
            parsedText = re.sub(httpPattern, "", re.sub(notLetterPattern, " ", re.sub(atPattern, "",
                                                                                      re.sub(hPattern, "",
                                                                                             re.sub(urlPattern, "",
                                                                                                    text))))).strip()
            dbtweetmap[item["id"]] = parsedText
        return dbtweetmap

     @staticmethod
     def computeCosineSimBasedonTweetId(ouput_db,query_set,tweettextmap,candidatesetTweetSet): #Stores  Simscore for Hashtag affliated to single tweet #Stores max sim score for unique set of hashtags(SimRank)

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

        for tweets in candidatesetTweetSet:
            simscore = simscoremap.get(tweets["id"])

            hashtagList = tweets["entities"]["hashtags"]
            for hashtag in hashtagList:
                tagsimscore = {"id": hashtag["text"],
                               "rawsimscore": simscore}
                ouput_db.insertCollection(tagsimscore,"author_cossim_tweetid_raw") #All Sim Score

                if hashtag["text"] in maxscoremap:
                    oldscore = maxscoremap[hashtag["text"]]
                    if oldscore < simscore:
                        maxscoremap[hashtag["text"]] = simscore
                else:
                    maxscoremap[hashtag["text"]] = simscore

        for hashtag, maxscore in maxscoremap.iteritems():
            collecmap = {"id" : hashtag,
                         "simscore" : maxscore}
            ouput_db.insertCollection(collecmap,"author_cossim_tweetid_max") # Max Sim Score


        print "Done"