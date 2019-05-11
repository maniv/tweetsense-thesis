# tweetsense-thesis

Twitter is a micro-blogging platform where the users can be social, informational or both. In certain cases, users generate tweets that have no "hashtags" or "@mentions"; we call it an orphaned tweet. The user will be more interested to find more "context" of an orphaned tweet presumably to engage with his/her friend on that topic. Finding context for an Orphaned tweet manually is challenging because of larger social graph of a user , the enormous volume of tweets generated per second, topic diversity, and limited information from tweet length of 140 characters. To help the user to get the context of an orphaned tweet, this thesis aims at building a hashtag recommendation system called TweetSense, to suggest hashtags as a context or metadata for the orphaned tweets. This in turn would increase user's social engagement and impact Twitter to maintain its monthly active online users in its social network. In contrast to other existing systems, this hashtag recommendation system recommends personalized hashtags by exploiting the social signals of users in Twitter. The novelty with this system is that it emphasizes on selecting the suitable candidate set of hashtags from the related tweets of user's social graph (timeline).The system then rank them based on the combination of features scores computed from their tweet and user related features. It is evaluated based on its ability to predict suitable hashtags for a random sample of tweets whose existing hashtags are deliberately removed for evaluation. I present a detailed internal empirical evaluation of TweetSense, as well as an external evaluation in comparison with current state of the art method.