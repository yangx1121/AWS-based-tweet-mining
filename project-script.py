
# coding: utf-8

# # Project 03 - Due Monday, November 13 at 12pm
# 
# *Objectives*: Use Spark to process and perform basic analysis on non-relational data, including its DataFrame and SQL interfaces.
# 
# *Grading criteria*: The tasks should all be completed, and questions should all be answered with Python code, SQL queries, shell commands, and markdown cells.  The notebook itself should be completely reproducible (using AWS EC2 instance based on the provided AMI) from start to finish; another person should be able to use the code to obtain the same results as yours.  Note that you will receive no more than partial credit if you do not add text/markdown cells explaining your thinking when appropriate.
# 
# *Attestation*: **Work in groups**.  At the end of your submitted notebook, identify the work each partner performed and attest that each contributed substantially to the work.
# 
# *Deadline*: Monday, November 13, 12pm.  One member of each group must submit your notebook to Blackboard; you should not submit it separately..

# ## Part 1 - Setup
# 
# Begin by setting up Spark and fetching the project data.  
# 
# **Note**: you may want to use a larger EC2 instance type than normal.  This project was prepared using a `t2.xlarge` instance.  Just remember that the larger the instance, the higher the per-hour charge, so be sure to remember to shut your instance down when you're done, as always.
# 
# ### About the data
# 
# We will use JSON data from Twitter; we saw an example of this in class.  It should parse cleanly, allowing you to focus on analysis.
# 
# This data was gathered using GWU Libraries' [Social Feed Manager](http://sfm.library.gwu.edu/) application during a recent game of the MLB World Series featuring the Los Angeles Dodgers and Houston Astros.  This first file tells you a little bit about how it was gathered:

# In[1]:

get_ipython().system('wget https://s3.amazonaws.com/2017-dmfa/project-3/9670f3399f774789b7c3e18975d25611-README.txt')


# In[2]:

get_ipython().system('cat 9670f3399f774789b7c3e18975d25611-README.txt')


# The most important pieces in that metadata are:
# 
#  * It tracked tweets that mentioned "dodgers" or "astros".  Every item in this set should refer to one or the other, or both.
#  * This data was not deduplicated; we may see individual items more than once.
#  * Data was collected between October 29 and October 30.  Game 5 of the Series was played during this time.
#  
# You should not need to know anything about baseball to complete this assignment.
# 
# **Please note**: sometimes social media data contains offensive material.  This data set has not been filtered; if you do come across something inappropriate, please do your best to ignore it if you can.

# ## Fetch the data
# 
# The following files are available:
# 
#  * https://s3.amazonaws.com/2017-dmfa/project-3/9670f3399f774789b7c3e18975d25611_003.json
#  * https://s3.amazonaws.com/2017-dmfa/project-3/9670f3399f774789b7c3e18975d25611_004.json
#  * https://s3.amazonaws.com/2017-dmfa/project-3/9670f3399f774789b7c3e18975d25611_005.json
#  * https://s3.amazonaws.com/2017-dmfa/project-3/9670f3399f774789b7c3e18975d25611_006.json
#  
# ### Q1.1 - Select at least one and obtain it using `wget`.  Verify the file sizes using the command line.
# 
# Each file should contain exactly 100,000 tweets.  
# 
# *Note*: you are only required to use one of these files, but you may use more than one.  It will be easier to process more data if you use a larger EC2 instance type, as suggested above.  Use the exact same set of files throughout the assignment.
# 
# **Answer**

# In[3]:

get_ipython().system('wget https://s3.amazonaws.com/2017-dmfa/project-3/9670f3399f774789b7c3e18975d25611_003.json')


# For your reference, here is the text of one Tweet, randomly selected from one of these files.  You might wish to study its structure and refer to it later.

# In[4]:

get_ipython().system('cat *.json | shuf -n 1 > example-tweet.json')


# In[5]:

import json
print(json.dumps(json.load(open("example-tweet.json")), indent=2))


# You can find several key elements in this example; the text, time, and language of the tweet, whether it was a reply to another user, the user's screen name along with their primary language and other account information like creation date, follower/friend/tweet counts, and perhaps their location.  If there are hashtags, user mentions, or urls present in their tweet, they will be present in the `entities` section; these are not present in every tweet.  If this is a retweet, you will see the original tweet and its information nested within.

# ### Q1.2 - Start up Spark, and verify the file sizes.

# We will use our normal startup sequence here:

# In[6]:

import os


# In[7]:

os.environ['SPARK_HOME'] = '/usr/local/lib/spark'


# In[8]:

import findspark


# In[9]:

findspark.init()


# In[10]:

from pyspark import SparkContext


# In[11]:

spark = SparkContext(appName='project-03')


# In[12]:

spark


# In[13]:

from pyspark import SQLContext


# In[14]:

sqlc = SQLContext(spark)


# In[15]:

sqlc


# In[16]:

tweets = sqlc.read.json("9670f3399f774789b7c3e18975d25611_*.json")


# In[17]:

tweets.printSchema()


# Verify that Spark has loaded the same number of tweets you saw before:
# 
# **Answer**

# In[18]:

get_ipython().system('wc -l 9670f3399f774789b7c3e18975d25611_*.json')


# In[19]:

tweets.count()


# ## Part 2 - Comparing DataFrames and Spark SQL
# 
# For the next three questions, we will look at operations using both DataFrames and SQL queries. Note that `tweets` is already a DataFrame:

# In[20]:

tweets


# To issue SQL queries, we need to register a table based on `tweets`:

# In[21]:

tweets.createOrReplaceTempView("tweets")


# ### Q2.1 - Which 10 languages are most commonly used in tweets?  Verify your result by executing it with both the dataframe and with SQL.
# 
# Hint: for the dataframe, use `groupBy`, `count`, and `orderBy`.  See the documentation at https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html for details on these and other functions.
# 
# **Answer**

# In[22]:

tweets.groupBy("lang").count().orderBy("count", ascending=False).show(10)


# In[23]:

sqlc.sql("SELECT lang, COUNT(*) AS count FROM tweets GROUP BY lang ORDER by count DESC").show(10)


# ### Q2.2 - Which 10 time zones are most common among users?  Verify your result with both the dataframe and SQL.
# 
# *Note*: for this question, you may leave NULL values present in your results, as a way to help you understand what data is present and what is missing.
# 
# **Answer**

# In[24]:

tweets.groupBy("user.time_zone").count().orderBy("count", ascending=False).show(10, False)


# In[25]:

sqlc.sql("SELECT user.time_zone, COUNT(*) AS count FROM tweets GROUP BY time_zone ORDER by count DESC").show(10, False)


# ### Q2.3 - How many tweets mention the Dodgers?  How many mention the Astros?  How many mention both?
# 
# You may use either the dataframe or SQL to answer.  Explain why you have chosen that approach.
# 
# Hint:  you will want to look at the value of the `text` field.
# 
# **Answer**

# In[26]:

sqlc.sql("SELECT text FROM tweets WHERE text LIKE '%Astros%' ").count()


# In[27]:

sqlc.sql("SELECT text FROM tweets WHERE text LIKE '%Dodgers%' ").count()


# In[28]:

sqlc.sql("SELECT text FROM tweets WHERE (text LIKE '%Dodgers%' AND text LIKE '%Astros%') ").count()


# We use SQL to answer this question because this is a simply query and can be easily done with SQL by using wildcards.

# ## Part 3 - More complex queries
# 
# For this section, you may choose to use dataframe queries or SQL.  If you wish, you may verify results by using both, as in Part 2, but this is not required for this section.
# 
# ### Q3.1 - Team mentions by location
# 
# In which users' locations are the Astros and the Dodgers being mentioned the most?  Consider each team separately, one at a time.  Discuss your findings.
# 
# Hint:  you may use either the time zones or user-specified locations for this question.
# 
# **Answer**

# In[29]:

sqlc.sql("SELECT user.time_zone, count(*) As count_tz FROM tweets WHERE text LIKE '%Astros%'     GROUP BY user.time_zone ORDER BY count_tz DESC").show(10, False)


# Among the users that provided their time zone information, whose tweets contain keyword 'Astros' are mostly from Central Time Zone, which makes sense because Astros is based in Houston in Central time.

# In[30]:

sqlc.sql("SELECT user.time_zone, count(*) As count_tz FROM tweets WHERE text LIKE '%Dodgers%'     GROUP BY user.time_zone ORDER BY count_tz DESC").show(10, False)


# Among the users that provided their time zone information, whose tweets contain keyword 'Dodgers' are mostly from Pacific Time Zone, which makes sense because Dodgers is based in LA in Pacific Time. 

# There are more people from Pacific time zone (possibly Dodgers fans) talking about Astros than people from Central time zone talking about Dodgers.

# ### Q3.2 - Which Twitter users are being replied to the most?
# 
# Discuss your findings.
# 
# Hint: use the top-level `in_reply_to_screen_name` for this.
# 
# **Answer**

# In[55]:

sqlc.sql("SELECT in_reply_to_screen_name, COUNT(*) AS count_replying FROM tweets     WHERE in_reply_to_screen_name <> 'null' GROUP BY in_reply_to_screen_name ORDER by count_replying DESC").show(10)


# "Astros" has been replied most among those whose users screen name is available.

# ### Q3.3 - Which 10 verified users have the most followers?  Which 10 unverified users have the most followers?
# 
# Provide both the screen names and follower counts for each.
# 
# Discuss your findings.
# 
# **Answer**

# In[32]:

sqlc.sql("SELECT user.name, MAX(user.followers_count) AS count_verified FROM tweets     WHERE user.verified = 'true' GROUP BY user.name ORDER by count_verified DESC").show(10)


# Top 10 verified users that have the most followers are, 'Reuters Top News' with 18937529 followers; 'Fox News', 16272836 followers; 'ABC News',12551437  followers and followed by ' Washington Post','MLB','NPR','Bill Simmons','NBC News','John Legere',and 'ABS-CBN News Channel'.

# In[33]:

sqlc.sql("SELECT user.name, MAX(user.followers_count) AS count_verified FROM tweets     WHERE user.verified = 'false' GROUP BY user.name ORDER by count_verified DESC").show(10)


# Top 10 unverified users that have the most followers are, 'TENIENTE CHOCHOS' with 833669 followers; 'Diario El Carabobeño', 725952 followers; '❤Ƥ▲ϻ(❛‿❛)❤',712254 followers and followed by '  🗽Jeffrey Levin 🗽','It's Bernard®','EP | Mundo',' LALATE','Captain Gigawatt','BLACK GOKU 😈🔥',and ' EP | Venezuela'. 

# The most popular verified users are mainly media channels, and they have 10x more followers than the most popular unverified users.

# ### Q3.4 - What are the most popular sets of hashtags among users with many followers?  Are they the same as among users with few followers?
# 
# Decide for yourself exactly how many followers you believe to be "many", and explain your decision.  You may use queries and statistics to support this decision if you wish.
# 
# Hint: if your sample tweet above does not include hashtags under the `entities` field, generate a new example by running the `shuf` command again until you find one that does.
# 
# Hint 2: the hashtag texts will be in an array, so you may need some functions you haven't used before.  If you're using SQL, see the docs for [Hive SQL](https://docs.treasuredata.com/articles/hive-functions) for details, (and consider `CONCAT_WS`, for example).
# 
# Discuss your findings.
# 
# **Answer**

# In[34]:

tweets.describe("user.followers_count").show()


# In[35]:

sqlc.sql('SELECT percentile_approx(user.followers_count, 0.25) AS 1st_Q,     percentile_approx(user.followers_count, 0.5) AS median,     percentile_approx(user.followers_count, 0.75) AS 3rd_Q FROM tweets').show()


# In[36]:

sqlc.sql('SELECT * FROM tweets WHERE user.followers_count > 5000').count()


# Among all 100,000 tweets, the mean of followers is 4990 and median is 372, which means the distribution of followers is extremely right skewed. 4625 of the tweets are from users with more than 5000 followers. We will compare the hashtags by users with more than 5000 followers (as 'many followers'), with users in the lowest quantile (followers <= 152).

# In[37]:

sqlc.sql("SELECT DISTINCT hashtag.text AS hashtag, COUNT(*) AS count         FROM tweets         LATERAL VIEW OUTER explode(entities.hashtags) hashtagsTable AS hashtag         WHERE (user.followers_count > 5000 AND hashtag is not null)         GROUP BY hashtag         ORDER BY count DESC").show(20)


# In[38]:

sqlc.sql("SELECT DISTINCT hashtag.text AS hashtag, COUNT(*) AS count         FROM tweets         LATERAL VIEW OUTER explode(entities.hashtags) hashtagsTable AS hashtag         WHERE (user.followers_count <= 152 AND hashtag is not null)         GROUP BY hashtag         ORDER BY count DESC").show(20)


# The most frequent hashtags by users with more than 5000 followers (aka ‘KOLs’) are: ‘EarnHistory’, ‘Astroswin’, ‘WorldSeries’, ‘MLB’, ‘Astros’, ‘walkoff’, ‘News’, ‘SerieMundial’.
# 
# The most frequent hashtags by users with less than 152 followers (aka ‘Fans’) are: ‘EarnHistory’, ‘Astroswin’, ‘WorldSeries’, ‘walkoff’, ‘Astros’, ‘HTownPride’, ‘Dodgers’, ‘ThisTeam’.
# 
# Both groups have this hashtags: ‘EarnHistory’, ‘Astroswin’, ‘WorldSeries’, ‘Astros’, ‘walkoff’. The difference is, KOLs use ‘News’, ‘MLB’ a lot, while Fans don’t; Fans also mentioned ‘HTownPride’, ‘Dodgers’, ‘ThisTeam’, while KOLs didn’t. 
# 

# * Some hashtags occur multiple times in this query, even if I used 'SELECT DISTINCT'. From a query on the original hashtag array we see that some hashtags are assigned multiple indices, for example we can see [WrappedArray(55, 67),EarnHistory] and [WrappedArray(29, 41),EarnHistory]. We don't know how the indices work and we don't know how to combine these hashtags with same value but different indices.

# * The above queries count only how many times the hashtags appear. If we are interested in how the hashtags are combined with each other in each tweet, please refer to the below queries, although we do not have time to concat the text yet.

# In[39]:

sqlc.sql("SELECT DISTINCT entities.hashtags AS hashtag, count(*) as count         FROM tweets         WHERE (user.followers_count > 5000)         GROUP BY hashtag ORDER BY count DESC").show(20, False)


# In[40]:

sqlc.sql("SELECT DISTINCT entities.hashtags AS hashtag, count(*) as count         FROM tweets         WHERE user.followers_count <= 152         GROUP BY hashtag ORDER BY count DESC").show(20, False)


# Some interesting findings: 
# * 'walkoff' is always used with 'WorldSeries';
# * 'MLB' is only used by KOLs and does not come with other hashtags;
# * 'Dodgers' are mentioned by KOLs only when they also mention 'Astros';
# * KOLs care about 'fashionweek' more than 'Dodgers';
# * Fans do talk about 'Dodgers' without 'Astros';
# * There is a weird combination of 9 hashtags used simultaneously by some Fans, which appears in as many as 55 tweets.

# ### Q3.5 - Analyze common words in tweet text
# 
# Following the example in class, use `tweets.rdd` to find the most common interesting words in tweet text.  To keep it "interesting", add a filter that removes at least 10 common stop words found in tweets, like "a", "an", "the", and "RT" (you might want to derive these stop words from initial results).  To split lines into words, a simple split on text whitespace like we had in class is sufficient; you do not have to account for punctuation.
# 
# After you find the most common words, use dataframe or SQL queries to find patterns among how those words are used.  For example, are they more frequently used by Dodgers or Astros fans, or by people in one part of the country over another?  Explore and see what you can find, and discuss your findings.
# 
# Hint: don't forget all the word count pipeline steps we used earlier in class.
# 
# **Answer**

# In[41]:

wordss = ['rt', 'the', 'in', 'a', 'to', 'of','is','i','and','this','for','it']
tweets.rdd.flatMap(lambda r: r['text'].lower().split(' '))     .filter(lambda t: t and t not in wordss)     .map(lambda t: (t, 1))     .reduceByKey(lambda a, b: a + b)     .takeOrdered(10, key=lambda pair: -pair[1])


# In[42]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%astros%' GROUP BY location ORDER by count DESC").show(10)


# In[43]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%dodgers%' GROUP BY location ORDER by count DESC").show(10)


# In[44]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%game%' GROUP BY location ORDER by count DESC").show(10)


# In[45]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%worldseries%' GROUP BY location ORDER by count DESC").show(10)


# In[46]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%win%' GROUP BY location ORDER by count DESC").show(10)


# In[47]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%earnhistory%' GROUP BY location ORDER by count DESC").show(10)


# In[48]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%world%' GROUP BY location ORDER by count DESC").show(10)


# In[49]:

sqlc.sql("SELECT user.location, COUNT(*) AS count FROM tweets WHERE text LIKE '%series%' GROUP BY location ORDER by count DESC").show(10)


# Among the users that provided their location information, most of the top 10 frequent keywords are tweeted by the users in Houston, TX while 'dogers', the second most frequent keywords are mostly posted from the users in LA. 

# In[50]:

freqwd = tweets.rdd.flatMap(lambda r: r['text'].lower().split())     .filter(lambda t: t and t not in wordss)     .map(lambda t: (t, 1))     .reduceByKey(lambda a, b: a + b)     .takeOrdered(10, key=lambda pair: -pair[1])


# In[51]:

freqword = []
for i in range(10):
    freqword.append(freqwd[i][0])


# In[52]:

freqword


# In[53]:

tweets.rdd.map(lambda r: r['text'].lower()).filter(lambda t: 'astros' in t)     .flatMap(lambda t: t.split(' '))     .filter(lambda t: t in freqword)     .map(lambda t: (t, 1))     .reduceByKey(lambda a, b: a + b)     .takeOrdered(10, key=lambda pair: -pair[1])


# In[54]:

tweets.rdd.map(lambda r: r['text'].lower()).filter(lambda t: 'dodgers' in t)     .flatMap(lambda t: t.split(' '))     .filter(lambda t: t in freqword)     .map(lambda t: (t, 1))     .reduceByKey(lambda a, b: a + b)     .takeOrdered(10, key=lambda pair: -pair[1])


# We know from Q2.3 that there are more tweets about Astros than Dodgers in our dataset. We also have a list of the most common words among the 100,000 tweets. Let's assume tweets mentioning 'Astros' as posted by Astros fans and mentioning 'Dodgers' are posted by Dodgers fans. By applying the list of most common words, we see that, fans of both teams frequently mention this words. Obviously they mention their own team more than the opposite team. Moreover, Astros fans talk about 'win' and '#earnhistory' more frequently than Dodgers fans, even if we take into account that there are more Astros fans. 

# Attestation: we each worked individually on the easier questions, and discussed together from Q3.3 to Q3.5 for the best result. For Q3.4, we've been looking for the solution of getting rid of the array thing for a whole evening, and finally borrowed some idea from http://www.enhgo.com/snippet/jupyter-notebook/bdd-import-nested-jsonipynb_rmoff_jupyter-notebook.
# 
# _Tianyi Chang, Yuebo Li, Yinlu Wu, Xuan Yang_
