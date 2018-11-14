# AWS-based-tweet-mining

## Objective：
Use Spark to process and perform basic text mining analysis on non-relational data, including its DataFrame and SQL interfaces. 

## Data:

The data was gathered using GWU Libraries' Social Feed Manager application during the 2017 MLB World Series featuring the Los Angeles Dodgers and Houston Astros. 

## My responsibilities:

* First, set up data files: 

There were 4 json files as our metadata. Each of them was an export created with Social Feed Manager and was stored in S3. We used json files imported from S3. 

The important pieces should be noticed for data are: 1) It tracked tweets that mentioned "dodgers" or "astros". Every item in this set should refer to one or the other, or both. 3) This data was not deduplicated; we may see individual items more than once. 3) Data was collected between October 29 and October 30. Game 5 of the Series was played during this time. 4) It contained exactly 100,000 tweets individually. 

Then, I used shell command to change the file name and used python code to look through the file content. We found there were several key elements in this example: the text, time, and language of the tweet, whether it was a reply to another user, the user's screen name along with their primary language and other account information like creation date, follower/friend/tweet counts, and perhaps their location.

* Secondly, start up Spark:

Read json by using .printschema() and verify that Spark has loaded the same number of tweets you saw before.

* Then, the next step was data exploration. 

Note in this step, we executed each question with both the dataframe and SQL. For the dataframe, I used groupBy, count, and orderBy. For SQL, I used SELECT, COUNT, AS, FROM, WHERE, MAX, MIN, GROUP BY, LIKE, ORDER BY, DESC and show(), count() and wildcards. Some questions were like that: 1)Which 10 languages are most commonly used in tweets? 2)Which 10 time zones are most common among users? 3)How many tweets mention the Dodgers? How many mention the Astros? How many mention both? 4)In which users' locations are the Astros and the Dodgers being mentioned the most? 5)Which Twitter users are being replied to the most?

Beside, we also executed more complex queries like: What are the most popular sets of hashtags among users with many followers? Are they the same as among users with few followers? For answer this, we need to decide for ourselves exactly how many followers we believe to be "many". So I used queries .describe("user.followers_count") to show the mean, sd, min and max of user followers and used statistics query percentile_approx() to finally determine that >5000 as ''many'' and <152 as “fewer”. Then the SQL queries to find hashtags were like this:

sqlc.sql("SELECT DISTINCT hashtag.text AS hashtag, COUNT(*) AS count \

        FROM tweets \

        LATERAL VIEW OUTER explode(entities.hashtags) hashtagsTable AS hashtag \

        WHERE (user.followers_count > 5000 AND hashtag is not null) \

        GROUP BY hashtag \

        ORDER BY count DESC").show(20)

sqlc.sql("SELECT DISTINCT hashtag.text AS hashtag, COUNT(*) AS count \

        FROM tweets \

        LATERAL VIEW OUTER explode(entities.hashtags) hashtagsTable AS hashtag \

        WHERE (user.followers_count <= 152 AND hashtag is not null) \

        GROUP BY hashtag \

        ORDER BY count DESC").show(20)

* Next, most common words:

In addition, our coursework also include the part that using tweets.rdd to find the most common interesting words in tweet text. 

To keep the result more meaningful, I at first added a filter that could remove at least 10 common stop words found in tweets, like "a", "an", "the", and "RT". The code looks like this: 

wordss = ['rt', 'the', 'in', 'a', 'to', 'of','is','i','and','this','for','it']

tweets.rdd.flatMap(lambda r: r['text'].lower().split(' ')) \

    .filter(lambda t: t and t not in wordss) \

    .map(lambda t: (t, 1)) \

    .reduceByKey(lambda a, b: a + b) \

    .takeOrdered(10, key=lambda pair: -pair[1])
    
Then we used SQL queries and MapReduce piplines to find patterns among how those words are used. For example, are they more frequently used by Dodgers or Astros fans, or by people in one part of the country over another? The results showed that: Among the users that provided their location information, most of the top 10 frequent keywords were tweeted by the users in Houston, TX while 'dogers', the second most frequent keywords were mostly posted from the users in LA. And fans of both teams frequently mentioned this words. Obviously they mentioned their own team more than the opposite team. Moreover, Astros fans talked about 'win' and '#earnhistory' more frequently than Dodgers fans, even if we took into account that there were more Astros fans.


