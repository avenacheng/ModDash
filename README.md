# ModDash

Keeping subreddits safe.

[https://docs.google.com/presentation/d/1Q7ybWYtu2dSSOjvPlj5QIxcfPIm90uV9eJfeR9wLb4w/edit?usp=sharing](#)

<hr/>

## Introduction
Reddit is a social media platform for people to form communities around their personal interests. It has the power to bring people together, but since each user is anonymous, it becomes easier for people to say anything they want -- including the bad stuff. Moderators are created in every subreddit to monitor user activity and ensure people are engaging in civil discussion; however, this can be a time consuming task. In order to make their jobs easier, I created ModDash, a dashboard for moderators to view user history and top posts/comments with negative sentiment. With access to user and comment history, moderators can see whether a specific user frequently engages in toxic behavior and if they may have missed a discussion thread that they didn't notice right away. This dashboard allows mods the freedom to decide what to do given the data: reach out to the user, remove the user from the subreddit, or lock down a thread.

## Architecture
![Avena Cheng_Data Model Diagram (10)](https://user-images.githubusercontent.com/27587714/86549193-cafb0400-bef3-11ea-8698-1b93aea23b4d.png)


## Dataset
Monthly Reddit Comments: https://files.pushshift.io/reddit/comments/
I used data from 2013 - 2017 stored as bz2 formats.

## Engineering challenges

**1. Slow processing with JSON**

Part of the reason Spark processing was slow was reading the data. JSON is known for slow reads and fast writes, but reading in a 5GB file would take ~4 minutes. Thus, I tried exploring other formats such as Parquet, a binary format that is column based and provides very quick reads. I discovered that the read time was 20 seconds! But the conversion from JSON to Parquet was ~25 minutes.

I discovered that predefining a schema ahead of time would prove to be very effective. Spark will automatically infer the schema of JSON which can take a very long time. However, after explicitly providing the format, I was able to reduce read time to 13 seconds.

**2. Slow queries in PostgreSQL**

Millions and millions of rows....can take forever to query. For my use case, there were only a few queries that users could make based on the post, sentiment, time, and author. After creating BTree Indexes on these columns, I was able to reduce query time from ~5 minutes to a few milliseconds!

*Example Query:*

SELECT post_id, AVG(sentiment), COUNT(comments) 

WHERE year = 2017 AND  month = 1 AND day = 1

GROUP BY post_id

ORDER BY avg_sentiment ASC

LIMIT 5;

## User Interface

Click here to see a live demo of ModDash!

https://www.youtube.com/watch?v=bAEKbJ5AG8Q
