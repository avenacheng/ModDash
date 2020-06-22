# ModDash

Keeping subreddits safe.

[https://docs.google.com/presentation/d/1Q7ybWYtu2dSSOjvPlj5QIxcfPIm90uV9eJfeR9wLb4w/edit?usp=sharing](#)
<hr/>

<hr/>

## Introduction
Reddit is a social media platform for people to form communities around their personal interests. It has the power to bring people together, but since each user is anonymous, it becomes easier for people to say anything they want -- including the bad stuff. Moderators are created in every subreddit to monitor user activity and ensure people are engaging in civil discussion; however, this can be a time consuming task. In order to make their jobs easier, I created ModDash, a dashboard for moderators to view user history and top posts/comments with negative sentiment. With access to user and comment history, moderators can see whether a specific user frequently engages in toxic behavior and if they may have missed a discussion thread that they didn't notice right away. This dashboard allows mods the freedom to decide what to do given the data: reach out to the user, remove the user from the subreddit, or lock down a thread.

## Architecture
![architecture](https://user-images.githubusercontent.com/27587714/85059662-e24a9b00-b158-11ea-9327-e164a3e02b58.png)

## Dataset
Monthly Reddit Comments: https://files.pushshift.io/reddit/comments/
I used data from 2013 - 2017 stored as bz2 formats.

## Engineering challenges

**1. Slow processing with JSON**

Spark is able to read directly from bz2. While this makes it convenient to read directly from S3, querying the data takes much longer. Parquet is optimized to read very quickly, which was helpful in my use case in which I only needed to use preprocess some columns. Testing with sample data ~ 115KB:

JSON - 7 minutes
Parquet - 42 seconds
