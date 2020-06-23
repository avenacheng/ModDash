from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import dayofmonth, year, month, col, udf
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrameWriter
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import string
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

file_name = "RC_2005-12.bz2"
file_path = "s3a://redditcommentsbz2/" + file_name

# create Spark context
master = os.getenv('master_host')
sc = SparkContext(master, 'preprocess')
sqlContext = SQLContext(sc)

# read in data

data = sqlContext.read.json(file_path).select('created_utc', 'controversiality', 'link_id', 'score', 'body', 'author', 'subreddit', 'id')\
        .withColumnRenamed('created_utc', 'time').withColumnRenamed('link_id','post_id').withColumnRenamed('body','comment').withColumnRenamed('id','comment_id') # rename columns
data = data.filter(~col('comment').isin(['[deleted]', '[removed]'])).filter(~col('author').isin(['[deleted]']))
data = data.withColumn('time', from_unixtime(data.time, format='yyyy-MM-dd HH:mm:ss')) # convert unixtime to datetime
df = data.withColumn('year', year(data.time)).withColumn('month', month(data.time)).withColumn('day', dayofmonth(data.time)) # calculate year, month, day

# Create sentiment score for each comment
sid = SentimentIntensityAnalyzer()

def remove_punctuation(x):
    """
    Removes punctuation from comment to calculate sentiment score
    :param: x, str, reddit comment
    :return: nopunc_str, str, comment without punctuation
    """
    punc='"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    for ch in punc:
         nopunc_str = x.replace(ch, '')
    return nopunc_str

def vader(x):
    """
    Calculates sentiment score of comment.
    :param: x, str, reddit comment with no punctuation
    :return: ss, double, sentiment score
    """
    ss = sid.polarity_scores(x)['compound']
    return ss

# apply udf so spark can interpret the functions
noPunctuation = udf(lambda x: remove_punctuation(x))
sentimentScore = udf(lambda x: vader(x))

df = df.withColumn('clean_comment', noPunctuation(df.comment))    # remove punctuation from comment
df = df.withColumn('sentiment', sentimentScore(df.clean_comment)) # calculate sentiment score
df = df.withColumn("sentiment", df["sentiment"].cast("double"))

# CREATE TABLES

# create comments table
comments = df.select('time', 'year','month','day','post_id','comment_id', 'author', 'comment', 'controversiality', 'score', 'sentiment', 'subreddit')
comments.show()
# create posts table, containing posts and the percentage of negative comments
#neg_comments = comments.filter("sentiment <= -0.7").groupby('post_id').count()
#neg_comments = neg_comments.withColumnRenamed("count","num_neg_comments")
#num_comments_per_post = comments.groupby('post_id').count()
#num_comments_per_post = num_comments_per_post.withColumnRenamed("count", "total_comments")
#posts = neg_comments.join(num_comments_per_post, 'post_id')
#posts = posts.withColumn("% neg comments", func.round(neg_comments["num_neg_comments"]/num_comments_per_post["total_comments"],2))
#posts.show()
# create user_history table
#user_avg = df1.select('author','controversiality', 'score', 'sentiment').groupby('author').mean()
#user_avg = user_avg.withColumnRenamed('avg(controversiality)','avg_controversiality').withColumnRenamed('avg(score)','avg_score').withColumnRenamed('avg(sentiment)','avg_sentiment')
#user_comments = df1.groupby('author').count()
#user_comments = user_comments.withColumnRenamed('count','num_comments')
#user_history = user_avg.join(user_comments, 'author')
#print('USER HISTORY')
#user_history.show()

# save file
#comments.repartition(10).write.option('maxRecordsPerFile',100000).mode('overwrite').csv('/reddit_data/')


# WRITE TO POSTGRES
db_host = os.getenv("db_host")
db_password = os.getenv("db_password")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")
db_url = "jdbc:postgresql://" + db_host + ':' + str(db_port) + '/' + db_name

comments_table_name = "comments"
posts_table_name = "posts"
properties = {
  "driver": "org.postgresql.Driver",
  "user": db_user,
  "password": db_password}
write_mode = 'append'
comments.write.jdbc(url = db_url, table = comments_table_name, mode = write_mode, properties = properties)
#posts.write.jdbc(url = db_url, table = posts_table_name, mode = write_mode, properties = properties)

