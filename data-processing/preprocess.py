from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
import pyspark.sql.functions as psf
from pyspark.sql.types import MapType, StringType, IntegerType, DoubleType
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import string
from pyspark.sql import DataFrameWriter

file_name = "practice.parquet"
file_path = "s3a://redditcommentsbz2/parquet/" + file_name

# create Spark context
sc = SparkContext("spark://ec2-100-21-167-148.us-west-2.compute.amazonaws.com:7077","job2")
sqlContext = SQLContext(sc)

# read in data
data = sqlContext.read.parquet(file_path).select('created_utc','controversiality','link_id','ups','body','author','subreddit', 'id')

# define some sentiment functions
sid = SentimentIntensityAnalyzer()

def remove_punctuation(x):
  punc='"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
  for ch in punc:
    nopunc_str = x.replace(ch, '')
  return nopunc_str

def vader(x):
    ss = sid.polarity_scores(x)
    return ss['compound']

noPunctuation = udf(lambda x: remove_punctuation(x))
sentimentScore = udf(lambda x: vader(x))

# do some sentiment analysis
cleaned_comment = data.withColumn('body_clean', noPunctuation(data.body))
df = cleaned_comment.withColumn('sentiment', sentimentScore(cleaned_comment.body_clean))\

# remember to change ups to scores, created_utc to time, and link_id to post_id
comments = df.select('created_utc', 'link_id', 'author', 'body', 'controversiality', 'ups', 'sentiment', 'subreddit')
# TO DO: calculate % neg comments and keywords
posts = df.select('created_utc', 'link_id')


# WRITE TO POSTGRES
__db_host = 'ec2-100-21-167-148.us-west-2.compute.amazonaws.com'
__db_port = '5432'
__db_name = 'reddit'
__db_url = "jdbc:postgresql://" + __db_host + ':' + str(__db_port) + '/' + __db_name

__table_name = "practice"
__properties = {
  "driver": "org.postgresql.Driver",
  "user": 'postgres',
  "password": 'hello'
}
__write_mode = 'Overwrite' # can have different mode like ‘Overwrite’
df.write.jdbc(url=__db_url,table=__table_name,mode=__write_mode,properties=__properties)

