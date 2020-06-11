from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_unixtime


file_name = 'RC_2014-02'
file_path = "s3a://redditcommentsbz2/" + file_name

# create Spark context
sc = SparkContext("spark://ec2-100-21-167-148.us-west-2.compute.amazonaws.com:7077","job1")
sqlContext = SQLContext(sc)

# read in JSON
df = sqlContext.read.json(file_path).select("created_utc", "subreddit","subreddit_id",\
"id","name","body","ups","score","author","controversiality","edited","parent_id","link_id")

# convert UNIX 
convertedUnix = df.withColumn('created_utc', from_unixtime(df.created_utc, format='yyyy-MM-dd HH:mm:ss'))

convertedUnix.show()
# save to S3
parquet_name = file_name + '.parquet'
saved_path = "s3a://redditcommentsbz2/" + parquet_name
convertedUnix.write.parquet(saved_path)
