from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# create Spark context
sc = SparkContext("spark://ec2-100-21-167-148.us-west-2.compute.amazonaws.com:7077","bz2 to JSON")
sqlContext = SQLContext(sc)

# read data
df = sqlContext.read.json(file_path).select("created_utc", "subreddit","subreddit_id",\
"author","id","body","ups","downs","controversiality","edited","parent_id","link_id")

df.write.json('s3a://redditcommentsbz2/parquet/practice.json')
