from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from dotenv import load_dotenv
load_dotenv()

file_name = 'RC_2005-12.bz2'
file_path = "s3a://redditcommentsbz2/" + file_name

# CREATE SPARK CONTEXT
master = os.getenv('master_host')
sc = SparkContext(master, "ingestion")
sqlContext = SQLContext(sc)

# READ IN DATA
df = sqlContext.read.json(file_path).select("created_utc", "subreddit","subreddit_id",\
"id","body","score","author","controversiality","link_id")

# SAVE TO S3
saved_path = "s3a://redditcommentsbz2/" + parquet_name


