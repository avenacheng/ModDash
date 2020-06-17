from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# create Spark context
sc = SparkContext("spark://ec2-100-21-167-148.us-west-2.compute.amazonaws.com:7077","ingestion")
sqlContext = SQLContext(sc)


for year in range(2013,2018):
    for month in ['01','02','03','04','05','06','07','08','09','10','11','12']:    
        file_name = 'RC_'+str(year)+'-'+ month
        file_path = "s3a://redditcommentsbz2/" + file_name + '.bz2'

        df = sqlContext.read.json(file_path).select("created_utc", "subreddit","subreddit_id","id","body","score","author","controversiality","link_id")

        parquet_name = file_name + '.parquet'
        saved_path = "s3a://redditcommentsbz2/" + parquet_name
        df.write.parquet(saved_path)


