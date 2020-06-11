from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import hour
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import col

file_name = 'RC_2005-12.bz2'
file_path = "s3a://redditcommentsbz2/" + file_name

# create Spark context
sc = SparkContext("spark://ec2-100-21-167-148.us-west-2.compute.amazonaws.com:7077","job1")
sqlContext = SQLContext(sc)

# read in JSON
df = sqlContext.read.json(file_path).select("created_utc", "subreddit","subreddit_id",\
"id","body","score","author","controversiality","link_id").withColumnRenamed('created_utc', 'time').na.fill('null')
# convert UNIX
#df.show()
df1 = df.withColumn('time', from_unixtime(df.time, format='yyyy-MM-dd HH:mm:ss'))
df2 = df1.withColumn('year', year(df1.time))\
        .withColumn('month', month(df1.time)) \
        .withColumn('day', dayofmonth(df1.time)) \
        .withColumn('hour', hour(df1.time))


# save to S3
parquet_name = file_name + '.parquet'
saved_path = "s3a://redditcommentsbz2/" + parquet_name
df2.write.parquet(saved_path)
