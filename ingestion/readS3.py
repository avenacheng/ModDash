from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_unixtime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'file paths')
    parser.add_argument('--input_path', required = True)
    parser.add_argument('--output_path', required = True)
    parser.add_argument('--filename', required = True)
    args = parser.parse_args()

    # create Spark context
    sc = SparkContext("spark://ec2-100-21-167-148.us-west-2.compute.amazonaws.com:7077","sparkJob1")
    sqlContext = SQLContext(sc)

    # read data
    # example input_path: "s3a://redditcommentsbz2/RC-2015-01"
    df = sqlContext.read.json(args.input_path).select("created_utc", "subreddit","subreddit_id",\
    "author","id","body","ups","downs","controversiality","edited","parent_id","link_id")
    df = df.withColumn('created_utc', from_unixtime(df.created_utc, format='yyyy-MM-dd HH:mm:ss'))
    parquet_name = args.filename + str('.parquet')
    final_output_path = args.output_path + parquet_name
    df.write.parquet(final_output_path)
