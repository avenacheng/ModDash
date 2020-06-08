# import boto3
# with open("s3://redditcommentsbz2/RC_2015-01") as f:
#     for line in f:
#         j_content = json.loads(line)
# jsonDF = spark.read.json("s3://redditcommentsbz2/RC_2015-01")
# jsonDF.printSchema()
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

# create Spark context
sc = SparkContext("spark://ec2-100-21-167-148.us-west-2.compute.amazonaws.com:7077","bz2 to JSON")

# read data
file_path = 's3a://redditcommentsbz2/RC_2014-01.bz2'

def read_fun_generator(filename):
    with bz2.open(filename, 'rb') as f:
        for line in f:
            yield line.strip()


bz2_filelist = sc.json(file_path)
rdd_from_bz2 = sc.flatMap(read_fun_generator)
