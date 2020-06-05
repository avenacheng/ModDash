import boto3

jsonDF = spark.read.json("s3://redditcommentsbz2/RC_2015-01")
jsonDF.printSchema()
