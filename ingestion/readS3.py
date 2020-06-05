# import boto3
# with open("s3://redditcommentsbz2/RC_2015-01") as f:
#     for line in f:
#         j_content = json.loads(line)
# jsonDF = spark.read.json("s3://redditcommentsbz2/RC_2015-01")
# jsonDF.printSchema()
import json

with open("RC_2015-01") as f:
    for line in f:
        j_content = json.loads(line)

with open('j_content.json', 'w') as outfile:
    json.dump(j_content, outfile)
