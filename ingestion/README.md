#Ingestion

Data was manually collected from pushshift.io by running the follow command:

```wget -i redditurls```

Once downloaded, these were put into S3 using the following command:

```s3cmd put _fileName_ s3://redditcommentbz2/```
