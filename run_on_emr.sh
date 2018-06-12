#!/bin/bash
# S3 url s3a://ec2-bucket-mm-spark/form4-results-cluster28.4-all.results
# s3-dist-cp --src /data/incoming/hourly_table --dest s3://my-tables/incoming/hourly_table
#-- copy jar and master file from bucket to EMR
#aws s3 cp s3://ec2-bucket-mm-spark/spark-examples.jar .
#aws s3 cp s3://ec2-bucket-mm-spark/run_on_emr.sh .
# hadoop fs -ls /output
# configure distcp
# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html
# https://stackoverflow.com/questions/43478985/aws-emr-s3distcp-the-auxservicemapreduce-shuffle-does-not-exist

#hadoop distcp hdfs:///output/form4-results-cluster14.5-all.results s3a://ec2-bucket-mm-spark/form4-results-cluster18.5-results
spark-submit --master yarn --deploy-mode cluster --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1 --class edgar.EdgarFilingReaderTaskNoPipeline spark-examples.jar s3a://ec2-bucket-mm-spark/master.idx.2017.q1  4  0.05 hdfs:///output/form4-results-cluster14.5-all.results




[
    {
        "Name":"S3DistCp step",
        "Args":["s3-dist-cp","--s3Endpoint=s3.amazonaws.com","--src=hdfs:///output","--srcPattern=.*[a-zA-Z,]","--dest=s3://ec2-bucket-mm-spark/edgarResults/form4",
        "ActionOnFailure":"CONTINUE",
        "Type":"CUSTOM_JAR",
        "Jar":"command-runner.jar"        
    }
]

[
    {
        "Name":"S3DistCp step",
        "Args":["s3-dist-cp","--s3Endpoint=s3.amazonaws.com","--src=hdfs:///output","--dest=s3://ec2-bucket-mm-spark/edgarResults/form4","--srcPattern=.*[a-zA-Z,]+"],
        "ActionOnFailure":"CONTINUE",
        "Type":"CUSTOM_JAR",
        "Jar":"command-runner.jar"        
    }
]
