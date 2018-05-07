#!/bin/bash

spark-submit --master yarn --deploy-mode cluster --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1 --class edgar.EdgarFilingReaderTaskNoPipeline spark-examples.jar s3a://ec2-bucket-mm-spark/master.idx.2017.q1  4  0.9 form4-results-cluster28.4-all.results
