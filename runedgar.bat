REM -- Edgar launcher -
ECHO "Launching Edgar Pipeline TAsk"

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1 --class edgar.EdgarFilingReaderTaskWithPipeline target\scala-2.11\sparkexamples_2.11-1.0.jar file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/master.20160422.idx 13F-HR true file:///c:/Users/marco/SparkExamples/form4.result.csv 