REM -- Edgar launcher -
ECHO "Launching Edgar Pipeline TAsk"

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1 --class edgar.EdgarFilingReaderTaskWithPipeline target\scala-2.11\sparkexamples_2.11-1.0.jar file:///c:/Users/marco/SparkExamples/master.idx 4 true file:///c:/Users/marco/SparkExamples/form4.result.csv 