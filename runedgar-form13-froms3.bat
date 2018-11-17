REM -- Edgar launcher -
ECHO "Launching Edgar Pipeline TAsk"

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1 --class EdgarForm13Task target\scala-2.11\spark-examples.jar %1 %2