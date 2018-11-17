REM Running Sector Performance script
spark-submit --class etl.StockHistoricalPerformanceTask target\\scala-2.11\\spark-examples.jar file:///c:/Users/marco/SparkExamples2/SparkExamples %1 %2
