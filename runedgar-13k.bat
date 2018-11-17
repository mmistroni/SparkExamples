REM -- Edgar launcher -
ECHO "Launching Edgar Pipeline TAsk"
REM %1 is the file name


spark-submit  --class edgar.EdgarFilingReaderForm13K target\scala-2.11\spark-examples.jar masterq118.idx 0.005  %1