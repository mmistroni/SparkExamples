# SparkExamples

This project contains different spark examples i have made to learn Spark

I Have used Spark 1.6.1 compiled with Scala 2.10.5, the same version that is used
to compile and build this project
the project directory contains sbt files for generating a fat jar and an eclipse project

Along different examples theres' also a EuroCupQualifierDecisionTree which attempts to predict which team will made it to the Euro 2016 seminfinals based on followig data back to Euro1992:
- performance at the previous world cup
- how many country's teams made it to the Champions League knock out stages
- how many country's teams made it to the Europa League knock out stages
- UEFA country ranking of previous year
- How many country's teams are present in Top 30 Uefa club ranking for the previous year
- How many players for each country are nominated in the previous ballon d'or

If you want to run it, you need to use the two files (both in src.main.resources)
- EurocupData.csv contains the test data
- Euro2016.csv contain data that need to be predicted
