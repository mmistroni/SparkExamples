
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import DecisionTreeExamples._
import DegreeOfSeparations._
import EdgarExtractor._
import MachineLearningExamples._
import MovieRecommendations._
import SparkFlumeIntegration._
import WordCountUsingSparkDropDirectory._

object SparkLauncher {
  
 def main(args: Array[String]) {
    val logFile = "c:/spark-1.5.2/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    
    //fetchFlumeEvents(conf)
    //val logData = sc.textFile(logFile, 2).cache()
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    wordCount(conf ,args(0)) 
 } 
  
}