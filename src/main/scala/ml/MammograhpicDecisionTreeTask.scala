package ml

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._
import scala.xml._
import scala.util.Try
import common.Pipeline
import common.DataReaderStep

/**
 * TAsk to predict benign/malign tumours based on a DecisionTree 
 * spark-submit  --class ml.MammographicDecsionTreeTask 
 *    target\scala-2.11\sparkexamples_2.11-1.0.jar 
 *    file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/mammographic_masses.data.txt
 * 
 * 
 * 
 */


object MammographicDecisionTreeTask {

  val logger: Logger = Logger.getLogger("MammographicDecisionTree.Task")

  def configureContext(args: Array[String]): SparkContext = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName("Spark Edgar Filing Reader task")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    session.sparkContext
  }

  def startComputation(sparkContext:SparkContext, args:Array[String]) = {
    
    val fileName = args(0)
    
    logger.info("------------ MammographicDecisionTreeTask -----------------")
    logger.info(s"FileName:$fileName")
    logger.info("-------------------------------------------------------")

    
    val extractor = new MammographicDataFrameReader()
    val transformer = new DataCleaningTransformer(Seq("BI-RADS", "Age", "Shape", "Margin", "Density", "Severity"))
    val loader =new RandomForestLoader("Severity") // new DecisionTreeLoader("Severity")    
    
    val mammograpicPipeline = new Pipeline(extractor, transformer, loader)
    mammograpicPipeline.runPipeline(sparkContext, fileName)
    
  }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))

    val sparkContext = configureContext(args)
    startComputation(sparkContext, args)
    
  }

}