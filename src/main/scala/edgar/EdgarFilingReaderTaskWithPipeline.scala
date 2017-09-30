package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._
import scala.xml._
import scala.util.Try

/**
 * Edgar task to Read a Form4 spark-stord file, and classify each
 * company based on the number of transaction being executed
 * TODO: Add a decision tree in the mix
 *
 * Run the code like this:
 *
 * * spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0 --class edgar.EdgarFilingReaderTask sparkexamples.jar <fileName> <formType> <debugFlag>
 
 *
 */


object EdgarFilingReaderTaskWithPipeline {

  val logger: Logger = Logger.getLogger("EdgarFilingReaderWithPipeline.Task")

  def configureContext(args: Array[String]): SparkContext = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName("Spark Edgar Filing Reader task")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    session.sparkContext
  }

  def startComputation(sparkContext:SparkContext, fileName:String,
                       formType:String, debugMode:Boolean) = {
    val dataReaderStep = new DataReaderStep(fileName, formType, debugMode)
    val processor = new Form4Processor()
    val persister = new MongoPersister()    
    
    val form4Pipeline = new Pipeline(dataReaderStep, processor, persister)
    form4Pipeline.runPipeline(sparkContext, fileName)
    
  }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))

    if (args.size < 3) {
      println("Usage: spark-submit --class edgar.spark.EdgarFilingReaaderTask <fileName> <formType> <debug>")
      System.exit(0)
    }

    val sparkContext = configureContext(args)
    val fileName = args(0)
    val formType = args(1)
    val debug = args(2).toBoolean

    logger.info("------------ Edgar Filing Reader Task -----------------")
    logger.info(s"FileName:$fileName")
    logger.info(s"FormType:$formType")
    logger.info(s"debug:$debug")
    logger.info("-------------------------------------------------------")

    logger.info(s"Fetching Data from Edgar file $fileName")
    startComputation(sparkContext, fileName, formType, debug)
    
  }

}