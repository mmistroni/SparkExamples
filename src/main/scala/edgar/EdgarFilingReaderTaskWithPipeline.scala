package edgar

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
 * Edgar task to Read a Form4 spark-stord file, and classify each
 * company based on the number of transaction being executed
 * TODO: Add a decision tree in the mix
 * Hadoop 2.7.1 is needed for accessing s3a filesystem
 * Run the code like this:
 *
 * * spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1
 *                --class edgar.EdgarFilingReaderTaskWithPipeline 
 *                sparkexamples.jar <fileName> <formType> <debugFlag> <outputFile>
 *                For saving in S3, use URI such as s3://<bucketName/<fileName>
 
 * to read the parquet file simply do  sqlContext.read.parquet("/tmp/testParquet")
 * 
 * EdgarFilingReaderTaskWithPipeline.scala
 * 
 * 
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
    session.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    session.sparkContext
  }

  def startComputation(sparkContext:SparkContext, args:Array[String]) = {
    
    val fileName = args(0)
    val formType = args(1)
    val debug = args(2).toBoolean
    val outputFile = args(3)

    logger.info("------------ Edgar Filing Reader Task -----------------")
    logger.info(s"FileName:$fileName")
    logger.info(s"FormType:$formType")
    logger.info(s"debug:$debug")
    logger.info(s"Outputfile:$outputFile")
    logger.info("-------------------------------------------------------")

    logger.info(s"Fetching Data from Edgar file $fileName")

    val dataReaderStep = new DataReaderStep(fileName, formType, debug)
    val processor = new Form4Processor()
    val persister = new PlainTextPersister(s"$outputFile")    
    
    val form4Pipeline = new Pipeline(dataReaderStep, processor, persister)
    form4Pipeline.runPipeline(sparkContext, fileName)
    
  }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))

    if (args.size < 4) {
      println("Usage: spark-submit --class edgar.spark.EdgarFilingReaaderTask <fileName> <formType> <debug> <fileName>")
      System.exit(0)
    }

    val sparkContext = configureContext(args)
    startComputation(sparkContext, args)
    
  }

}