package etl

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._
import org.apache.spark.sql.functions._
import scala.xml._
import scala.util.Try
import common.Pipeline
import common.DataReaderStep
import common.Loader

/**
 * Spark task to find stock performance over last 3 months
 * 
 * spark-submit --class etl.StockPerformanceResultTask target\scala-2.11\spark-examples.jar 
 * 				<file:///c:/Users/marco/SparkExamples2/SparkExamples/<3m performance> <file:///c:/Users/marco/SparkExamples2/SparkExamples/<3m performance> 
 *    
 * 
 */
object StockPerformanceResultTask {

  val logger: Logger = Logger.getLogger("StockPerformanceResult.Task")

  def configureContext(args: Array[String]): SparkSession = {
    val session = SparkSession
      .builder()
      .appName("Spark HistoricalPerformaceTAsk")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    session
  }

  def computeResults(sparkSession:SparkSession, firstFile:String, secondFile:String):DataFrame = {
    logger.info("------------ Share Performance -----------------")
    
    val colsToDrop = Seq("Name","LastSale","MarketCap","IPOYear","Summary Quote", 
                             "month6ChangePercent","month3ChangePercent", "month1ChangePercent", 
                             "day30ChangePercent", "day5ChangePercent")
    
    val df1 = sparkSession.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(firstFile)
                .drop(colsToDrop:_*)
    
                  
    val df2 = sparkSession.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(secondFile)
                .drop(colsToDrop:_*)
                .drop("industry")
                .drop("sector")
    
    logger.info("Joining.......")
    return df1.join(df2, ("Symbol"))
                
  }
  
  
  
  def startComputation(sparkSession:SparkSession, args:Array[String]) = {
    val formattedTime = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    
    val firstFile = args(0)
    val secondFile = args(1)
    
    val suffix = new java.text.SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date())
    logger.info("--------------")
    logger.info(s"FirstFile:$firstFile")
    logger.info(s"SecondFile:$secondFile")
    logger.info(s"Suffix:$suffix")
    val resultsDf = computeResults(sparkSession, firstFile, secondFile)
    val loader = new StockPerformancePersister(s"Shares-Performance-Result.$suffix", coalesce=true)
    loader.load(sparkSession.sparkContext, resultsDf)

   }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    logger.info(s"Input Args:" + args.mkString(","))
    if (args.size < 2) {
      println("Usage: spark-submit --class etl.StockPerformanceResultTask target\\scala-2.11\\spark-examples.jar <firstFile> <secondFile>")
      System.exit(0)
    }
    
    val sparkContext = configureContext(args)
    startComputation(sparkContext, args)
    
  }

}