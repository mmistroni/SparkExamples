package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._
import scala.xml._
import scala.util.Try
import common.Pipeline
import common.DataReaderStep
import common.PlainTextPersister

/**
 * Edgar task to Analyze results of a Form13k between two periods
 *
 * * spark-submit --class edgar.EdgarFilingReaderForm13Analysis 
 *                sparkexamples.jar <fileNamePeriod1> <fileNamePeriod2>
 
 * 
 * 
 * 
 */


object EdgarFilingReaderForm13KAnalysis {

  val logger: Logger = Logger.getLogger("EdgarFilingReaderForm13K.Task")

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

  
  def toDataFrame(sqlCtx:SQLContext, fileName:String, renamedCol:String):DataFrame = {
    import sqlCtx.implicits._
    val newNames = Seq("Company", "Counts")
    val df = sqlCtx.read.format("csv")
            .option("header", false)
            .load(fileName)
    val renamedDf = df.toDF(newNames: _*)
      .withColumn(renamedCol, $"Counts".cast(sql.types.IntegerType)).drop("Counts")
    renamedDf.groupBy("Company").agg(sum(renamedCol))
  }
  
  
  
  def startComputation(sparkContext:SparkContext, args:Array[String]) = {
    
    val fileName1 = args(0)
    val fileName2 = args(1)
    logger.info("------------ Form13HF Analysis -----------------")
    logger.info(s"FileName1:$fileName1")
    logger.info(s"FileName2:$fileName2")
    logger.info("-------------------------------------------------------")

    val sqlCtx = new SQLContext(sparkContext)
    import sqlCtx.implicits._
    
    val firstDfGrouped = toDataFrame(sqlCtx, fileName1, "CountPrev")
    val secondDfGrouped = toDataFrame(sqlCtx, fileName2, "CountNext")
    
    val joined = firstDfGrouped.join(secondDfGrouped, "Company")
    
    
    val res = firstDfGrouped.join(secondDfGrouped, Seq("Company"))
              .withColumn("Diff", $"sum(CountNext)" - $"sum(CountPrev)").orderBy(desc("Diff"))
    
    val ordered = res.orderBy(desc("Diff")).take(30).foreach(println)
    
    
    //val persister = new PlainTextPersister("Form13HF-results.csv", coalesce=true)
    //persister.persistDataFrame(sparkContext, ordered)
    
    
    // sort and filter
    
  }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))

    if (args.size < 2) {
      println("Usage: spark-submit --class edgar.EdgarFilingReaderForm13KAnalysis target/scala-2.11/spark-examples.jar <prevFileName> <latestFileName>")
      System.exit(0)
    }

    val sparkContext = configureContext(args)
    startComputation(sparkContext, args)
    
  }

}