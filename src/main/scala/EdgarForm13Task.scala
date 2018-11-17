

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql.{ DataFrame, SparkSession, SaveMode }
import org.apache.log4j.{ Level, Logger }
import utils.SparkUtil._

case class Form13Tuple(companyName: String, count: Int)

/**
 * This task aggregates all form 13f to find out the top 10 companies
 * Fund Manageres invest to
 * All these files have been stored in a S3 bucket
 * Run file like this
 * spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1 --class EdgarForm13Task target\scala-2.11\spark-examples.jar 2016*01*securit*txt,2016*02*securit*txt,2016*03*securit*txt,  Q117
 * 
 * 
 * */
class DataTransformer extends java.io.Serializable {
    val logger: Logger = Logger.getLogger("EdgarAggregator.DataTransformer")

    
    def transform(inputData: RDD[(String, Int)], sc: SparkContext): DataFrame = {
      logger.info("Transforming RDD into DataFrame")
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      inputData.map { case (name, counts) => Form13Tuple(name, counts) }.toDF
    }

  }

  class DataFramePersister extends java.io.Serializable {
    val logger: Logger = Logger.getLogger("EdgarAggregator.DataPersister")


    def persist(inputData: DataFrame, tableName:String): Unit = {
      logger.info("Persisting DataFrame to $tableName")
      logger.info("DataFrame has:" + inputData.count() + " items");
      inputData.write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite).option("header", "true").save(s"$tableName")
      
    }
  }



object EdgarForm13Task {

  val logger: Logger = Logger.getLogger("EdgarAggregator.Form13")

  
  def getFile(filePattern: String, sc: SparkContext): RDD[String] = {
    logger.info(s"fetching file:$filePattern")
    
    val s3Url = s"s3://edgar-bucket-mm/"
    
    val listOfArgs = filePattern.split(",").map { item => s3Url + item }.mkString(",")
    
    logger.info(s"Fetching data from $listOfArgs")
    sc.textFile(listOfArgs)
  }

  def configureContext(args: Array[String]): SparkContext = {

    val session = SparkSession
      .builder()
      .master("local")
      .appName("Spark Reading Share CSV file")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    // Replace with SparkContext
    //logger.info(s"AccessKey:${args(1)}, secretKey:${args(2)}")
    logger.info(session.sparkContext.hadoopConfiguration)
    session.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    session.sparkContext

  }

  def loggingDisabled = {

    disableSparkLogging
  }

  def groupData(input: RDD[String]): RDD[(String, Int)] = {
    val mapped = input.map(item => (item, 1))
    mapped.reduceByKey(_ + _)
  }

  

  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    loggingDisabled
    logger.info(s"Input Args:" + args.mkString(","))

    /* Use this pattern to load multiple files
     * sc.textFile("/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file")
     */
    
    
    
    val transformer = new DataTransformer()
    val persister = new DataFramePersister()
    
    if (args.size < 2) {
      println("Usage: spark-submit --class edgar.spark.EdgarAggregatorTask <filePattern> <outputFileName>")
    }

    args.foreach { x => println(x) }
    
    val sparkContext = configureContext(args)
    val fileName = args(0)
    val output = args(1)

    val rdd = getFile(fileName, sparkContext)

    val grouped = groupData(rdd)

    logger.info("Top 10  Securities..")

    val sortedRdd = grouped.sortBy(item => item._2, false)

    logger.info("Creating dataframe..")

    val dataFrame = transformer.transform(sortedRdd, sparkContext)
    
    logger.info(s"Persisting..to $output ")
    
    persister.persist(dataFrame, s"$output-Results.csv")
    
    logger.info("OUtta here..")
  }

}