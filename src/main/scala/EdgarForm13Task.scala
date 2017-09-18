

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.log4j.{ Level, Logger }
import SparkUtil._

case class Form13Tuple(companyName: String, count: Int)

/**
 * This task aggregates all form 13f to find out the top 10 companies
 * Fund Manageres invest to
 * All these files have been stored in a S3 bucket
 * Run file like this
 * spark-submit --packages org.apache.hadoop:hadoop-aws:2.6.0
 *              --class EdgarForm13Task target\scala-2.11\sparkexamples_2.11-1.0.jar *securit*txt <accessKey> <secretAccessKey>
 
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
      logger.info("Persisting DataFrame into Mongo..")
      SparkUtil.storeDataInMongo("mongodb://localhost:27017/test", tableName, inputData, appendMode = true)

    }
  }



object EdgarForm13Task {

  val logger: Logger = Logger.getLogger("EdgarAggregator.Form13")

  
  def getFile(filePattern: String, sc: SparkContext): RDD[String] = {
    logger.info(s"fetching file:$filePattern")
    val s3Url = s"s3://edgar-bucket-mm/$filePattern"
    logger.info(s"Fetching data from $s3Url")
    sc.textFile(s3Url)
  }

  def configureContext(args: Array[String]): SparkContext = {

    val session = SparkSession
      .builder()
      .master("local")
      .appName("Spark Reading Share CSV file")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    // Replace with SparkContext
    logger.info(s"AccessKey:${args(1)}, secretKey:${args(2)}")
    logger.info(session.sparkContext.hadoopConfiguration)
    val accessKey = session.conf.get("spark.hadoop.fs.s3.access.key")
    val secretKey = session.conf.get("spark.hadoop.fs.s3.secret.key")
    val hadoopConf = session.conf.get("spark.hadoop.fs.s3.impl")
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

  def storeInMongo(inputData: RDD[(String, Int)], sc: SparkContext): Unit = {

    logger.info("Storing in mongo/.///")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val toDataFrame = inputData.map { case (name, counts) => Form13Tuple(name, counts) }.toDF
    logger.info("########DataFrame has ;" + toDataFrame.count())
  }

  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    loggingDisabled
    logger.info(s"Input Args:" + args.mkString(","))

    val transformer = new DataTransformer()
    val persister = new DataFramePersister()
    
    if (args.size < 1) {
      println("Usage: spark-submit --class edgar.spark.EdgarAggregatorTask <filePattern> <accessKeyId> <secretAccessKey>")
    }

    val sparkContext = configureContext(args)
    val fileName = args(0)

    val rdd = getFile(fileName, sparkContext)

    val grouped = groupData(rdd)

    logger.info("Top 10  Securities..")

    val sortedRdd = grouped.sortBy(item => item._2, false)

    logger.info("Creating dataframe..")

    val dataFrame = transformer.transform(sortedRdd, sparkContext)
    
    logger.info("Persisting..")
    
    persister.persist(dataFrame, "FORM13f")
    
    logger.info("OUtta here..")
  }

}