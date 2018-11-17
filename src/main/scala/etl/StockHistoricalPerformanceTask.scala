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
 * spark-submit --class etl.StockHistoricalPerformanceTask target\scala-2.11\spark-examples.jar <file:///c:/Users/marco/SparkExamples2/SparkExamples> <3m>
 *    
 * 
 */
object StockHistoricalPerformanceTask {

  val logger: Logger = Logger.getLogger("EdgarFilingReaderWithPipeline.Task")

  def configureContext(args: Array[String]): SparkContext = {
    val session = SparkSession
      .builder()
      .appName("Spark HistoricalPerformaceTAsk")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    session.sparkContext
  }

  def computeSharesResults(sparkContext:SparkContext, sourceDir:String, period:String, debugPcnt:Double):DataFrame = {
    val fileName = s"$sourceDir/nyse.csv"
    logger.info("------------ Share Performance -----------------")
    val stockSchemaField = Seq("symbol", "name", "lastSale", "marketCap", "ipoYear", "sector",
                         "industry", "summaryQuote")
    val dataReaderStep = new StockDatasetReader(debugPcnt)
    val transformer = new StockPerformanceTransformer(period)
    val dSet  = dataReaderStep.extract(sparkContext, fileName)
    dSet.cache()
    
    val transformed = transformer.transform(sparkContext, dSet)
    transformed.cache()
    transformed
  }
  
  def computeSectorResults(sparkContext:SparkContext, sourceDir:String, period:String):DataFrame = {
    logger.info("Computing Sector..")
    val fileName = s"$sourceDir/nasdaq_sectors.csv"
    val dataReaderStep = new StockDatasetReader(1.0, Some(Seq("sector", "symbol")))
    val transformer = new StockTransformer(period)
    val dSet  = dataReaderStep.extract(sparkContext, fileName)
    dSet.cache()
    val transformed = transformer.transform(sparkContext, dSet)
                .withColumnRenamed(s"stats.performance$period", s"stats.sectorpf$period")
    transformed.cache()
    return transformed
  }
  
  def computeStats(sparkContext:SparkContext, sharesDf:DataFrame):DataFrame = {
    logger.info("------------ Company Stats -----------------")
    // diff approach, read the csv file from share and produce just few columns   
    new StatsTransformer().transform(sparkContext, sharesDf)
  }
  
  
  def computeHistStats(sparkContext:SparkContext, sharesDf:DataFrame, period:String):DataFrame = {
    logger.info("------------ Historical Stats -----------------")
    // diff approach, read the csv file from share and produce just few columns   
    new HistStatsTransformer(period).transform(sparkContext, sharesDf)
  }
  
  
  def startComputation(sparkContext:SparkContext, args:Array[String]) = {
    val formattedTime = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val sqlContext = new SQLContext(sparkContext)
    
    import sqlContext.implicits._
    val inputDir = args(0)
    val period = args(1)
    val debugPcnt = args(2).toFloat
    
    val suffix = new java.text.SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date())
    logger.info("--------------")
    logger.info(s"INput Dir:$inputDir")
    logger.info(s"Period:$period")
    logger.info(s"Suffix:$suffix")
    logger.info(s"DebugPcnt:$debugPcnt")
    val sharesDf = computeSharesResults(sparkContext, inputDir, period, debugPcnt).drop("summaryQuote")
    logger.info("----Before stats we ave:" + sharesDf.count())
    logger.info("Shares df schema:" + sharesDf.schema )
    logger.info()
    
    val statsDf = computeStats(sparkContext, sharesDf)
    logger.info("Statsdf.schema" + statsDf.schema)
    val histStats = computeHistStats(sparkContext, statsDf, period).drop("companyName")
                            .drop("marketCap")
    logger.info("HistStats has:" + histStats.count())
    logger.info(histStats.schema)
    //val sectorDf = computeSectorResults(sparkContext, inputDir, period).withColumnRenamed("symbol", "sectorTicker")
       //               .drop($"currentStockPrice")
    
    val joined = sharesDf.join(histStats, "symbol")
    //joined.cache()
    
    val loader = new StockPerformancePersister(s"Results\\Shares-$period-performance.results.$suffix")
    loader.load(sparkContext, joined)

   }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))

    if (args.size < 3) {
      println("Usage: spark-submit --class etl.StockHistoricalPerformanceTask target\\scala-2.11\\spark-examples.jar <inputDir> <period> <debugPcnt>")
      System.exit(0)
    }
    
    val sparkContext = configureContext(args)
    startComputation(sparkContext, args)
    
  }

}