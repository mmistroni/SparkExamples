package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._
import scala.io.Source
import scala.util.parsing.json._
import scala.xml._
import scala.util.Try
import common.Pipeline
import common.DataReaderStep
import common.DataFrameToCsvPersister
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}


/**
 * Edgar task to Analyze results of a Form13k between two periods
 *
 
 *  spark-submit --class edgar.EdgarFilingReaderForm13KAnalysis target/scala-2.11/spark-examples.jar Q 18
 *  to pick up all the files for any quarters of 2018
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

  
  def toDataFrame(sqlCtx:SQLContext, fileName:String):DataFrame = {
    import sqlCtx.implicits._
    val df = sqlCtx.read.format("csv")
            .option("header", true)
            .load(fileName)
    return df.withColumn("numfilings", col("count").cast(IntegerType)).drop("count")

  }
  
  
  def get_future_earnings_df(symbol:String):Unit = {

    val base_url = "https://us-central1-datascience-projects.cloudfunctions.net/future_earnings/$symbol"
    /**
     * sample output would be
     * {"Fiscal":{"0":"Dec 2019","1":"Dec 2020","2":"Dec 2021","3":"Dec 2022"},
     *  "Consensus":{"0":"26.96","1":"39.86","2":"60.49","3":"82.26"},"High EPS*":{"0":"32.84","1":"47.27","2":"69.29","3":"94.25"},
     *  "Low EPS*":{"0":"22.26","1":"31.2","2":"51.5","3":"70.28"},"Number of":{"0":"16","1
     * 
     * 
     * "https://us-central1-datascience-projects.cloudfunctions.net/future_earnings/amzn"
     *  val jsonString = Source.fromURL(baseUrl).mkString
     *  val res = parse(jsonString)
     *  val jsonData =res.extract[Map[String, Map[String, String]]]
     *  val fiscaMap = jsonData.get("Fiscal")
     *  val consensusMap = jsonData.get("Consensus")
     *  val consensus = consensusMap.getOrElse(Map[String,String]())
     *  
     *  scala> val timings = for {m <- fiscal.values} yield m
				timings: Iterable[String] = List(Dec 2019, Dec 2020, Dec 2021, Dec 2022)

				scala> val earnings  = for {m <- consensus.values} yield m
				earnings: Iterable[String] = List(26.66, 39.86, 60.49, 82.26)
				
				List(26.66, 39.86, 60.49, 82.26)
				
				al fields = List("Dec 2019", "Dec 2020", "Dec 2021", "Dec 2022")
				val schemaf = fields.map(fname => StructField(fname, StringType, true))
				val mappedRdd = rdd.map(items => Row(items))
				val rdd = sc.parallelize((26.66, 39.86, 60.49, 82.26))
				
				val df = sqlCtx.createDataFrame(rdd, schema)
				
				
     * 
     *  scala> val schemaf = fields.map(fname => StructField(fname, StringType, true))
schemaf: List[org.apache.spark.sql.types.StructField] = List(StructField(Dec 2019,StringType,true), StructField(Dec 2020,StringType,true), StructField(Dec 2021,StringType,true), StructField(Dec 2022,StringType,true))

scala> val schema = StructType(schemaf)
schema: org.apache.spark.sql.types.StructType = StructType(StructField(Dec 2019,StringType,true), StructField(Dec 2020,StringType,true), StructField(Dec 2021,StringType,true), StructField(Dec 2022,StringType,true)) 
     * 
     * 
     * 
    res = urllib.urlopen(base_url)
    json_dict = res.read() #json.load(res)
    try:
      df = pd.read_json(json_dict)
  
      cons  = df['Consensus'].values
      fisc = df['Fiscal'].values
  
      #print('Cons:{}, Fisc:{}'.format(type(cons), type(fisc)))
  
      converted = pd.DataFrame([cons], columns=fisc)
      converted['symbol'] = symbol
      return converted
    except Exception:
      print('exception in retrieving earnings for {}'.format(symbol))
  	*/
  }
  
  def fetchTickerAndNameFromCusip(cusip:String):(String, String) = {
    // prenepd 0 to cuisp
    // load jsob
    // add extra col sto dataframe
    // 
    val cusipLength = cusip.length()
    val zeroPrefix = "0" * (8 - cusipLength)
    val fullCusip = zeroPrefix + cusip
    logger.info(s"FEtching data for cusip:$fullCusip")
    val cusipUrl = s"https://us-central1-datascience-projects.cloudfunctions.net/cusip2ticker/$fullCusip"
    val jsonString = Source.fromURL(cusipUrl).mkString
    val json:Option[Any] = JSON.parseFull(jsonString)
    val map:Map[String,String] = json.get.asInstanceOf[Map[String, String]]
    (map.get("name").getOrElse(""), map.get("ticker").getOrElse(""))
  }
  
  
  def startComputation(sparkContext:SparkContext, args:Array[String]) = {
    
    val prefix = args(0)
    val suffix = args(1)
    val fileName1 = s"$prefix*$suffix"
    logger.info("------------ Form13HF Analysis -----------------")
    logger.info(s"FileName1:$fileName1")
    logger.info("-------------------------------------------------------")

    val sqlCtx = new SQLContext(sparkContext)
    import sqlCtx.implicits._
    
    val all_df = toDataFrame(sqlCtx, fileName1)
    val grouped = all_df.groupBy("Company").agg(sum("numfilings")).withColumn("counts", col("sum(numfilings)"))
                        .drop("sum(numfilings)")
    val sorted = grouped.orderBy(desc("counts"))
    val selected = sorted.limit(30)
    val tickerAndNameFunc: (String => (String, String)) =  cusip => fetchTickerAndNameFromCusip(cusip)
    val returnAndSharpeSchema =  StructType(Array(
                                    StructField(s"name", StringType, false),
                                    StructField(s"ticker", StringType, false)))
    val cusipToNameAndTickerFunc = udf(tickerAndNameFunc, returnAndSharpeSchema)
    
    val top20 = selected.withColumn(s"stats", cusipToNameAndTickerFunc(col("Company")))
                        .withColumn(s"name", col(s"stats.name"))
                        .withColumn(s"ticker", col(s"stats.ticker"))
                        .drop(col("stats"))
                
    
    
    val outputPrefix = new java.text.SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date())
    
    val persister = new DataFrameToCsvPersister(s"$outputPrefix-Form13HF-sorted-results.csv", coalesce=true)
    persister.persistDataFrame(sparkContext, sorted)
    
    val persister2 = new DataFrameToCsvPersister(s"$outputPrefix-Form13HF-results.with.cusip", coalesce=true)
    persister2.persistDataFrame(sparkContext, top20)
    
    
    
    // sort and filter
    
  }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    logger.info(s"Input Args:" + args.mkString(","))
    

    if (args.size < 1) {
      println("Usage: spark-submit --class edgar.EdgarFilingReaderForm13KAnalysis target/scala-2.11/spark-examples.jar <prefix> <suffix>")
      System.exit(0)
    }

    val sparkContext = configureContext(args)
    startComputation(sparkContext, args)
    
  }

}