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
import common.DebuggableDataReaderStep

/**
 * Edgar task to Read a Form13HF spark-stord file, and classify each
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
 * spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1 
 *    --class edgar.EdgarFilingReaderTaskWithPipeline 
 *    target\scala-2.11\sparkexamples_2.11-1.0.jar 
 *    s3a://ec2-bucket-mm-spark/master.idx 4 true s3a://ec2-bucket-mm-spark/Form4Output.txt
 * 
 * 
 * 
 */

class StockPerformancePersister(fileName:String, coalesce:Boolean=true , sortDf:Boolean=true)  {
  @transient
  val logger: Logger = Logger.getLogger("StockPerformancePersister")
  
  
  
  def coalesceDs(inputDs:DataFrame):DataFrame = {
    if (coalesce) {
      inputDs.coalesce(1)
    } else inputDs
  } 
  
  def persistDataFrame(sc: SparkContext, inputDataSet:DataFrame, sortDf:Boolean=true): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val sortedDf = sortDf match {
      case true => inputDataSet.orderBy($"count".desc)
      case _    => inputDataSet
    }
    logger.info(s"Persisting data to text file: $fileName.Coalescing?$coalesce")
    coalesceDs(sortedDf).write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite).option("header", "true").save(fileName) //rdd.saveAsTextFile(fileName)
    
  }
}



object EdgarFilingReaderForm13K {

  val logger: Logger = Logger.getLogger("EdgarFilingReaderForm13K.Task")

  def configureContext(args: Array[String]): SparkContext = {
    val session = SparkSession
      .builder()
      .appName("Spark Edgar Form13hf Filing Reader task")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    session.sparkContext
  }

  def createForm13FileName(fileName:String):String = {
    val suffix = fileName.substring(fileName.lastIndexOf("/")+1)
    val dirName = suffix.replace("-","").replace(".txt","")
    fileName.substring(0, fileName.lastIndexOf("/")) + "/" + dirName + "/" + suffix
  }
  
  def startComputation(sparkContext:SparkContext, args:Array[String]) = {
    
    val fileName = args(0)
    val debug = args(1).toFloat
    val outputFile = args(2)

    logger.info("------------ Edgar Filing Reader Task -----------------")
    logger.info(s"FileName:$fileName")
    logger.info(s"debug:$debug")
    logger.info(s"Outputfile:$outputFile")
    logger.info("-------------------------------------------------------")
    logger.info(s"Fetching Data from Edgar file $fileName")

    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val dataReaderStep = new DebuggableDataReaderStep(fileName, "13F-HR", debug)
    
    
    val processor = new Form13KFileParser()
    val persister = new DebugPersister(s"$outputFile")    
    
    val allForm13 = dataReaderStep.extract(sparkContext, fileName)
    val form13Transformed = allForm13.map(createForm13FileName)
    
    allForm13.take(10).foreach (println )
    
    logger.info("Now processing.....${allForm13.count}..");
    val withContent = processor.transform(sparkContext, form13Transformed)
    
    val aggregated = withContent.flatMap(row => row.split(","))
    
    logger.info("AFter Aggregation...grouping..")
                                 
    val res = aggregated.groupByKey(identity).count
                        .toDF("company", "count")
                        
    
                        
    logger.info("Now outputitting.")
    //implicit val formEncoder = org.apache.spark.sql.Encoders.kryo[Form4Filing]
    //val output = res.map(tpl => Form4Filing(tpl._1, tpl._2))
    new StockPerformancePersister(outputFile).persistDataFrame(sparkContext, res   )
                         
  }
  
  
  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))

    if (args.size < 3) {
      println("Usage: spark-submit --class edgar.EdgarFilingReaderForm13Task target/scala-2.11/sparkexamples.jar <inputFileName>  <debugPercentage> <outputFileName>")
      System.exit(0)
    }
    // THIS IS THE FILE TO RUN TO FETCH FORM 13K

    val sparkContext = configureContext(args)
    startComputation(sparkContext, args)
    
  }

}