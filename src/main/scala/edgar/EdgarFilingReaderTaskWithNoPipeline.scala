package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.spark.sql.Encoders
import utils.SparkUtil._
import scala.xml._
import scala.util.Try
import common.Pipeline
import common.DebuggableDataReaderStep
import utils.HttpsFtpClient


object EdgarFilingReaderTaskNoPipeline {

  val logger: Logger = Logger.getLogger("EdgarFilingReaderWithPipeline.Task")

  def configureContext(args: Array[String]): SparkSession = {
    val session = SparkSession
      .builder()
      .appName("Spark Edgar Filing Reader task")
      .getOrCreate()
    session.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    session
  }

   // checklut this thread
  // https://stackoverflow.com/questions/47174344/using-spark-to-download-file-distributed-and-upload-to-s3
  // also check this for setting master
  // https://stackoverflow.com/questions/33504798/how-to-find-the-master-url-for-an-existing-spark-cluster

  def startComputation(sparkSession: SparkSession, args: Array[String]) = {

    implicit val optionEncoder = org.apache.spark.sql.Encoders.kryo[Option[String]]
    implicit val tplEncoder = org.apache.spark.sql.Encoders.kryo[(String, String)]

    val fileName = args(0)
    val formType = args(1)
    val samplePercentage = args(2).toFloat
    val outputFile = args(3)

    
    
    logger.info("------------ Edgar Filing Reader Task -----------------")
    logger.info(s"FileName:$fileName")
    logger.info(s"FormType:$formType")
    logger.info(s"samplePercentage:$samplePercentage")
    logger.info(s"downloadFile:$outputFile")
    logger.info("-------------------------------------------------------")

    logger.info(s"Fetching Data from Edgar file $fileName")

    samplePercentage match {
      case low if low > 0.005 => logger.info("disabling spark logging");disableSparkLogging
      case _ => logger.info("Spark logging enabled..")
    }
    
    
    val dataReaderStep = new DebuggableDataReaderStep(fileName, formType, samplePercentage)
    val processor = new Form4Processor()
    val persister = new PlainTextPersister(s"$outputFile")    
    
    val form4Pipeline = new Pipeline(dataReaderStep, processor, persister, 
                                      sparkSession.sparkContext)
    form4Pipeline.runPipeline2(fileName)
    
        
    //https://stackoverflow.com/questions/22343918/how-do-i-use-hdfs-with-emr    
    
    // you can use reduce to combine multiple functions together

  }

  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    //disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))
    
    // optimize persistence. check article below
    // https://stackoverflow.com/questions/46754432/how-to-tune-spark-job-on-emr-to-write-huge-data-quickly-on-s3
    // alternative store data on HDFS
    // https://www.slideshare.net/AmazonWebServices/spark-and-the-hadoop-ecosystem-best-practices-for-amazon-emr-80673533
    if (args.size < 4) {
      println("Usage: spark-submit --class edgar.spark.EdgarFilingReaderTaskNoPipeline <fileName> <formType> <samplePercentage> <outputfile> ")
      System.exit(0)
    }

    val sparkSession = configureContext(args)
    startComputation(sparkSession, args)

  }

}