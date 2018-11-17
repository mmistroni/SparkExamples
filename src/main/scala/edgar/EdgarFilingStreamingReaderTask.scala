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
import common.{PlainTextPersister, NoOpPersister}
import org.apache.spark.streaming._


object EdgarFilingStreamingReaderTaskNoPipeline {

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

  
  def parseForm4(fileContent: String): (String, String) = {
    if (fileContent.length() > 0) {
      val start = fileContent.indexOf("<ownershipDocument>")
      val end = fileContent.indexOf("</XML")
      println(s"Substring from $start to $end")
      val content = fileContent.substring(start, end)
      val xml = XML.loadString(content)
      val formType = xml \\ "submissionType"
      val issuerName = xml \\ "issuerName"
      val issuerCik = xml \\ "issuerCik"
      val reportingOwnerCik = xml \\ "rptOwnerCik"
      val transactionCode = xml \\ "transactionCode"
      (issuerCik.text, transactionCode.text)

    } else {
      ("Unknown", "-1")
    }
    
  }
  
  
  def startComputation(sparkSession: SparkSession, args: Array[String]) = {

    logger.info("------------ Edgar Streaming Reader Task -----------------")
    logger.info(s"Listening to directory:${args(0)}")
    logger.info("-------------------------------------------------------")

    
    import sparkSession.implicits._
    
    val ssc = new org.apache.spark.streaming.StreamingContext(sparkSession.sparkContext,Seconds(10))
    val lines = ssc.textFileStream(args(0))
    // https://stackoverflow.com/questions/46198243/how-to-continuously-monitor-a-directory-by-using-spark-structured-streaming
    
    //val mapped  = lines.map { content => parseForm4(content) }
    // Check this link to see how to process XML files in spark streamimg
    // https://stackoverflow.com/questions/46004610/how-to-read-streaming-data-in-xml-format-from-kafka
    
    // we are only interested in certain entries, such as trans code and issuerCik, so we could just
    // parse these lines
    println("Ablut to execute..");
    val reduced = lines.reduce(_ + _).map(content => parseForm4(content))
    println("----------- reduced ----")
    reduced.print() // Reducing seems towork. will process what we got and see.
    
    //mapped.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()    
    // TODO: Add Process that will read from a directory
    
    // Alternatively, use readWholeTexTFiles to read each downloaded file as  a whole
    /**
    	Consider following approaches
    	- 1. store each file then read them using sc.wholeTextFile (once processed). You can store file types by directory
    	  2. store XML content / or whole text file, do a Reduce and process
    **/
    
    
    
    
    
  }

  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    //disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))
    
    // optimize persistence. check article below
    // https://stackoverflow.com/questions/46754432/how-to-tune-spark-job-on-emr-to-write-huge-data-quickly-on-s3
    // alternative store data on HDFS
    // https://www.slideshare.net/AmazonWebServices/spark-and-the-hadoop-ecosystem-best-practices-for-amazon-emr-80673533
    // csv route https://stackoverflow.com/questions/51605098/spark-structured-streaming-read-file-from-nested-directories
    
    
    if (args.size < 1) {
      println("Usage: spark-submit --packages com.databricks:spark-xml_2.11:0.4.1 --class edgar.EdgarFilingStreamingReaderTaskNoPipeline target\\scala-2.11\\spark-examples.jar file:///c:/temp ")
      System.exit(0)
    }

    val sparkSession = configureContext(args)
    startComputation(sparkSession, args)

  }

}