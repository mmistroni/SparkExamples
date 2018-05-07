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
import common.DataReaderStep
import utils.HttpsFtpClient

/**
 * Edgar task to Read a Form4 spark-stord file, and classify each
 * company based on the number of transaction being executed
 * TODO: Add a decision tree in the mix
 * Hadoop 2.7.1 is needed for accessing s3a filesystem
 * Run the code like this:
 *
 * * spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:2.2.0,org.apache.hadoop:hadoop-aws:2.7.1
 *                --class edgar.EdgarFilingReaderTaskNoPipeline
 *                target/scala-2.11/sparkexamples.jar  <formType> <debugFlag> <outputFile>
 *                For saving in S3, use URI such as s3://<bucketName/<fileName>
 *
 * to read the parquet file simply do  sqlContext.read.parquet("/tmp/testParquet")
 *
 * EdgarFilingReaderTaskWithPipeline.scala
 *
 *
 *
 */

object EdgarFilingReaderTaskNoPipeline {

  val logger: Logger = Logger.getLogger("EdgarFilingReaderWithPipeline.Task")

  private def createXml(it: Iterator[String], acc: String, itemFound: Int): String = {
    if (itemFound == 0) acc
    else {
      val curr = it.next()
      curr.indexOf("ownershipDocument") match {
        case found if found > 0 => createXml(it, acc.concat(curr), itemFound - 1)
        case _                  => if (itemFound < 2) createXml(it, acc.concat(curr), itemFound) else createXml(it, acc, itemFound)
      }
    }
  }

  private def form4Function(lines: Iterator[String]): String = {
    createXml(lines, "", 2)
  }

  private[edgar] def downloadFtpFile(fileName: String): Option[String] = {
    val result = Try {
      val lines = HttpsFtpClient.retrieveFileStream(fileName)
      createXml(lines, "", 2)

    }
    logger.info(s"$fileName downloaded")
    result.toOption
  }

  def parseForm4(fileContent: String): (String, String) = {
    if (fileContent.length() > 0) {
      val xml = XML.loadString(fileContent)
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

  def configureContext(args: Array[String]): SparkSession = {
    val session = SparkSession
      .builder()
      //.master("local")
      .appName("Spark Edgar Filing Reader task")
      .getOrCreate()
    session.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    session.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    session
  }

  def parseFile(input: String, sparkContext: SparkContext, formType: String, samplePercentage: Double): Dataset[String] = {
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val masterFile = sparkContext.textFile(input)
    val dataSet = normalize(masterFile, formType).toDF("fileName").as[String]
    val sampleDataSet = dataSet.sample(false, samplePercentage, System.currentTimeMillis().toInt)
    sampleDataSet.cache()
    sampleDataSet

  }

  def normalize(linesRdd: RDD[String], formType: String): RDD[String] = {
    val filtered = linesRdd.map(l => l.split('|')).filter(arr => arr.length > 2).map(arr => (arr(0), arr(2), arr(4))).zipWithIndex
    val noHeaders = filtered.filter(tpl => tpl._2 > 0).map(tpl => tpl._1).filter(tpl => tpl._2 == formType).map(tpl => tpl._3)
    noHeaders.cache()
    logger.info("Found:" + noHeaders.count())
    noHeaders
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

    val sampleDataSet = parseFile(fileName, sparkSession.sparkContext, formType, samplePercentage)
    logger.info(s"We have found ${sampleDataSet.count()} available filings")

    val downloadFunc: String => Option[String] = item => downloadFtpFile(item)
    val parseContentFun: Option[String] => (String, String) = op => op match {
      case Some(content) => parseForm4(content)
      case _             => ("", "-1")
    }
    val filingTypeFun: (String, String) => String = (cik, filingType) => filingType

    // dowloading file contents
    val form4Contents = sampleDataSet.map { downloadFunc }
    form4Contents.cache()
    // extracting data from contents , that will map to company, form type
    logger.info("####.......Now extracting file content......")

    val results = form4Contents.map(parseContentFun)
    results.cache()

    logger.info("Now Grouping...")
    // now we might want to ignore  the company and focus just on the filings
    import sparkSession.implicits._
    logger.info("..... Now Grouping....")
    val res = results.flatMap { tpl => tpl._2.map(_.toString) }.groupByKey(identity).count
    res.cache()

    val s3File = s"s3a://ec2-bucket-mm-spark/$outputFile"

    logger.info("Now Persisting:" + s3File);

    val filePersister = new PlainTextPersister(s3File);
    filePersister.persistDataFrame(sparkSession.sparkContext, res);

    // you can use reduce to combine multiple functions together

  }

  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    //disableSparkLogging
    logger.info(s"Input Args:" + args.mkString(","))

    if (args.size < 4) {
      println("Usage: spark-submit --class edgar.spark.EdgarFilingReaderTaskNoPipeline <fileName> <formType> <samplePercentage> <outputfile> ")
      System.exit(0)
    }

    val sparkSession = configureContext(args)
    startComputation(sparkSession, args)

  }

}