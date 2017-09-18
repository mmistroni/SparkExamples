import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.{ Level, Logger }
import SparkUtil._
import scala.xml._
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import utils.Step

/**
 * Edgar task to Read a Form4 spark-stord file, and classify each
 * company based on the number of transaction being executed
 * TODO: Add a decision tree in the mix
 *
 * Run the code like this:
 *
 * spark-submit --class EdgarFilingReaderTask sparkexamples.jar <fileName> <formType> <debugFlag>
 *
 */

case class Transaction(
  cik: String, purchase: Int, sale: Int, voluntaryTransaction: Int, award: Int, disposition: Int, exercise: Int,
  discretionaryTransaction: Int, exercise2: Int, conversion: Int, expirationShort: Int, expirationLong: Int, //H
  outOfMoneyExercise: Int, inTheMonyExercise: Int, bonaFideGift: Int, smallAcquisition: Int, willAcqOrDisp: Int, //W
  depositOrWithdrawal: Int, otherAcquisitionOrDisp: Int, transInEquitySwap: Int, dispositionChangeControl: Int) //U

object Transaction {
  def apply(cik: String, input: Map[Char, Int]): Transaction = {
    Transaction(cik,
      input.getOrElse('P', 0), input.getOrElse('S', 0), input.getOrElse('V', 0), input.getOrElse('A', 0),
      input.getOrElse('D', 0), input.getOrElse('F', 0), input.getOrElse('I', 0), input.getOrElse('M', 0),
      input.getOrElse('C', 0), input.getOrElse('E', 0), input.getOrElse('H', 0), input.getOrElse('O', 0),
      input.getOrElse('X', 0), input.getOrElse('G', 0), input.getOrElse('L', 0), input.getOrElse('W', 0),
      input.getOrElse('Z', 0), input.getOrElse('J', 0), input.getOrElse('K', 0), input.getOrElse('U', 0))
  }
}

class DataReaderStep(input: String, formType: String, sampleData: Boolean) extends Serializable {

  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.DataReaderSTep")

  def processData(sparkContext: SparkContext): Dataset[String] = {
    logger.info(s"fetching file:$input")
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val masterFile = sparkContext.textFile(input)
    val generatedRdd = normalize(masterFile, formType).toDF("fileName")
    val dataSet = generatedRdd.map(row => row.getAs[String](0))
    sampleData match {
      case true  => dataSet.sample(false, 0.0001, System.currentTimeMillis().toInt)
      case false => dataSet
    }

  }

  private def normalize(linesRdd: RDD[String], formType: String): RDD[String] = {
    // cik|xxx
    val filtered = linesRdd.map(l => l.split('|')).filter(arr => arr.length > 2).map(arr => (arr(0), arr(2), arr(4))).zipWithIndex
    val noHeaders = filtered.filter(tpl => tpl._2 > 0).map(tpl => tpl._1).filter(tpl => tpl._2 == formType).map(tpl => tpl._3)
    noHeaders.cache()
    noHeaders
  }
}

object ProcessorStep extends Serializable {
  // this processor should process the RDD, extract the data and return a DataFrame of Transactions
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Processor")
  val ftpClient = new utils.HttpsFtpClient("ftpclient")

  private def downloadFtpFile(fileName: String): Try[String] = {
    Try(ftpClient.retrieveFile(fileName))
  }

  def processData(sparkContext: SparkContext, inputDataSet: Dataset[String]): DataFrame = {
    import org.apache.spark.sql.Encoders
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val edgarXmlContent = inputDataSet.flatMap(item => downloadFtpFile(item).toOption)
      .map(parseXMLFile)
    logger.info(s"Obtained:$edgarXmlContent")
    val flatMapped = edgarXmlContent.flatMap { tpl => tpl._2.map(_.toString) }
    val res = flatMapped.groupByKey(identity).count
    res.foreach(tpl => println(tpl))
    null
  }

  private def parseXMLFile(fileContent: String): (String, String) = {
    if (fileContent.length() > 0) {
      val content = fileContent.substring(fileContent.indexOf("?>") + 2, fileContent.indexOf("</XML"))
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

  def toDataFrame(rdd: RDD[Transaction], sc: SparkContext): DataFrame = {
    println("Making a DataFrame now....")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF()
  }

}

object Persister extends Serializable {
  // Persist DataFrame 

  def persistDataFrame(inputDataFrame: DataFrame, sc: SparkContext): Int = {
    0
  }
}

object EdgarFilingReaderTask {

  val logger: Logger = Logger.getLogger("EdgarFilingReader.DataREader")

  def getFile(fileName: String, sc: SparkContext): RDD[String] = {
    println(s"fetching file:$fileName")
    sc.textFile(fileName)
  }

  def configureContext(args: Array[String]): SparkContext = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName("Spark Edgar Filing Reader task")
      .getOrCreate()
    session.conf.set("spark.driver.memory", "4g")
    // Replace with SparkContext
    session.sparkContext
  }

  def loggingDisabled = {

    disableSparkLogging
  }

  def main(args: Array[String]) {
    logger.info("Keeping only error logs..")
    loggingDisabled
    logger.info(s"Input Args:" + args.mkString(","))

    if (args.size < 3) {
      println("Usage: spark-submit --class edgar.spark.EdgarFilingReaaderTask <fileName> <formType> <debug>")
      System.exit(0)
    }

    val sparkContext = configureContext(args)
    val fileName = args(0)
    val formType = args(1)
    val debug = args(2).toBoolean

    logger.info("------------ Edgar Filing Reader Task -----------------")
    logger.info(s"FileName:$fileName")
    logger.info(s"FormType:$formType")
    logger.info(s"debug:$debug")
    logger.info("-------------------------------------------------------")

    logger.info(s"Fetching Data from Edgar file $fileName")
    // Parsing the file. we should get back only information we need
    val dataReaderStep = new DataReaderStep(fileName, formType, debug)

    val dataSet = dataReaderStep.processData(sparkContext)

    logger.info(s"Simpl.ified  has ${dataSet.count}")

    // Now downloading the data.... this  should result in a DataFrame of Transactions

    logger.info("Now processing the retrieved data..")
    ProcessorStep.processData(sparkContext, dataSet) //
    //processorStep.processData(sparkContext, dataSet)

    // And then persisting it somewhere so that we can re-read it

  }

}