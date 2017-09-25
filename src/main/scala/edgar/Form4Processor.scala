package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import scala.util._
import scala.xml._
import utils.HttpsFtpClient

class Form4Processor extends Transformer[Dataset[String], Dataset[(String, Long)]]  {
  // this processor should process the RDD, extract the data and return a DataFrame of Transactions
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Processor")
  
  //val ftpClient:utils.HttpsFtpClient = new utils.HttpsFtpClient("ftpclient")
  
  
  private def downloadFtpFile(fileName: String): Try[String] = {
    Try(HttpsFtpClient.retrieveFile(fileName))
  }

  override def transform(sparkContext: SparkContext, inputDataSet: Dataset[String]):Dataset[(String, Long)] = {
    processData(sparkContext, inputDataSet)
  }
  
  
  def processData(sparkContext: SparkContext, inputDataSet: Dataset[String]): Dataset[(String, Long)] = {
    import org.apache.spark.sql.Encoders
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val edgarXmlContent = inputDataSet.flatMap(item => downloadFtpFile(item).toOption)
    
    val mapped = edgarXmlContent.map(parseXMLFile)
    val flatMapped = mapped.flatMap { tpl => tpl._2.map(_.toString) }
    val res = flatMapped.groupByKey(identity).count
    res
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

}
