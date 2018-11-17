package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import scala.util._
import scala.xml._
import utils.HttpsFtpClient
import common.Transformer


// This class contains template steps that applies to all form types being
// downloaded
abstract class EdgarProcessor[A]  extends Transformer[Dataset[String], Dataset[A]] {
  @transient
  val logger: Logger = Logger.getLogger("EdgarProcessor.GenericEdgarProcessor")
  
  
  private[edgar] def downloadFtpFile(fileName: String): Try[String] = {
    Try {
      val res = HttpsFtpClient.retrieveFile(fileName)
      res
    }
  
  }
  
  override def transform(sparkContext: SparkContext, inputDataSet: Dataset[String]):Dataset[A] = {
    processData(sparkContext, inputDataSet)
  }
  
  def processData(sparkContext:SparkContext, inputDataSet:Dataset[String]):Dataset[A]
      
}

/** 
 *  Abstracts a filing processor that parses a filing and return a result
 */
abstract class EdgarFilingProcessor[A] extends EdgarProcessor[A] {
  
  
  
  def processData(sparkContext: SparkContext, inputDataSet: Dataset[String]): Dataset[A] = {
    import org.apache.spark.sql.Encoders
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    logger.info("About to process an edgar filing,....")
    val edgarXmlContent = inputDataSet.flatMap(item => downloadFtpFile(item).toOption)
    logger.info("aBout to parse" + edgarXmlContent.count())
    
    parseFile(sqlContext, edgarXmlContent)
    
  }
  
  def parseFile(sqlContext:SQLContext, inputDataSet:Dataset[String]):Dataset[A] = {
     // Standard parsing file. Should return
    import sqlContext.implicits._
    implicit val tupleEncoder = edgarEncoder
    inputDataSet.map(parseXML) 
  }  
    
  def parseXML:String => A
  
  def edgarEncoder:Encoder[A] 

  
}


