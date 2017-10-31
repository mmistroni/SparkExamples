package edgar

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import scala.util._
import scala.xml._

/**
 * Transformer to parse form4 filing
 */
class Form13KFileParser extends EdgarFilingProcessor[String]  {
  
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Form13KFileParser")
  
  override def edgarEncoder = org.apache.spark.sql.Encoders.kryo[String]
  override def parseXML = content => parseForm13k(content)
  
  def parseForm13k(fileContent: String): String = {
    if (fileContent.length() > 0 && fileContent.indexOf("<informationTable") >= 0) {
      val informationTable = fileContent.substring(fileContent.indexOf("<informationTable"),
        fileContent.indexOf("</informationTable>") + 20)
      val infoTableXml = XML.loadString(informationTable)
      val purchasedShares = infoTableXml \\ "nameOfIssuer"
      val holdingSecurities = infoTableXml \\ "nameOfIssuer"
      val resList = holdingSecurities.map(_.text.toUpperCase()).distinct 
      
      val result = resList.mkString(",")
      // So this will return a list of companies included in the form 13k
      // if instead we return a list of (name, count)
      result
    } else {
      ""
    }
  }
}

class Form13KAggregator extends Transformer[Dataset[String], Dataset[Form4Filing]] {
  // this processor should aggregate what has been extracted from the file
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Form13KAggregator")
  
  
  override def transform(sparkContext:SparkContext, inputDataSet:Dataset[String]):Dataset[Form4Filing] = {
    // and at the end we need to reduce them to a company, # investors
    implicit val encoder = org.apache.spark.sql.Encoders.kryo[String]
    logger.info("----- aggregating----")
    println("We got:" + inputDataSet.count())
    val flatMapped = inputDataSet.flatMap(row => row.split(","))
    
    // and then we group. that means that from a list of companies we need to move to a
    // list of (company, count)
    val res = flatMapped.groupByKey(identity).count
    toForm4Filing(res)
  }
  
  private def toForm4Filing(inputDs:Dataset[(String, Long)]):Dataset[Form4Filing] = {
    implicit val formEncoder = org.apache.spark.sql.Encoders.kryo[Form4Filing]
    inputDs.map(tpl => Form4Filing(tpl._1, tpl._2))  
  }
  
  
}

class Form13KProcessor extends Transformer[Dataset[String], Dataset[Form4Filing]] { 
  private val form13KFileParser = new Form13KFileParser();
  private val form13KAggregator = new Form13KAggregator();
  
  private[edgar] def parseFunction(implicit sparkContext:SparkContext) =
      (inputDataSet:Dataset[String]) => form13KFileParser.transform(sparkContext, inputDataSet)
  
  private[edgar] def aggregateFunction(implicit sparkContext:SparkContext) = 
      (inputDataSet:Dataset[String]) => form13KAggregator.transform(sparkContext, inputDataSet)
      
  override def transform(sc: SparkContext, inputDataSet: Dataset[String]):Dataset[Form4Filing] = {
    implicit val sparkContext = sc
    val composed = parseFunction andThen aggregateFunction
    composed(inputDataSet)
  }
    
  
}

/**
 * 
 * val rdd = sc.parallelize(Seq(Seq("a","b","c"), Seq("c","d","e")))
 * 
 * 
 */
