package common
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._


class DataReaderStep(input: String, formType: String, sampleData: Boolean) extends Extractor[String, Dataset[String]] {

  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.DataReaderSTep")
  
  override def extract(sparkContext:SparkContext, input:String):Dataset[String] = {
    processData(sparkContext)
  }
  
  
  def processData(sparkContext: SparkContext): Dataset[String] = {
    logger.info(s"fetching file:$input")
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val masterFile = sparkContext.textFile(input)
    val generatedRdd = normalize(masterFile, formType).toDF("fileName")
    val dataSet = generatedRdd.map(row => row.getAs[String](0))
    
    trimData(dataSet)
  }
  
  private[common] def trimData(dataSet:Dataset[String]):Dataset[String] = {
    sampleData match {
      case true  => dataSet.sample(false, 0.0002, System.currentTimeMillis().toInt)
      case false => dataSet
    }
    
  }
  
  

  private def normalize(linesRdd: RDD[String], formType: String): RDD[String] = {
    val filtered = linesRdd.map(l => l.split('|')).filter(arr => arr.length > 2).map(arr => (arr(0), arr(2), arr(4))).zipWithIndex
    val noHeaders = filtered.filter(tpl => tpl._2 > 0).map(tpl => tpl._1).filter(tpl => tpl._2 == formType).map(tpl => tpl._3)
    noHeaders.cache()
    logger.info("Found:" + noHeaders.count())
    noHeaders
  }
}

class DebuggableDataReaderStep(input: String, formType: String, sampleSize: Float) 
          extends DataReaderStep(input, formType, true) {

  private[common] override def trimData(dataSet:Dataset[String]):Dataset[String] = {
    dataSet.sample(false, sampleSize, System.currentTimeMillis().toInt)
    
  }
}


