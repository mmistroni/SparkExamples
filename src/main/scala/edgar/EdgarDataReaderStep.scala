package edgar
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._
import common._

class EdgarDataReaderStep(input: String, formType: String, sampleData: Boolean) extends Extractor[String, Dataset[EdgarFiling]] {

  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.DataReaderSTep")
  
  override def extract(sparkContext:SparkContext, input:String):Dataset[EdgarFiling] = {
    processData(sparkContext)
  }
  
  
  def processData(sparkContext: SparkContext): Dataset[EdgarFiling] = {
    logger.info(s"fetching file:$input")
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val masterFile = sparkContext.textFile(input)
    val typedRDD = normalize(masterFile, formType).toDF("fileName")
    val dataSet = typedRDD.map(row => row.getAs[EdgarFiling](0))
    
    trimData(dataSet)
  }
  
  private[edgar] def trimData(dataSet:Dataset[EdgarFiling]):Dataset[EdgarFiling] = {
    sampleData match {
      case true  => dataSet.sample(false, 0.0002, System.currentTimeMillis().toInt)
      case false => dataSet
    }
    
  }
  
  

  private def normalize(linesRdd: RDD[String], formType: String): RDD[EdgarFiling] = {
    val filtered = linesRdd.map(l => l.split('|')).filter(arr => arr.length > 2)
            .map(arr => EdgarFiling(arr(0), arr(1), arr(2), arr(4))).zipWithIndex
    val noHeaders = filtered.filter(tpl => tpl._2 > 0).map( tpl => tpl._1)
                  .filter(edgarFiling => edgarFiling.formType == formType)
    noHeaders.cache()
    logger.info("Found:" + noHeaders.count())
    noHeaders
  }
}



