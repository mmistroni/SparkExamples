package etl

import common.Extractor
import common.BaseDataFrameReader
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ StructField, StringType, StructType, IntegerType, BooleanType }


/**
 * Reader of stock data in csv format
 */
class StockDatasetReader(debugPcnt:Double ,fieldNames:Option[Seq[String]]=None) extends Extractor[String, DataFrame] with utils.LogHelper{
  
  
  
  def extract(sparkContext: SparkContext, inputData: String): DataFrame = {
    logger.info(s"REading from $inputData")
    val sqlCtx = new SQLContext(sparkContext)
    import sqlCtx.implicits._
    val df = sqlCtx.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputData).sample(false, debugPcnt)
    
      fieldNames match {
      case Some(fieldNames) => df.toDF(fieldNames:_*)
      case _ => df
    }
      
  }
  
  
  
}