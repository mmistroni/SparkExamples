package edgar

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import scala.util._
import scala.xml._
import common.Transformer
import org.apache.spark.sql.types.{ StructField, StringType, StructType , IntegerType, BooleanType}
import org.apache.spark.sql._
import utils.SparkUtil
import org.apache.spark.sql.functions._



/**
 * Transformer to perform some data cleaning
 */
class DataCleaningTransformer extends Transformer[DataFrame,DataFrame]  {
  @transient
  val logger: Logger = Logger.getLogger("MLPipeline.DataCleaning")

  
  private def findMostCommonValue[A](colName:String, df:DataFrame)(f:Row =>A):A = {
    println(s"Finding most common value for $colName")
    val result = df.groupBy(colName).count().sort(desc("count")).first()
    f(result)    
  }
  
  private def cleanUpData(inputDataFrame:DataFrame):DataFrame = {
    null
    
    /**
    BI-RADS assessment:    2
    - Age:                   5
    - Shape:                31
    - Margin:               48
    - Density:              76
    **/
    
    
    
    
  }
  
  
  override def transform(sc: SparkContext, inputDataSet: DataFrame):DataFrame = {
    implicit val sparkContext = sc
    cleanUpData(inputDataSet)
  }
    
  
}

