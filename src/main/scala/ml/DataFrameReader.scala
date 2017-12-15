package ml

import common.Extractor
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ StructField, StringType, StructType, IntegerType, BooleanType }

/**
 * DataFrame Reader specific for mammograpic data
 * "file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/mammographic_masses.data.txt"
 */
abstract class BaseDataFrameReader extends Extractor[String, DataFrame] {
  @transient
  val logger: Logger = Logger.getLogger("MLPipeline.DataFrameReader")

  private[ml] def generateDataFrameWithSchema(inputDf:DataFrame):DataFrame
  
  def extract(sparkContext: SparkContext, inputData: String): DataFrame = {
    logger.info(s"REading from $inputData")
    val sqlCtx = new SQLContext(sparkContext)
    import sqlCtx.implicits._

    val baseDataFrame = sqlCtx.read.csv(inputData)
    generateDataFrameWithSchema(baseDataFrame)

  }
}
  
class MammographicDataFrameReader extends BaseDataFrameReader {
  
  private[ml] override def generateDataFrameWithSchema(inputDf: DataFrame): DataFrame = {
    val fieldNames = Seq("BI-RADS", "Age", "Shape", "Margin", "Density", "Severity")
    val dfWithHeader = inputDf.toDF(fieldNames: _*)
    // find a way to modify all in one programmatically
    fieldNames.foldLeft(dfWithHeader)((dfWithHeader, colName) => {
      dfWithHeader.withColumn(colName, dfWithHeader.col(colName).cast(IntegerType))
    })
    
    

  }
  

}