package ml

import common.Extractor
import common.BaseDataFrameReader
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ StructField, StringType, StructType, IntegerType, BooleanType }

/**
 * DataFrame Reader specific for mammograpic data
 * "file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/mammographic_masses.data.txt"
 */

  
class MammographicDataFrameReader extends BaseDataFrameReader {
  
  override def generateDataFrameWithSchema(inputDf: DataFrame): DataFrame = {
    val fieldNames = Seq("BI-RADS", "Age", "Shape", "Margin", "Density", "Severity")
    val dfWithHeader = inputDf.toDF(fieldNames: _*)
    // find a way to modify all in one programmatically
    fieldNames.foldLeft(dfWithHeader)((dfWithHeader, colName) => {
      dfWithHeader.withColumn(colName, dfWithHeader.col(colName).cast(IntegerType))
    })
    
    

  }
  

}