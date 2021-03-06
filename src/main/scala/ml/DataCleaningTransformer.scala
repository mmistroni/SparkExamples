package ml

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
class DataCleaningTransformer(colNames:Seq[String]) extends Transformer[DataFrame,DataFrame]  {
  @transient
  val logger: Logger = Logger.getLogger("MLPipeline.DataCleaning")

  
  private def findMostCommonValue[A](colName:String, df:DataFrame)(f:Row =>A):A = {
    println(s"Finding most common value for $colName")
    val result = df.groupBy(colName).count().sort(desc("count")).first()
    f(result)    
  }
  
  private def cleanUpData(inputDataFrame:DataFrame):DataFrame = {
    val mostCommonColMap = colNames.foldLeft(Map[String, Int]())((accumulator, key) => {
      val mostCommonVal = findMostCommonValue(key, inputDataFrame){row:Row => row.getInt(0)}
      accumulator + {key -> mostCommonVal}
    })
    
    val nonEmptyDf = mostCommonColMap.toList.foldLeft(inputDataFrame)((acc, tpl) => {
          println("Replacing:" + tpl._1 + " with " + tpl._2)
          acc.na.fill(tpl._2, Seq(tpl._1))
        })
    nonEmptyDf
    //categorizeData(nonEmptyDf)
  }
  
  private[ml] def categorizeData(inputDf:DataFrame):DataFrame = {
    val ageToCategoryFunc:(Int=>Int) = age => age match {
          case teen if teen <= 40 => 1
          case adult if (adult > 40) => 2
          
        }
        
    val ageFunc = udf(ageToCategoryFunc)
    
    inputDf.withColumn("AgeCategory",ageFunc(col("Age")))
                            .drop("Age").withColumnRenamed("AgeCategory", "Age")  
    
  }
  
  
  
  override def transform(sc: SparkContext, inputDataSet: DataFrame):DataFrame = {
    implicit val sparkContext = sc
    val res = cleanUpData(inputDataSet)
    categorizeData(res)
  }
    
  
}

