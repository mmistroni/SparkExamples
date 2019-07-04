package common

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import utils.SparkUtil._
import common.Loader
import edgar.Form4Filing

/**
 * Persist DataFrames/DataSet to plain text files
 */

class PlainTextPersister(fileName:String, coalesce:Boolean=false ) extends Loader[Dataset[(String, Long)]] {
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Persiste")
  
  
  override def load(sc:SparkContext, inputData:Dataset[(String,Long)]):Unit = {
    persistDataFrame(sc, inputData)
  }
  
  def coalesceDs(inputDs:DataFrame):DataFrame = {
    if (coalesce) {
      inputDs.coalesce(1)
    } else inputDs
  }
  
  def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[(String, Long)]): Unit = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Form4Filing]
    val mapped = inputDataSet.toDF("filingType", "count")
    mapped.cache()
    logger.info(s"Persisting data to $fileName.")
    logger.info(s"Persisting data to text file: $fileName.Coalescing?$coalesce")
      
    coalesceDs(mapped).write.format("com.databricks.spark.csv")
            .option("header", "true").save(fileName) //rdd.saveAsTextFile(fileName)
    
  }
}

class NoOpPersister(fileName:String, coalesce:Boolean=false ) extends Loader[Dataset[(String, Long)]] {
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Persiste")
  
  
  override def load(sc:SparkContext, inputData:Dataset[(String,Long)]):Unit = {
    persistDataFrame(sc, inputData)
  }
  
  def coalesceDs(inputDs:DataFrame):DataFrame = {
    if (coalesce) {
      inputDs.coalesce(1)
    } else inputDs
  }
  
  def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[(String, Long)]): Unit = {
    logger.info(s" Not Persisting data to $fileName.")
    
  }
}






class ParquetPersister(fileName:String) extends PlainTextPersister(fileName) {
  @transient
  override val logger: Logger = Logger.getLogger("EdgarFilingReader.ParquetPersister")
  
  override def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[(String, Long)]): Unit = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Form4Filing]
    val mapped = inputDataSet.toDF("filingType", "count")
    mapped.cache()
    logger.info(s"Persisting data to:" + fileName)
    //mapped.foreach { x => println(x) }
    logger.info(s"Persisting data to text file: $fileName")
    mapped.write.parquet(fileName)
  }
}


class DataFrameToCsvPersister(fileName:String, coalesce:Boolean=false ) extends Loader[DataFrame] {
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Persiste")
  
  
  override def load(sc:SparkContext, inputData:DataFrame):Unit = {
    persistDataFrame(sc, inputData)
  }
  
  def coalesceDs(inputDs:DataFrame):DataFrame = {
    if (coalesce) {
      inputDs.coalesce(1)
    } else inputDs
  }
  
  def persistDataFrame(sc: SparkContext, inputDataFrame:DataFrame): Unit = {
    inputDataFrame.cache()
    logger.info(s"Persisting data to $fileName.")
    logger.info(s"Persisting data to text file: $fileName.Coalescing?$coalesce")
      
    coalesceDs(inputDataFrame).write.format("com.databricks.spark.csv")
            .option("header", "true").save(fileName) //rdd.saveAsTextFile(fileName)
    
  }
}











