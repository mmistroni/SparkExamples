package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._
import common.Loader

/**
 * Persist DataFrames/DataSet to plain text files
 */

class PlainTextPersister(fileName:String, coalesce:Boolean=true) extends Loader[Dataset[(String, Long)]] {
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
    logger.info(s"Persisting data to $fileName.")
    mapped.foreach { x => println(x) }
    logger.info(s"Persisting data to text file: $fileName.Coalescing?$coalesce")
    coalesceDs(mapped).write.format("com.databricks.spark.csv").save(fileName) //rdd.saveAsTextFile(fileName)
    
  }
}

class ParquetPersister(fileName:String) extends PlainTextPersister(fileName) {
  @transient
  override val logger: Logger = Logger.getLogger("EdgarFilingReader.ParquetPersister")
  
  override def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[(String, Long)]): Unit = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Form4Filing]
    val mapped = inputDataSet.toDF("filingType", "count")
    logger.info(s"Persisting data to:" + fileName)
    mapped.foreach { x => println(x) }
    logger.info(s"Persisting data to text file: $fileName")
    mapped.write.parquet(fileName)
  }
}





