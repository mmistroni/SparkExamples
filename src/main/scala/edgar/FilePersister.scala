package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._

/**
 * Persist DataFrames/DataSet to plain text files
 */

class PlainTextPersister(fileName:String) extends Loader[Dataset[(String, Long)]] {
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Persiste")
  
  
  override def load(sc:SparkContext, inputData:Dataset[(String,Long)]):Unit = {
    persistDataFrame(sc, inputData)
  }
  
  def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[(String, Long)]): Unit = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Form4Filing]
    val mapped = inputDataSet.toDF("filingType", "count")
    logger.info(s"Persisting data to $fileName.")
    mapped.foreach { x => println(x) }
    logger.info(s"Persisting data to text file: $fileName")
    mapped.coalesce(1).write.csv(fileName) //rdd.saveAsTextFile(fileName)
    
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

class DebugPersister(fileName:String) extends Loader[Dataset[Form4Filing]] {
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.DebugPersister")
  
  override def load(sc:SparkContext, inputData:Dataset[Form4Filing]):Unit = {
    persistDataFrame(sc, inputData)
  }
  
  
  def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[Form4Filing]): Unit = {
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._
    
    inputDataSet.take(5).foreach(println)
    
    val mapped = inputDataSet.map(fff => (fff.transactionType, fff.transactionCount))
    
    
    
    println("again...")
    val toDataFrame = mapped.toDF("company", "count")
    
    toDataFrame.printSchema()
    
    val ordered = toDataFrame.orderBy($"count".desc)
    ordered.coalesce(1).write.format("csv").save(fileName)
    
  }
}




