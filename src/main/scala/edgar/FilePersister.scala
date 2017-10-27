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
    //repartition(1).write.csv(fileName) // rdd.saveAsTextFile(fileName)
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

class DebugPersister(fileName:String) extends PlainTextPersister(fileName) {
  @transient
  override val logger: Logger = Logger.getLogger("EdgarFilingReader.ParquetPersister")
  
  override def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[(String, Long)]): Unit = {
    println(inputDataSet.columns.mkString("|"))
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._
    //dsMapped.orderBy(desc("transactionCount")).take(10).foreach(println)
    //mapped.coalesce(1).rdd.saveAsTextFile(fileName)
    val ordered = inputDataSet.orderBy($"count(1)".desc)
    ordered.take(10).foreach(println)
    
    println("----schena---")
    ordered.printSchema()
    
    //ordered.coalesce(1).rdd.saveAsTextFile(fileName)
    inputDataSet.coalesce(1).write.json(fileName)
    
  }
}




