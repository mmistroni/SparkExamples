package edgar

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import utils.SparkUtil._

class MongoPersister extends Loader[Dataset[(String, Long)]] {
  @transient
  val logger: Logger = Logger.getLogger("EdgarFilingReader.Persiste")
  
  
  override def load(sc:SparkContext, inputData:Dataset[(String,Long)]):Unit = {
    persistDataFrame(sc, inputData)
  }
  
  def persistDataFrame(sc: SparkContext, inputDataSet:Dataset[(String, Long)]): Unit = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Form4Filing]
    val mapped = inputDataSet.toDF("filingType", "count")
    logger.info("Persisting data into MOngo...")
    mapped.printSchema()
    mapped.foreach { x => println(x) }
    storeDataInMongo("mongodb://localhost:27017/test", "Form4Filing", mapped, appendMode = true)
  }
}

