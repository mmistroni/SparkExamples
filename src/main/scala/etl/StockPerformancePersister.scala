package etl
import common.Loader
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

class StockPerformancePersister(fileName:String, coalesce:Boolean=true , sortField:Option[String] = None) extends Loader[DataFrame] {
  @transient
  val logger: Logger = Logger.getLogger("StockPerformancePersister")
  
  
  override def load(sc:SparkContext, inputData:DataFrame):Unit = {
    persistDataFrame(sc, inputData)
  }
  
  def coalesceDs(inputDs:DataFrame):DataFrame = {
    if (coalesce) {
      inputDs.coalesce(1)
    } else inputDs
  }
  
  def persistDataFrame(sc: SparkContext, inputDataSet:DataFrame): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val sortedDf = sortField match {
      case Some(sortField) => inputDataSet.orderBy(desc(sortField))
      case _    => inputDataSet
    }
    logger.info(s"Persisting data to text file: $fileName.Coalescing?$coalesce")
    coalesceDs(sortedDf).write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite).option("header", "true").save(fileName) //rdd.saveAsTextFile(fileName)
    
  }
}
