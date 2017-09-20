package utils

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }

trait DataSource extends Serializable{
 
  val logger: Logger = Logger.getLogger("Spark.Processor")
  def readInput[OUT](location:String, sparkContext:SparkContext):OUT
}

trait Step extends Serializable {
  @transient
  val logger: Logger = Logger.getLogger("Spark.Processor")
  
  def processData[A,B](sparkContext: SparkContext, input: A): B 

}

class SparkExecutor(val sparkContext:SparkContext,
                    val dataSource:DataSource,
                    val pipeline:Seq[Step])  {
  
  val logger: Logger = Logger.getLogger("Spark.DataReader")
  
  def execute(inputData:String):Unit = {
    logger.info("SparkExecutor.....starting processing......")
    val startingData = loadSourceData(inputData)
    logger.info("Executing Pipeline......")
    pipeline.foldLeft(startingData)((currentResult, currentProcessor) => currentProcessor.processData(
                                                                                    sparkContext,
                                                                                    currentResult))
    
  }
  
  private def loadSourceData(input:String):DataFrame = {
    dataSource.readInput(input, sparkContext)
  }
  
}