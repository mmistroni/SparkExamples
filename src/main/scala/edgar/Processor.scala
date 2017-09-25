package edgar
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger
import scala.util._
import scala.xml._

/**
 * Extract data
 */
trait Extractor[IN, OUT] {
  def extract(sparkContext:SparkContext, inputData:IN):OUT
}

/**
 * Trait for a Transformer, which will transform data 
 */
trait Transformer[IN,OUT] extends Serializable {
  
  def transform(sparkContext: SparkContext, inputDataSet: IN): OUT 
}

/**
 * Trait for a Persister, which will persist the processed data
 */
trait Loader[IN] extends Serializable {
  def load(sparkContext:SparkContext, inputData:IN):Unit
}

/**
 * Models a PIpeline
 * INPUT is the input data of the pipeline (after reading sourcedata from an URL
 * OUTPUT is the output data of the pipelie, which will be persisted to storate
 */
trait Pipeline[INPUT,OUTPUT] {
  
  def extract(sparkContext:SparkContext, inputData:String):INPUT
  
  def transform(sparkContext:SparkContext, inputData:INPUT):OUTPUT
  
  def load(sparkContext:SparkContext, format:OUTPUT):Unit
}
  










