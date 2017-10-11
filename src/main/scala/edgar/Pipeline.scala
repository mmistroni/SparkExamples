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
class Pipeline[T,U,V](extractor:Extractor[T,U],
                               transformer:Transformer[U,V],
                               loader:Loader[V])
                               {
  
  def extractFunction = 
    (sc:SparkContext, input:T) => extractor.extract(sc, input) 
  
  def transformFunction = 
    (sc:SparkContext, inputDataSet:U) => transformer.transform(sc, inputDataSet)
  
  def loadFunction = 
    (sc:SparkContext, transformedData:V) => loader.load(sc, transformedData)
  
  
  def runPipeline(sparkContext:SparkContext, input:T):Unit = {
    
    val extractFun:T =>U = input => extractor.extract(sparkContext, input)
    val transformFun:U=>V = inputDataSet => transformer.transform(sparkContext, inputDataSet)
    val loadFun:V=>Unit = transformedData => loader.load(sparkContext, transformedData)
    
    val executorFunction = extractFun andThen transformFun andThen loadFun
    
    executorFunction(input)
    
  }
  
}
  










