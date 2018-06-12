package common
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
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
                               loader:Loader[V],
                               sparkContext:SparkContext)
                               {
  def this(extractor:Extractor[T,U],
           transformer:Transformer[U,V],
           loader:Loader[V]) = {
          this(extractor, transformer, loader, null)
          
  }
                     
  
  private def extractFunction:T => U = 
    input => extractor.extract(sparkContext, input) 
  
  private def transformFunction:U => V = 
    inputDataSet => transformer.transform(sparkContext, inputDataSet)
  
  private def loadFunction:V=>Unit = 
    transformedData => loader.load(sparkContext, transformedData)
  
  private def dataPipelineFunction = 
    extractFunction andThen transformFunction andThen loadFunction
  
    
  // Implementing a Pipeline
  // https://stackoverflow.com/questions/24883765/function-composition-dynamically?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
    
  // refine this to make it more dynamic. Perhaps follow how the spark ML Pipeline is implemented to add more 
  // than one transformers. Stantard ETL involve extracting transforming and loading
  // ML pipeline involves also ETSL. try to genreralize and reuse Function chain
    
    
  def runPipeline(sparkContext:SparkContext, input:T):Unit = {
    
    val extractFun:T =>U = input => extractor.extract(sparkContext, input)
    val transformFun:U=>V = inputDataSet => transformer.transform(sparkContext, inputDataSet)
    val loadFun:V=>Unit = transformedData => loader.load(sparkContext, transformedData)
    
    val executorFunction = extractFun andThen transformFun andThen loadFun
    
    executorFunction(input)
    
  }
   
  def runPipeline2(input:T):Unit = {
    dataPipelineFunction(input)
  }  
    
    
    
}
  










