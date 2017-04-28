
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.{ RandomForestClassifier, RandomForestClassificationModel }
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{ StringIndexer, IndexToString, VectorIndexer, VectorAssembler }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.sql.functions._

import scala.util.Random

/**
 * This example builds a DecisionTree to Predict italian Referendum.
 * It is based on some data i have collected regarding the past 17 referendums,
 * their outcomes, and italian political, economic and media situation at the time
 * each refrendum was called
 *
 * copy relevant code here
 *
 * A  good example for this is at this link
 *
 * https://github.com/sryza/aas/blob/master/ch04-rdf/src/main/scala/com/cloudera/datascience/rdf/RunRDF.scala#L220
 *
 *
 *
 *
 *
 * Run it like this
 * C:\Users\marco\SparkExamples>spark-submit
 * --class RamdomForestExampleML
 * target\scala-2.11\sparkexamples.jar
 * <path to tree_addhealth.csv>
 *
 *
 *
 */

trait BaseStockData {
  def asOfDate:String
  def close:Double
}

case class StockData (asOfDate:String, close:Double) extends BaseStockData

case class EnhancedStockData(asOfDate:String, close:Double, 
                             debugAsOfDate1:String,
                             debugAsOfDate2:String) extends BaseStockData


object FTSEDecisionTreeExampleML {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  def getFilePath(ticker: String): String = {
    import java.io.File
    val loc = s"tickers/$ticker.csv"
    new File(this.getClass.getClassLoader.getResource(loc).toURI).getPath
  }

  def getDataFrame(sc: SparkContext, ticker: String): DataFrame = {
    val fileName = getFilePath(ticker)
    println(s"Loading data from $fileName")
    val sqlContext = new SQLContext(sc)
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(fileName)
  }

  def disableLogging = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR) 
    
  }

  def normalize(ticker:String, inputDf:DataFrame):DataFrame = {
    inputDf.withColumn("Close", col("Close").cast("double")).drop("High").drop("Open")
      .drop("Low").drop("Volume").drop("Adj Close").withColumnRenamed("Close", ticker)
    
  }
  
  
  def loadRddFile(ticker:String, sc:SparkContext):RDD[StockData] = {
     val path = getFilePath(ticker)
     println(s"Finding  data from $path")
     val stockData = sc.textFile(path)
     stockData.zipWithIndex.filter(tpl => tpl._2 > 1)
         .map(tpl => tpl._1)
         .map(row => row.split(','))
         .map(row => StockData(row(0), row(4).toDouble))
  }
  
  def normalizedRdd(input:RDD[StockData])= {
    val mappedWithIdx = input.zipWithIndex
    val count = mappedWithIdx.count()
    
    // taking different ranges to compute difference in prices
    
    val mappedwithIdx1 = mappedWithIdx.filter{case (data, idx) => idx < count-1}.sortBy(tpl => tpl._2, false)
            .map(tpl => tpl._1)

    val mappedwithIdx2 = mappedWithIdx.filter{case (data, idx) => idx > 0}.sortBy(tpl => tpl._2, false)
            .map(tpl => tpl._1)     
    // zipping it
    val zipped = mappedwithIdx2.zip(mappedwithIdx1)        
    // map to extended data 
    zipped.map{case (stock1, stock2) => 
      (StockData(stock2.asOfDate, stock2.close - stock1.close),stock1.asOfDate,stock2.asOfDate)}
  }
  
  def toDataFrame(rdd:RDD[EnhancedStockData], sc:SparkContext):DataFrame ={
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF()
  }
  
  
  def generateDecisionTree(sconf: SparkConf, args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "FTSE Predictor")
    sc.setLogLevel("ERROR")
    val allDfs = for (ticker <- Seq("NIKKEI")) yield {normalizedRdd(loadRddFile(ticker,sc))}
    
    
    //val finalDf = allDfs.reduce((df1, df2) => df1.join(df2, "Date"))
    
    println("=== OUTTA HERE =====")
    //finalDf.show()
    //finalDf.printSchema()
    
  }

  def main(args: Array[String]) = {
    disableLogging
    val conf = new SparkConf().setAppName("FTSE PREDICTOR")
    generateDecisionTree(conf, args)
  }

}