


import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._

object SimpleReadMongoDataFile {

  def createDataFrameFromRDD(existingRdd:RDD[String]):DataFrame = null
  
  def loadDataFrame(spark:SparkSession, fileName:String):DataFrame = {
    
    println(s"Loadign data from file:$fileName")
    
    
    val sharesDf = spark.sqlContext.read.format("com.databricks.spark.csv")
                                        .option("header", false)
                                        .option("inferSchema", true)
                                        .load(fileName)
    
    
    println("Loaded:" + sharesDf.count())
    sharesDf.printSchema()
    sharesDf
  }
  
  
  def normalizeData(sharesDf:DataFrame):DataFrame = {
    val dateToStringFunc: (java.sql.Date => String) = asOfDate => new java.text.SimpleDateFormat("MM/dd/yyyy").format(asOfDate)
    val stringToDoubleFunc:(String => Double) = doubleStr => if (doubleStr.equals("N/A")) 0.0 else doubleStr.toDouble
    val tstampToDateFunc:(java.sql.Timestamp => java.sql.Date) = ts => new java.sql.Date(ts.getTime)
    val dateConversionFunc = udf(dateToStringFunc)
    val doubleConversionFunc = udf(stringToDoubleFunc)
    val tsampConversionFunc = udf(tstampToDateFunc)   
    
    val optimalDf = sharesDf.withColumn("price", col("_c2").cast("double"))
                            .withColumn("created_time", tsampConversionFunc(col("_c1")))
                            .withColumnRenamed("_c0", "ticker")
                            .withColumnRenamed("_c3", "currentEps")
                            .withColumnRenamed("_c4", "forwardEps")
                            .withColumnRenamed("_c5", "movingAvg")
                            .withColumnRenamed("_c6", "shortRatio")
                            .withColumnRenamed("_c7", "exDivDate")
                            .withColumn("asOfDateStr", dateConversionFunc(col("created_time")))
                            .drop(col("_c1")).drop(col("_c2"))
    
    println("------ Normalized Data----------------------------")
    optimalDf.printSchema()
    optimalDf
  }
  
  
  def readDataFrame(mongoDbUrl:String, spark:SparkSession):DataFrame = {
    spark.sqlContext.read.format("com.mongodb.spark.sql.DefaultSource")
                                     .option("spark.mongodb.input.uri", mongoDbUrl).load()
  }
  
  def main(args: Array[String]) = {
    
    if (args.size < 2) {
      println ("Usage:SimpleReadMongoDataFile <tableName> <inputDataFile>")
      System.exit(0)
    }
    
    
    val tableName  =args(0)
    val inputDataFile = args(1)
    
    println(s"Running application against table:$tableName, sourcing data from file:$inputDataFile")
    
    val spark = SparkSession
         .builder()
         .master("local")
         .appName("Spark Reading Share CSV file")
         .getOrCreate() 
    spark.conf.set("spark.driver.memory", "4g") 
    
    val sharesDf = loadDataFrame(spark, inputDataFile)
    
    
    println("Spark Properties :" + spark.conf.getAll.mkString(","))
    
    
    println("LoadedData has ${sharesDf.count} rows...")
    
    println("Normalizing...")
    
    val optimalDf = normalizeData(sharesDf)
    
    import SparkUtil.storeDataInMongo
    
    println("...SAving to mongo...")
    
    storeDataInMongo("mongodb://localhost:27017/test", tableName, optimalDf, appendMode=true)
    
    println("Selecting agian.,.,.")
    
    val mongoDb = s"mongodb://localhost:27017/test.$tableName"
    val mongoDf = readDataFrame(mongoDb, spark)
    println(s"LoadedMongo DataFrame has :${mongoDf.count}")
    
    
    
    

  }

}
