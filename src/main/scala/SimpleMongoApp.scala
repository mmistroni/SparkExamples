


import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.mongodb.spark.sql._
import com.mongodb.spark._

object SimpleMongoApp{

  def getDataFrame(sc:SparkSession) = {
    sc.read.format("com.databricks.spark.csv").option("header", true).load("file:///c:/Users/marco/SparkReproduce/tree_addhealth.csv")
  }
  
  
  def main(args: Array[String]) = {
    val spark = SparkSession
         .builder()
         .master("local")
         .appName("Spark Mongo Example")
         .getOrCreate() 
    spark.conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/")
    spark.conf.set("spark.mongodb.output.uri", "mongodb://localhost:27017/")
    spark.conf.set("spark.mongodb.output.database", "test")
    
    println(s"SparkPRoperties:${spark.conf.getAll}")
    
    
    
    //val dframe = spark.sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri", "mongodb://localhost:27017/test.sectors").load()
    //dframe.printSchema()
    
    
    val df = getDataFrame(spark) // Loading any dataframe from a file
    
    df.printSchema()

    println(s"Head:${df.head()}")
    println(s"Count:${df.count()}")
    println("##################  SAVING TO MONGODB #####################")
    import com.mongodb.spark.config._
    
    import com.mongodb.spark.config._
    //MongoSpark.save(df.write.option("spark.mongodb.output.uri", "mongodb://localhost:27017/test.tree"))
    
    
    
    
    val dfWriter = df.write
    dfWriter.option("spark.mongodb.output.uri", "mongodb://localhost:27017/test.tree2")
    
    MongoSpark.save(dfWriter)
    /**
    case class Person(name: String, age: Long)
    
    val parallelSeq = spark.sparkContext.parallelize(List(Person("Person1", 11))) 
    val personDf = spark.createDataFrame(parallelSeq, classOf[Person])
    personDf.count()
    personDf.printSchema()
    **/
    
    
  }

}
