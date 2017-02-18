


import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SimpleApp {

  def main(args: Array[String]) = {
    /**
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
   

    val logFile = "file:///C:/spark-1.6.1/README.md" // Should be some file on your system

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val content = sqlContext.jsonFile("file:///c:/tmp/1973-01-11.json")
    println(content.count())
    
    
    
    val logData = sc.textFile(logFile, 2).cache()

    val numAs = logData.filter(line => line.contains("a")).count()

    val numBs = logData.filter(line => line.contains("b")).count()

    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
		**/
        import org.apache.spark.sql.types._
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkConf
    import org.apache.spark.rdd._
    import org.apache.spark.SparkContext._
    import org.apache.spark.sql._

    
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    // no schema
    val jsonContentNoSchema = sqlContext.read.json("file:///c:/tmp/1973-01-11.json")
    jsonContentNoSchema.printSchema()
    println(s"TheJsonContent with No SChema has ${jsonContentNoSchema.count()}")   
    // with schema
    
    
    import sqlContext.implicits._
    val jsonRdd = sc.textFile("file:///c:/tmp/1973-01-11.json")
    
    val schema = (new StructType).add("hour", StringType).add("month", StringType)
                  .add("second", StringType).add("year", StringType)
                  .add("timezone", StringType).add("day", StringType)
                  .add("minute", StringType)
    
    val jsonContentWithSchema = sqlContext.jsonRDD(jsonRdd, schema)
    println(s"----- And the Json withSchema has ${jsonContentWithSchema.count()} rows")
 

  }

}
