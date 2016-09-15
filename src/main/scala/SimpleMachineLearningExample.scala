
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Simple DecisionTree example using Spark
 * Run it like this
 *
 *
 * * spark-submit  --class MachineLearningExamples
 * 								target\scala-2.10\sparkexamples.jar
 *
 *
 */
object SimpleMachineLearningExamples {

  def createDataFrame(sqlContext: SQLContext, sc: SparkContext) = {
    val lst = for (i <- 1 to 1000) yield { ("First", "Second") }
    val rdd = sc.parallelize(lst)
    import sqlContext.implicits._
    rdd.toDF("Col1", "Col2")

  }

  def execute(conf: SparkConf) = {

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    
    
    def getLookupMap(): Map[String, String] = {
      return Map("First" -> "1", "Second" -> "2")
    }
    
    val lookupMapBrdcst = sc.broadcast(getLookupMap)

    def deriveFunc: (String => String) = (str: String) => {
      val stn: String = lookupMapBrdcst.value.getOrElse(str, "no_mapping_found")
      stn

    }

    def deriveStr = udf(deriveFunc)

    
    
    def getFunction(sqlContext: SQLContext, sc: SparkContext): Unit = {

        val df = createDataFrame(sqlContext, sc)
        val df2 = df.withColumn("deriveCol",lit(deriveStr(col("Col1"))))

        println("-----DF2---")
        df2.take(30).foreach(println)
    }

    println ("Calling getFunction....")
    getFunction(sqlContext, sc)
    
    
    
    
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application")
    execute(conf)
  }

}