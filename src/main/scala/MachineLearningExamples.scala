
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification._

object MachineLearningExamples {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  case class Feature(v:Vector)
  
  def logisticRegression(sconf: SparkConf): Unit = {
    
    SparkUtil.disableSparkLogging
    val sc = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val lebron = LabeledPoint(1.0, Vectors.dense(80.0, 250.0))
    val tim = LabeledPoint(0.0, Vectors.dense(70.0, 150.0))
    val brittany = LabeledPoint(1.0, Vectors.dense(80.0, 207.0))
    val stacey = LabeledPoint(0.0, Vectors.dense(65.0, 120.0))

    val trainingList = List(lebron, tim, brittany, stacey) 
    val trainingDF = sc.parallelize(trainingList).toDF()

    val estimator  = new LogisticRegression
    val transformer = estimator.fit(trainingDF)
    
    val testRDD  =  sc.parallelize(List(Vectors.dense(90.0, 270.0), Vectors.dense(62.0, 120.0)))
    val featuresDF  =testRDD.map(v=> Feature(v)).toDF("features")
    
    val predictionsDF = transformer.transform(featuresDF)
    
    println("Predictions....")
    predictionsDF.foreach(println)
    
    val shorterPredictionsDF = predictionsDF.select("features", "prediction")
    val playerDF = shorterPredictionsDF.toDF("features", "ISBASKETBALLPLAYER")
    
    playerDF.foreach(println)
    playerDF.printSchema()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}