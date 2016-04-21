
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits

/**
 * This example builds a decision tree based on patient health data, 
 * to determine if an individual is a compulsive smoker
 * THis dataset was part of Coursera' s course Machine Learning for Data Analysis
 * (the original code was written using python and scikit
 * 
 * Please note that this code depends on pacakge spark-csv, so make sure when you
 * launch spark-submit you provide --packages com.databricks:spark-csv_2.10:1.4.0
 * 
 */
object AnotherDecisionTreeExample {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  def getDataFrame(sc: SparkContext, filePath:String):DataFrame = {
    println(s"Creating RDD from $filePath")
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(filePath)

    // sorting out null values. we set them to zero by default
    df.na.fill(0, df.columns)
  }
  

  def createLabeledPoint(row:Seq[Double]) = {
    val features = row.zipWithIndex.filter{case (seq, idx) => idx !=7}.map(tpl => tpl._1)
    val main = row(7)
    LabeledPoint(main, Vectors.dense(features.toArray))
  }
  
  
  def toLabeledPointsRDD(healthData: RDD[Seq[Double]]) = {
    // in this health data, it will be array[7] the field that determines if an individual is a compulsive smokmer
    healthData.map(seq => createLabeledPoint(seq))
  }

  
  def createVectorRDD(row:Row):Seq[Double] = {
    row.toSeq.map(_.asInstanceOf[Number].doubleValue)
  }
  
  
  def createModel(sc:SparkContext, data: RDD[LabeledPoint]):Unit = {
   
    // splitting
    println("Splitting training and test")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

  }

  def generateDecisionTree(sconf: SparkConf, args:Array[String] ): Unit = {

    SparkUtil.disableSparkLogging
    val sc = new SparkContext(sconf)
    if (args.length < 2) {
      println("Usage: SparkLauncher AnotherDecisionTreeExample <path to tree_addhealth.csv>")
      sys.exit()
    }
    val df = getDataFrame(sc, args(1))

    println("InputData:" + df.count())
    df.take(10).foreach(println)

    println("Converting to RDD")
    
    val vectorRdd = df.map(createVectorRDD)
        
        // Transforming data to LabeledPoint
    println("Creating labeled points")
    
    // ccrete labeled points. rmeember above we only have tuples
    val data = toLabeledPointsRDD(vectorRdd)
    // create model
    createModel(sc , data)
    
    
    
    
  }
}