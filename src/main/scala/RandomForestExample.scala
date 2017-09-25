
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, RandomForestModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import utils.SparkUtil

/**
 * This example builds a random forest tree and it's based on this video
 * on youtube
 * 
 * https://www.youtube.com/watch?v=ObiCMJ24ezs
 * 
 * Please note that this code depends on pacakge spark-csv, so make sure when you
 * launch spark-submit you provide --packages com.databricks:spark-csv_2.10:1.4.0
 * 
 * Run it like this 
 * C:\Users\marco\SparkExamples>spark-submit 
 * --class RamdomForestExample 
 * target\scala-2.11\sparkexamples.jar 
 * <path to tree_addhealth.csv>
 * 
 * 
 * 
 */
object RandomForestExample {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  def getRDD(sc: SparkContext, filePath:String):RDD[String] = {
    println(s"Creating RDD from $filePath")
    return sc.textFile(filePath)
  }
  
  def createLabeledPoint(row:Array[Double]) = {
    //val expected = row.last -1
    //val features = row.init
    val wilderness = row.slice(10,14).indexOf(1.0).toDouble
    val soil = row.slice(14,54).indexOf(1.0).toDouble
    
    val vector = Vectors.dense(row.slice(0,10) :+ wilderness :+ soil) 
    
    LabeledPoint(row.last - 1, vector)
    
  }
  
  def toLabeledPointsRDD(forestData: RDD[String]) = {
    
    val mapped = forestData.map(line=> line.split(",").map(_.toDouble))
    // in this health data, it will be array[7] the field that determines if an individual is a compulsive smokmer
    mapped.map(seq => createLabeledPoint(seq))
  }

  
  def getMetrics(model:RandomForestModel, data:RDD[LabeledPoint]) = {
    // Evaluate model on test instances and compute test error
    val predictionsAndLabels = data.map { example =>
      (model.predict(example.features), example.label)
    }
    new MulticlassMetrics(predictionsAndLabels)

  }
  
  def classProbabilities(data:RDD[LabeledPoint]) = {
    val countByCategory = data.map(_.label).countByValue()
    val counts = countByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }
  
  
  def createModel(sc:SparkContext, data: RDD[LabeledPoint]):Unit = {
   
    // splitting
    println("Splitting training and test")
    val splits = data.randomSplit(Array(0.9, 0.1))
    val (trainingData, testData) = (splits(0), splits(1))

    trainingData.cache()
    
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 7
    val categoricalFeaturesInfo = Map[Int, Int](10->4, 11->40)
    val impurity = "entropy"
    val depth = 30
    val bins = 300
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
          5, // num of tree to build 
          "auto",
          "entropy", 
          30, // how deep is the tree 
          300)  // how many diff values to consider

    val accuracy = getMetrics(model, testData).precision
    ((impurity, depth, bins), accuracy)
    
    println(s"Precision:$accuracy")
    
    
    println(s"------------- vs Random guess")
    val trainProbe = classProbabilities(trainingData)
    val testProbe  = classProbabilities(testData)
    val cf = trainProbe.zip(testProbe).map {
                                  case (trainProb, testProb) => trainProb * testProb
                                   }.sum
		println(cf)
    			
    
        
  }

  def generateDecisionTree(sconf: SparkConf, args:Array[String] ): Unit = {

    SparkUtil.disableSparkLogging
    val sc = new SparkContext(sconf)
    if (args.length < 1) {
      println("Usage:  AnotherDecisionTreeExample <path to tree_addhealth.csv>")
      sys.exit()
    }
    val rdd = getRDD(sc, args(0))

    println("InputData:" + rdd.count())
    
    println("Creating labeled points")
    
    // ccrete labeled points. rmeember above we only have tuples
    val data = toLabeledPointsRDD(rdd)
    // create model
    createModel(sc , data)
  }
  
  def main(args:Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application")
    generateDecisionTree(conf, args)
  }
  
  
}