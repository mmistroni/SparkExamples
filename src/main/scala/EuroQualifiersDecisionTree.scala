
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.evaluation._
import utils.SparkUtil._

/**
 * THis example attempts to predict european championship semifinalists based on following past data(back to euro 1992)
 * - previous world cup result (if team reached knock out stages)
 * - number of team in the CL knock out stages
 * - number of team in the EL knock out stages
 * - country UEFA rank
 * - number of teams in UEFA top 30 team rank
 * - number of nominations in previous Ballon d'Or prize
 * 
 * I have some mixed results which i need to analyze.....
 * There are two input files to supply
 * - a TrainData <Euro2008-2.csv which contains statistics for the last 5 Eurcup
 * - a Testdata, which is data related to Euro2016 finalists
 * 
 * 
 * * C:\Users\marco\SparkExamples>spark-submit
 * --packages com.databricks:spark-csv_2.10:1.4.0   --class EuroQualifierDecisionTree
 * target\scala-2.11\sparkexamples.jar
 * <Euro2008-2.csv>  <Euro2016-2.csv>
 *
 *
 */
object EuroQualifierDecisionTree {

  def getDataSet(sqlContext: SQLContext, filePath: String) = {
    println(s"Creating RDD from $filePath")
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filePath)

    df.registerTempTable("euroteamdata")
    df
  }

  def cleanUpData(sqlContext: SQLContext, dataFrame: DataFrame): DataFrame = {
    // THis is the cleanup needed
    // 1. at the moment we remove team name only

    val res = dataFrame.drop("TEAM")
    res

  }

  def getMetrics2(model: RandomForestModel, data: RDD[LabeledPoint]) = {
    // Evaluate model on test instances and compute test error
    val predictionsAndLabels = data.map { example =>
      (model.predict(example.features), example.label)
    }
    val testErr = predictionsAndLabels.filter(r => r._1 != r._2).count.toDouble / data.count()
    (new BinaryClassificationMetrics(predictionsAndLabels), testErr)

  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]) = {
    // Evaluate model on test instances and compute test error
    val predictionsAndLabels = data.map { example =>
      (model.predict(example.features), example.label)
    }

    val testErr = predictionsAndLabels.filter(r => r._1 != r._2).count.toDouble / data.count()
    (new BinaryClassificationMetrics(predictionsAndLabels), testErr)

  }

  def useSVM(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint],
             euro2016Data: RDD[LabeledPoint]) = {
    // Run training algorithm to build the model
    val numIterations = 150
    val model = SVMWithSGD.train(trainingData, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    
    println("Now runnign with real data...")
    val realScoreAndLabels = euro2016Data.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    
    realScoreAndLabels.foreach(println)
    
    
  }

  def useRandomForestModel(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint],
                           euro2016Data: RDD[LabeledPoint]) = {

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    //val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.

    val randomForestEvaluationsTreeEvaluations =
      for (
        impurity <- Array("gini", "entropy");
        depth <- Array(1, 2,3,4, 5, 6);
        bins <- Array(2,4,6,8,10, 12, 13, 15, 17, 20, 25, 30);
        trees <- Array(1, 2, 3,4,5,6)
      ) yield {
        val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
          trees, featureSubsetStrategy, impurity, depth, bins)
        val (metrics, testErr) = getMetrics2(model, testData)
        val topPrecision = metrics.areaUnderROC()
        ((impurity, depth, bins, trees), topPrecision, testErr)
      }

    val sorted = randomForestEvaluationsTreeEvaluations.sortBy(_._3)
    sorted.take(10).foreach(println)

    val ((imp, depth, bins, trees), accuracy, testErr) = sorted.head

    println(s"TEsting model with Impurity:{$imp}|Depth:{$depth}|BIns:{$bins}|Trees:{$trees}|Accuracy:{$accuracy}")

    val randomForestModel = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      trees, featureSubsetStrategy, imp, depth, bins)

    println("Now predicting EURO 2016 USING RANDOM FOREST")

    val predictions = euro2016Data.map { point =>
      val prediction = randomForestModel.predict(point.features)
      (point.label, prediction)
    }

    predictions.foreach(println)

  }

  def useDecisionTreeModel(trainingData: DataFrame, testData: DataFrame,
                           euro2016Data: DataFrame) = {

    /**
    println("--------------- DECISION TREES -------------------")
    val decisionTreeEvaluations =
      for (
        impurity <- Array("gini", "entropy");
        depth <- Array(1,2,3,4, 5,6);
        bins <- Array(10,20, 25,28,30)
      ) yield {
        val model = DecisionTree.trainClassifier(
          trainingData, 2, Map[Int, Int](),
          impurity, depth, bins)

        val (metrics, testErr) = getMetrics(model, testData)
        val topPrecision = metrics.areaUnderROC()
        ((impurity, depth, bins), topPrecision, testErr)
      }

    val decTree = decisionTreeEvaluations.sortBy(_._2)
    decTree.take(10).foreach(println)

    val ((dt_impurity, dt_depth, dt_bins), dt_accuracy, err) = decTree.head

    val dt_optimal_model = DecisionTree.trainClassifier(
      trainingData, 2, Map[Int, Int](),
      dt_impurity, dt_depth, dt_bins)

    val dt_predictions = euro2016Data.map { point =>
      val prediction = dt_optimal_model.predict(point.features)
      (point.label, prediction)
    }
    dt_predictions.foreach(println)
		**/
  }

  def generateModel(data: DataFrame, sqlContext: SQLContext, euro2016Data: DataFrame): Unit = {
    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    //val numTrees = 3 // Use more in practice.
    //val maxDepth = 4
    //val maxBins = 25
    trainingData.cache()
    testData.cache()
    euro2016Data.cache()
    
    
    println("_------- RANDOM FOREST ------------------")
    //useRandomForestModel(trainingData, testData,euro2016Data)
    println("--------- NOW WITH DECISION TREE....")
    useDecisionTreeModel(trainingData, testData, euro2016Data)
    println("--------- USING SVM --------")
    //useSVM(trainingData, testData, euro2016Data)

  }

  def euroQualifiers(sconf: SparkConf, trainDataPath: String, testDataPath: String): Unit = {

    disableSparkLogging

    val sc = new SparkContext(sconf)
    val sqlContext = new SQLContext(sc)

    println("Creating DataSet")
    val euroQualifierDataFrame = getDataSet(sqlContext, trainDataPath)
    println("Cleaning up data...")
    val cleanedDataSet = cleanUpData(sqlContext, euroQualifierDataFrame)
    //val vectorRdd = cleanedDataSet.map(createVectorRDD)
    println("Creating labeled points")
    // ccrete labeled points. rmeember above we only have tuples
    //val data = toLabeledPointsRDD(vectorRdd, 0)
    println("TrainData")

    println("Now loading euro data")
    val euro2016df = getDataSet(sqlContext, testDataPath)
    val cleanedEuro2016DataSet = cleanUpData(sqlContext, euro2016df)
    //val euro2016vectorRdd = cleanedEuro2016DataSet.map(createVectorRDD)
    //val euro2016Data = toLabeledPointsRDD(euro2016vectorRdd, 0)

    println("Now feeding the model..")
    generateModel(cleanedDataSet, sqlContext, cleanedEuro2016DataSet)

  }

  def main(args: Array[String]) = {

    if (args.size < 2) {
      println("Usage: <spark..>   <train-data-path> <test-data-path>")
      sys.exit()
    }

    val conf = new SparkConf().setAppName("Simple Application")
    euroQualifiers(conf, args(0), args(1))

  }

}