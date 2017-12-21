package ml

import common.Loader
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger

import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.{ RandomForestClassifier, RandomForestClassificationModel }
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{ StringIndexer, IndexToString, VectorIndexer, VectorAssembler }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.feature._
import scala.util.Random

/**
 *   Use a RandomForestClassifier to generate predictions on DataFrame
 *   By default it outputs the accuracy and the Best model fond
 */

class RandomForestLoader(label: String, dataSplit: Array[Double] = Array(0.7, 0.3)) extends Loader[DataFrame] {

  @transient
  val logger: Logger = Logger.getLogger("MLPipeline.RandomForestLoader")

      
  
  def load(sparkContext: SparkContext, inputData: DataFrame): Unit = {
    logger.info("Generating Decisiontree...")
    logger.info("Preparing indexes and classifiers....")
    
    val assembler = new VectorAssembler().
      setInputCols(inputData.columns.filter(_ != "Severity")).
      setOutputCol("featureVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Severity").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler, classifier))
 
    
    val Array(trainingData, testData) = inputData.randomSplit(Array(0.8, 0.2))
    
    println("^^^^^^^^ TRAINING CLASSIFIER.......")
    
    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Severity").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    //spark.sparkContext.setLogLevel("DEBUG")
    val validatorModel = validator.fit(trainingData)
    
    val bestModel = validatorModel.bestModel
    println("---------------BEST MODEL PARAMS --------------------------------")
    println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

    println("-------------MAX VALIDATION METRICS--------------------------------------")
    println(validatorModel.validationMetrics.max)
    println("---------------------------------------------------")
    
    val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
    println(s"------------- TEST ACCURACY is: $testAccuracy")

    val trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainingData))
    println(s"----------------- TRAIN ACCURACY IS : $trainAccuracy")
  }
    
  
  
  
  

}