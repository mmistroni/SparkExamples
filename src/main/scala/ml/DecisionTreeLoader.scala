package ml

import common.Loader
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger

import org.apache.spark.ml.classification.{DecisionTreeClassifier,
                                           DecisionTreeClassificationModel,
                                           RandomForestClassifier, 
                                           RandomForestClassificationModel }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.SparkContext

import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.feature._
import scala.util.Random
import MLUtils._

/**
 *   Use a DecisionTree Classifier to generate predictions on DataFrame
 *   By default it outputs the accuracy and the Best model fond
 */

class DecisionTreeLoader(label: String, dataSplit: Array[Double] = Array(0.7, 0.3)) extends Loader[DataFrame] {

  @transient
  val logger: Logger = Logger.getLogger("MLPipeline.DataCleaning")

  
  
  
  def load(sparkContext: SparkContext, inputData: DataFrame): Unit = {
    logger.info("Generating Decisiontree...")
    logger.info("Preparing indexes and classifiers....")
    val assembler = createAssembler("Severity", inputData)
    val (labelIndexer, featuresIndexer) = createIndexers("Severity")
    val Array(trainingData, testData) = inputData.randomSplit(Array(0.8, 0.2))
    println("^^^^^^^^ TRAINING CLASSIFIER.......")
    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    // Convert indexed labels back to original labels.
    val labelConverter =  createLabelConverter

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, labelIndexer, featuresIndexer, dt, labelConverter))

    trainingData.cache()
    testData.cache()  
    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)
    // Make predictions.
    val predictions = model.transform(testData)
    // Select example rows to display.
    predictions.select("predictedLabel", "indexedLabel", "indexedFeatures").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = createEvaluator
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
    
    val treeModel = model.stages(3).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    
    findBestModel(dt, evaluator, pipeline, trainingData, testData)
    
  }
  
  private def findBestModel(classifier:DecisionTreeClassifier,
                            multiclassEval:MulticlassClassificationEvaluator,
                            pipeline:Pipeline,
                            trainData:DataFrame,
                            testData:DataFrame):Unit = {
    println("######### FINDING BEST MODEL-------------")
    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    
    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(trainData)
    val bestModel = validatorModel.bestModel
    logger.info("============== BEST MODEL ")
    logger.info(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

    logger.info("============== VALIDATION METRICS ")
    logger.info(validatorModel.validationMetrics.max)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
    logger.info("============== TEST ACCURACY MODEL ")
    logger.info(testAccuracy)
    val trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainData))
    logger.info("============== TRAIN ACCURACY MODEL ")

    logger.info(trainAccuracy)

    (bestModel.asInstanceOf[PipelineModel], trainAccuracy, testAccuracy)

  }
  
  
  
  

}