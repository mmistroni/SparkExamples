
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.{ RandomForestClassifier, RandomForestClassificationModel }
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{ StringIndexer, IndexToString, VectorIndexer, VectorAssembler }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.sql.functions._
import scala.util.Random

/**
 * This example builds a DecisionTree to Predict italian Referendum.
 * It is based on some data i have collected regarding the past 17 referendums,
 * their outcomes, and italian political, economic and media situation at the time
 * each refrendum was called
 * 
 * copy relevant code here
 *
 * A  good example for this is at this link
 *
 * https://github.com/sryza/aas/blob/master/ch04-rdf/src/main/scala/com/cloudera/datascience/rdf/RunRDF.scala#L220
 *
 *
 *
 *
 *
 * Run it like this
 * C:\Users\marco\SparkExamples>spark-submit
 * --class RamdomForestExampleML
 * target\scala-2.11\sparkexamples.jar
 * <path to tree_addhealth.csv>
 *
 *
 *
 */
object ItalianReferendumDecisionTreeExampleML {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  def getDataFrame(sc: SparkContext, filename:String): DataFrame = {

    println(s"Loading data from $filename")

    val sqlContext = new SQLContext(sc)

    val dataWithoutHeader = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    val colNames = Seq(
        "OUTCOME", "GDP",
        "UNEMPLOYMENTRATE", "INFLATIONRATE",
        "WAGEGROWTH",
        "PRODUCTIVITY", "MILITARYEXPENDITURE", "GOVERNMENTMEDIA",
        "GOVERNMENTPOSITION"
      )
    val data = dataWithoutHeader.toDF(colNames:_*).withColumn("OUTCOME", col("OUTCOME").cast("double"))  
   
    data.printSchema()
    
    data
      
  }
  

  def findBestDecisionTreeModel(multiclassEval:MulticlassClassificationEvaluator,
                                validator:TrainValidationSplit,
                                trainData:DataFrame, 
                                testData:DataFrame,
                                predictionData:DataFrame) = {
    
    //spark.sparkContext.setLogLevel("DEBUG")
    val validatorModel = validator.fit(trainData)
    /*
    DEBUG TrainValidationSplit: Got metric 0.6315930234779452 for model trained with {
      dtc_ca0f064d06dd-impurity: gini,
      dtc_ca0f064d06dd-maxBins: 10,
      dtc_ca0f064d06dd-maxDepth: 1,
      dtc_ca0f064d06dd-minInfoGain: 0.0
    }.
    */
    //spark.sparkContext.setLogLevel("WARN")

    val bestModel = validatorModel.bestModel

    println("============== BEST MODEL ")
    println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

    println("============== VALIDATION METRICS ")
    println(validatorModel.validationMetrics.max)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
    println("============== TEST ACCURACY MODEL ")
    println(testAccuracy)
    val trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainData))
    println("============== TRAIN ACCURACY MODEL ")
    
    println(trainAccuracy)
    
    println("============ predicting =============")
    val predictions = bestModel.transform(predictionData)
    
    predictions.select("prediction","featureVector").show(5)

    
        

    
  }

  
  
  
  def evaluate(trainData: DataFrame, testData: DataFrame, predictionData:DataFrame): Unit = {
    
    trainData.cache()
    testData.cache()
    predictionData.cache()

    val assembler = new VectorAssembler().
      setInputCols(trainData.columns.filter(_ != "OUTCOME")).
      setOutputCol("featureVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("OUTCOME").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("OUTCOME").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.8)

    findBestDecisionTreeModel(multiclassEval, validator, trainData, testData, predictionData)
  }
  
  
  def generateDecisionTree(sconf: SparkConf, args: Array[String]): Unit = {

    SparkUtil.disableSparkLogging
    
    val sc = new SparkContext("local[*]", "ItalianReferendumeExampleML")
    
    val existingData  = "file:///c:/Users/marco/SparkExamples/src/main/resources/PredictingItalianReferendum.csv"
    
    val predictionData =  "file:///c:/Users/marco/SparkExamples/src/main/resources/ReferendumPredictions.csv"
    
    
    val df = getDataFrame(sc, existingData)
    
    val predictionDf = getDataFrame(sc, predictionData).drop("OUTCOME")
    
    println("PredictionDF schema")
    
    predictionDf.printSchema()
    
    predictionDf.cache()

    println("InputData:" + df.count())
    val reduced = df

    val Array(trainData, testData) = reduced.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    testData.cache()

    evaluate(trainData, testData, predictionDf)

  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Italian REferendum Simple Application")
    generateDecisionTree(conf, args)
  }

}