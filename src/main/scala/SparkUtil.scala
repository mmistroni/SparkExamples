import org.apache.log4j.Level
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkUtil {

  def findBestModel(pipeline: Pipeline,
                    paramGrid: Array[ParamMap],
                    multiclassEval: MulticlassClassificationEvaluator,
                    trainData: DataFrame,
                    testData:DataFrame) = {
    import scala.util.Random
    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(trainData)
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
    bestModel
  }

  def disableSparkLogging = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

  }

  def createVectorRDD(row: Row): Seq[Double] = {
    row.toSeq.map(_.asInstanceOf[Number].doubleValue)
  }

  def createLabeledPoint(row: Seq[Double], targetFeatureIdx: Int) = {

    val features = row.zipWithIndex.filter(tpl => tpl._2 != targetFeatureIdx).map(tpl => tpl._1)

    val main = row(targetFeatureIdx)
    LabeledPoint(main, Vectors.dense(features.toArray))
  }

  def toLabeledPointsRDD(rddData: RDD[Seq[Double]], targetFeatureIdx: Int) = {
    // in this health data, it will be array[7] the field that determines if an individual is a compulsive smokmer
    rddData.map(seq => createLabeledPoint(seq, targetFeatureIdx))
  }

  def storeDataInMongo(mongoUrl: String, tableName: String, dataFrame: DataFrame,
                       appendMode: Boolean = false) = {
    import org.apache.spark.sql._
    import com.mongodb.spark.sql._
    import com.mongodb.spark._

    val dfWriter = dataFrame.write
    val mode = appendMode match {
      case true  => "append"
      case false => "insert"
    }

    println(s"$mode data into $mongoUrl for table $tableName.AppendMode:$appendMode")

    dfWriter.option("spark.mongodb.output.uri", s"$mongoUrl")
      .option("collection", tableName)
      .mode(mode)
    println(s"$mode ${dataFrame.count}  into $tableName")
    MongoSpark.save(dfWriter)

  }

  def readDataFrameFromMongo(mongoDbUrl: String, spark: SparkSession): DataFrame = {
    println(s"REading data from MongoUrl:$mongoDbUrl")

    spark.sqlContext.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("spark.mongodb.input.uri", mongoDbUrl).load()
  }

  def loadDataFrameFromFile(fileName: String, headerOption: Boolean, inferSchema: Boolean,
                            session: SparkSession) = {
    session.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", headerOption)
      .option("inferSchema", inferSchema)
      .load(fileName)

  }

  def findBestDecisionTreeModel(multiclassEval: MulticlassClassificationEvaluator,
                                validator: TrainValidationSplit,
                                trainData: DataFrame,
                                testData: DataFrame) = {

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

  }

}