import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }




object SparkUtil {
  def disableSparkLogging ={
      import org.apache.log4j.Logger
      import org.apache.log4j.Level
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)
    
  }
  
  def createVectorRDD(row:Row):Seq[Double] = {
    row.toSeq.map(_.asInstanceOf[Number].doubleValue)
  }
  
  
  def createLabeledPoint(row:Seq[Double], targetFeatureIdx:Int) = {
    
    val features = row.zipWithIndex.filter(tpl => tpl._2 != targetFeatureIdx).map(tpl => tpl._1)
    
    val main = row(targetFeatureIdx)
    LabeledPoint(main, Vectors.dense(features.toArray))
  }
  
  
  def toLabeledPointsRDD(rddData: RDD[Seq[Double]], targetFeatureIdx:Int) = {
    // in this health data, it will be array[7] the field that determines if an individual is a compulsive smokmer
    rddData.map(seq => createLabeledPoint(seq, targetFeatureIdx))
  }

  
  def findBestDecisionTreeModel(multiclassEval:MulticlassClassificationEvaluator,
                                validator:TrainValidationSplit,
                                trainData:DataFrame, 
                                testData:DataFrame) = {
    
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