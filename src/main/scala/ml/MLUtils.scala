package ml

import org.apache.spark.ml.feature.{ StringIndexer, IndexToString, VectorIndexer, VectorAssembler }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.sql.DataFrame

object MLUtils {
  def createAssembler(labelCol:String, inputData:DataFrame) = {
    new VectorAssembler().
      setInputCols(inputData.columns.filter(_ != labelCol)).
      setOutputCol("features")
  }
  
  def createIndexers(labelCol:String) = {
    val stringIndexer = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol("indexedLabel")
    val vectorIndexer =  new VectorIndexer()
                                          .setInputCol("features")
                                          .setOutputCol("indexedFeatures")
                                          .setMaxCategories(5)
    (stringIndexer, vectorIndexer)
  }
  
  def createLabelConverter = {
    new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(Array("indexedLabel"))
  }
  
  def createEvaluator = {
    new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")  
  }
  
  
  
  
  
}