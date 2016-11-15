
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{ DecisionTreeRegressor, DecisionTreeRegressionModel }
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.{ StringIndexer, IndexToString, VectorIndexer, VectorAssembler }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import SparkUtil._


/**
 * THis example has been ported from the original
 * @ www.kaggle.com. The original example uses python/scikit,
 * this one uses Spark and spark-csv
 *
 * * C:\Users\marco\SparkExamples>spark-submit
 * --packages com.databricks:spark-csv_2.10:1.4.0
 * --class TitanicSurvivorsDecisionTreeWithML
 * target\scala-2.11\sparkexamples.jar
 * <path to train.csv>
 *
 *
 */
object TitanicSurvivorsDecisionTreeWithML {

  def getDataSet(sqlContext: SQLContext, filePath: String) = {
    println(s"Creating RDD from $filePath")
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("file:///c:/Users/marco/SparkExamples/src/main/resources/train.csv")

    df.registerTempTable("survivors")
    df
  }

  def findAgeMedian(sqlContext: SQLContext): Double = {
    val allAges = sqlContext.sql("select Age FROM survivors where Age is not null ORDER BY Age ASC").collect().map(_.getDouble(0))
    allAges(allAges.size / 2)

  }

  def getMostCommmonEmbarked(df: DataFrame): String = {
    // one solution, select and sort
    df.select("Embarked").groupBy("Embarked").count().sort(desc("count")).first().getString(0)
  }

  def cleanUpData(sqlContext: SQLContext, dataFrame: DataFrame): DataFrame = {
    // THis is the cleanup needed
    // 1. gender. from M/F to 0 or 1
    // 2. Embarked: from 'C', 'Q', 'S' to 1, 2 , 3
    // 3. Age, when not present we need to take the median

    val medianAge = dataFrame.select(avg("Age")).head().getDouble(0)
    val mostCommonEmbarked = getMostCommmonEmbarked(dataFrame)

    val fillAge = dataFrame.na.fill(medianAge, Seq("Age"))
    val fillEmbarked = fillAge.na.fill(mostCommonEmbarked, Seq("Embarked"))

    //scala> val df = Seq((25.0, "foo"), (30.0, "bar")).toDF("age", "name")
    //scala> df.withColumn("AgeInt", when(col("age") > 29.0, 1).otherwise(0)).show

    println("Median Age is:" + medianAge)

    // Converting  sex column to binary
    val binaryFunc: (String => Double) = sex => if (sex.equals("male")) 1 else 0
    val sexToIntFunc = udf(binaryFunc)
    val binarySexColumnDataFrame = fillEmbarked.withColumn("SexInt", sexToIntFunc(col("Sex")))

    // embarked. converting it to number
    val binEmbarked: (String => Double) = embarked => if (embarked.equals("C")) 0 else if (embarked.equals("Q")) 1 else 2
    val embarkedFunc = udf(binEmbarked)
    
    // converting Survived from double to integer
    val withBinaryEmbarked = binarySexColumnDataFrame.withColumn("EmbarkedInt",
      embarkedFunc(col("Embarked")))
      .withColumn("SurvivedDbl", col("Survived").cast("double")).drop("Survived")

    // Dropping column we dont need
    val res = withBinaryEmbarked.drop("Name").drop("Sex").drop("Ticket").drop("Cabin").drop("PassengerId").drop("Embarked").drop("Survived")
    println("Before decision tree. data is\n")
    res.show()
    res

  }

  def generateDecisionTree(data: DataFrame) = {
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Declare label
    val labelIndexer = new StringIndexer()
      .setInputCol("SurvivedDbl")
      .setOutputCol("indexedLabel")
      .fit(data)

    // Other features. crete a Vecto assembler if there are more than 1 feature
    val features = new VectorAssembler()
      .setInputCols(Array(
        "Pclass", "Age", "Parch", "SexInt", "EmbarkedInt",
        "SibSp", "Fare"))
      .setOutputCol("features")

    
    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, features, dt, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "SurvivedDbl", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    
    val accuracy = evaluator.evaluate(predictions)
    println("TestError= " + (1 - accuracy))

    
    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    
    
    // now improving the model
    //We can reuse the RegressionEvaluator, regEval, to judge the model based on the best Root Mean Squared Error
    // Let's create our CrossValidator with 3 fold cross validation
    val crossval = new CrossValidator()
    crossval.setEstimator(pipeline)
    crossval.setEvaluator(evaluator)
    crossval.setNumFolds(3)
    
    // tuning params
    //Let's tune over our regularization parameter from 0.01 to 0.10
    val regParam = Array(2,3)
    // Let's tune over our dt.maxDepth parameter on the values 2 and 3, create a paramter grid using the ParamGridBuilder
    val paramGrid = (new ParamGridBuilder()
                 .addGrid(dt.maxDepth, regParam)
                 .build())
    
    // Add the grid to the CrossValidator
    crossval.setEstimatorParamMaps(paramGrid)
    
    // Now let's find and return the best model
    val bestDtModel = crossval.fit(trainingData).bestModel

    
    println("----------- Evaluating with best model -----------------")
    // Make predictions.
    val improvedPredictions = bestDtModel.transform(testData)

    // Select example rows to display.
    improvedPredictions.select("predictedLabel", "SurvivedDbl", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val improvedEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    
    val improvedAccuracy = improvedEvaluator.evaluate(predictions)
    println("Improved error = " + (1 - improvedAccuracy))

    
    
  }

  def generateModel(data: DataFrame): Unit = {

    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val vectorizer = new VectorAssembler()
      .setInputCols(Array(
        "Pclass", "Age", "Parch", "SexInt", "EmbarkedInt",
        "SibSp", "Fare"))
      .setOutputCol("features")

    val dt = new DecisionTreeRegressor()
    dt.setLabelCol("SurvivedDbl")
      .setPredictionCol("Predicted_Survived")
      .setFeaturesCol("features")

    val dtPipeline = new Pipeline().setStages(Array(vectorizer, dt))

    // Using regression
    // Train model.  This also runs the indexer.
    val model = dtPipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("Predicted_Survived", "SurvivedDbl", "features").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("SurvivedDbl")
      .setPredictionCol("Predicted_Survived")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModel.toDebugString)

  }

  def titanicSurvivors(sconf: SparkConf, fileName: String): Unit = {

    disableSparkLogging

    val sc = new SparkContext(sconf)
    val sqlContext = new SQLContext(sc)

    println("Creating DataSet")
    val titanicDataFrame = getDataSet(sqlContext, fileName)
    println("Cleaning up data...")
    val cleanedDataSet = cleanUpData(sqlContext, titanicDataFrame)

    cleanedDataSet.show()

    cleanedDataSet.printSchema()

    println("Now feeding the model..")
    //generateModel(cleanedDataSet)
    generateDecisionTree(cleanedDataSet)

  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application")
    titanicSurvivors(conf, "")

  }

}