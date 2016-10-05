
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.{ RandomForestClassifier, RandomForestClassificationModel }
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{ StringIndexer, IndexToString, VectorIndexer, VectorAssembler }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline

/**
 * This example builds a random forest tree and it's based on this video
 * on youtube
 * It has been rewritten to use spark ml instead of mllib
 *
 * https://www.youtube.com/watch?v=ObiCMJ24ezs
 *
 * Please note that this code depends on pacakge spark-csv, so make sure when you
 * launch spark-submit you provide --packages com.databricks:spark-csv_2.10:1.4.0
 *
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
object RandomForestExampleML {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  def getRDD(sc: SparkContext, fileName:String): RDD[String] = {

    /**
     * val sqlContext = new SQLContext(sc)
     * println(s"Creating RDD ......")
     * sqlContext.read
     * .format("com.databricks.spark.csv")
     * .option("header", "true") // Use first line of all files as header
     * .option("inferSchema", "true") // Automatically infer data types
     * .load("file://c:/Users/marco/SparkExamples/src/main/resources/covtype.data.gz")
     * // return integers
     */
    return sc.textFile(fileName)
  }

  def toReducedFeatures(row: Array[Double]) = {
    //val expected = row.last -1
    //val features = row.init
    val wilderness = row.slice(10, 14).indexOf(1.0).toDouble
    val soil = row.slice(14, 54).indexOf(1.0).toDouble

    val features = row.slice(0, 10) :+ wilderness :+ soil
    val label = row.last - 1

    val allValues = List(label) ::: features.toList
    Row.fromSeq(allValues)
  }

  def createSchema(row: Row) = {
    val first = row.toSeq
    val firstWithIdx = first.zipWithIndex
    val fields = firstWithIdx.map(tpl => StructField("Col" + tpl._2, DoubleType, false))
    StructType(fields)

  }

  def toDataFrame(forestData: RDD[String], sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)

    // From RDD[String] to RDD[Array[Double]]
    val mapped = forestData.map(line => line.split(",").map(_.toDouble))
    // see this https://spark.apache.org/docs/1.3.0/sql-programming-guide.html#creating-dataframes

    // From RDD[Array[Double]] to RDD[Row[Double]]
    val toRddOfRow = mapped.map(toReducedFeatures)
    // Creating schema
    val schema = createSchema(toRddOfRow.first())

    // returning DataFrame
    sqlContext.createDataFrame(toRddOfRow, schema)

  }

  def classProbabilities(data: RDD[LabeledPoint]) = {
    val countByCategory = data.map(_.label).countByValue()
    val counts = countByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }

  def createModel(sc: SparkContext, data: DataFrame): Unit = {

    // splitting
    println("Splitting training and test")
    val splits = data.randomSplit(Array(0.9, 0.1))
    val (trainingData, testData) = (splits(0), splits(1))

    trainingData.cache()

    val labelIndexer = new StringIndexer()
      .setInputCol("Col0")
      .setOutputCol("indexedLabel")
      .fit(data)

    // Other features. crete a Vecto assembler if there are more than 1 feature
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "Col1", "Col2", "Col3", "Col4", "Col5",
        "Col6", "Col7", "Col8", "Col9", "Col10"))
      .setOutputCol("featuresVector")
      
     val indexer = new VectorIndexer().
      setMaxCategories(40).
      setInputCol("featuresVector").
      setOutputCol("indexedVector")  
      

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedVector")
      .setMaxDepth(30)
      .setMaxBins(300)
      .setImpurity("entropy")

    println("Kicking off pipeline..")

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, assembler, indexer, rf, labelConverter))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(trainingData)

    println("Predicting.................")

    // Make predictions.
    val predictions = model.transform(testData)

    println("--- After transformation----")

    // Select example rows to display.
    predictions.select("predictedLabel", "Col0", "features").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    println("Trying to find best fit....")

    val crossval = new CrossValidator()
    crossval.setEstimator(pipeline)
    crossval.setEvaluator(evaluator)
    crossval.setNumFolds(3)

    /**
     * val numClasses = 7
     * val categoricalFeaturesInfo = Map[Int, Int](10->4, 11->40)
     * val impurity = "entropy"
     * val depth = 30
     * val bins = 300
     * val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
     * 5, // num of tree to build
     * "auto",
     * "entropy",
     * 30, // how deep is the tree
     * 300)  // how many diff values to consider
     *
     *
     * val paramGrid = new ParamGridBuilder()
     * .addGrid(rf.numTrees, Array(3,4,5))
     * .addGrid(rf.maxDepth, Array(10,20,30))
     * .addGrid(rf.impurity, Array("entropy"))
     * .build()
     *
     * crossval.setEstimatorParamMaps(paramGrid)
     *
     * //# Now let's find and return the best model
     * val bestModel = crossval.fit(trainingData).bestModel
     *
     *
     */
  }

  def generateDecisionTree(sconf: SparkConf, args: Array[String]): Unit = {

    SparkUtil.disableSparkLogging
    
    println("Attempting to load:$args(0)")
    val sc = new SparkContext(sconf)
    val rdd = getRDD(sc, args(0))
    println("InputData:" + rdd.count())
    // ccrete labeled points. rmeember above we only have tuples
    val dataFrame = toDataFrame(rdd, sc)
    // create model
    createModel(sc, dataFrame)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application")
    generateDecisionTree(conf, args)
  }

}