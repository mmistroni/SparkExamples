
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
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
import utils.SparkUtil._


/**
 * THis example has been ported from the original
 * @ www.kaggle.com. The original example uses python/scikit,
 * this one uses Spark and spark-csv
 * 
 * * C:\Users\marco\SparkExamples>spark-submit 
 * --packages com.databricks:spark-csv_2.10:1.4.0  
 * --class TitanicSurvivorsDecisionTree 
 * target\scala-2.11\sparkexamples.jar 
 * <path to train.csv>
 
 * 
 */
object TitanicSurvivorsDecisionTree {

  def getDataSet(sqlContext: SQLContext, filePath: String) = {
    println(s"Creating RDD from $filePath")
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filePath)

    // sorting out null values. we set them to zero by default
    //df.na.fill(0, df.columns)
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

    val medianAge = dataFrame.select(avg("Age")).collect()(0)(0).asInstanceOf[Double]
    val mostCommonEmbarked = getMostCommmonEmbarked(dataFrame)
    
    val fillAge = dataFrame.na.fill(medianAge, Seq("Age"))
    val fillEmbarked = fillAge.na.fill(mostCommonEmbarked, Seq("Embarked"))

    
    //scala> val df = Seq((25.0, "foo"), (30.0, "bar")).toDF("age", "name")
    //scala> df.withColumn("AgeInt", when(col("age") > 29.0, 1).otherwise(0)).show
    
    
    println("Median Age is:" + medianAge)
    
    val binaryFunc:(String=>Double) = sex => if (sex.equals("male")) 1 else 0
    val sexToIntFunc = udf(binaryFunc)
    
    val binarySexColumnDataFrame = fillEmbarked.withColumn("SexInt",  sexToIntFunc(col("Sex")))
                
    
    val binEmbarked:(String => Double) = embarked => if (embarked.equals("C")) 0 else if (embarked.equals("Q")) 1 else 2
    
    val embarkedFunc = udf(binEmbarked)
        
    val withBinaryEmbarked = binarySexColumnDataFrame.withColumn("EmbarkedInt",
          embarkedFunc(col("Embarked")))
    
    
    withBinaryEmbarked.show()      
    /**      
    val binFunc:(Double => Double) = ageDbl => if (ageDbl>medianAge) 1.0 else 0.0
    val func2 = udf(binFunc)
		
    
    //val binaryAgeDs  = withBinaryEmbarked.withColumn("AgeInt", func2(col("Age")))
		  **/	
   val res = withBinaryEmbarked.drop("Name").drop("Sex").drop("Ticket").drop("Cabin").drop("PassengerId").drop("Embarked")
   println("Before decision tree. data is\n")
   res.show()
   res
  
  }

  def generateModel(data: RDD[LabeledPoint]): Unit = {
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    
  
    
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32
		
    
    
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        
    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Learned classification forest model:\n" + model.toDebugString)

    println("Test Error = " + testErr)
    
    
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
    
    
    val vectorRdd = cleanedDataSet.rdd.map(row => row.toSeq.map(_.asInstanceOf[Number].doubleValue)) 
      
      
    //cleanedDataSet.rdd.map(row => createVectorRDD(row))

    vectorRdd.take(10).foreach { x => println(x.mkString(" ")) }
    // Transforming data to LabeledPoint
    println("Creating labeled points")

    // ccrete labeled points. rmeember above we only have tuples
    val data = toLabeledPointsRDD(vectorRdd, 0)

    println("Now feeding the model..")
    generateModel(data)

  }

  def main(args: Array[String]) = {
    if (args.length < 1) {
      println("Usage   TitanicSurvivorDecisionTree <path to train.csv>")
      sys.exit()
    }
    val conf = new SparkConf().setAppName("Simple Application")
    titanicSurvivors(conf, args(0))

  }

}