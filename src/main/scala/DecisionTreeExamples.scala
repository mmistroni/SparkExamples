
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

/**
 * This example build a decision tree based on old car data
 * to verify if a car will have a high mpg depending on 
 * power and weight
 */
object DecisionTreeExamples {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  case class Feature(v: Vector)

  case class CarData(mpg: Double, displacement: Double, hp: Int, torque: Int,
                     CRatio: Double, RARatio: Double, CarbBarrells: Int,
                     NoOfSpeed: Int, length: Double, width: Double,
                     weigth: Double, automatic: Int) {
    def toArray = Array(mpg, displacement, hp, torque, CRatio, RARatio,
      CarbBarrells, NoOfSpeed, length, width, weigth, automatic)
  }

  def parseLine(inputLine: String): CarData = {
    val values = inputLine.split(',')
    CarData(values(0).toDouble, values(1).toDouble, values(2).toInt,
      values(3).toInt, values(4).toDouble, values(5).toDouble, values(6).toInt,
      values(7).toInt, values(8).toDouble, values(9).toDouble,
      values(10).toDouble, values(11).toInt)
  }

  def generateStatistics(carData: RDD[CarData]): Unit = {
    val vectors = carData.map(car => Vectors.dense(car.toArray))
    val summary = Statistics.colStats(vectors)
    carData.foreach(println)
    print("Max :"); summary.max.toArray.foreach(m => print("%5.1f |     ".format(m))); println
    print("Min :"); summary.min.toArray.foreach(m => print("%5.1f |     ".format(m))); println
    print("Mean :"); summary.mean.toArray.foreach(m => print("%5.1f     | ".format(m))); println
  }

  def getDataSet(sc: SparkContext) = {

    sc.textFile("file:///vagrant/car-milage-no-hdr.csv").map(parseLine)

  }

  def reduceFeatures(carData: RDD[CarData]) = {
    // we map only hp, weight, and mpg
    carData.map(car => (car.hp, car.weigth, car.mpg))
  }

  def toLabeledPoints(carData: RDD[(Int, Double, Double)]) =
    carData.map(tpl => LabeledPoint(tpl._3, Vectors.dense(tpl._1.toDouble, tpl._2)))

  def createModelAndPredict(sc:SparkContext, data: RDD[LabeledPoint], inputHp: Int, inputWeight: Double):DecisionTreeModel = {
    println(s"Creating model which will predict mileage for horsepower of $inputHp and weight $inputWeight")
    // splitting
    println("Splitting training and test")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    model
    
  }

  def decisionTreeExample(sconf: SparkConf, args:Array[String] ): Unit = {

    val horsePowerFromArgs = 90
    val weightFromArgs = 2500.0
    SparkUtil.disableSparkLogging
    val sc = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val rdd = getDataSet(sc)

    println("Few samples...")
    rdd.take(10).foreach(println)

    println("Generating Statistics.....")
    generateStatistics(rdd)

    
    // extracct all features you need
    
    val reducedRdd = reduceFeatures(rdd)

    reducedRdd.take(20).foreach(println)

    // we need to map weight and mpg to low and highs
    // mpg mean  = 20.0
    // weight mean = 3625.8
    // map to 1.0
    val mapped = reducedRdd.map(tpl => (tpl._1, if (tpl._2 >= 3625.8) 1.0 else 0.0,
      if (tpl._3 >= 20.0) 1.0 else 0.0))

    println("Sample of mapped values..")
    mapped.take(10).foreach(println)

    // Transforming data to LabeledPoint
    println("Creating labeled points")
    
    
    // ccrete labeled points. rmeember above we only have tuples
    val data = toLabeledPoints(mapped)
    // create model
    val decisionTreeModel = createModelAndPredict(sc , data, horsePowerFromArgs, weightFromArgs)
    
    println("---------------------------------------")
    println("Creating our own vector to predict outcome")
    
    
    // predictions. models only acts on RDD. so we need to create an RDD for our data
    val testRDD  =  sc.parallelize(List(Vectors.dense(horsePowerFromArgs.toDouble, weightFromArgs)))
    
    // and..... predict
    val predictionsDF = decisionTreeModel.predict(testRDD)
    
    println(s"Predictions for HP:$horsePowerFromArgs|W:$weightFromArgs")
    
    val weightVar = if(weightFromArgs > 3625) "high" else "low"
      
    val mileage = if(predictionsDF.first() > 0.0) "high" else "low"
    
    
    println(s"HorsePower:$horsePowerFromArgs|Weight:$weightVar|Mileage:$mileage")
    
    
    
    
  }
}