import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.broadcast.Broadcast
import utils.SparkUtil

object MovieRecommendations {
  
  def getMovieNames(sc:SparkContext, moviesDict:org.apache.spark.broadcast.Broadcast[scala.collection.mutable.Map[Int,String]]) = {
    val movieNamesRdd = sc.textFile("file:///vagrant/ml-100k/u.item")
    val mapped = movieNamesRdd.map(line => line.split('|'))
    mapped.foreach(movieNameArr => moviesDict.value += (movieNameArr(0).toInt -> movieNameArr(1)))
  }
  
  def getRatingsData(sc:SparkContext) = {
    val data = sc.textFile("file:///vagrant/ml-100k/u.data")
    val mapped = data.map(line => line.split("\t")).map(arr=> Rating(arr(0).toInt, arr(1).toInt, arr(2).toFloat))
    mapped
  }
    
  
  def movieRecommendations(conf:SparkConf, userId:Int):Unit = {
    println("Finding movie recommendations for user:" + +userId)
    SparkUtil.disableSparkLogging
    
    val sc  = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")
    val moviesDict  =sc.broadcast(scala.collection.mutable.Map[Int, String]())
    
    println("Creating Movies Dict..")
    getMovieNames(sc, moviesDict)
    
    println("Movie dict created with "+ moviesDict.value.size)
    
    println("Getting rating data...")
    val ratings = getRatingsData(sc)
    println("Ratings data first =" +ratings.first())

    println( "\nTraining recommendation model...")
    val rank = 10
    //Lowered numIterations to ensure it works on lower-end systems
    val numIterations = 6
    val model = ALS.train(ratings, rank, numIterations)

    println( "\nRatings for user ID " + userId + ":")
    val userRatings = ratings.filter(rt => rt.user == userId)

    userRatings.collect().foreach(rt => {
      println(moviesDict.value.get(rt.product).get+ ":" + rt.rating)
    })
    

    println( "\nTop 10 recommendations:")
    val recommendations = model.recommendProducts(userId, 10)
    
    recommendations.foreach(rt => {
      println(moviesDict.value.get(rt.product).get + " score: " + rt.rating)
    }
    )


  
    
    
    
  }
  
  
}