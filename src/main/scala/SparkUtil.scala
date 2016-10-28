import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }


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

  
}