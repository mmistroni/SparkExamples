import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/**
 * This application extracts form4 (insider trading) filing from an EdgarIndex file, grouping the result by
 * company's cik
 * Run it like this
 *
 * spark-submit
 * 						--class EdgarExtractor
 * 						target\scala-2.11\sparkexamples.jar
 * 						<path to master.20160422.idx>
 *
 */
object EdgarAggregator {

  def extractEdgarFilingsFromFile(conf: SparkConf, args: Array[String]): Unit = {
    val bucketName = args(0)
    println(s"Aggregating all files from bucket:$bucketName")
    val sc = new SparkContext(conf)

    val listings = sc.textFile(s"s3n://$bucketName/*-securites.txt")
    println(s"File has $listings.count() entries")
  
    val reduced = listings.map(w => (w,1)).reduceByKey((first, second) => first + second)

    
    val top15Filings = reduced.takeOrdered(15)(Ordering[Int].reverse.on(tpl => tpl._2))
  
    println("---------- TOP 15 COMPANY FILINGS ---------")
    top15Filings.foreach(println)
  
  }

  def main(args: Array[String]) = {
    if (args.length < 1) {
      println("Usage  EdgarAggregator <bucketname>")
      sys.exit()
    }

    val conf = new SparkConf().setAppName("Edgar Aggregator")
    extractEdgarFilingsFromFile(conf, args)
  }

}