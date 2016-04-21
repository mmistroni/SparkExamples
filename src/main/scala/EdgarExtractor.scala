import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object EdgarExtractor {
  
  def filterLines[T](filterFunction:T => Boolean,
                  lines:RDD[T]):RDD[T] = {
    lines.filter(filterFunction)
  }
  
  def extractListingFromFile(conf:SparkConf, args:Array[String]):Unit = {
    val fileName = args(1)
    val sc = new SparkContext(conf)
    val listings = sc.textFile(fileName)
    println(s"File has $listings.count() entries")
    println("Removing headerss")
    
    val validLinesFilter:String=>Boolean = lines => lines.split('|').size > 2 
    val noHeaderLines:((String, Long)) => Boolean = tpl => tpl._2 > 0
    val filteredLines2 = filterLines(validLinesFilter, listings)
    val linesWithIndex = filteredLines2.zipWithIndex
    val noHeaderRdd = filterLines(noHeaderLines, linesWithIndex).map(tpl => tpl._1)
    
    /**
    val filteredLines = listings.filter(lines => lines.split('|').size > 2)
                          .zipWithIndex.filter(tpl =>tpl._2 > 0)
                          .map(tpl => tpl._1)
    
    * 
    */
    println("Splitting lines and extracting form 4")
    val splitted = noHeaderRdd.map(line=> line.split('|'))
                              .map(arr=> (arr(0), arr(2)))
                              .filter(tpl => tpl._2 == "4")
                              .map(tpl=> (tpl._1, 1))
                              .reduceByKey(_ + _)
    val ordered = splitted.sortBy(tpl => tpl._2, false).take(20)                          
    ordered.foreach(println)
  }

}