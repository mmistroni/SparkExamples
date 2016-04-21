

object WordCountUsingSparkDropDirectory {

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.util.IntParam
  import org.apache.spark.streaming._
  import org.apache.log4j.Logger
  import org.apache.log4j.Level
  import SparkUtil._

  def wordCount(sconf: SparkConf, args:Array[String]) = {

    /**
     *  Streaming example using fileStream.
     *  Directory where files are dropped is read from sys args*/

    
    val baseDir = args(1)
    println(s"Reading files from $baseDir")
    
    // disablinglogging
    disableSparkLogging
    val batchInterval = Seconds(5)

    // Create the context and set the batch size
    val ssc = new StreamingContext(sconf, batchInterval)
    // Checkpoint
    ssc.checkpoint("file:///c:/spoolcheck")
    // Create a flume stream
    val stream = ssc.textFileStream("file:///c:/spooldir")
    
    println("Stream count is:" + stream.count())
    
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    val wordDStream = stream.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    wordDStream.print()
    
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
      state.update(sum)
      output
      }
     
      val stateDstream = wordDStream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))
      stateDstream.print()
     
    ssc.start()
    ssc.awaitTermination()

  }
}