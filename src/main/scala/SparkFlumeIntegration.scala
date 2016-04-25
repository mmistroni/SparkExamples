

object SparkFlumeIntegration {

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.flume._
  import org.apache.spark.util.IntParam
  import org.apache.spark.streaming._
  import org.apache.log4j.Logger
  import org.apache.log4j.Level
  import SparkUtil._

  def fetchFlumeEvents(sconf: SparkConf, host: String = "localhost", port: Int = 55555) = {

    /**
     *  Produces a wordmap from events  received from Flume.
     *
     *  This should be used in conjunction with an AvroSink in Flume. It will start
     *  an Avro server on at the request host:port address and listen for requests.
     *  Your Flume AvroSink should be pointed to this address.
     *  src/main/resources contains few samples of flume config files
     *
     */

    // disablinglogging
    disableSparkLogging
    val batchInterval = Seconds(10)

    // Create the context and set the batch size
    val ssc = new StreamingContext(sconf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)

    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events.").print()

    println("Now checking every event int he stream...")

    val fileRdd = stream.map(event => new String(event.event.getBody.array))

    val wordDStream = fileRdd.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

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