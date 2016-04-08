

object SparkFlumeIntegration {

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.flume._
  import org.apache.spark.util.IntParam
  import org.apache.spark.streaming._
  import org.apache.log4j.Logger
import org.apache.log4j.Level

  def fetchFlumeEvents(sconf: SparkConf, host: String = "localhost", port: Int = 55555) = {

    /**
     *  Produces a count of events received from Flume.
     *
     *  This should be used in conjunction with an AvroSink in Flume. It will start
     *  an Avro server on at the request host:port address and listen for requests.
     *  Your Flume AvroSink should be pointed to this address.
     *
     *  Usage: FlumeEventCount <host> <port>
     *    <host> is the host the Flume receiver will be started on - a receiver
     *           creates a server and listens for flume events.
     *    <port> is the port the Flume receiver will listen on.
     *
     *  To run this example:
     *    `$ bin/run-example org.apache.spark.examples.streaming.FlumeEventCount <host> <port> `
     */
    
    // disablinglogging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val batchInterval = Seconds(5)

    
    // Create the context and set the batch size
    val ssc = new StreamingContext(sconf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events.").print()
    
    println("Now checking every event int he stream...")

    
    stream.map(event=>"Event: header:"+ event.event.get(0).toString+" body:"+ new String(event.event.getBody.array) ).print()
    
    ssc.start()
    ssc.awaitTermination()

  }
}