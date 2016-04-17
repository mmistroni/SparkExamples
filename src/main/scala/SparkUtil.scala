

object SparkUtil {
  def disableSparkLogging ={
      import org.apache.log4j.Logger
      import org.apache.log4j.Level
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
    
  }
}