package utils
import scala.xml._
import org.apache.log4j.{ Level, Logger }

object EdgarDecoder extends Serializable {
  
  @transient
  val logger = Logger.getLogger("edgar decoders")
  
  def parseForm13HF(content:String):String = {
    if (content.length() > 0 && content.indexOf("<?xml") >= 0) {
      try {
        val filerInfos = content.substring(content.lastIndexOf("<?xml"), content.lastIndexOf("</XML"))
        val infoTableXml = XML.loadString(filerInfos)
        val purchasedShares = infoTableXml \\ "cusip"
        purchasedShares.map(_.text.toUpperCase()).distinct.mkString(",")
      }catch {
        case e: Exception => e.toString();
      }
    } else {
      println
      ""
    }
  }
  
  
}