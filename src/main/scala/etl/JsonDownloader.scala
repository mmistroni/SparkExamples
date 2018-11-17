package etl

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.JsonMethods._
import scala.io.Source
import scala.math._



/** 
 *  download data from Json
 *  
 */ 
object JsonDownloader extends utils.LogHelper {
  
  implicit val formats = DefaultFormats
  private val baseUrl = "https://api.iextrading.com/1.0/stock/{ticker}/chart/{history}"
  private val statsUrl = "https://api.iextrading.com/1.0/stock/{ticker}/stats"
  private val priceUrl = "https://api.iextrading.com/1.0//stock/{ticker}/quote"
  
  private def downloadJson(historicalDataUrl:String):JValue = {
    val jsonString = Source.fromURL(historicalDataUrl).mkString
    parse(jsonString)
  }
  
  def fetchHistoricalIncrease(ticker:String, history:String="3m"):Double  = {
    val stockUrl = baseUrl.replace("{ticker}", ticker).replace("{history}", history) 
    val jValue = downloadJson(stockUrl)
    val stockData  =   jValue.extract[Array[StockQuote]]
     
     //val closes  = ((jValue \\ "close") \ "close") .children.map(item => item.extract[Double])
    val oldest = stockData(0).close
    val newest = stockData(stockData.length -1).close
    // 
    newest / oldest
  }
  
  def fetchHistoricalStatistics(ticker:String, history:String="3m"):(Double,Double,Double)  = {
    val tmpHistory = history match {
      case s if s == "3-6m" => "6m"
      case _ => history 
    }
    
    val stockUrl = baseUrl.replace("{ticker}", ticker).replace("{history}", tmpHistory) 
    val jValue = downloadJson(stockUrl)
    val stockData  =   jValue.extract[Array[StockQuote]].toSeq
    
    val oldest = stockData(0).close
    val endpoint = history match {
      case m if m == "3-6m" =>  stockData.size / 2   // need to find the half way point
      case _  =>  stockData.length -1 // return most recent
    }
    
    val newest = stockData(endpoint).close
    val stockReturn = (newest / oldest) -1
    val sharpeRatio = calculateSharpeRatio(stockData.slice(0, endpoint).map { quote => quote.close })
  
    (stockReturn, sharpeRatio, oldest)
  
  }
  
  def fetchHistoricalPriceStatistics(ticker:String, history:String="3m"):(Double,Double,Double,Double)  = {
    val tmpHistory = history match {
      case s if s == "3-6m" => "6m"
      case _ => history 
    }
    
    val stockUrl = baseUrl.replace("{ticker}", ticker).replace("{history}", tmpHistory) 
    val jValue = downloadJson(stockUrl)
    val stockDataPrices  =   jValue.extract[Array[StockQuote]].toSeq.map { data => data.close }
    
    val maxPrice = stockDataPrices.max
    val minPrice = stockDataPrices.min
    val avg = stockDataPrices.sum / stockDataPrices.size
    val dayAboveAvg = stockDataPrices.filter(price => price < avg).size
    val dayBelowAvg = stockDataPrices.size - dayAboveAvg
    (maxPrice, minPrice, dayAboveAvg, dayBelowAvg)
  
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  def calculateSharpeRatio(quotes:Seq[Double]):Double = {
    
    val nextChange = quotes.slice(1, quotes.size).zip(quotes)
                        .map(tpl => (tpl._1 / tpl._2) -1)
                        
    // calculate average daily return
    val avgDaily = nextChange.sum / nextChange.size                    
    
    val diffSquared = nextChange.map(item => pow(item - avgDaily, 2))
    
    val stdDev = sqrt(diffSquared.sum / diffSquared.size)
                        
    // calculate standard dev
                        
    // calculate sharpe/ sqrt(250)* avg(daily) / stddev
    sqrt(250) * avgDaily / stdDev
    
    
  }
  
  
  
  def fetchCompanyStats(ticker:String):CompanyStats = {
    val stockUrl = statsUrl.replace("{ticker}", ticker) 
    val jValue = downloadJson(stockUrl)
    jValue.extract[CompanyStats]
  }
  
  def fetchCompanyByName(name:String):Option[CompanyData]= {
      None
    }
  
  def fetchCompanyPrice(ticker:String):Double= {
    val stockUrl = priceUrl.replace("{ticker}", ticker) 
    val jValue = downloadJson(stockUrl)
    val stockData  =   jValue.extract[StockData]
    stockData.close
  }
  
}



