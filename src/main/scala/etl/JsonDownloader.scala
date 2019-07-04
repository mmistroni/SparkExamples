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
  private val statsUrl = "https://cloud.iexapis.com/stable/stock/{ticker}/stats?token=sk_98e397d4bee940488e1f48e9b419508f&filter=companyName,beta,symbol,day50MovingAvg,day200MovingAvg,month6ChangePercent,month3ChangePercent,month1ChangePercent,ytdChangePercent,day5ChangePercent"
  //"https://api.iextrading.com/1.0/stock/{ticker}/stats"
  private val priceUrl = "https://api.iextrading.com/1.0//stock/{ticker}/quote"
  private val futureEarningsUrl = "https://us-central1-datascience-projects.cloudfunctions.net/future_earnings/{symbol}"

  private def downloadJson(historicalDataUrl: String): JValue = {
    val jsonString = Source.fromURL(historicalDataUrl).mkString
    parse(jsonString)
  }

  def fetchHistoricalIncrease(ticker: String, history: String = "3m"): Double = {
    val stockUrl = baseUrl.replace("{ticker}", ticker).replace("{history}", history)
    val jValue = downloadJson(stockUrl)
    val stockData = jValue.extract[Array[StockQuote]]

    //val closes  = ((jValue \\ "close") \ "close") .children.map(item => item.extract[Double])
    val oldest = stockData(0).close
    val newest = stockData(stockData.length - 1).close
    // 
    newest / oldest
  }

  def fetchHistoricalStatistics(ticker: String, history: String = "3m"): (Double, Double, Double, Double, Double) = {
    val tmpHistory = history match {
      case s if s == "3-6m" => "6m"
      case _                => history
    }

    val stockUrl = baseUrl.replace("{ticker}", ticker).replace("{history}", tmpHistory)
    val jValue = downloadJson(stockUrl)
    val stockData = jValue.extract[Array[StockQuote]].toSeq

    val oldest = stockData(0).close
    val endpoint = history match {
      case m if m == "3-6m" => stockData.size / 2 // need to find the half way point
      case _                => stockData.length - 1 // return most recent
    }

    val newest = stockData(endpoint).close
    val maxPrice = stockData.map { squote => squote.close }.max
    val stockReturn = (newest / oldest) - 1
    val sharpeRatio = calculateSharpeRatio(stockData.slice(0, endpoint).map { quote => quote.close })

    (stockReturn, sharpeRatio, oldest, newest, maxPrice)

  }

  def fetchHistoricalPriceStatistics(ticker: String, history: String = "3m"): (Double, Double, Double, Double) = {
    val tmpHistory = history match {
      case s if s == "3-6m" => "6m"
      case _                => history
    }

    val stockUrl = baseUrl.replace("{ticker}", ticker).replace("{history}", tmpHistory)
    val jValue = downloadJson(stockUrl)
    val stockDataPrices = jValue.extract[Array[StockQuote]].toSeq.map { data => data.close }

    // Need to find out 50day moving avg

    val maxPrice = stockDataPrices.max
    val minPrice = stockDataPrices.min
    val avg = stockDataPrices.sum / stockDataPrices.size
    val dayAboveAvg = stockDataPrices.filter(price => price < avg).size
    val dayBelowAvg = stockDataPrices.size - dayAboveAvg
    (maxPrice, minPrice, dayAboveAvg, dayBelowAvg)

  }

  def calculateSharpeRatio(quotes: Seq[Double]): Double = {

    val nextChange = quotes.slice(1, quotes.size).zip(quotes)
      .map(tpl => (tpl._1 / tpl._2) - 1)

    // calculate average daily return
    val avgDaily = nextChange.sum / nextChange.size

    val diffSquared = nextChange.map(item => pow(item - avgDaily, 2))

    val stdDev = sqrt(diffSquared.sum / diffSquared.size)

    // calculate standard dev

    // calculate sharpe/ sqrt(250)* avg(daily) / stddev
    sqrt(250) * avgDaily / stdDev

  }

  def fetchCompanyStats(ticker: String): CompanyStats = {
    val stockUrl = statsUrl.replace("{ticker}", ticker)
    val jValue = downloadJson(stockUrl)
    println(jValue)
    val jsonCs = jValue.extract[CompanyStats]
    return CompanyStats(jsonCs.companyName, 
                        jsonCs.beta,
                        ticker,
                        jsonCs.day200MovingAvg,
                        jsonCs.day50MovingAvg,
                        jsonCs.ytdChangePercent,
                        jsonCs.month6ChangePercent,
                        jsonCs.month3ChangePercent,
                        jsonCs.month1ChangePercent,
                        jsonCs.day5ChangePercent)
                        
  }

  def fetchCompanyByName(name: String): Option[CompanyData] = {
    None
  }

  def fetchCompanyPrice(ticker: String): Double = {
    val stockUrl = priceUrl.replace("{ticker}", ticker)
    val jValue = downloadJson(stockUrl)
    val stockData = jValue.extract[StockData]
    stockData.close
  }

  def fetchFutureEarnings(ticker: String): (Seq[String], Seq[String]) = {
    val retrieveUrl = futureEarningsUrl.replace("{symbol}", ticker)
    println(s"Fetching from $retrieveUrl")
    val jsonString = Source.fromURL(retrieveUrl).mkString
    val res = parse(jsonString)
    val jsonData = res.extract[Map[String, Map[String, String]]]
    val fiscalMap = jsonData.get("Fiscal").getOrElse(Map[String, String]())
    val consensusMap = jsonData.get("Consensus")
    val consensus = consensusMap.getOrElse(Map[String, String]())
    val timings = for { m <- fiscalMap.values } yield m

    val timingStrs = timings.zipWithIndex.map(tpl => "T" + tpl._2)

    val earnings = for { m <- consensus.values } yield m
    return (timingStrs.toSeq, earnings.toSeq)

  }
  
  def fetchEarningsForTickers(tickers:Seq[String]):Unit = {
    val results = tickers.map(tick => fetchFutureEarnings(tick))
    results.foreach(println)
    println("--------------")
    val minEarnings = results.map(tpl => tpl._1.size).min
    println("Min period to evaluate is:" + minEarnings)
    val remapped = results.map(tpl => (tpl._1.slice(0,minEarnings), tpl._2.slice(0,minEarnings)))
    remapped.zip(tickers).foreach(println)
    
    
    
    
    
  }
  
  
  
  

}



