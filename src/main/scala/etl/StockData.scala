package etl

case class StockData(symbol:String, companyName:String, close:Double)
                     
case class StockQuote(date:String, open:Double, close:Double, volume:Long, changePercent:Double)
                     
                     
case class StockPerformance(symbol:String, sector:String, industry:String, performance:Double)


case class CompanyStats(companyName:String, 
                        //marketcap:Int, 
                        beta:Double,
                        symbol:String="",
                        //latestEPS:Double, 
                        //returnOnEquity:Double,
                        //EBITDA:Long, revenue:Long, grossProfit:Int,
                        //cash:Long, debt:Long, ttmEPS:Double,
                        //revenuePerShare:Long, revenuePerEmployee:Long,
                        //returnOnAssets: Double, 
                        //profitMargin:Double,
                        //priceToSales:Double,
                        day200MovingAvg:Double,day50MovingAvg:Double,
                        //insiderPercent:Double,
                        //shortRatio:Double,
                        //year5ChangePercent:Double,
                        //year2ChangePercent:Double,year1ChangePercent:Double,
                        ytdChangePercent:Double,month6ChangePercent:Double,
                        month3ChangePercent:Double,month1ChangePercent:Double,
                        day5ChangePercent:Double
                        //day30ChangePercent:Double)
                        )
                        



case class CompanyData(cik:String, companyName:String, primarySymbol:String)    
                        
                        
 




