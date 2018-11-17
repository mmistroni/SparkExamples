package etl
import common.Transformer
import org.apache.spark.sql.types.{ StructField, StringType, StructType, IntegerType, BooleanType }
import org.apache.spark.sql._
import org.apache.spark._
import utils.SparkUtil
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import scala.util.Try
import org.apache.spark.sql.types.{DoubleType, StructType}

class StockTransformer(period: String) extends Transformer[DataFrame, DataFrame] {

  @transient
  val logger: Logger = Logger.getLogger("ETLPipeline.StockTransformer")

  def downloadIncrease(period: String, ticker: String): (Double, Double, Double) = {
    Try {
      JsonDownloader.fetchHistoricalStatistics(ticker, period)
    }.toOption.getOrElse(0.0, 0.0, 0.0)
  }

  def downloadCurrentPrice(ticker: String): Double = {
    Try {
      JsonDownloader.fetchCompanyPrice(ticker)
    }.toOption.getOrElse(0.0)
  }

  override def transform(sc: SparkContext, inputDataSet: DataFrame): DataFrame = {
    //implicit val encoder = org.apache.spark.sql.Encoders.kryo[(StockData, Double)]
    inputDataSet.cache()
    val performanceFunc: ((String, String) => (Double, Double,Double)) = (period, symbol) => downloadIncrease(period, symbol)
    val currentPriceFunc: (String => Double) = ticker => downloadCurrentPrice(ticker)
    
    val returnAndSharpeSchema =  StructType(Array(
                                    StructField(s"performance$period", DoubleType, false),
                                    StructField(s"sharpeRatio$period", DoubleType, false),
                                    StructField(s"startPrice$period", DoubleType, false)))
                                   
    
    val symbolToPerformanceFunc = udf(performanceFunc, returnAndSharpeSchema)
    
    // Check results here
    inputDataSet.withColumn(s"stats", symbolToPerformanceFunc(lit(period), col("symbol")))
                .withColumn(s"sharpeRatio$period", col(s"stats.sharpeRatio$period"))
                .withColumn(s"performance$period", col(s"stats.performance$period"))
                .withColumn(s"startPrice$period", col(s"stats.performance$period"))
                .drop(col("stats"))
  }

}

class HistStatsTransformer(period: String) extends Transformer[DataFrame, DataFrame] {

  @transient
  val logger: Logger = Logger.getLogger("ETLPipeline.StockTransformer")

  def downloadHistStas(period: String, ticker: String): (Double, Double, Double, Double) = {
    Try {
      JsonDownloader.fetchHistoricalPriceStatistics(ticker, period)
    }.toOption.getOrElse(0.0, 0.0, 0.0, 0.0)
  }

  override def transform(sc: SparkContext, inputDataSet: DataFrame): DataFrame = {
    //implicit val encoder = org.apache.spark.sql.Encoders.kryo[(StockData, Double)]
    inputDataSet.cache()
    
    val performanceFunc: ((String, String) => (Double, Double,Double, Double)) = (period, symbol) => downloadHistStas(period, symbol)
    
    val returnAndSharpeSchema =  StructType(Array(
                                    StructField(s"maxPrice$period", DoubleType, false),
                                    StructField(s"minPrice$period", DoubleType, false),
                                    StructField(s"dayAboveAvg$period", DoubleType, false),
                                    StructField(s"dayBelowAvg$period", DoubleType, false)))
                                   
    
    val symbolToPerformanceFunc = udf(performanceFunc, returnAndSharpeSchema)
    
    // Check results here
    inputDataSet.withColumn(s"stats", symbolToPerformanceFunc(lit(period), col("symbol")))
                .withColumn(s"maxPrice$period", col(s"stats.maxPrice$period"))
                .withColumn(s"minPrice$period", col(s"stats.minPrice$period"))
                .withColumn(s"dayBelowAvg$period", col(s"stats.dayBelowAvg$period"))
                .withColumn(s"dayAboveAvg$period", col(s"stats.dayAboveAvg$period"))
                .drop(col("stats"))
  }

}


class StatsTransformer extends Transformer[DataFrame, DataFrame] {

  @transient
  val logger: Logger = Logger.getLogger("ETLPipeline.StockTransformer")

  def downloadStats(ticker: String): Option[CompanyStats] = {
    Try {
      JsonDownloader.fetchCompanyStats(ticker)
    }.toOption
  }

  override def transform(sc: SparkContext, inputDataSet: DataFrame): DataFrame = {
    //implicit val encoder = org.apache.spark.sql.Encoders.kryo[(Double, Double)]
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._
    import org.apache.spark.sql.catalyst.ScalaReflection

    inputDataSet.cache()
    // Fetch all Tickers
    val tickers = inputDataSet.collect().map(row => row.getString(0)).toSeq
    val companyStatsRdd = sc.parallelize(tickers, 5).flatMap { ticker => downloadStats(ticker) }
    companyStatsRdd.toDF().cache().select("symbol", "companyName", "marketcap", "month6ChangePercent",
      "month3ChangePercent", "month1ChangePercent", "day30ChangePercent",
      "day5ChangePercent")

  }
}

class StockFilter(filterField: Option[String] = None) extends Transformer[DataFrame, DataFrame] {

  @transient
  val logger: Logger = Logger.getLogger("ETLPipeline.StockFilter")

  override def transform(sc: SparkContext, inputDataSet: DataFrame): DataFrame = {
    //implicit val encoder = org.apache.spark.sql.Encoders.kryo[(StockData, Double)]
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._
    inputDataSet.cache()
    
    logger.info(s"Filtering by $filterField")
    filterField match {
      case Some(field) => inputDataSet.filter(col(field) > 1.0)
      case None        => inputDataSet
    }

  }

}

class StockPerformanceTransformer(period: String) extends Transformer[DataFrame, DataFrame] {
  private val periodTransformer = new StockTransformer(period)
  private val stockFilter = new StockFilter()

  private def transformFunction(implicit sparkContext: SparkContext) =
    (inputDf: DataFrame) => periodTransformer.transform(sparkContext, inputDf)

  private def filterFunction(implicit sparkContext: SparkContext) =
    (inputDf: DataFrame) => stockFilter.transform(sparkContext, inputDf)

  override def transform(sc: SparkContext, inputDataSet: DataFrame): DataFrame = {
    //implicit val encoder = org.apache.spark.sql.Encoders.kryo[(StockData, Double)]
    implicit val sparkContext = sc
    periodTransformer.transform(sparkContext, inputDataSet)

  }

}

