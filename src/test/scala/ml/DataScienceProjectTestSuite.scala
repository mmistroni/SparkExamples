package ml
import org.scalatest._
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types.{ StructField, StringType, StructType , IntegerType, BooleanType}
import org.apache.spark.sql._
import utils.SparkUtil
import org.apache.spark.sql.functions._
    

class DataScienceProjectTestSuite extends FreeSpec with DataFrameSuiteBase {
  
  
  override def beforeAll() {
    SparkUtil.disableSparkLogging
        
    super.beforeAll() // To be stackable, must call super.beforeEach
  }
  
  def generateProperDataFrame(inputDf:DataFrame):DataFrame = {
    val fieldNames = Seq("BI-RADS", "Age", "Shape", "Margin", "Density", "Severity")
    val dfWithHeader = inputDf.toDF(fieldNames:_*)
    dfWithHeader.printSchema()   
    // find a way to modify all in one programmatically
    
    
    val reversedNames = fieldNames.reverse 
    val updatedDf = reversedNames.tail.foldLeft(dfWithHeader)((dfWithHeader, colName) => {
                  dfWithHeader.withColumn(colName, dfWithHeader.col(colName).cast(IntegerType))
                  })
    updatedDf.withColumn(reversedNames.head, dfWithHeader.col(reversedNames.head).cast(BooleanType))
        
    
  }
  
  
  "The DataScienceTransformer" - {
    "when calling transform with a DataFrame" - {
      "should return a new data frame with new col types" in {
  
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val baseDataFrame = sqlCtx.read
                .csv("file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/mammographic_masses.data.txt")
    
        
        val properDf = generateProperDataFrame(baseDataFrame)
        properDf.printSchema()
        
        val dfSchema = properDf.schema
        
        val colNames = dfSchema.fields.map(f => f.name)
        
        val expectedColNames = Seq("BI-RADS", "Age", "Shape", "Margin", "Density", "Severity")
    
        val colPredicate = colNames.forall { colName => expectedColNames.contains(colName) }
        // asserting all expeced colnames are there
        assert(colPredicate)
      }
    }
  }
  
  private def findMostCommonValue[A](colName:String, df:DataFrame)(f:Row =>A):A = {
    println(s"Finding most common value for $colName")
    val result = df.groupBy(colName).count().sort(desc("count")).first()
    f(result)    
  }
  
  private def findMean(colName:String, df:DataFrame):Row = {
    import org.apache.spark.sql.functions._
    println(s"Finding most common value for $colName")
    df.groupBy(colName).mean(colName).first()
  }
  
  
  private def findAverage[A](colName:String, df:DataFrame)(f:Row=>A):A = {
    import org.apache.spark.sql.functions._
    println(s"Finding most common value for $colName")
    val res = df.select(avg(colName)).first()
    f(res)
  }
  
  
  
  
  
  "The DataScienceTransformer" - {
    "when selecting most common age" - {
      "should return the most common age" in {
  
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        
        val df = sqlCtx.read
        .csv("file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/mammographic_masses.data.txt")
    
        SparkUtil.disableSparkLogging

        val properDf = generateProperDataFrame(df)
      
        // dropping null values
        val mostCommonAge = findMostCommonValue("Age", properDf) {row:Row => row.getInt(0)}
        println(s"--------- The MOst Common age is:$mostCommonAge")
        
        val averageAge = findAverage("Age", properDf){_.getDouble(0)}
        
        println(s"----------- Average Age is:$averageAge")
        
        
        
      }
    }
  }
  
  "The DataScienceTransformer" - {
    "when filling null age columns" - {
      "should return a dataframe with no null columns the most common age" in {
  
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        
        
        val df = sqlCtx.read
        .csv("file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/mammographic_masses.data.txt")
    
        SparkUtil.disableSparkLogging

        val properDf = generateProperDataFrame(df)

        val mostCommonAge = findMostCommonValue("Age", properDf) {row:Row => row.getInt(0)}
        
        val noNullDf = properDf.na.fill(mostCommonAge, Seq("Age"))
        
        val nullAgeCols = noNullDf.filter(col("Age").isNull).count()
        
        assert(nullAgeCols == 0)
      }
    }
  }
  
  
  
  
}

