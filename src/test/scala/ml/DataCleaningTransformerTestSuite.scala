package ml
import org.scalatest._
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types.{ StructField, StringType, StructType , IntegerType, BooleanType}
import org.apache.spark.sql._
import utils.SparkUtil
import org.apache.spark.sql.functions._
    

class DataCleaningTransformerTestSuite extends FreeSpec with DataFrameSuiteBase
              with SharedSparkContext{
  
  
  override def beforeAll() {
    SparkUtil.disableSparkLogging
        
    super.beforeAll() // To be stackable, must call super.beforeEach
  }
  
  def generateProperDataFrame(inputDf:DataFrame):DataFrame = {
    val fieldNames = Seq("BI-RADS", "Age", "Shape", "Margin", "Density", "Severity")
    val dfWithHeader = inputDf.toDF(fieldNames:_*)
    dfWithHeader.printSchema()   
    // find a way to modify all in one programmatically
    
    
    fieldNames.foldLeft(dfWithHeader)((dfWithHeader, colName) => {
                  dfWithHeader.withColumn(colName, dfWithHeader.col(colName).cast(IntegerType))
                  })
    //updatedDf.withColumn(reversedNames.head, dfWithHeader.col(reversedNames.head).cast(BooleanType))
        
    
  }
  
  
  "The DataScienceTransformer" - {
    "when calling transform with the input DataFrame" - {
      "should return a new data frame with no null columns" in {
  
        val sqlCtx = sqlContext
        val sparkCtx = sc
        import sqlCtx.implicits._

        val baseDataFrame = sqlCtx.read
                .csv("file:///c:/Users/marco/SparkExamples2/SparkExamples/src/main/resources/mammographic_masses.data.txt")
    
        
        val properDf = generateProperDataFrame(baseDataFrame)
        val colNames = Seq("BI-RADS", "Age", "Shape", "Margin", "Density")
        
        val dataCleaningTransformer = new DataCleaningTransformer(colNames)
    
        val outputDataFrame = dataCleaningTransformer.transform(sc, properDf)
        
        // Checking that there are no null values in the DF
        
        for (colName <- colNames) {
          val nullColDf = outputDataFrame.filter(col(colName).isNull).count()
          assert(nullColDf == 0)
          
        }
        
      }
    }
  }
  
}

