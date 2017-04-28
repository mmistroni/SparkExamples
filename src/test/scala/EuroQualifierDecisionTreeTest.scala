import org.scalatest._
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing._
import EuroQualifierDecisionTree.cleanUpData
import org.apache.spark.sql.types.{ StructField, StringType, StructType }
import org.apache.spark.sql.Row

class EuroQualifierDecisionTreeTest extends FreeSpec with DataFrameSuiteBase {

  "The EuroQualifierDecisionTree" - {
    "when calling cleanUpData with a DataFrame" - {
      "should return the original dataframe minus the TEAM column" in {
  
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val headers = Seq("PLAYER", "TEAM")
        val schema = StructType(headers.map(name => StructField(name, StringType)))

        val inputRow = sc.parallelize(Array(Row("PLAYER1", "TEAM1")))

        val inputDataFrame = sqlContext.createDataFrame(inputRow, schema)

        val expectedDf = inputDataFrame.drop("TEAM")

        val normalizedData = cleanUpData(sqlContext, inputDataFrame)

        assertDataFrameEquals(normalizedData, expectedDf) // not equal
        

      }
    }
  }
}

