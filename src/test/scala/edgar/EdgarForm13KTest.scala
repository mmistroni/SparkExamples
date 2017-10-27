package edgar

import org.scalatest._
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types.{ StructField, StringType, StructType }
import org.apache.spark.sql.Row
import com.holdenkarau.spark.testing.DatasetSuiteBase
class EdgarForm13KTest extends FreeSpec with DatasetSuiteBase {

  "The Form13KAggregator" - {
    "when calling transform with a DataSet[String]" - {
      "should return a DataSet[(String, Long)]" in {
  
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val transformer = new Form13KAggregator()
        
        
        val headers = Seq("PLAYER", "TEAM")
        val schema = StructType(headers.map(name => StructField(name, StringType)))

        val inputDF = sc.parallelize(List("abc,def,abc","def,def,ghi")).toDF("fileName")
        val inputDataSet = inputDF.map(row => row.getAs[String](0))
        
        val returnedDs = transformer.transform(sc, inputDataSet)
        
        returnedDs.take(5).foreach(println)
        
                

      }
    }
  }
}

