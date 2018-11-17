package edgar

import org.scalatest._
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types.{ StructField, StringType, StructType, BinaryType ,LongType }
import org.apache.spark.sql.Row
import com.holdenkarau.spark.testing.DatasetSuiteBase
class EdgarForm13KTest extends FreeSpec with DatasetSuiteBase
      with SharedSparkContext {

  "The Form13KAggregator" - {
    "when calling transform with a DataSet[String]" - {
      "should return a DataSet[(String, Long)]" in {
  
        val sqlCtx = sqlContext
        //import sqlCtx.implicits._
        import spark.implicits._

        val transformer = new Form13KAggregator()
        
        val inputData = Seq("abc,def,abc","def,def,ghi")
        val inputDs = sqlCtx.createDataset(inputData)
        
        val data = Seq(Form4Filing("abc", 2), Form4Filing("def", 3), Form4Filing("ghi", 1)) 
        val expectedDs = sqlCtx.createDataset(data)
      //Read more at https://indatalabs.com/blog/data-engineering/convert-spark-rdd-to-dataframe-dataset#htudIZrlIfVqcLkb.99
        
        
        //val returnedDs = transformer.transform(sc, inputDs)
        
        //assertDatasetEquals(expectedDs, returnedDs) # to check later
        
      }
    }
  }
  
  "The Form13KFileParser" - {
    "when calling transform with a DataSet[String]" - {
      "should return a DataSet[String]" in {
  
        val sqlCtx = sqlContext
        //import sqlCtx.implicits._
        import spark.implicits._

        val transformer = new Form13KFileParser()
        
        val inputData = Seq("abc,def,abc","def,def,ghi")
        val inputDs = sqlCtx.createDataset(inputData)
        
        val data = Seq(Form4Filing("abc", 2), Form4Filing("def", 3), Form4Filing("ghi", 1)) 
        val expectedDs = sqlCtx.createDataset(data)
      //Read more at https://indatalabs.com/blog/data-engineering/convert-spark-rdd-to-dataframe-dataset#htudIZrlIfVqcLkb.99
        
        
        //val returnedDs = transformer.transform(sc, inputDs)
        
        
        
      }
    }
  }
  
  
  
}

