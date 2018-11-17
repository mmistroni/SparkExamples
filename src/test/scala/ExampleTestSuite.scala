import org.scalatest._
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing._
import EuroQualifierDecisionTree.cleanUpData
import org.apache.spark.sql.types.{StructField, StringType, StructType}
import org.apache.spark.sql.Row


class SimpleTestSuite  extends FunSuite with Matchers
  {

  test("simple test suite") {
    "foo" shouldEqual("foo")
  }
}
