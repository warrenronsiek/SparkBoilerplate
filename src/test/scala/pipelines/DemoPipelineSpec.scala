package pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import utils.SparkContextMixin

class DemoPipelineSpec extends FlatSpec with SparkContextMixin {
  val demoPipeline = new DemoPipeline(getClass.getResource("/iris.csv").getPath)
  val result: DataFrame = demoPipeline.run()

  "iris dataframe" should "have 100 rows" in {
    assert(result.count() == 100)
  }

  it should "pass deequ validation" in {
    val verificationSuite = VerificationSuite().onData(result)
      .addCheck(Check(CheckLevel.Error, "simple iris tests")
          .hasSize(_ == 100)
          .isComplete("sepal_width")
          .isComplete("species")
          .isContainedIn("species", Array("Iris-versicolor", "Iris-virginica"))
      ).run()

    assert(verificationSuite.status == CheckStatus.Success)
  }
}
