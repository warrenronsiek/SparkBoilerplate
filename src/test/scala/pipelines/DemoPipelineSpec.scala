package pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.typesafe.config.{Config, ConfigFactory}
import utils.{SnapshotTest, SparkContextMixin}
import net.ceedubs.ficus.Ficus._

class DemoPipelineSpec extends FlatSpec with SnapshotTest {
  val demoPipeline = new DemoPipeline("demopipelinetest.conf")
  val result: DataFrame = demoPipeline.run()

  "iris datafraome" should "match snapshot" in {
    assertSnapshot("irisFiltered", result, List("sepal_length", "sepal_width", "species"))
  }

  it should "pass deequ validation" in {
    val verificationSuite = VerificationSuite().onData(result)
      .addCheck(Check(CheckLevel.Error, "simple iris tests")
          .isContainedIn("species", Array("Iris-versicolor", "Iris-virginica"))
      ).run()

    assert(verificationSuite.status == CheckStatus.Success)
  }
}
