package com.warrenronsiek.demopipeline

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.warrenronsiek.pipelines.DemoPipeline
import com.warrenronsiek.utils.SnapshotTest
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

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
