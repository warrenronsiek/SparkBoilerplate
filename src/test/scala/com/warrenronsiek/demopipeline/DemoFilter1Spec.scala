package com.warrenronsiek.demopipeline

import com.warrenronsiek.schemas.Iris
import com.warrenronsiek.transfomers.demopipeline.DemoFilter1
import com.warrenronsiek.utils.{SnapshotTest, SparkContextMixin}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FlatSpec

class DemoFilter1Spec extends FlatSpec with SparkContextMixin with SnapshotTest{
  import spark.sqlContext.createDataFrame
  val irisDf: DataFrame = createDataFrame(spark.sparkContext.parallelize(
    Seq(
      Row(1.0, 2.0, 3.0, 3.0, "Iris-versicolor"),
      Row(3.0, 4.0, 5.0, 5.0, "Iris-setosa"))), Iris.schema)

  val filter: DemoFilter1 = new DemoFilter1()
  val filtered: DataFrame = filter.transform(irisDf)

  "DemoFilter1" should "match snapshot" in {
    assert(assertSnapshot("iris", filtered, List("species")))
  }
}
