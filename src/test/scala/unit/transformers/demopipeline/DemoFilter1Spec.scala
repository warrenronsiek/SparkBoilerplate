package unit.transformers.demopipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import transfomers.demopipeline.DemoFilter1
import utils.{SnapshotTest, SparkContextMixin}
import org.apache.spark.sql.{DataFrame, Dataset}
import schemas.Iris

class DemoFilter1Spec extends FlatSpec with SparkContextMixin with SnapshotTest{
  import spark.sqlContext.implicits._
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
