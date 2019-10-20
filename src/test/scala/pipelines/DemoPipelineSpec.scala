package pipelines

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite}

trait SparkContextMixin extends BeforeAndAfterAll {this: Suite =>
  val spark: SparkSession = SparkSession
    .builder
    .master("local")
    .appName("RunningTests")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}

class DemoPipelineSpec extends FlatSpec with SparkContextMixin {
  val demoPipeline = new DemoPipeline(getClass.getResource("/iris.csv").getPath)
  val result: DataFrame = demoPipeline.run()

  "iris dataframe" should "not have Iris-setosa" in {
    assert(result.count() == 100)
  }
}
