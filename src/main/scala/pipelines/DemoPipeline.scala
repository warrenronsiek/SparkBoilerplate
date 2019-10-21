package pipelines

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, types}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructField, StructType}
import transfomers.demopipeline.{DemoFilter1, DemoFilter1Validator}

class DemoPipeline(irisFilePath: String)(implicit spark: SparkSession = SparkSession.builder.getOrCreate()) extends GenericPipeline {
  val filePath = new Param[String](this, "filePath", "s3 location to read data from")
  def setFilePath(value: String): this.type = set(filePath, value)
  setFilePath(irisFilePath)

  val df: DataFrame = spark.read.schema(StructType(Array(
    StructField("petal_length", DoubleType, nullable = false),
    StructField("petal_width", DoubleType, nullable = false),
    StructField("sepal_length", DoubleType, nullable = false),
    StructField("sepal_width", DoubleType, nullable = false),
    StructField("species", StringType, nullable = false)
  ))).csv(irisFilePath)

  this.setStages(Array(
    new DemoFilter1(),
    new DemoFilter1Validator()
  ))

}

object DemoPipeline {
  def apply(irisFilePath: String): DemoPipeline = new DemoPipeline(irisFilePath)
}