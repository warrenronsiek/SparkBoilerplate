package pipelines

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, types}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructField, StructType}
import schemas.Iris
import transfomers.demopipeline.{DemoFilter1, DemoFilter1Validator}

class DemoPipeline(irisFilePath: String)(implicit spark: SparkSession = SparkSession.builder.getOrCreate()) extends GenericPipeline {
  val filePath = new Param[String](this, "filePath", "s3 location to read data from")
  def setFilePath(value: String): this.type = set(filePath, value)
  setFilePath(irisFilePath)

  val df: DataFrame = spark.read.schema(Iris.schema).csv(irisFilePath)

  this.setStages(Array(
    new DemoFilter1(),
    new DemoFilter1Validator()
  ))

}

object DemoPipeline {
  def apply(irisFilePath: String): DemoPipeline = new DemoPipeline(irisFilePath)
}