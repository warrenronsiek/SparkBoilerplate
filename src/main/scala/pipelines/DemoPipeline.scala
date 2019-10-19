package pipelines

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.param.Param
import transfomers.demopipeline.DemoFilter1

class DemoPipeline(implicit spark: SparkSession = SparkSession.builder.getOrCreate()) extends Pipeline {
  val filePath = new Param[String](this, "filePath", "s3 location to read data from")
  def setFilePath(value: String): this.type = set(filePath, value)

  val df: DataFrame = spark.read.csv(filePath.toString())
  this.setStages(Array(
    new DemoFilter1()
  ))
  def fit(): PipelineModel = super.fit(df)

}

object DemoPipeline {
  def apply(): DemoPipeline = new DemoPipeline()
}