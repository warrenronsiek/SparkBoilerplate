package pipelines

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, types}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructField, StructType}
import schemas.Iris
import transfomers.demopipeline.{DemoFilter1, DemoFilter1Validator}
import net.ceedubs.ficus.Ficus._

class DemoPipeline(configName: String)(implicit spark: SparkSession = SparkSession.builder.getOrCreate()) extends GenericPipeline {
  System.setProperty("config.file", getClass.getResource("/" + configName).getPath)
  ConfigFactory.invalidateCaches()
  val config: Config = ConfigFactory.load()
  val irisFilePath: String = config.as[String]("filePath") match {
    case s: String if s.startsWith("s3://") => s
    case s: String => getClass.getResource(s).getPath
    case _ => throw new Exception("invalid file path")
  }

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