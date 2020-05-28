package com.warrenronsiek.pipelines

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.warrenronsiek.schemas.Iris
import com.warrenronsiek.transfomers.demopipeline.{DemoFilter1, DemoFilter1Validator}
import com.warrenronsiek.utils.ResourceReader

class DemoPipeline(configName: String)(implicit spark: SparkSession = SparkSession.builder.getOrCreate()) extends GenericPipeline {
  val resourceReader = new ResourceReader(configName)
  val config: Config = resourceReader.config
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