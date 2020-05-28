package com.warrenronsiek.pipelines

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.warrenronsiek.schemas.MediaSalaries
import com.warrenronsiek.transfomers.mediasalariesclean._
import com.warrenronsiek.utils.ResourceReader

class MediaSalariesClean(configName: String)(implicit spark: SparkSession = SparkSession.builder.getOrCreate()) extends GenericPipeline {
  val resourceReader = new ResourceReader(configName)
  val config: Config = resourceReader.config
  val mediaSalariesFilePath: String = config.as[String]("filePath") match {
    case s: String if s.startsWith("s3://") => s
    case s: String => getClass.getResource(s).getPath
    case _ => throw new Exception("invalid file path")
  }
  val df: DataFrame = spark.read.schema(MediaSalaries.schema).csv(mediaSalariesFilePath)

  this.setStages(Array(
    new DropBadCols(),
    new CleanTitles(),
    new CleanExperience(),
    new CleanLocation(),
    new CleanSalary(),
    new CleanGenderEthnicity()
  ))
}

object MediaSalariesClean {
  def apply(configName: String): MediaSalariesClean = new MediaSalariesClean(configName)
}