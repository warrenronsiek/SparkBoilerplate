package utils

import org.apache.spark.sql.SparkSession

trait SparkProvider {

  val spark: SparkSession = SparkSession
      .builder
      .master("local")
      .appName("RunningTests")
      .getOrCreate()
}
