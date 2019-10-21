package utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkContextMixin extends BeforeAndAfterAll {
  this: Suite =>
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