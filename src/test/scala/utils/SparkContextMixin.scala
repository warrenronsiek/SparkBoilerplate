package utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkContextMixin extends BeforeAndAfterAll with SparkProvider {
  this: Suite =>

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}