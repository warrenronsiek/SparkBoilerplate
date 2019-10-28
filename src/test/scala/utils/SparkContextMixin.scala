package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SparkContextMixin extends BeforeAndAfterAll with BeforeAndAfterEach with SparkProvider {
  this: Suite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    SparkSession.clearActiveSession()
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    // do not stop the sparksession here - it will under certain circumstances cause the deequ tests to fail
    super.afterAll()
  }
}