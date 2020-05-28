package com.warrenronsiek.utils

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level



trait SparkProvider {

  val spark: SparkSession = SparkSession
      .builder
      .master("local")
      .appName("RunningTests")
      .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
