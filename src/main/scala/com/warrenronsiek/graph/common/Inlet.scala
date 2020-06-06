package com.warrenronsiek.graph.common
import org.apache.spark.sql.{DataFrame, Dataset}

class Inlet(df: Dataset[_]) {
  def get: Dataset[_] = df
}
