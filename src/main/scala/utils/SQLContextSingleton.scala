package utils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getSqlContext: SQLContext = {
    if (instance == null) {
      instance = SparkSession.builder.getOrCreate().sqlContext
    }
    instance
  }
}