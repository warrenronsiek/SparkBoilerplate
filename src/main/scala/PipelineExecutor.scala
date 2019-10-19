import org.apache.spark.sql.SparkSession
import pipelines.DemoPipeline

object PipelineExecutor extends App {
  val spark = SparkSession
    .builder
    .appName("DemoSpark")
    .getOrCreate()

  val pipeline = args(0) match {
    case "DemoPipeline" =>
      DemoPipeline()
  }

  spark.stop()
}
