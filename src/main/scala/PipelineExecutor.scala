import org.apache.spark.sql.{DataFrame, SparkSession}
import pipelines.{DemoPipeline, GenericPipeline}

object PipelineExecutor extends App {

  val pipeline: GenericPipeline = args(0) match {
    case "DemoPipeline" =>
      DemoPipeline(args(1))
    case _ => throw new IllegalArgumentException("Provided pipeline name does not match an existing pipeline.")
  }

  pipeline.run()
}
