package pipelines

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

trait GenericPipeline extends Pipeline{
  def run(): DataFrame
}
