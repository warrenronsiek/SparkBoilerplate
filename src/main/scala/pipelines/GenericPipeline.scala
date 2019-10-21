package pipelines

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

trait GenericPipeline extends Pipeline{
  val df: DataFrame
  def run(): DataFrame = super.fit(df).transform(df)
}
