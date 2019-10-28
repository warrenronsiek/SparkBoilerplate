import pipelines.{DemoPipeline, GenericPipeline}
import cli.CLIArgParse


object PipelineExecutor extends App {

  val conf = new CLIArgParse(args)

  val pipeline: GenericPipeline = conf.pipelineName() match {
    case "DemoPipeline" =>
      DemoPipeline(conf.filePath())
    case _ => throw new IllegalArgumentException("Provided pipeline name does not match an existing pipeline.")
  }

  pipeline.run()
}
