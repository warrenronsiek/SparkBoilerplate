package scala

import org.rogach.scallop.ScallopConfBase
import cli.{CLIArgParse, EMRConfigReader, EMRManager}
import pipelines.{DemoPipeline, GenericPipeline}

object CLIEntryPoint extends App {
  val parser = new CLIArgParse(args)
  parser.subcommands match {
    case commands: List[ScallopConfBase] if commands.contains(parser.runPipeline) =>
      val pipeline: GenericPipeline = parser.runPipeline.pipelineName() match {
        case "DemoPipeline" => new DemoPipeline(parser.runPipeline.configName())
      }
      pipeline.run()

    case commands: List[ScallopConfBase] if commands.contains(parser.createCluster) =>
      val params: cli.EMRParams = new EMRConfigReader(parser.createCluster.configName()).getParams
      val emrManager = new EMRManager()
      emrManager.build(params)

    case commands: List[ScallopConfBase] if commands.contains(parser.terminateCluster) =>
      val emrManager = new EMRManager()
      parser.terminateCluster.clusterId.toOption match {
        case Some(clusterId) => emrManager.terminate(clusterId)
        case None => emrManager.terminate()
      }

    case commands: List[ScallopConfBase] if commands.contains(parser.sparkSubmitLocal) =>
      val emrManager = new EMRManager()
      emrManager.submitLocalJob(parser.sparkSubmitLocal.pipelineName(), parser.sparkSubmitLocal.configName())

    case _ => throw new IllegalArgumentException("Could not match provided command")
  }
  System.exit(0)
}
