import org.rogach.scallop.ScallopConfBase
import cli.{CLIArgParse, EMRConfigReader, EMRManager}
import pipelines.{DemoPipeline, GenericPipeline}

object Main extends App {

  val parser = new CLIArgParse(args)
  parser.subcommands match {
    case commands: List[ScallopConfBase] if commands.contains(parser.runPipeline) =>
      val pipeline: GenericPipeline = parser.runPipeline.pipelineName().toString match {
        case "DemoPipeline" => new DemoPipeline(parser.runPipeline.configName().toString)
      }
      pipeline.run()

    case commands: List[ScallopConfBase] if commands.contains(parser.createCluster) =>
      val params: cli.EMRParams = new EMRConfigReader(parser.createCluster.configName()).getParams
      val emrManager = new EMRManager(params)
      emrManager.build

    case commands: List[ScallopConfBase] if commands.contains(parser.terminateCluster) =>
      val emrManager = new EMRManager()
      parser.terminateCluster.clusterId.toOption match {
        case Some(clusterId) => emrManager.terminate(clusterId)
        case None => emrManager.terminate()
      }

    case commands: List[ScallopConfBase] if commands.contains(parser.sparkSubmit) =>
      val emrManager = new EMRManager()
      emrManager.submitJob(parser.sparkSubmit.pipelineName.toString(), parser.sparkSubmit.configName.toString())

    case _ => throw new IllegalArgumentException("Could not match provided command")
  }
}
