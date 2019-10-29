import org.rogach.scallop.ScallopConfBase
import cli.CLIArgParse

object Main extends App {

  val conf = new CLIArgParse(args)
  conf.subcommands match {
    case commands: List[ScallopConfBase] if commands.contains(conf.sparkSubmit) =>
      print(conf.sparkSubmit.filePath())
      print(conf.sparkSubmit.pipelineName())
    case commands: List[ScallopConfBase] if commands.contains(conf.createCluster) =>
    case commands: List[ScallopConfBase] if commands.contains(conf.submitJob) =>
    case _ => throw new IllegalArgumentException("Could not match provided command")
  }
}
