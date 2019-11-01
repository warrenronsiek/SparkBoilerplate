import org.rogach.scallop.ScallopConfBase
import cli.CLIArgParse
import com.typesafe.config.ConfigFactory

object Main extends App {

  val parser = new CLIArgParse(args)
  parser.subcommands match {
    case commands: List[ScallopConfBase] if commands.contains(parser.sparkSubmit) =>
      print(parser.sparkSubmit.config())
      print(parser.sparkSubmit.pipelineName())
    case commands: List[ScallopConfBase] if commands.contains(parser.createCluster) =>
      val conf = ConfigFactory.load(parser.createCluster.config())

    case _ => throw new IllegalArgumentException("Could not match provided command")
  }
}
