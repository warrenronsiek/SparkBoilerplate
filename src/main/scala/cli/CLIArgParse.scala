package cli

import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

class CLIArgParse(arguments: Seq[String]) extends ScallopConf(arguments) {
  // for some weird reason, adding type defs will make the main class not compile.
  val sparkSubmit = new Subcommand("spark-submit") {
    val pipelineName: ScallopOption[String] = opt[String](required = true)
    val filePath: ScallopOption[String] = opt[String](required = true)
  }
  addSubcommand(sparkSubmit)

  val createCluster = new Subcommand("create-cluster") {

  }
  addSubcommand(createCluster)

  val submitJob = new Subcommand("submit-job") {

  }
  addSubcommand(submitJob)
  verify()
}