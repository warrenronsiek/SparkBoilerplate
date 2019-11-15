package cli

import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

class CLIArgParse(arguments: Seq[String]) extends ScallopConf(arguments) {
  // for some weird reason, adding type defs will make the main class not compile.
  val sparkSubmit = new Subcommand("submit-job") {
    val pipelineName: ScallopOption[String] = opt[String](required = true)
    val configName: ScallopOption[String] = opt[String](required = true)
  }
  addSubcommand(sparkSubmit)

  val runPipeline = new Subcommand("run-pipeline") {
    val pipelineName: ScallopOption[String] = opt[String](required = true)
    val configName: ScallopOption[String] = opt[String](required = true)
  }
  addSubcommand(runPipeline)

  val createCluster = new Subcommand("create-cluster") {
    val configName: ScallopOption[String] = opt[String](required = true)
  }
  addSubcommand(createCluster)

  val createClusterWithJob = new Subcommand("create-cluster-with-job") {
    val config: ScallopOption[String] = opt[String](required = true)
  }
  addSubcommand(createClusterWithJob)

  val terminateCluster = new Subcommand("terminate-cluster") {
    val clusterId: ScallopOption[String] = opt[String]()
  }
  addSubcommand(terminateCluster)
  verify()
}
