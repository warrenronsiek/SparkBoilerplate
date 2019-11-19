package cli

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite}


class CLIArgParseSpec extends FlatSpec {

  "cli argparse" should "parse subcommand submit-job" in {
    val args = Array("spark-submit-local", "-p", "test", "-c", "test_config.conf")
    val parser = new CLIArgParse(args)
    assert(parser.subcommands.contains(parser.sparkSubmitLocal))
    assert(parser.sparkSubmitLocal.pipelineName() == "test")
    assert(parser.sparkSubmitLocal.configName() == "test_config.conf")
  }

  it should "parse subcommand run-pipeline" in {
    val args = Array("run-pipeline", "-p", "test", "-c", "test_config.conf")
    val parser = new CLIArgParse(args)
    assert(parser.subcommands.contains(parser.runPipeline))
    assert(parser.runPipeline.pipelineName() == "test")
    assert(parser.runPipeline.configName() == "test_config.conf")
  }

  it should "parse subcommand create-cluster" in {
    val args = Array("create-cluster", "-c", "test_config.conf")
    val parser = new CLIArgParse(args)
    assert(parser.subcommands.contains(parser.createCluster))
    assert(parser.createCluster.configName() == "test_config.conf")
  }

  it should "parse subcommand terminate-cluster" in {
    val args = Array("terminate-cluster", "-c", "testid123")
    val parser = new CLIArgParse(args)
    assert(parser.subcommands.contains(parser.terminateCluster))
    assert(parser.terminateCluster.clusterId() == "testid123")
  }
}
