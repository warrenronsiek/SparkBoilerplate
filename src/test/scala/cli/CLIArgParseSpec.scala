package cli

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite}


class CLIArgParseSpec extends FlatSpec {
  val args = Array("submit-job", "-p", "test", "-c", "/test/resources/test_config.conf")
  val parser = new CLIArgParse(args)

  "cli argparse" should "parse subcommand submit-job" in {
    assert(parser.subcommands.contains(parser.sparkSubmit))
  }

  it should "get the right pipeline name" in {
    assert(parser.sparkSubmit.pipelineName() == "test")
  }

  it should "identify the pipeline name" in {
    assert(parser.sparkSubmit.config() == "/test/resources/test_config.conf")
  }

}
