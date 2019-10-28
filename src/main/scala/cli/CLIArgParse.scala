package cli

import org.rogach.scallop.{ScallopConf, ScallopOption}

class CLIArgParse(arguments: Seq[String]) extends ScallopConf(arguments) {
  val pipelineName: ScallopOption[String] = opt[String](required = true)
  val filePath: ScallopOption[String] = opt[String](required = true)
}