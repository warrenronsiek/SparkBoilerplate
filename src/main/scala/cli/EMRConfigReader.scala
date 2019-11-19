package cli

import net.ceedubs.ficus.Ficus._
import com.typesafe.config.{Config, ConfigFactory}
import EC2Data.ec2types
import utils.ResourceReader

class EMRConfigReader(configName: String) {

  val resourceReader = new ResourceReader(configName)
  val config: Config =  resourceReader.config

  private def validateSubnet(subnet: String): String = {
    """subnet-\S+""".r.findFirstIn(subnet.toString) match {
      case Some(subnet: String) => subnet
      case None => throw new Error("Subnet string has incorrect format")
    }
  }

  private def validateLogUri(logUri: String): String = {
    """s3://\S+""".r.findFirstIn(logUri.toString) match {
      case Some(logUri: String) => logUri
      case None => throw new Error("LogUri is incorrect")
    }
  }

  private def validateInstanceType(instanceType: String): String = {
    if (ec2types.keys.toIterator.contains(instanceType)) {
      instanceType
    } else {
      throw new Error("Invalid instance type")
    }
  }

  val getParams: EMRParams = EMRParams(
    validateSubnet(config.as[String]("subnet")),
    config.as[String]("name"),
    validateLogUri(config.as[String]("logUri")),
    config.as[String]("serviceRole"),
    config.as[String]("instanceRole"),
    config.as[String]("key"),
    config.as[Int]("instanceCount"),
    validateInstanceType(config.as[String]("instanceType")),
    config.as[Option[Double]]("bidPrice")
  )
}
